#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Manipulate Hadoop cluster on Google Compute Engine."""



import logging
import os
import os.path
import subprocess
import time

import gce_api


def MakeScriptRelativePath(relative_path):
  """Converts file path relative to this script to valid path for OS."""
  return os.path.join(os.path.dirname(__file__), relative_path)


class ClusterSetUpError(Exception):
  """Error during Hadoop cluster set-up."""


class EnvironmentSetUpError(Exception):
  """Exception raised when environment set-up script has an error."""


class RemoteExecutionError(Exception):
  """Remote command execution has an error."""


class MapReduceError(Exception):
  """MapReduce job start failure."""


class GceCluster(object):
  """Class to start Compute Engine server farm for Hadoop cluster.

  This class starts up Compute Engines with appropriate configuration for
  Hadoop cluster.  The server farm consists of 1 'master' and multiple
  'workers'.  Hostnames are set by /etc/hosts so that master and workers
  can recognize each other by hostnames.  The common SSH key is distributed
  so that user hadoop can ssh with each other without password.  (SSH is
  the way Hadoop uses for communication.)
  """

  CLIENT_ID = '{{{{ client_id }}}}'
  CLIENT_SECRET = '{{{{ client_secret }}}}'

  DEFAULT_ZONE = 'us-central1-a'
  DEFAULT_IMAGE = ('projects/debian-cloud/global/images/'
                   'debian-7-wheezy-v20131014')
  NAT_ENABLED_KERNEL = 'gce-v20130813'
  DEFAULT_MACHINE_TYPE = 'n1-highcpu-4-d'
  COMPUTE_STARTUP_SCRIPT = 'startup-script.sh'

  LOCAL_TMP_DIR = '.'
  SSH_KEY_DIR_NAME = 'ssh-key'
  PRIVATE_KEY_NAME = 'id_rsa'
  PUBLIC_KEY_NAME = PRIVATE_KEY_NAME + '.pub'
  PRIVATE_KEY_FILE = os.path.join(
      LOCAL_TMP_DIR, SSH_KEY_DIR_NAME, PRIVATE_KEY_NAME)
  PUBLIC_KEY_FILE = os.path.join(
      LOCAL_TMP_DIR, SSH_KEY_DIR_NAME, PUBLIC_KEY_NAME)

  MASTER_NAME = 'hm'
  WORKER_NAME_CORE = 'hw'
  WORKER_TAG_CORE = 'hadoop-workers'
  ROUTE_NAME_CORE = 'hadoop-worker-route'

  INSTANCE_ROLES = {
      'master': ['NameNode', 'JobTracker'],
      'worker': ['DataNode', 'TaskTracker'],
  }

  INSTANCE_STATUS_CHECK_INTERVAL = 15
  MAX_MASTER_STATUS_CHECK_TIMES = 40  # Waits up to 10min (15s x 40)
  MAX_WORKERS_CHECK_TIMES = 120  # Waits up to 30min (15s x 120)

  def __init__(self, flags):
    self.instances = []  # instance names
    self.api = None
    self.flags = flags
    if getattr(flags, 'bucket', ''):
      self.tmp_storage = 'gs://%s/mapreduce/tmp' % flags.bucket

    if getattr(flags, 'prefix', ''):
      self.master_name = flags.prefix + '-' + self.MASTER_NAME
      self.worker_name_template = '%s-%s-%%03d' % (
          flags.prefix, self.WORKER_NAME_CORE)
      self.worker_name_pattern = '^%s-%s-\\d+$' % (
          flags.prefix, self.WORKER_NAME_CORE)
      self.worker_tag = '%s-%s' % (flags.prefix, self.WORKER_TAG_CORE)
      self.route_name = '%s-%s' % (flags.prefix, self.ROUTE_NAME_CORE)
    else:
      self.master_name = self.MASTER_NAME
      self.worker_name_template = self.WORKER_NAME_CORE + '-%03d'
      self.worker_name_pattern = '^%s-\\d+$' % self.WORKER_NAME_CORE
      self.worker_tag = self.WORKER_TAG_CORE
      self.route_name = self.ROUTE_NAME_CORE

    self.zone = getattr(self.flags, 'zone', None) or self.DEFAULT_ZONE
    self.startup_script = None
    self.private_key = None
    self.public_key = None
    logging.debug('Current directory: %s', os.getcwd())

  def EnvironmentSetUp(self):
    """Sets up Hadoop-on-Compute environment.

    Must be run once per project/Cloud Storage bucket pair.

    Raises:
      EnvironmentSetUpError: Script failed.
    """
    command = ' '.join([MakeScriptRelativePath('preprocess.sh'),
                        self.LOCAL_TMP_DIR, self.flags.project,
                        self.tmp_storage])
    logging.debug('Environment set-up command: %s', command)
    if subprocess.call(command, shell=True):
      # Non-zero return code indicates an error.
      raise EnvironmentSetUpError('Environment set up failed.')

  def _WorkerName(self, index):
    """Returns Hadoop worker name with specified worker index."""
    return self.worker_name_template % index

  def _GetApi(self):
    if not self.api:
      self.api = gce_api.GceApi('hadoop_on_compute',
                                self.CLIENT_ID, self.CLIENT_SECRET,
                                self.flags.project, self.zone)
    return self.api

  def _StartInstance(self, instance_name, role):
    """Starts single Compute Engine instance.

    Args:
      instance_name: Name of the instance.
      role: Instance role name.  Must be one of the keys of INSTANCE_ROLES.
    Raises:
      ClusterSetUpError: Role name was invalid.
    """
    logging.info('Starting instance: %s', instance_name)

    # Load start-up script.
    if not self.startup_script:
      self.startup_script = open(
          MakeScriptRelativePath(self.COMPUTE_STARTUP_SCRIPT)).read()

    # Load SSH keys.
    if not self.private_key:
      self.private_key = open(self.PRIVATE_KEY_FILE).read()
    if not self.public_key:
      self.public_key = open(self.PUBLIC_KEY_FILE).read()

    metadata = {
        'num-workers': self.flags.num_workers,
        'hadoop-master': self.master_name,
        'hadoop-worker-template': self.worker_name_template,
        'tmp-cloud-storage': self.tmp_storage,
        'custom-command': self.flags.command,
        'hadoop-private-key': self.private_key,
        'hadoop-public-key': self.public_key,
        'worker-external-ip': int(self.flags.external_ip == 'all'),
    }

    if role not in self.INSTANCE_ROLES:
      raise ClusterSetUpError('Invalid instance role name: %s' % role)
    for command in self.INSTANCE_ROLES[role]:
      metadata[command] = 1

    # Assign an external IP to the master all the time, and to the worker
    # with external IP address.
    external_ip = False
    if role == 'master' or self.flags.external_ip == 'all':
      external_ip = True

    can_ip_forward = False
    kernel = None
    if role == 'master' and self.flags.external_ip == 'master':
      # Enable IP forwarding and use NAT-enabled kernel on master with
      # workers without external IP addresses.
      can_ip_forward = True
      kernel = self.NAT_ENABLED_KERNEL

    # Assign a tag to workers for routing.
    tags = None
    if role == 'worker':
      tags = [self.worker_tag]

    self._GetApi().CreateInstance(
        instance_name,
        self.flags.machinetype or self.DEFAULT_MACHINE_TYPE,
        self.flags.image or self.DEFAULT_IMAGE,
        startup_script=self.startup_script,
        service_accounts=[
            'https://www.googleapis.com/auth/devstorage.full_control'],
        external_ip=external_ip,
        metadata=metadata, tags=tags,
        can_ip_forward=can_ip_forward,
        kernel=kernel)
    self.instances += [instance_name]

  def _CheckInstanceRunning(self, instance_name):
    """Checks if instance status is 'RUNNING'."""
    instance_info = self._GetApi().GetInstance(instance_name)
    if not instance_info:
      logging.info('Instance %s has not yet started', instance_name)
      return False
    instance_status = instance_info.get('status', None)
    logging.info('Instance %s status: %s', instance_name, instance_status)
    return True if instance_status == 'RUNNING' else False

  def _CheckSshReady(self, instance_name):
    """Checks if the instance is ready to connect via SSH.

    Hadoop-on-Compute uses SSH to copy script files and execute remote commands.
    Connects with SSH and exits immediately to see if SSH connection works.

    Args:
      instance_name: Name of the instance.
    Returns:
      Boolean to indicate whether the instance is ready to SSH.
    """
    command = ('gcutil ssh --project=%s --zone=%s '
               '--ssh_arg "-o ConnectTimeout=10" '
               '--ssh_arg "-o StrictHostKeyChecking=no" '
               '%s exit') % (self.flags.project, self.zone, instance_name)
    logging.debug('SSH availability check command: %s', command)
    if subprocess.call(command, shell=True):
      # Non-zero return code indicates an error.
      logging.info('SSH is not yet ready on %s', instance_name)
      return False
    else:
      return True

  def _MasterSshChecker(self):
    """Returns generator that indicates whether master is ready to SSH.

    Yields:
      False until master is ready to SSH.
    """
    while not self._CheckInstanceRunning(self.master_name):
      yield False
    while not self._CheckSshReady(self.master_name):
      yield False

  def _WaitForMasterSsh(self):
    """Waits until the master instance is ready to SSH.

    Raises:
      ClusterSetUpError: Master set-up timed out.
    """
    wait_counter = 0
    for _ in self._MasterSshChecker():
      if wait_counter >= self.MAX_MASTER_STATUS_CHECK_TIMES:
        logging.critical('Hadoop master set up time out')
        raise ClusterSetUpError('Hadoop master set up time out')
      logging.info('Waiting for the master instance to get ready...')
      time.sleep(self.INSTANCE_STATUS_CHECK_INTERVAL)
      wait_counter += 1

  def _WorkerStatusChecker(self):
    """Returns generator that indicates how many workers are RUNNING.

    The returned generator finishes iteration when all workers are in
    RUNNING status.

    Yields:
      Number of RUNNING workers.
    """
    workers = [self._WorkerName(i) for i in xrange(self.flags.num_workers)]
    while True:
      running_workers = 0
      for worker_name in workers:
        if self._CheckInstanceRunning(worker_name):
          running_workers += 1
      if running_workers == self.flags.num_workers:
        return
      yield running_workers

  def _WaitForWorkersReady(self):
    """Waits until all workers are in RUNNING status.

    Raises:
      ClusterSetUpError: Workers set-up timed out.
    """
    wait_counter = 0
    for running_workers in self._WorkerStatusChecker():
      logging.info('%d out of %d workers RUNNING',
                   running_workers, self.flags.num_workers)
      if wait_counter >= self.MAX_WORKERS_CHECK_TIMES:
        logging.critical('Hadoop worker set up time out')
        raise ClusterSetUpError('Hadoop worker set up time out')
      logging.info('Waiting for the worker instances to start...')
      time.sleep(self.INSTANCE_STATUS_CHECK_INTERVAL)
      wait_counter += 1
    logging.info('All workers are RUNNING now.')

  def StartCluster(self):
    """Starts Hadoop cluster on Compute Engine."""
    # Create a route if no external IP addresses are assigned to the workers.
    if self.flags.external_ip == 'all':
      self._GetApi().DeleteRoute(self.route_name)
    else:
      self._GetApi().AddRoute(self.route_name, self.master_name,
                              tags=[self.worker_tag])

    # Start master instance.
    self._StartInstance(self.master_name, role='master')
    self._WaitForMasterSsh()

    # Start worker instances.
    for i in xrange(self.flags.num_workers):
      self._StartInstance(self._WorkerName(i), role='worker')

    self._WaitForWorkersReady()
    self._ShowHadoopInformation()

  def TeardownCluster(self):
    """Deletes Compute Engine instances with likely names."""
    # Delete route that might have been created at start up time.
    self._GetApi().DeleteRoute(self.route_name)

    # Delete instances.
    instances = self._GetApi().ListInstances('name eq "%s|%s"' % (
        self.master_name, self.worker_name_pattern))
    for instance in instances:
      instance_name = instance['name']
      logging.info('Shutting down %s', instance_name)
      self._GetApi().DeleteInstance(instance_name)

  def _StartScriptAtMaster(self, script, *params):
    """Injects script to master instance and runs it as hadoop user.

    run-script-remote.sh script copies the specified file to the master
    instance, and executes it on the master with specified parameters.
    Additinal parameters are passed to the script.

    Args:
      script: Script file to be run on master instance.
      *params: Additional parameters to be passed to the script.
    Raises:
      RemoteExecutionError: Remote command has an error.
    """
    command = ' '.join([
        MakeScriptRelativePath('run-script-remote.sh'),
        self.flags.project or '""', self.zone or '""',
        self.master_name, script, 'hadoop'] + list(params))
    logging.debug('Remote command at master: %s', command)
    if subprocess.call(command, shell=True):
      # Non-zero return code indicates an error.
      raise RemoteExecutionError('Remote execution error')

  def _ShowHadoopInformation(self):
    """Shows Hadoop master information."""
    instance = self._GetApi().GetInstance(self.master_name)
    external_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
    logging.info('')
    logging.info('Hadoop cluster is set up, and workers will be eventually '
                 'recognized by the master.')
    logging.info('HDFS Console  http://%s:50070/', external_ip)
    logging.info('MapReduce Console  http://%s:50030/', external_ip)
    logging.info('')

  def _SetUpMapperReducer(self, mr_file, mr_dir):
    """Prepares mapper or reducer program.

    If local program is specified as mapper or reducer, uploads it to Cloud
    Storage so that Hadoop master downloads it.  If program is already on
    Cloud Storage, just use it.  If empty, use 'cat' command as identity
    mapper/reducer.

    Args:
      mr_file: Mapper or reducer program on local, on Cloud Storage or empty.
      mr_dir: Location on Cloud Storage to store mapper or reducer program.
    Returns:
      Mapper or reducer to be passed to MapReduce script to run on master.
    Raises:
      MapReduceError: Error on copying mapper or reducer to Cloud Storage.
    """
    if mr_file:
      if mr_file.startswith('gs://'):
        return mr_file
      else:
        mr_on_storage = mr_dir + '/mapper-reducer/' + os.path.basename(mr_file)
        copy_command = 'gsutil cp %s %s' % (mr_file, mr_on_storage)
        logging.debug('Mapper/Reducer copy command: %s', copy_command)
        if subprocess.call(copy_command, shell=True):
          # Non-zero return code indicates an error.
          raise MapReduceError('Mapper/Reducer copy error: %s' % mr_file)
        return mr_on_storage
    else:
      # In streaming, 'cat' works as identity mapper/reducer (nop).
      return 'cat'

  def StartMapReduce(self):
    """Starts MapReduce job with specified mapper, reducer, input, output."""
    mapreduce_dir = 'gs://%s/mapreduce' % self.flags.bucket
    if self.flags.input:
      # Remove trailing '/' if any.
      if self.flags.input[-1] == '/':
        input_dir = self.flags.input[:-1]
      else:
        input_dir = self.flags.input
    else:
      input_dir = mapreduce_dir + '/inputs'

    if self.flags.output:
      # Remove trailing '/' if any.  mapreduce__at__master.sh adds '/' to
      # the output and treat it as directory.
      if self.flags.output[-1] == '/':
        output_dir = self.flags.output[:-1]
      else:
        output_dir = self.flags.output
    else:
      output_dir = mapreduce_dir + '/outputs'

    mapper = self._SetUpMapperReducer(self.flags.mapper, mapreduce_dir)
    reducer = self._SetUpMapperReducer(self.flags.reducer, mapreduce_dir)

    # Upload mappers to copy files between Google Cloud Storage and HDFS.
    command = 'gsutil cp %s %s %s' % (
        MakeScriptRelativePath('gcs_to_hdfs_mapper.sh'),
        MakeScriptRelativePath('hdfs_to_gcs_mapper.sh'),
        mapreduce_dir + '/mapper-reducer/')
    logging.debug('GCS-HDFS mappers upload command: %s', command)
    if subprocess.call(command, shell=True):
      # Non-zero return code indicates an error.
      raise MapReduceError('GCS/HDFS copy mapper upload error')

    self._StartScriptAtMaster(
        'mapreduce__at__master.sh', self.flags.bucket,
        mapper, str(self.flags.mapper_count),
        reducer, str(self.flags.reducer_count),
        input_dir, output_dir)
