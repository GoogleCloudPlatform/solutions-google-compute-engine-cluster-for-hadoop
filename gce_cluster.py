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
import tarfile
import time

from gce_api import GceApi


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

  DEFAULT_ZONE = 'us-central2-a'
  DEFAULT_IMAGE = 'gcel-12-04-v20130104'
  DEFAULT_MACHINE_TYPE = 'n1-highcpu-4-d'
  COMPUTE_STARTUP_SCRIPT = 'startup-script.sh'
  GENERATED_FILES_DIR = 'generated_files'
  MASTER_NAME = 'hm'
  WORKER_NAME_CORE = 'hw'

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
    else:
      self.master_name = self.MASTER_NAME
      self.worker_name_template = self.WORKER_NAME_CORE + '-%03d'
      self.worker_name_pattern = '^%s-\\d+$' % self.WORKER_NAME_CORE

    self.zone = getattr(self.flags, 'zone', None) or self.DEFAULT_ZONE
    self.startup_script = None

  def _GetApi(self):
    if not self.api:
      self.api = GceApi('hadoop_on_compute',
                        self.CLIENT_ID, self.CLIENT_SECRET,
                        self.flags.project, self.zone)
    return self.api

  def _StartInstance(self, instance_name):
    """Starts single Compute Engine instance."""
    logging.info('Starting instance: %s', instance_name)
    if not self.startup_script:
      self.startup_script = open(
          MakeScriptRelativePath(self.COMPUTE_STARTUP_SCRIPT)).read()
    self._GetApi().CreateInstance(
        instance_name,
        self.flags.machinetype or self.DEFAULT_MACHINE_TYPE,
        self.flags.image or self.DEFAULT_IMAGE,
        startup_script=self.startup_script,
        service_accounts=[
            'https://www.googleapis.com/auth/devstorage.full_control'])
    self.instances += [instance_name]

  def _SpawnPostprocessScript(self, instance_name):
    """Runs postprocess script on remote instance.

    Args:
      instance_name: Name of the instance.
    Returns:
      Popen object of the postprocess script process.
    """
    postprocess_command = ' '.join([
        MakeScriptRelativePath('run-script-remote.sh'),
        self.flags.project or '""', self.zone or '""',
        instance_name, 'postprocess__at__remote.sh', '--',
        self.tmp_storage, self.master_name,
        self.flags.command])
    logging.debug('Postprocess command: %s', postprocess_command)
    return subprocess.Popen(postprocess_command, shell=True)

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
               '--ssh_arg "-o ConnectTimeout=10" %s exit') % (
                   self.flags.project, self.zone, instance_name)
    logging.debug('SSH availability check command: %s', command)
    if subprocess.call(command, shell=True):
      # Non-zero return code indicates an error.
      logging.info('SSH is not yet ready for %s', instance_name)
      return False
    else:
      return True

  def _WaitForInstancesReady(self):
    """Waits for all Compte Engine instances to get ready.

    When instance is ready (in RUNNING status), starts postprocess script
    in the instance.  Postprocess scripts are spawned as background process,
    so that the scripts for different instances run simultaneously.  This
    funciton waits for postprocess scripts in all instances to finish.

    Raises:
      ClusterSetUpError: Postprocess script process finished with error.
    """
    postprocesses = []
    # Key: name of instance that hasn't run postprocess script.
    # Value: indicates if instance's status is RUNNING, therefore ready to
    # run postprocess when SSH is ready.
    not_yet_ready = dict.fromkeys(self.instances, False)

    unchanged_count = 0
    while not_yet_ready:
      logging.info('Waiting for instances to start')
      time.sleep(5)
      unchanged_count += 1
      managed_instances = self._GetApi().ListInstances(
          'name eq "%s"' % '|'.join(self.instances))
      waiting_instance_status = []
      for instance in managed_instances:
        instance_name = instance['name']
        if instance_name in not_yet_ready:
          if instance['status'] == 'RUNNING':
            if not_yet_ready[instance_name]:
              if self._CheckSshReady(instance_name):
                # Start postprocess script as background process.
                # If status is 'RUNNING' but sshd is not yet ready,
                # _CheckSshReady() returns False, in which case, the instance
                # status is added to waiting_instance_status below,
                # and SSH readiness is checked in the next cycle.
                postprocesses.append(
                    self._SpawnPostprocessScript(instance_name))
                # Clear from not_yet_ready list.
                del not_yet_ready[instance_name]
                # Postprocess started.  Reset unchanged count.
                unchanged_count = 0
            else:
              # Instead of running post process immediately, give one more
              # cycle after status becomes RUNNING for sshd to warm up.
              not_yet_ready[instance_name] = True
              # Status changed.  Reset unchanged count.
              unchanged_count = 0
          waiting_instance_status.append({
              'name': instance_name, 'status': instance['status']})
      for instance in waiting_instance_status:
        logging.info('%s: %s', instance['name'], instance['status'])

      # No situation change for 10 minutes.
      if unchanged_count > 120:
        logging.critical('Hadoop cluster instances did not start up normally.')
        raise ClusterSetUpError('Failed to start cluster instances.')

    # Wait for all postprocess scripts to finish.
    still_running = True
    unchanged_count = 0
    while still_running:
      logging.info('Waiting for postprocess to finish on all instances.')
      time.sleep(5)
      unchanged_count += 1
      still_running = False
      for p in postprocesses:
        if p.returncode is None:
          # The postprocess hadn't finished last time.  Check if it's finished.
          returncode = p.poll()
          if returncode is not None:
            # Postprocess is finished.  Check the finish status.
            if returncode:
              # Non-zero return code indicates an error.
              logging.error('Postprocess finished with error.  Return code: %d',
                            p.returncode)
              raise ClusterSetUpError('Postprocess script finished abnormally.')
            # Postprocess finished.  Reset unchanged count.
            unchanged_count = 0
          else:
            # This indicates at least one postprocess is still running.
            still_running = True
      # No situation change for 10 minutes.
      if unchanged_count > 120:
        logging.critical('Postprocess did not finish normally.')
        raise ClusterSetUpError('Postprocess script did not finish.')

  def _PrepareHostsFile(self):
    """Creates /etc/hosts file to be propageted.

    The generated hosts file includes all instances in the same Hadoop cluster.
    """
    f = open(os.path.join(self.GENERATED_FILES_DIR, 'hosts'), 'w')
    for instance_name in self.instances:
      instance = self._GetApi().GetInstance(instance_name)
      internal_ip = instance['networkInterfaces'][0]['networkIP']
      f.write('%s %s %s.localdomain\n' % (
          internal_ip, instance_name, instance_name))
    f.close()

  def EnvironmentSetUp(self):
    """Sets up Hadoop-on-Compute environment.

    Must be run once per project/Cloud Storage bucket pair.

    Raises:
      EnvironmentSetUpError: Script failed.
    """
    command = ' '.join([MakeScriptRelativePath('preprocess.sh'),
                        self.flags.project, self.tmp_storage])
    logging.debug('Enviroment set-up command: %s', command)
    if subprocess.call(command, shell=True):
      # Non-zero return code indicates an error.
      raise EnvironmentSetUpError('Environment set up failed.')

  def _Postprocess(self):
    """Performs cluster configuration after all instances are started.

    Raises:
      ClusterSetUpError: Generated file archive is not copied correctly.
    """
    self._PrepareHostsFile()
    # Archive generaged_files and upload to Cloud Storage
    generated_files_archive = self.GENERATED_FILES_DIR + '.tar.gz'
    tgz = tarfile.open(generated_files_archive, 'w|gz')
    tgz.add(self.GENERATED_FILES_DIR)
    tgz.close()
    genfile_copy_command = 'gsutil cp %(filename)s %(gcs_dir)s/%(filename)s' % {
        'filename': generated_files_archive,
        'gcs_dir': self.tmp_storage
    }
    logging.debug('Geenrated file copy command: %s', genfile_copy_command)
    if subprocess.call(genfile_copy_command, shell=True):
      # Non-zero return code indicates an error.
      raise ClusterSetUpError('Generated file copy error.')

    self._WaitForInstancesReady()

  def _WorkerName(self, index):
    """Returns Hadoop worker name with spedified worker index."""
    return self.worker_name_template % index

  def _StartMaster(self):
    """Starts Hadoop master Compute Engine instance."""
    self._StartInstance(self.master_name)
    f = open(os.path.join(self.GENERATED_FILES_DIR, 'masters'), 'w')
    f.write(self.master_name + '\n')
    f.close()

  def _StartWorkers(self):
    """Starts Hadoop worker Compute Engine instances."""
    f = open(os.path.join(self.GENERATED_FILES_DIR, 'slaves'), 'w')
    for i in range(self.flags.num_workers):
      name = self._WorkerName(i)
      self._StartInstance(name)
      f.write(name + '\n')
    f.close()

  def StartCluster(self):
    """Starts Hadoop cluster on Compute Engine."""
    self._StartMaster()
    self._StartWorkers()
    self._Postprocess()
    self._StartHadoopDaemons()

  def TeardownCluster(self):
    """Deletes Compute Engine instances with likely names."""
    instances = self._GetApi().ListInstances('name eq "%s|%s"' %(
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

  def _StartHadoopDaemons(self):
    """Starts Hadoop deamons (HDFS and MapRecuce)."""
    self._StartScriptAtMaster('start-hadoop__at__master.sh')
    instance = self._GetApi().GetInstance(self.master_name)
    external_ip = instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']
    logging.info('')
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
