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

"""Unit tests of gce_cluster.py."""



import argparse
import unittest

import mock

import gce_cluster
from gce_cluster import GceCluster


class GceClusterTest(unittest.TestCase):
  """Unit test class for GceCluster."""

  def tearDown(self):
    mock.patch.stopall()

  def _SetUpMocksForClusterStart(self):
    """Sets up mocks for cluster start tests.

    Returns:
      Parent mock that enables calls of other mocks.
    """
    # Patch functions.
    mock_gce_api_class = mock.patch('gce_api.GceApi').start()
    mock_tarfile_open = mock.patch('tarfile.open').start()
    mock_subprocess_call = mock.patch('subprocess.call', return_value=0).start()
    mock_popen = mock.patch('subprocess.Popen').start()
    mock_popen.return_value.returncode = None
    mock_popen.return_value.poll.return_value = 0
    mock_builtin_open = mock.patch('__builtin__.open').start()
    mock_sleep = mock.patch('time.sleep').start()

    # Create parent mock and attach other mocks to it, so that we can
    # track call order of all mocks.
    parent_mock = mock.MagicMock()
    parent_mock.attach_mock(mock_gce_api_class, 'GceApi')
    parent_mock.attach_mock(
        mock_gce_api_class.return_value.CreateInstance,
        'CreateInstance')
    parent_mock.attach_mock(
        mock_gce_api_class.return_value.GetInstance,
        'GetInstance')
    parent_mock.attach_mock(mock_tarfile_open, 'tarfile_open')
    parent_mock.attach_mock(mock_subprocess_call, 'subprocess_call')
    parent_mock.attach_mock(mock_popen, 'Popen')
    parent_mock.attach_mock(mock_popen.return_value.poll, 'poll')
    parent_mock.attach_mock(mock_builtin_open, 'open')
    parent_mock.attach_mock(mock_sleep, 'sleep')

    mock_gce_api_class.return_value.GetInstance.return_value = {
        'status': 'RUNNING',
        'networkInterfaces': [{
            'accessConfigs': [{
                'natIP': '1.2.3.4',
            }],
        }],
    }

    return parent_mock

  def testEnvironmentSetUp_Success(self):
    """Unit test of EnvironmentSetUp()."""
    with mock.patch('subprocess.call', return_value=0) as mock_subprocess_call:
      GceCluster(
          argparse.Namespace(project='project-foo',
                             bucket='bucket-bar')).EnvironmentSetUp()
      mock_subprocess_call.assert_called_once_with(mock.ANY, shell=True)
      self.assertRegexpMatches(
          mock_subprocess_call.call_args[0][0],
          '/preprocess.sh \\S+ project-foo gs://bucket-bar/mapreduce/tmp$')

  def testEnvironmentSetUp_Error(self):
    """Unit test of EnvironmentSetUp() with non-zero return value."""
    # subprocess.call() returns 1.
    with mock.patch('subprocess.call', return_value=1) as mock_subprocess_call:
      # Return value 1 causes EnvironmentSetUpError.
      self.assertRaises(
          gce_cluster.EnvironmentSetUpError,
          GceCluster(
              argparse.Namespace(project='project-foo', bucket='bucket-bar')
              ).EnvironmentSetUp)
      mock_subprocess_call.assert_called_once_with(mock.ANY, shell=True)
      self.assertRegexpMatches(
          mock_subprocess_call.call_args[0][0],
          '/preprocess.sh \\S+ project-foo gs://bucket-bar/mapreduce/tmp$')

  def testStartCluster(self):
    """Unit test of StartCluster()."""
    parent_mock = self._SetUpMocksForClusterStart()

    GceCluster(argparse.Namespace(
        project='project-hoge', bucket='bucket-fuga',
        machinetype='', image='', zone='us-central2-a', num_workers=2,
        command='', external_ip='all')).StartCluster()

    # Make sure internal calls are made with expected order with
    # expected arguments.
    method_calls = parent_mock.method_calls.__iter__()
    # Create GceApi.
    call = method_calls.next()
    self.assertEqual('GceApi', call[0])
    # Open start up script for Compute Engine instance.
    call = method_calls.next()
    self.assertEqual('open', call[0])
    self.assertRegexpMatches(call[1][0], 'startup-script\\.sh$')
    # Open private key to pass to Compute Engine instance.
    call = method_calls.next()
    self.assertEqual('open', call[0])
    self.assertRegexpMatches(call[1][0], 'id_rsa$')
    # Open private key to pass to Compute Engine instance.
    call = method_calls.next()
    self.assertEqual('open', call[0])
    self.assertRegexpMatches(call[1][0], 'id_rsa\\.pub$')
    # Create master instance.
    call = method_calls.next()
    self.assertEqual('CreateInstance', call[0])
    self.assertEqual('hm', call[1][0])
    self.assertTrue(call[2]['external_ip'])
    self.assertFalse(call[2]['can_ip_forward'])
    # Check master status.
    call = method_calls.next()
    self.assertEqual('GetInstance', call[0])
    self.assertEqual('hm', call[1][0])
    # Check if master is ready to SSH.
    call = method_calls.next()
    self.assertEqual('subprocess_call', call[0])
    self.assertRegexpMatches(call[1][0], '^gcutil ssh')
    # Create worker instance #000.
    call = method_calls.next()
    self.assertEqual('CreateInstance', call[0])
    self.assertEqual('hw-000', call[1][0])
    self.assertTrue(call[2]['external_ip'])
    self.assertFalse(call[2]['can_ip_forward'])
    # Create worker instance #001.
    call = method_calls.next()
    self.assertEqual('CreateInstance', call[0])
    self.assertEqual('hw-001', call[1][0])
    self.assertTrue(call[2]['external_ip'])
    self.assertFalse(call[2]['can_ip_forward'])
    # Check worker 000's status
    call = method_calls.next()
    self.assertEqual('GetInstance', call[0])
    self.assertEqual('hw-000', call[1][0])
    # Check worker 001's status
    call = method_calls.next()
    self.assertEqual('GetInstance', call[0])
    self.assertEqual('hw-001', call[1][0])
    # Get master's external IP address.
    call = method_calls.next()
    self.assertEqual('GetInstance', call[0])
    self.assertEqual('hm', call[1][0])
    # End of call list.
    self.assertRaises(StopIteration, method_calls.next)

  def testStartCluster_NoExternalIp(self):
    """Unit test of StartCluster() with no external IP addresses for workers."""
    parent_mock = self._SetUpMocksForClusterStart()

    GceCluster(argparse.Namespace(
        project='project-hoge', bucket='bucket-fuga',
        machinetype='', image='', zone='us-central2-a', num_workers=2,
        command='', external_ip='master')).StartCluster()

    # Just check parameters of CreateInstance.
    # Master instance.
    call = parent_mock.method_calls[4]
    self.assertEqual('CreateInstance', call[0])
    self.assertEqual('hm', call[1][0])
    self.assertTrue(call[2]['external_ip'])
    self.assertTrue(call[2]['can_ip_forward'])

    # Worker 000.
    call = parent_mock.method_calls[7]
    self.assertEqual('CreateInstance', call[0])
    self.assertEqual('hw-000', call[1][0])
    self.assertFalse(call[2]['external_ip'])
    self.assertFalse(call[2]['can_ip_forward'])

    # Worker 001.
    call = parent_mock.method_calls[8]
    self.assertEqual('CreateInstance', call[0])
    self.assertEqual('hw-001', call[1][0])
    self.assertFalse(call[2]['external_ip'])
    self.assertFalse(call[2]['can_ip_forward'])

  def testStartCluster_InstanceStatusError(self):
    """Unit test of StartCluster() instance status error.

    This unit test simulates the situation where status of one of the instances
    doesn't turn into RUNNING.
    """
    parent_mock = self._SetUpMocksForClusterStart()

    # Set hw-000's status to forever STAGING.
    parent_mock.GceApi.return_value.GetInstance.return_value = {
        'status': 'STAGING',
    }

    self.assertRaises(
        gce_cluster.ClusterSetUpError,
        gce_cluster.GceCluster(argparse.Namespace(
            project='project-hoge', bucket='bucket-fuga',
            machinetype='', image='', zone='', num_workers=2,
            command='', external_ip='all')).StartCluster)

    # Ensure ListInstances() and sleep() are called more than 120 times.
    self.assertEqual(41, parent_mock.GetInstance.call_count)
    self.assertEqual(40, parent_mock.sleep.call_count)

  def testTeardownCluster(self):
    """Unit test of TeardownCluster()."""
    with mock.patch('gce_api.GceApi') as mock_gce_api_class:
      mock_gce_api_class.return_value.ListInstances.return_value = [
          {'name': 'fugafuga'},
          {'name': 'hogehoge'},
          {'name': 'piyopiyo'},
      ]

      GceCluster(argparse.Namespace(
          project='project-hoge', zone='zone-fuga')).TeardownCluster()

      mock_gce_api_class.assert_called_once_with(
          'hadoop_on_compute', mock.ANY, mock.ANY,
          'project-hoge', 'zone-fuga')
      (mock_gce_api_class.return_value.ListInstances.
       assert_called_once_with(
           'name eq "hm|^hw-\\d+$"'))
      # Make sure DeleteInstance() is called for each instance.
      self.assertEqual(
          [mock.call('fugafuga'), mock.call('hogehoge'),
           mock.call('piyopiyo')],
          mock_gce_api_class.return_value.DeleteInstance.call_args_list)

  def testTeardownCluster_WithPrefix(self):
    """Unit test of TeardownCluster() with prefix."""
    with mock.patch('gce_api.GceApi') as mock_gce_api_class:
      mock_gce_api_class.return_value.ListInstances.return_value = [
          {'name': 'wahoooo'},
      ]

      GceCluster(argparse.Namespace(
          project='project-hoge', zone='zone-fuga',
          prefix='boo')).TeardownCluster()

      mock_gce_api_class.assert_called_once_with(
          'hadoop_on_compute', mock.ANY, mock.ANY,
          'project-hoge', 'zone-fuga')
      # Make sure prefix is included in instance name patterns.
      (mock_gce_api_class.return_value.ListInstances.
       assert_called_once_with(
           'name eq "boo-hm|^boo-hw-\\d+$"'))
      self.assertEqual(
          [mock.call('wahoooo')],
          mock_gce_api_class.return_value.DeleteInstance.call_args_list)

  def testTeardownCluster_NoInstance(self):
    """Unit test of TeardownCluster() with no instance returned by list."""
    with mock.patch('gce_api.GceApi') as mock_gce_api_class:
      # ListInstances() returns empty list.
      mock_gce_api_class.return_value.ListInstances.return_value = []

      GceCluster(argparse.Namespace(
          project='project-hoge', zone='zone-fuga')).TeardownCluster()

      mock_gce_api_class.assert_called_once_with(
          'hadoop_on_compute', mock.ANY, mock.ANY,
          'project-hoge', 'zone-fuga')
      (mock_gce_api_class.return_value.ListInstances.
       assert_called_once_with(
           'name eq "hm|^hw-\\d+$"'))
      # Make sure DeleteInstance() is not called.
      self.assertFalse(
          mock_gce_api_class.return_value.DeleteInstance.called)

  def testStartMapReduce(self):
    """Unit test of StartMapReduce()."""
    mock_subprocess_call = mock.patch('subprocess.call', return_value=0).start()
    mock.patch('gce_cluster.MakeScriptRelativePath',
               side_effect=lambda x: '/path/to/program/' + x).start()

    GceCluster(argparse.Namespace(
        project='project-hoge', bucket='tmp-bucket', zone='zone-fuga',
        input='gs://data/inputs', output='gs://data/outputs',
        mapper='mapper.exe', reducer='reducer.exe',
        mapper_count=5, reducer_count=1,
        prefix='boo')).StartMapReduce()

    # Check all subprocess.call() calls have expected arguments.
    self.assertEqual(4, mock_subprocess_call.call_count)
    self.assertEqual(
        mock.call('gsutil cp mapper.exe '
                  'gs://tmp-bucket/mapreduce/mapper-reducer/mapper.exe',
                  shell=True),
        mock_subprocess_call.call_args_list[0])
    self.assertEqual(
        mock.call('gsutil cp reducer.exe '
                  'gs://tmp-bucket/mapreduce/mapper-reducer/reducer.exe',
                  shell=True),
        mock_subprocess_call.call_args_list[1])
    self.assertEqual(
        mock.call('gsutil cp /path/to/program/gcs_to_hdfs_mapper.sh '
                  '/path/to/program/hdfs_to_gcs_mapper.sh '
                  'gs://tmp-bucket/mapreduce/mapper-reducer/',
                  shell=True),
        mock_subprocess_call.call_args_list[2])
    self.assertEqual(
        mock.call('/path/to/program/run-script-remote.sh project-hoge '
                  'zone-fuga boo-hm mapreduce__at__master.sh hadoop '
                  'tmp-bucket '
                  'gs://tmp-bucket/mapreduce/mapper-reducer/mapper.exe 5 '
                  'gs://tmp-bucket/mapreduce/mapper-reducer/reducer.exe 1 '
                  'gs://data/inputs gs://data/outputs',
                  shell=True),
        mock_subprocess_call.call_args_list[3])


if __name__ == '__main__':
  unittest.main()
