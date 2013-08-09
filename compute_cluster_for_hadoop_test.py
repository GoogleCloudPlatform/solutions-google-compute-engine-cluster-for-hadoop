#!/usr/bin/python
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

"""Unit tests of compute_cluster_for_hadoop.py."""



import argparse
import unittest

import mock

import compute_cluster_for_hadoop


class ComputeClusterForHadoopTest(unittest.TestCase):
  """Unit test class for ComputeClusterForHadoop."""

  def _GetFlags(self, mock_cluster):
    flags = mock_cluster.call_args[0][0]
    self.assertIsInstance(flags, argparse.Namespace)
    return flags

  def testSetUp(self):
    """Setup sub-command unit test."""
    with mock.patch('gce_cluster.GceCluster') as mock_cluster:
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      hadoop_cluster.ParseArgumentsAndExecute([
          'setup', 'project-name', 'bucket-name'])

      self.assertEqual(1, mock_cluster.call_count)
      flags = self._GetFlags(mock_cluster)
      self.assertEqual('project-name', flags.project)
      self.assertEqual('bucket-name', flags.bucket)
      mock_cluster.return_value.EnvironmentSetUp.assert_called_once_with()

  def testSetUp_NoBucket(self):
    """Setup sub-command unit test with no bucket option."""
    with mock.patch('gce_cluster.GceCluster'):
      # Fails to execute sub-command.
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      self.assertRaises(SystemExit,
                        hadoop_cluster.ParseArgumentsAndExecute,
                        ['setup', 'project-name'])

  def testStart(self):
    """Start sub-command unit test."""
    with mock.patch('gce_cluster.GceCluster') as mock_cluster:
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      hadoop_cluster.ParseArgumentsAndExecute([
          'start', 'project-name', 'bucket-name', '10'])

      self.assertEqual(1, mock_cluster.call_count)
      flags = self._GetFlags(mock_cluster)
      self.assertEqual('project-name', flags.project)
      self.assertEqual('bucket-name', flags.bucket)
      self.assertEqual(10, flags.num_workers)
      mock_cluster.return_value.StartCluster.assert_called_once_with()

  def testStart_DefaultClusterSize(self):
    """Start sub-command unit test with default cluster size."""
    with mock.patch('gce_cluster.GceCluster') as mock_cluster:
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      hadoop_cluster.ParseArgumentsAndExecute([
          'start', 'project-foo', 'bucket-bar'])

      self.assertEqual(1, mock_cluster.call_count)
      flags = self._GetFlags(mock_cluster)
      self.assertEqual('project-foo', flags.project)
      self.assertEqual('bucket-bar', flags.bucket)
      self.assertEqual(5, flags.num_workers)
      mock_cluster.return_value.StartCluster.assert_called_once_with()

  def testStart_OptionalParams(self):
    """Start sub-command unit test with optional params."""
    with mock.patch('gce_cluster.GceCluster') as mock_cluster:
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      hadoop_cluster.ParseArgumentsAndExecute([
          'start', 'project-name', 'bucket-name', '--prefix', 'fuga',
          '--zone', 'piyo', '--command', '"additional command"'])

      self.assertEqual(1, mock_cluster.call_count)
      flags = self._GetFlags(mock_cluster)
      self.assertEqual('project-name', flags.project)
      self.assertEqual('bucket-name', flags.bucket)
      self.assertEqual(5, flags.num_workers)
      self.assertEqual('fuga', flags.prefix)
      self.assertEqual('piyo', flags.zone)
      self.assertEqual('"additional command"', flags.command)
      mock_cluster.return_value.StartCluster.assert_called_once_with()

  def testStart_Prefix(self):
    """Start sub-command unit test with long prefix."""
    with mock.patch('gce_cluster.GceCluster'):
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      hadoop_cluster.ParseArgumentsAndExecute([
          'start', 'project-piyo', 'bucket-bar',
          '--prefix', 'a6b-c'])
      hadoop_cluster.ParseArgumentsAndExecute([
          'start', 'project-piyo', 'bucket-bar',
          '--prefix', 'ends-with-dash-'])

      # Invalid patterns.
      self.assertRaises(SystemExit,
                        hadoop_cluster.ParseArgumentsAndExecute,
                        ['start', 'project-piyo', 'bucket-bar',
                         '--prefix', 'insanely-long-prefix'])
      self.assertRaises(SystemExit,
                        hadoop_cluster.ParseArgumentsAndExecute,
                        ['start', 'project-piyo', 'bucket-bar',
                         '--prefix', 'upperCase'])
      self.assertRaises(SystemExit,
                        hadoop_cluster.ParseArgumentsAndExecute,
                        ['start', 'project-piyo', 'bucket-bar',
                         '--prefix', 'invalid*char'])
      self.assertRaises(SystemExit,
                        hadoop_cluster.ParseArgumentsAndExecute,
                        ['start', 'project-piyo', 'bucket-bar',
                         '--prefix', '0number'])

  def testStart_NoBucket(self):
    """Start sub-command unit test with no bucket option."""
    with mock.patch('gce_cluster.GceCluster'):
      # Fails to execute sub-command.
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      self.assertRaises(SystemExit,
                        hadoop_cluster.ParseArgumentsAndExecute,
                        ['start', 'project-hoge'])

  def testShutdown(self):
    """Shutdown sub-command unit test."""
    with mock.patch('gce_cluster.GceCluster') as mock_cluster:
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      hadoop_cluster.ParseArgumentsAndExecute([
          'shutdown', 'project-name'])

      self.assertEqual(1, mock_cluster.call_count)
      flags = self._GetFlags(mock_cluster)
      self.assertEqual('project-name', flags.project)
      mock_cluster.return_value.TeardownCluster.assert_called_once_with()

  def testShutdown_OptionalParams(self):
    """Shutdown sub-command unit test with optional params."""
    with mock.patch('gce_cluster.GceCluster') as mock_cluster:
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      hadoop_cluster.ParseArgumentsAndExecute([
          'shutdown', 'project-name', '--prefix', 'foo',
          '--zone', 'abc'])

      self.assertEqual(1, mock_cluster.call_count)
      flags = self._GetFlags(mock_cluster)
      self.assertEqual('project-name', flags.project)
      self.assertEqual('foo', flags.prefix)
      self.assertEqual('abc', flags.zone)
      mock_cluster.return_value.TeardownCluster.assert_called_once_with()

  def testShutdown_MissingParamValue(self):
    """Shutdown sub-command unit test with missing param value."""
    with mock.patch('gce_cluster.GceCluster'):
      # Fails to execute sub-command.
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      self.assertRaises(SystemExit,
                        hadoop_cluster.ParseArgumentsAndExecute,
                        ['shutdown', 'project-name', '--prefix'])

  def testShutdown_InvalidOption(self):
    """Shutdown sub-command unit test with invalid optional param."""
    with mock.patch('gce_cluster.GceCluster'):
      # Fails to execute sub-command.
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      self.assertRaises(SystemExit,
                        hadoop_cluster.ParseArgumentsAndExecute,
                        ['shutdown', 'project-name', '--image', 'foo'])

  def testMapReduce(self):
    """MapReduce sub-command unit test."""
    with mock.patch('gce_cluster.GceCluster') as mock_cluster:
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      hadoop_cluster.ParseArgumentsAndExecute([
          'mapreduce', 'project-name', 'bucket-name',
          '--input', 'gs://some-bucket/inputs',
          '--output', 'gs://some-bucket/outputs'])

      self.assertEqual(1, mock_cluster.call_count)
      flags = self._GetFlags(mock_cluster)
      self.assertEqual('project-name', flags.project)
      self.assertEqual('bucket-name', flags.bucket)
      self.assertEqual('gs://some-bucket/inputs', flags.input)
      self.assertEqual('gs://some-bucket/outputs', flags.output)
      mock_cluster.return_value.StartMapReduce.assert_called_once_with()

  def testMapReduce_NoInputOutput(self):
    """MapReduce sub-command unit test."""
    with mock.patch('gce_cluster.GceCluster'):
      hadoop_cluster = compute_cluster_for_hadoop.ComputeClusterForHadoop()
      self.assertRaises(SystemExit,
                        hadoop_cluster.ParseArgumentsAndExecute,
                        ['mapreduce', 'project-name', 'bucket-name'])


if __name__ == '__main__':
  unittest.main()
