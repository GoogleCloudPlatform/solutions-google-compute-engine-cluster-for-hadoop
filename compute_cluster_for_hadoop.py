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

"""Start up Hadoop on Google Compute Engine."""



import argparse
import logging
import re
import sys

import oauth2client

from gce_cluster import GceCluster


class ComputeClusterForHadoop(object):
  """Class to manage Hadoop on Google Compute Engine."""

  @staticmethod
  def SetUp(flags):
    """Set up environment for Hadoop on Compute."""
    GceCluster(flags).EnvironmentSetUp()

  @staticmethod
  def Start(flags):
    """Starts Google Compute Engine cluster with Hadoop set up."""
    GceCluster(flags).StartCluster()

  @staticmethod
  def ShutDown(flags):
    """Deletes all instances included in the Hadoop cluster."""
    GceCluster(flags).TeardownCluster()

  @staticmethod
  def MapReduce(flags):
    """Starts MapReduce job."""
    GceCluster(flags).StartMapReduce()

  def __init__(self):
    self._parser = argparse.ArgumentParser()

    # Specify --noauth_local_webserver as instructed when you use remote
    # terminal such as ssh.
    class SetNoAuthLocalWebserverAction(argparse.Action):
      def __call__(self, parser, namespace, values, option_string=None):
        oauth2client.tools.gflags.FLAGS.auth_local_webserver = False

    self._parser.add_argument(
        '--noauth_local_webserver', nargs=0,
        action=SetNoAuthLocalWebserverAction,
        help='Do not attempt to open browser on local machine.')

    self._parser.add_argument(
        '--debug', action='store_true',
        help='Debug mode.  Shows verbose log.')

    self._subparsers = self._parser.add_subparsers(
        title='Sub-commands', dest='subcommand')

  def _AddSetUpSubcommand(self):
    """Sets up parameters for 'setup' subcommand."""
    parser_setup = self._subparsers.add_parser(
        'setup',
        help='Sets up environment of project and bucket.  Setup must be '
        'performed once per same project/bucket pair.')
    parser_setup.set_defaults(handler=self.SetUp, prefix='')
    parser_setup.add_argument(
        'project',
        help='Project ID to start Google Compute Engine instances in.')
    parser_setup.add_argument(
        'bucket',
        help='Google Cloud Storage bucket name for temporary use.')

  def _AddStartSubcommand(self):
    """Sets up parameters for 'start' subcommand."""
    parser_start = self._subparsers.add_parser(
        'start',
        help='Start Hadoop cluster.')
    parser_start.set_defaults(handler=self.Start)
    parser_start.add_argument(
        'project',
        help='Project ID to start Google Compute Engine instances in.')
    parser_start.add_argument(
        'bucket',
        help='Google Cloud Storage bucket name for temporary use.')
    parser_start.add_argument(
        'num_workers', default=5, type=int, nargs='?',
        help='Number of worker instances in Hadoop cluster. (default 5)')
    parser_start.add_argument(
        '--prefix', default='',
        help='Name prefix of Google Compute Engine instances. (default "")')
    parser_start.add_argument(
        '--zone', default='',
        help='Zone name where to add Hadoop cluster.')
    parser_start.add_argument(
        '--image', default='',
        help='Machine image of Google Compute Engine instance.')
    parser_start.add_argument(
        '--machinetype', default='',
        help='Machine type of Google Compute Engine instance.')
    parser_start.add_argument(
        '--command', default='',
        help='Additional command to run on each instance.')

  def _AddShutdownSubcommand(self):
    """Sets up parameters for 'shutdown' subcommand."""
    parser_shutdown = self._subparsers.add_parser(
        'shutdown',
        help='Tear down Hadoop cluster.')
    parser_shutdown.set_defaults(handler=self.ShutDown,
                                 image='', machinetype='')
    parser_shutdown.add_argument(
        'project',
        help='Project ID where Hadoop cluster lives.')
    parser_shutdown.add_argument(
        '--prefix', default='',
        help='Name prefix of Google Compute Engine instances. (default "")')
    parser_shutdown.add_argument(
        '--zone', default='',
        help='Zone name where Hadoop cluster lives.')

  def _AddMapReduceSubcommand(self):
    """Sets up parameters for 'mapreduce' subcommand."""
    parser_mapreduce = self._subparsers.add_parser(
        'mapreduce',
        help='Start MapReduce job.')
    parser_mapreduce.set_defaults(handler=self.MapReduce,
                                  image='', machinetype='')
    parser_mapreduce.add_argument(
        'project',
        help='Project ID where Hadoop cluster lives.')
    parser_mapreduce.add_argument(
        'bucket',
        help='Google Cloud Storage bucket name for temporary use.')
    parser_mapreduce.add_argument(
        '--zone', default='',
        help='Zone name where Hadoop cluster lives.')
    parser_mapreduce.add_argument(
        '--prefix', default='',
        help='Name prefix of Google Compute Engine instances. (default "")')
    parser_mapreduce.add_argument(
        '--mapper',
        help='Mapper program file either on local or on Cloud Storage.')
    parser_mapreduce.add_argument(
        '--reducer',
        help='Reducer program file either on local or on Cloud Storage.')
    parser_mapreduce.add_argument(
        '--input', required=True,
        help='Input data directory on Cloud Storage.')
    parser_mapreduce.add_argument(
        '--output', required=True,
        help='Output data directory on Cloud Storage.')
    parser_mapreduce.add_argument(
        '--mapper-count', type=int, dest='mapper_count', default=5,
        help='Number of mapper tasks.')
    parser_mapreduce.add_argument(
        '--reducer-count', type=int, dest='reducer_count', default=1,
        help='Number of reducer tasks.  Make this 0 to skip reducer.')

  def ParseArgumentsAndExecute(self, argv):
    """Parses command-line arguments and executes sub-command handler."""
    self._AddSetUpSubcommand()
    self._AddStartSubcommand()
    self._AddShutdownSubcommand()
    self._AddMapReduceSubcommand()

    # Parse command-line arguments and execute corresponding handler function.
    params = self._parser.parse_args(argv)

    # Check prefix length.
    if hasattr(params, 'prefix') and params.prefix:
      # Prefix:
      #   - 15 characters or less.
      #   - May use lower case, digits or hyphen.
      #   - First character must be lower case alphabet.
      #   - May use hyphen at the end, since actual hostname continues.
      if not re.match('^[a-z][-a-z0-9]{0,14}$', params.prefix):
        logging.critical('Invalid prefix pattern.  Prefix must be 15 '
                         'characters or less.  Only lower case '
                         'alphabets, numbers and hyphen ("-") can be '
                         'used.  The first character must be '
                         'lower case alphabet.')
        sys.exit(1)

    # Set debug mode.
    if params.debug:
      logging.basicConfig(
          level=logging.DEBUG,
          format='%(asctime)s [%(module)s:%(levelname)s] '
          '(%(filename)s:%(funcName)s:%(lineno)d) %(message)s')
    else:
      logging.basicConfig(
          level=logging.INFO,
          format='%(asctime)s [%(module)s:%(levelname)s] %(message)s')

    logging.debug('***** DEBUG LOGGING MODE *****')

    # Execute handler function.
    # Handler functions are set as default parameter value of "handler"
    # by each subparser's set_defaults() method.
    params.handler(params)


def main():
  ComputeClusterForHadoop().ParseArgumentsAndExecute(sys.argv[1:])


if __name__ == '__main__':
  main()
