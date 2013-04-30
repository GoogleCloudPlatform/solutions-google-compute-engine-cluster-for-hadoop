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

"""Unit tests of gce_api.py."""



import unittest

import apiclient.discovery

import mock
from mock import MagicMock

import oauth2client.file
import oauth2client.tools

from gce_api import GceApi


class GceApiTest(unittest.TestCase):
  """Unit test class of GceApi."""

  def setUp(self):
    self.gce_api = GceApi('gce_api_test', 'CLIENT_ID', 'CLIENT_SECRET',
                          'project-name', 'zone-name')

  def tearDown(self):
    mock.patch.stopall()

  def _MockGoogleClientApi(self, credentials_validity=True):
    """Sets up mocks for Google Client API library.

    Args:
      credentials_validity: Type/validity of locally cached credentials.
          None for no local credentials, False for invalid local credentials,
          True for valid local credentials.
    Returns:
      Dictionary that holds mocks created.
    """
    mock_local_credentials = MagicMock(
        spec=oauth2client.client.Credentials, name='Mock Credentials')
    mock_http_local = MagicMock(name='HTTP authorized by local credentials')
    mock_local_credentials.authorize.return_value = mock_http_local

    mock_new_credentials = MagicMock(
        spec=oauth2client.client.Credentials, name='Mock New Credentials')
    mock_http_new = MagicMock(name='HTTP authorized by new credentials')
    mock_new_credentials.authorize.return_value = mock_http_new
    mock_api = MagicMock(name='Google Client API')

    mock_storage_class = mock.patch('oauth2client.file.Storage').start()
    mock_flow_class = mock.patch('gce_api.OAuth2WebServerFlow').start()
    mock.patch('oauth2client.tools.run',
               return_value=mock_new_credentials).start()
    mock.patch('apiclient.discovery.build', return_value=mock_api).start()
    mock.patch('httplib2.Http').start()

    mock_storage = mock_storage_class.return_value
    if credentials_validity is None:
      mock_storage.get.return_value = None
    else:
      mock_storage.get.return_value = mock_local_credentials
      mock_local_credentials.invalid = not credentials_validity
    mock_flow = mock_flow_class.return_value
    apiclient.discovery.build = MagicMock(return_value=mock_api)

    return {'api': mock_api,
            'storage_class': mock_storage_class,
            'storage': mock_storage,
            'flow_class': mock_flow_class,
            'flow': mock_flow,
            'local_credentials': mock_local_credentials,
            'http_authorized_by_local_credentials': mock_http_local,
            'new_credentials': mock_new_credentials,
            'http_authorized_by_new_credentials': mock_http_new}

  def testGetApi_CachedCredentials(self):
    """Unit test of GetApi().  Local credentials are valid."""
    my_mocks = self._MockGoogleClientApi()

    api = self.gce_api.GetApi()

    self.assertEqual(my_mocks['api'], api)
    self.assertEqual(1, my_mocks['storage_class'].call_count)
    # When cached credentials are valid, OAuth2 dance won't happen.
    self.assertFalse(my_mocks['flow_class'].called)
    self.assertFalse(oauth2client.tools.run.called)
    self.assertEqual(1, my_mocks['local_credentials'].authorize.call_count)
    apiclient.discovery.build.assert_called_once_with(
        'compute', mock.ANY,
        http=my_mocks['http_authorized_by_local_credentials'])
    self.assertRegexpMatches(
        apiclient.discovery.build.call_args[0][1], '^v\\d')

  def testGetApi_InvalidCachedCredentials(self):
    """Unit test of GetApi().  Local credentials are invalid."""
    my_mocks = self._MockGoogleClientApi(False)

    api = self.gce_api.GetApi()

    self.assertEqual(my_mocks['api'], api)
    self.assertEqual(1, my_mocks['storage_class'].call_count)
    self.assertTrue(my_mocks['flow_class'].called)
    oauth2client.tools.run.assert_called_once_with(
        my_mocks['flow'], my_mocks['storage'])
    # New credentials are used.
    self.assertEqual(1, my_mocks['new_credentials'].authorize.call_count)
    apiclient.discovery.build.assert_called_once_with(
        'compute', mock.ANY,
        http=my_mocks['http_authorized_by_new_credentials'])
    self.assertRegexpMatches(
        apiclient.discovery.build.call_args[0][1], '^v\\d')

  def testGetApi_NoCachedCredentials(self):
    """Unit test of GetApi().  Local credentials are invalid."""
    my_mocks = self._MockGoogleClientApi(None)

    api = self.gce_api.GetApi()

    self.assertEqual(my_mocks['api'], api)
    self.assertEqual(1, my_mocks['storage_class'].call_count)
    self.assertTrue(my_mocks['flow_class'].called)
    oauth2client.tools.run.assert_called_once_with(
        my_mocks['flow'], my_mocks['storage'])
    # New credentials are used.
    self.assertEqual(1, my_mocks['new_credentials'].authorize.call_count)
    apiclient.discovery.build.assert_called_once_with(
        'compute', mock.ANY,
        http=my_mocks['http_authorized_by_new_credentials'])
    self.assertRegexpMatches(
        apiclient.discovery.build.call_args[0][1], '^v\\d')

  def testGetInstance(self):
    """Unit test of GetInstance()."""
    mock_api = MagicMock(name='Mock Google Client API')
    self.gce_api.GetApi = MagicMock(return_value=mock_api)

    instance_info = self.gce_api.GetInstance('instance-name')

    self.gce_api.GetApi.assert_called_once()
    mock_api.instances.return_value.get.assert_called_once_with(
        project='project-name', zone='zone-name', instance='instance-name')
    (mock_api.instances.return_value.get.return_value.execute.
     assert_called_once_with())
    self.assertEqual(mock_api.instances.return_value.get.return_value.
                     execute.return_value,
                     instance_info)

  def testListInstance_NoFilter(self):
    """Unit test of ListInstance() without filter string."""
    mock_api = MagicMock(name='Mock Google Client API')
    self.gce_api.GetApi = MagicMock(return_value=mock_api)
    mock_api.instances.return_value.list.return_value.execute.return_value = {
        'items': ['dummy', 'list']
    }

    instance_list = self.gce_api.ListInstances()

    self.gce_api.GetApi.assert_called_once()
    mock_api.instances.return_value.list.assert_called_once_with(
        project='project-name', zone='zone-name', filter=None)
    (mock_api.instances.return_value.list.return_value.execute.
     assert_called_once_with())
    self.assertEqual(['dummy', 'list'], instance_list)

  def testListInstance_Filter(self):
    """Unit test of ListInstance() with filter string."""
    mock_api = MagicMock(name='Mock Google Client API')
    self.gce_api.GetApi = MagicMock(return_value=mock_api)
    mock_api.instances.return_value.list.return_value.execute.return_value = {
        'items': ['dummy', 'list']
    }

    instance_list = self.gce_api.ListInstances('filter condition')

    self.gce_api.GetApi.assert_called_once()
    mock_api.instances.return_value.list.assert_called_once_with(
        project='project-name', zone='zone-name', filter='filter condition')
    (mock_api.instances.return_value.list.return_value.execute.
     assert_called_once_with())
    self.assertEqual(['dummy', 'list'], instance_list)

  def testCreateInstance_Success(self):
    """Unit test of CreateInstance() with success result."""
    mock_api = MagicMock(name='Mock Google Client API')
    self.gce_api.GetApi = MagicMock(return_value=mock_api)
    mock_api.instances.return_value.insert.return_value.execute.return_value = {
        'name': 'instance-name'
    }

    self.assertTrue(self.gce_api.CreateInstance(
        'instance-name', 'machine-type', 'image-name'))

    self.gce_api.GetApi.assert_called_once()
    mock_api.instances.return_value.insert.assert_called_once_with(
        project='project-name', zone='zone-name', body=mock.ANY)
    (mock_api.instances.return_value.insert.return_value.execute.
     assert_called_once_with())

  def testCreateInstance_SuccessWithWarning(self):
    """Unit test of CreateInstance() with warning."""
    mock_api = MagicMock(name='Mock Google Client API')
    self.gce_api.GetApi = MagicMock(return_value=mock_api)
    mock_api.instances.return_value.insert.return_value.execute.return_value = {
        'name': 'instance-name',
        'warnings': [
            {
                'code': 'some warning code',
                'message': 'some warning message'
            }
        ]
    }

    # CreateInstance() returns True for warning.
    self.assertTrue(self.gce_api.CreateInstance(
        'instance-name', 'machine-type', 'image-name'))

    self.gce_api.GetApi.assert_called_once()
    mock_api.instances.return_value.insert.assert_called_once_with(
        project='project-name', zone='zone-name', body=mock.ANY)
    (mock_api.instances.return_value.insert.return_value.execute.
     assert_called_once_with())

  def testCreateInstance_Error(self):
    """Unit test of CreateInstance() with error."""
    mock_api = MagicMock(name='Mock Google Client API')
    self.gce_api.GetApi = MagicMock(return_value=mock_api)
    mock_api.instances.return_value.insert.return_value.execute.return_value = {
        'name': 'instance-name',
        'error': {
            'errors': [
                {
                    'code': 'some error code',
                    'message': 'some error message'
                }
            ]
        }
    }

    # CreateInstance() returns False.
    self.assertFalse(self.gce_api.CreateInstance(
        'instance-name', 'machine-type', 'image-name'))

    self.gce_api.GetApi.assert_called_once()
    mock_api.instances.return_value.insert.assert_called_once_with(
        project='project-name', zone='zone-name', body=mock.ANY)
    (mock_api.instances.return_value.insert.return_value.execute.
     assert_called_once_with())

  def testDeleteInstance(self):
    """Unit test of DeleteInstance()."""
    mock_api = MagicMock(name='Mock Google Client API')
    self.gce_api.GetApi = MagicMock(return_value=mock_api)
    mock_api.instances.return_value.insert.return_value.execute.return_value = {
        'name': 'instance-name'
    }

    self.assertTrue(self.gce_api.DeleteInstance('instance-name'))

    self.gce_api.GetApi.assert_called_once()
    mock_api.instances.return_value.delete.assert_called_once_with(
        project='project-name', zone='zone-name', instance='instance-name')
    (mock_api.instances.return_value.delete.return_value.execute.
     assert_called_once_with())


if __name__ == '__main__':
  unittest.main()
