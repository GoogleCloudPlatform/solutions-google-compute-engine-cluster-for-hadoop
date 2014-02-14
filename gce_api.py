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

"""Module to provide Google Client API wrapper for Google Compute Engine."""



import logging
import os
import os.path

import apiclient.discovery
import apiclient.errors
import httplib2

import oauth2client.client
import oauth2client.file
import oauth2client.tools


class ResourceZoning(object):
  """Constants to indicate which zone type the resource belongs to."""
  NONE = 0
  GLOBAL = 1
  ZONE = 2


class GceApi(object):
  """Google Client API wrapper for Google Compute Engine."""

  COMPUTE_ENGINE_SCOPE = 'https://www.googleapis.com/auth/compute'
  COMPUTE_ENGINE_API_VERSION = 'v1'

  def __init__(self, name, client_id, client_secret, project, zone):
    """Constructor.

    Args:
      name: Name of the user of the class.  Used for credentials filename.
      client_id: Client ID of the user of the class.
      client_secret: Client secret of the user of the class.
      project: Project ID.
      zone: Zone name, e.g. 'us-east-a'
    """
    self._name = name
    self._client_id = client_id
    self._client_secret = client_secret
    self._project = project
    self._zone = zone

  def GetApi(self):
    """Does OAuth2 authorization and prepares Google Compute Engine API.

    Since access keys may expire at any moment, call the function every time
    making API call.

    Returns:
      Google Client API object for Google Compute Engine.
    """
    # First, check local file for credentials.
    homedir = os.environ['HOME']
    storage = oauth2client.file.Storage(
        os.path.join(homedir, '.%s.credentials' % self._name))
    credentials = storage.get()

    if not credentials or credentials.invalid:
      # If local credentials are not valid, do OAuth2 dance.
      flow = oauth2client.client.OAuth2WebServerFlow(
          self._client_id, self._client_secret, self.COMPUTE_ENGINE_SCOPE)
      credentials = oauth2client.tools.run(flow, storage)

    # Set up http with the credentials.
    authorized_http = credentials.authorize(httplib2.Http())
    return apiclient.discovery.build(
        'compute', self.COMPUTE_ENGINE_API_VERSION, http=authorized_http)

  @staticmethod
  def IsNotFoundError(http_error):
    """Checks if HttpError reason was 'not found'.

    Args:
      http_error: HttpError
    Returns:
      True if the error reason was 'not found', otherwise False.
    """
    return http_error.resp['status'] == '404'

  @classmethod
  def _ResourceUrlFromPath(cls, path):
    """Creates full resource URL from path."""
    return 'https://www.googleapis.com/compute/%s/%s' % (
        cls.COMPUTE_ENGINE_API_VERSION, path)

  def _ResourceUrl(self, resource_type, resource_name,
                   zoning=ResourceZoning.ZONE, project=None):
    """Creates URL to indicate Google Compute Engine resource.

    Args:
      resource_type: Resource type.
      resource_name: Resource name.
      zoning: Which zone type the resource belongs to.
      project: Overrides project for the resource.
    Returns:
      URL in string to represent the resource.
    """
    if not project:
      project = self._project

    if zoning == ResourceZoning.NONE:
      resource_path = 'projects/%s/%s/%s' % (
          project, resource_type, resource_name)
    elif zoning == ResourceZoning.GLOBAL:
      resource_path = 'projects/%s/global/%s/%s' % (
          project, resource_type, resource_name)
    else:
      resource_path = 'projects/%s/zones/%s/%s/%s' % (
          project, self._zone, resource_type, resource_name)

    return self._ResourceUrlFromPath(resource_path)

  def _ParseOperation(self, operation, title):
    """Parses operation result and log warnings and errors if any.

    Args:
      operation: Operation object as result of operation.
      title: Title used for log.
    Returns:
      Boolean to indicate whether the operation was successful.
    """
    if 'error' in operation and 'errors' in operation['error']:
      for e in operation['error']['errors']:
        logging.error('%s: %s: %s',
                      title, e.get('code', 'NO ERROR CODE'),
                      e.get('message', 'NO ERROR MESSAGE'))
      return False

    if 'warnings' in operation:
      for w in operation['warnings']:
        logging.warning('%s: %s: %s',
                        title, w.get('code', 'NO WARNING CODE'),
                        w.get('message', 'NO WARNING MESSAGE'))
    return True

  def GetInstance(self, instance_name):
    """Gets instance information.

    Args:
      instance_name: Name of the instance to get information of.
    Returns:
      Google Compute Engine instance resource.  None if error.
      https://developers.google.com/compute/docs/reference/latest/instances
    """
    try:
      return self.GetApi().instances().get(
          project=self._project, zone=self._zone,
          instance=instance_name).execute()
    except apiclient.errors.HttpError as e:
      if self.IsNotFoundError(e):
        logging.warning('Get instance: %s not found', instance_name)
        return None
      raise

  def ListInstances(self, filter_string=None):
    """Lists instances that matches filter condition.

    Format of filter string can be found in the following URL.
    http://developers.google.com/compute/docs/reference/latest/instances/list

    Args:
      filter_string: Filtering condition.
    Returns:
      List of compute#instance.
    """
    result = self.GetApi().instances().list(
        project=self._project, zone=self._zone, filter=filter_string).execute()
    return result.get('items', [])

  def CreateInstance(self, instance_name, machine_type, boot_disk, disks=None,
                     startup_script='', service_accounts=None,
                     external_ip=True, metadata=None, tags=None,
                     can_ip_forward=False):
    """Creates Google Compute Engine instance.

    Args:
      instance_name: Name of the new instance.
      machine_type: Machine type.  e.g. 'n1-standard-2'
      boot_disk: Name of the persistent disk to be used as a boot disk.
          The disk must preexist in the same zone as the instance.
      disks: List of the names of the extra persistent disks attached to
          the instance in addition to the boot disk.
      startup_script: Content of start up script to run on the new instance.
      service_accounts: List of scope URLs to give to the instance with
          the service account.
      external_ip: Boolean value to indicate whether the new instance has
          an external IP address.
      metadata: Additional key-value pairs in dictionary to add as
          instance metadata.
      tags: String list of tags to attach to the new instance.
      can_ip_forward: Boolean to indicate if the new instance can forward IP
          packets.
    Returns:
      Boolean to indicate whether the instance creation was successful.
    """
    params = {
        'kind': 'compute#instance',
        'name': instance_name,
        'zone': self._ResourceUrl('zones', self._zone,
                                  zoning=ResourceZoning.NONE),
        'machineType': self._ResourceUrl('machineTypes', machine_type),
        'disks': [
            {
                'kind': 'compute#attachedDisk',
                'boot': True,
                'source': self._ResourceUrl('disks', boot_disk),
                'mode': 'READ_WRITE',
                'type': 'PERSISTENT',
            },
        ],
        'metadata': {
            'kind': 'compute#metadata',
            'items': [
                {
                    'key': 'startup-script',
                    'value': startup_script,
                },
            ],
        },
        'canIpForward': can_ip_forward,
        'networkInterfaces': [
            {
                'kind': 'compute#instanceNetworkInterface',
                'accessConfigs': [],
                'network': self._ResourceUrl('networks', 'default',
                                             zoning=ResourceZoning.GLOBAL)
            },
        ],
        'serviceAccounts': [
            {
                'kind': 'compute#serviceAccount',
                'email': 'default',
                'scopes': service_accounts or [],
            },
        ],
    }

    # Attach extra disks.
    if disks:
      for disk in disks:
        params['disks'].append({
            'kind': 'compute#attachedDisk',
            'boot': False,
            'source': self._ResourceUrl('disks', disk),
            'deviceName': disk,
            'mode': 'READ_WRITE',
            'type': 'PERSISTENT',
        })

    # Request external IP address if necessary.
    if external_ip:
      params['networkInterfaces'][0]['accessConfigs'].append({
          'kind': 'compute#accessConfig',
          'type': 'ONE_TO_ONE_NAT',
          'name': 'External NAT',
      })

    # Add metadata.
    if metadata:
      for key, value in metadata.items():
        params['metadata']['items'].append({'key': key, 'value': value})

    # Add tags.
    if tags:
      params['tags'] = {'items': tags}

    operation = self.GetApi().instances().insert(
        project=self._project, zone=self._zone, body=params).execute()

    return self._ParseOperation(
        operation, 'Instance creation: %s' % instance_name)

  def DeleteInstance(self, instance_name):
    """Deletes Google Compute Engine instance.

    Args:
      instance_name: Name of the instance to delete.
    Returns:
      Boolean to indicate whether the instance deletion was successful.
    """
    try:
      operation = self.GetApi().instances().delete(
          project=self._project, zone=self._zone,
          instance=instance_name).execute()
      return self._ParseOperation(
          operation, 'Instance deletion: %s' % instance_name)
    except apiclient.errors.HttpError as e:
      if self.IsNotFoundError(e):
        logging.warning('Delete instance: %s not found', instance_name)
        return False
      raise

  def GetDisk(self, disk_name):
    """Gets persistent disk information.

    Args:
      disk_name: Name of the persistent disk to get information about.
    Returns:
      Google Compute Engine disk resource.  None if not found.
      https://developers.google.com/compute/docs/reference/latest/disks
    Raises:
      HttpError on API error, except for 'resource not found' error.
    """
    try:
      return self.GetApi().disks().get(
          project=self._project, zone=self._zone, disk=disk_name).execute()
    except apiclient.errors.HttpError as e:
      if self.IsNotFoundError(e):
        return None
      raise

  def ListDisks(self, filter_string=None):
    """Lists disks that match filter condition.

    Format of filter string can be found in the following URL.
    https://developers.google.com/compute/docs/reference/latest/disks/list

    Args:
      filter_string: Filtering condition.
    Returns:
      List of compute#disk.
    """
    result = self.GetApi().disks().list(
        project=self._project, zone=self._zone, filter=filter_string).execute()
    return result.get('items', [])

  def CreateDisk(self, disk_name, size_gb=10, image=None):
    """Creates persistent disk in the zone of this API.

    Args:
      disk_name: Name of the new persistent disk.
      size_gb: Size of the new persistent disk in GB.
      image: Machine image name for the new disk to base upon.
          e.g. 'projects/debian-cloud/global/images/debian-7-wheezy-v20131014'
    Returns:
      Boolean to indicate whether the disk creation was successful.
    """
    params = {
        'kind': 'compute#disk',
        'sizeGb': '%d' % size_gb,
        'name': disk_name,
    }
    source_image = self._ResourceUrlFromPath(image) if image else None
    operation = self.GetApi().disks().insert(
        project=self._project, zone=self._zone, body=params,
        sourceImage=source_image).execute()
    return self._ParseOperation(
        operation, 'Disk creation %s' % disk_name)

  def DeleteDisk(self, disk_name):
    """Deletes persistent disk.

    Args:
      disk_name: Name of the persistent disk to delete.
    Returns:
      Boolean to indicate whether the disk deletion was successful.
    """
    operation = self.GetApi().disks().delete(
        project=self._project, zone=self._zone, disk=disk_name).execute()

    return self._ParseOperation(
        operation, 'Disk deletion: %s' % disk_name)

  def AddRoute(self, route_name, next_hop_instance,
               network='default', dest_range='0.0.0.0/0',
               tags=None, priority=100):
    """Adds route to the specified instance.

    Args:
      route_name: Name of the new route.
      next_hop_instance: Instance name of the next hop.
      network: Network to which to add the route.
      dest_range: Destination IP range for the new route.
      tags: List of strings of instance tags.
      priority: Priority of the route.
    Returns:
      Boolean to indicate whether the route creation was successful.
    """
    params = {
        'kind': 'compute#route',
        'name': route_name,
        'destRange': dest_range,
        'priority': priority,
        'network': self._ResourceUrl(
            'networks', network, zoning=ResourceZoning.GLOBAL),
        'nextHopInstance': self._ResourceUrl('instances', next_hop_instance),
    }

    if tags:
      params['tags'] = tags

    operation = self.GetApi().routes().insert(
        project=self._project, body=params).execute()
    return self._ParseOperation(operation, 'Route creation: %s' % route_name)

  def DeleteRoute(self, route_name):
    """Deletes route by name.

    Args:
      route_name: Name of the route to delete.
    Returns:
      Boolean to indicate whether the route deletion was successful.
    """
    try:
      operation = self.GetApi().routes().delete(
          project=self._project, route=route_name).execute()
      return self._ParseOperation(operation, 'Route deletion: %s' % route_name)
    except apiclient.errors.HttpError as e:
      if self.IsNotFoundError(e):
        logging.warning('Delete route: %s not found', route_name)
        return False
      raise
