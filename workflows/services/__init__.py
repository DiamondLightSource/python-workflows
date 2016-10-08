from __future__ import absolute_import, division
from workflows.services.common_service import CommonService

def lookup(service):
  '''Find a service class based on a name.
     :param service: Name of the service
     :return: A service class
  '''
  return CommonService.service_register.get(service)

def get_known_services():
  '''Return a dictionary of all known services.
     :return: A dictionary containing entries { service name : service class }
  '''
  return CommonService.service_register

def import_services(path):
  '''Import all python files in this path to register services.
     :param path: A path or list of paths containing files to import.
  '''
  return CommonService.load(path)

import_services(__path__)
