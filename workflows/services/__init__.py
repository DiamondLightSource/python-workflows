from __future__ import absolute_import, division
import workflows
from workflows.services.common_service import CommonService

def lookup(service):
  '''Find a service class based on a name.
     :param service: Name of the service
     :return: A service class
  '''
  return CommonService.plugin_register.get(service)

def get_known_services():
  '''Return a dictionary of all known services.
     :return: A dictionary containing entries { service name : service class }
  '''
  return CommonService.plugin_register

workflows.load_plugins(__path__)
