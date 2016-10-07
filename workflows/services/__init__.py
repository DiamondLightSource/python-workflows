from __future__ import division, absolute_import
from workflows.services.common_service import CommonService

def lookup(service):
  '''Find a service class based on a name.'''
  return CommonService.register.get(service)

def get_known_services():
  '''Return a dictionary of all known services.'''
  return CommonService.register

CommonService.load(__path__)
