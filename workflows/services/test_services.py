from __future__ import absolute_import, division
import workflows.services

def test_known_services_is_a_dictionary():
  '''Check services register build in CommonService.'''
  assert isinstance(workflows.services.get_known_services(), dict)
