from __future__ import division

import workflows.services
import workflows.services.sample_services
import mock
import Queue

def test_service_can_be_looked_up():
  '''Attempt to look up the service by its name'''
  service_class = workflows.services.lookup('sample_consumer')
  assert service_class == workflows.services.sample_services.Consumer

