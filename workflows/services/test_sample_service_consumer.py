from __future__ import division

import workflows.services
import workflows.services.sample_consumer
import mock

def test_service_can_be_looked_up():
  '''Attempt to look up the service by its name'''
  service_class = workflows.services.lookup('Consumer')
  assert service_class == workflows.services.sample_consumer.Consumer

