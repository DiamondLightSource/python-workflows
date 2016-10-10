from __future__ import absolute_import, division

import workflows.services
import workflows.services.sample_consumer
import mock

def test_service_can_be_looked_up():
  '''Attempt to look up the service by its name'''
  service_class = workflows.services.lookup('Consumer')
  assert service_class == workflows.services.sample_consumer.Consumer

def test_service_subscribes_to_channel():
  '''Check that the service registers an idle event handler.'''
  p = workflows.services.sample_consumer.Consumer()
  mock_transport = mock.Mock()
  setattr(p, '_transport', mock_transport)

  p.initializing()

  mock_transport.subscribe.assert_called_once_with(mock.ANY, p.consume_message)

def test_service_can_consume_messages():
  '''Check that the service registers an idle event handler.'''
  p = workflows.services.sample_consumer.Consumer()
  p.consume_message(None, mock.sentinel.message)
  p.consume_message({'some': 'header'}, mock.sentinel.message)
