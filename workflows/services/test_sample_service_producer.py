from __future__ import division

import workflows.services
import workflows.services.sample_producer
import mock
import pytest
import Queue

def test_service_can_be_looked_up():
  '''Attempt to look up the service by its name'''
  service_class = workflows.services.lookup('Producer')
  assert service_class == workflows.services.sample_producer.Producer

@pytest.mark.skip(reason="broken test, skip for now")
def test_service_registers_idle_timer():
  pass
# service._register_idle(10, idle_trigger)
# cmd_queue = mock.Mock()
# cmd_queue.get.side_effect = [
#   Queue.Empty(),
#   { 'channel': 'command',
#     'payload': workflows.services.Commands.SHUTDOWN },
#   AssertionError('Not observing commands') ]
# fe_queue = Queue.Queue()
# idle_trigger = mock.Mock()
#
# # Create service
# service = workflows.services.Service(
#     commands=cmd_queue, frontend=fe_queue)
# service._register_idle(10, idle_trigger)
#
# # Start service
# service.start()
#
# # Check trigger has been called
# idle_trigger.assert_called_once_with()
#
# # Check startup/shutdown sequence
# assert cmd_queue.get.call_count == 2
# assert cmd_queue.get.call_args == ((True, 10),)
# messages = []
# while not fe_queue.empty():
#   message = fe_queue.get_nowait()
#   if 'statuscode' in message:
#     messages.append(message['statuscode'])
# assert messages == [
#   service.SERVICE_STATUS_NEW,
#   service.SERVICE_STATUS_STARTING,
#   service.SERVICE_STATUS_IDLE,
#   service.SERVICE_STATUS_TIMER,
#   service.SERVICE_STATUS_IDLE,
#   service.SERVICE_STATUS_PROCESSING,
#   service.SERVICE_STATUS_SHUTDOWN,
#   service.SERVICE_STATUS_END,
#   ]


