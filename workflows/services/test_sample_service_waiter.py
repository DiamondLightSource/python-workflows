from __future__ import absolute_import, division

import workflows.services
from workflows.services.common_service import Commands
import workflows.services.sample_services
import mock
import Queue

def test_service_can_be_looked_up():
  '''Attempt to look up the service by its name'''
  service_class = workflows.services.lookup('Waiter')
  assert service_class == workflows.services.sample_services.Waiter

@mock.patch('workflows.services.sample_services.time')
def test_start_and_shutdown_waiter_service(mock_time):
  '''Start the waiter service, process some stuff and shut it down again.'''
  cmd_queue = mock.Mock()
  cmd_queue.get.side_effect = [
    { 'band': 'stuff', 'payload': mock.sentinel.stuff },
    { 'band': 'command',
      'payload': Commands.SHUTDOWN },
    AssertionError('Not observing commands') ]
  fe_queue = Queue.Queue()

  # Create service
  service = workflows.services.sample_services.Waiter(
      commands=cmd_queue, frontend=fe_queue)

  # Start service
  service.start()

  # Check all messages consumed
  assert cmd_queue.get.call_count == 2

  # Check outgoing messages
  messages, logs = [], []
  while not fe_queue.empty():
    message = fe_queue.get_nowait()
    if 'statuscode' in message:
      messages.append(message['statuscode'])
    else:
      logs.append(message)
  assert messages == [
      service.SERVICE_STATUS_NEW,
      service.SERVICE_STATUS_STARTING,
      service.SERVICE_STATUS_IDLE,
      service.SERVICE_STATUS_PROCESSING,
      service.SERVICE_STATUS_IDLE,
      service.SERVICE_STATUS_PROCESSING,
      service.SERVICE_STATUS_SHUTDOWN,
      service.SERVICE_STATUS_END,
    ]
  assert len(logs) == 4
  assert mock_time.sleep.call_count == 3
