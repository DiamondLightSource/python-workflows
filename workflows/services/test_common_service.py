from __future__ import absolute_import, division

import workflows.services
from workflows.services.common_service import Commands, CommonService
import mock
import Queue

def test_instantiate_basic_service():
  '''Create a basic service object'''
  service = CommonService()

  assert service.get_name() is not None

def test_logging_to_frontend():
  '''Log messages should be passed to frontend'''
  fe_queue = mock.Mock()
  service = CommonService(frontend=fe_queue)

  service.log(mock.sentinel.logmessage)

  fe_queue.put_nowait.assert_called()
  assert fe_queue.put_nowait.called_once_with \
    ({'band': 'logging',
      'payload': { 'log': mock.sentinel.logmessage, 'source': 'other' }})

def test_logging_to_dummy():
  '''Should run without errors, message should be dropped.'''
  service = CommonService()
  service.log(mock.sentinel.logmessage)

def test_send_status_updates_to_frontend():
  '''Status updates should be passed to frontend'''
  fe_queue = mock.Mock()
  service = CommonService(frontend=fe_queue)

  service.update_status(mock.sentinel.status)

  fe_queue.put_nowait.assert_called()
  assert fe_queue.put_nowait.called_once_with \
    ({'band': 'status_update',
      'status': mock.sentinel.status })

def test_send_status_dummy():
  '''Should run without errors, status should be dropped.'''
  service = CommonService()
  service.update_status(mock.sentinel.status)

def test_receive_and_follow_shutdown_command():
  '''Receive a shutdown message via the command queue and act on it.
     Check that status codes are updated properly.'''
  cmd_queue = mock.Mock()
  cmd_queue.get.side_effect = [
    { 'band': 'command',
      'payload': Commands.SHUTDOWN },
    AssertionError('Not observing commands') ]
  fe_queue = Queue.Queue()

  # Create service
  service = CommonService(
      commands=cmd_queue, frontend=fe_queue)
  # override class API to ensure overidden functions are called
  service.initializing = mock.Mock()
  service.in_shutdown = mock.Mock()

  # Check new status
  messages = []
  while not fe_queue.empty():
    message = fe_queue.get_nowait()
    if 'statuscode' in message:
      messages.append(message['statuscode'])
  assert messages == [ service.SERVICE_STATUS_NEW ]

  # Start service
  service.start()

  # Check startup/shutdown sequence
  service.initializing.assert_called_once()
  service.in_shutdown.assert_called_once()
  cmd_queue.get.assert_called_once()
  assert cmd_queue.get.call_args == ()
  messages = []
  while not fe_queue.empty():
    message = fe_queue.get_nowait()
    if 'statuscode' in message:
      messages.append(message['statuscode'])
  assert messages == [
    service.SERVICE_STATUS_STARTING,
    service.SERVICE_STATUS_IDLE,
    service.SERVICE_STATUS_PROCESSING,
    service.SERVICE_STATUS_SHUTDOWN,
    service.SERVICE_STATUS_END,
    ]

def test_idle_timer_is_triggered():
  '''Check that the idle timer callback is run if set.'''
  cmd_queue = mock.Mock()
  cmd_queue.get.side_effect = [
    Queue.Empty(),
    { 'band': 'command',
      'payload': Commands.SHUTDOWN },
    AssertionError('Not observing commands') ]
  fe_queue = Queue.Queue()
  idle_trigger = mock.Mock()

  # Create service
  service = CommonService(commands=cmd_queue, frontend=fe_queue)
  service._register_idle(10, idle_trigger)

  # Start service
  service.start()

  # Check trigger has been called
  idle_trigger.assert_called_once_with()

  # Check startup/shutdown sequence
  assert cmd_queue.get.call_count == 2
  assert cmd_queue.get.call_args == ((True, 10),)
  messages = []
  while not fe_queue.empty():
    message = fe_queue.get_nowait()
    if 'statuscode' in message:
      messages.append(message['statuscode'])
  assert messages == [
    service.SERVICE_STATUS_NEW,
    service.SERVICE_STATUS_STARTING,
    service.SERVICE_STATUS_IDLE,
    service.SERVICE_STATUS_TIMER,
    service.SERVICE_STATUS_IDLE,
    service.SERVICE_STATUS_PROCESSING,
    service.SERVICE_STATUS_SHUTDOWN,
    service.SERVICE_STATUS_END,
    ]

def test_callbacks_are_routed_correctly():
  '''Incoming messages are routed to the correct callback functions'''
  cmd_queue = mock.Mock()
  cmd_queue.get.side_effect = [
    { 'band': mock.sentinel.band,
      'payload': mock.sentinel.payload },
    { 'band': 'command',
      'payload': Commands.SHUTDOWN },
    AssertionError('Not observing commands') ]
  fe_queue = Queue.Queue()
  callback = mock.Mock()

  # Create service
  service = CommonService(commands=cmd_queue, frontend=fe_queue)
  service._register(mock.sentinel.band, callback)

  # Start service
  service.start()

  # Check callback occured
  callback.assert_called_with(mock.sentinel.payload)

def test_log_unknown_band_data():
  '''All unidentified messages should be logged to the frondend.'''
  cmd_queue = mock.Mock()
  cmd_queue.get.side_effect = [
    { 'band': mock.sentinel.band, 'payload': mock.sentinel.failure1 },
    { 'payload': mock.sentinel.failure2 },
    { 'band': 'command',
      'payload': Commands.SHUTDOWN },
    AssertionError('Not observing commands') ]
  fe_queue = Queue.Queue()

  # Create service
  service = CommonService(commands=cmd_queue, frontend=fe_queue)

  # Start service
  service.start()

  # Check startup/shutdown sequence
  messages = []
  while not fe_queue.empty():
    message = fe_queue.get_nowait()
    if message.get('band') == 'log':
      messages.append(message.get('payload'))
  assert len(messages) == 2
  assert messages[0]['source'] == 'service'
  assert messages[0]['log']['band'] == mock.sentinel.band
  assert messages[1]['source'] == 'service'
  assert messages[1]['log'].get('band') == None

