from __future__ import absolute_import, division

import workflows.services
from workflows.services.common_service import Commands, CommonService
import mock
from multiprocessing import Pipe

def test_instantiate_basic_service():
  '''Create a basic service object'''
  service = CommonService()

  assert service.get_name() is not None

def test_logging_to_frontend():
  '''Log messages should be passed to frontend'''
  fe_pipe = mock.Mock()
  service = CommonService(frontend=fe_pipe)

  service.log(mock.sentinel.logmessage)

  fe_pipe.send.assert_called()
  assert fe_pipe.send.called_once_with \
    ({'band': 'logging',
      'payload': { 'log': mock.sentinel.logmessage, 'source': 'other' }})

def test_logging_to_dummy():
  '''Should run without errors, message should be dropped.'''
  service = CommonService()
  service.log(mock.sentinel.logmessage)

def test_send_status_updates_to_frontend():
  '''Status updates should be passed to frontend'''
  fe_pipe = mock.Mock()
  service = CommonService(frontend=fe_pipe)

  service.update_status(mock.sentinel.status)

  fe_pipe.send.assert_called()
  assert fe_pipe.send.called_once_with \
    ({'band': 'status_update',
      'status': mock.sentinel.status })

def test_send_status_dummy():
  '''Should run without errors, status should be dropped.'''
  service = CommonService()
  service.update_status(mock.sentinel.status)

def test_receive_and_follow_shutdown_command():
  '''Receive a shutdown message via the command pipe and act on it.
     Check that status codes are updated properly.'''
  cmd_pipe = mock.Mock()
  cmd_pipe.poll.return_value = True
  cmd_pipe.recv.side_effect = [
    { 'band': 'command',
      'payload': Commands.SHUTDOWN },
    AssertionError('Not observing commands') ]
  fe_pipe, fe_pipe_out = Pipe()

  # Create service
  service = CommonService(
      commands=cmd_pipe, frontend=fe_pipe)
  # override class API to ensure overidden functions are called
  service.initializing = mock.Mock()
  service.in_shutdown = mock.Mock()

  # Check new status
  messages = []
  while fe_pipe_out.poll():
    message = fe_pipe_out.recv()
    if 'statuscode' in message:
      messages.append(message['statuscode'])
  assert messages == [ service.SERVICE_STATUS_NEW ]

  # Start service
  service.start()

  # Check startup/shutdown sequence
  service.initializing.assert_called_once()
  service.in_shutdown.assert_called_once()
  cmd_pipe.recv.assert_called_once_with()
  messages = []
  while fe_pipe_out.poll():
    message = fe_pipe_out.recv()
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
  cmd_pipe = mock.Mock()
  cmd_pipe.poll.side_effect = [ False, True ]
  cmd_pipe.recv.side_effect = [
    { 'band': 'command',
      'payload': Commands.SHUTDOWN },
    AssertionError('Not observing commands') ]
  fe_pipe, fe_pipe_out = Pipe()
  idle_trigger = mock.Mock()

  # Create service
  service = CommonService(commands=cmd_pipe, frontend=fe_pipe)
  service._register_idle(10, idle_trigger)

  # Start service
  service.start()

  # Check trigger has been called
  idle_trigger.assert_called_once_with()

  # Check startup/shutdown sequence
  cmd_pipe.recv.assert_called_once_with()
  assert cmd_pipe.poll.call_count == 2
  assert cmd_pipe.poll.call_args == ((10,),)
  messages = []
  while fe_pipe_out.poll():
    message = fe_pipe_out.recv()
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
  cmd_pipe = mock.Mock()
  cmd_pipe.poll.return_value = True
  cmd_pipe.recv.side_effect = [
    { 'band': mock.sentinel.band,
      'payload': mock.sentinel.payload },
    { 'band': 'command',
      'payload': Commands.SHUTDOWN },
    AssertionError('Not observing commands') ]
  fe_pipe, _ = Pipe()
  callback = mock.Mock()

  # Create service
  service = CommonService(commands=cmd_pipe, frontend=fe_pipe)
  service._register(mock.sentinel.band, callback)

  # Start service
  service.start()

  # Check callback occured
  callback.assert_called_with(mock.sentinel.payload)

def test_log_unknown_band_data():
  '''All unidentified messages should be logged to the frondend.'''
  cmd_pipe = mock.Mock()
  cmd_pipe.poll.return_value = True
  cmd_pipe.recv.side_effect = [
    { 'band': mock.sentinel.band, 'payload': mock.sentinel.failure1 },
    { 'payload': mock.sentinel.failure2 },
    { 'band': 'command',
      'payload': Commands.SHUTDOWN },
    AssertionError('Not observing commands') ]
  fe_pipe, fe_pipe_out = Pipe()

  # Create service
  service = CommonService(commands=cmd_pipe, frontend=fe_pipe)

  # Start service
  service.start()

  # Check startup/shutdown sequence
  messages = []
  while fe_pipe_out.poll():
    message = fe_pipe_out.recv()
    if message.get('band') == 'log':
      messages.append(message.get('payload'))
  assert len(messages) == 2
  assert messages[0]['source'] == 'service'
  assert str(messages[0]['log']['band']) == str(mock.sentinel.band)
  assert messages[1]['source'] == 'service'
  assert messages[1]['log'].get('band') == None

