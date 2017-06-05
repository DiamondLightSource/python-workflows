from __future__ import absolute_import, division

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

  # Start service to initialize logging
  service.start()

  # Note that by default only warning and higher are passed to frontend
  service.log.warn(mock.sentinel.logmessage)

  fe_pipe.send.assert_called()
  assert fe_pipe.send.call_args == (({'band': 'log', 'payload': mock.ANY},), {})
  logrec = fe_pipe.send.call_args[0][0]['payload']
  assert logrec.levelname == 'WARNING'
  assert str(mock.sentinel.logmessage) in logrec.message

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
  assert messages[0].name == 'workflows.service'
  assert 'unregistered band' in messages[0].message
  assert str(mock.sentinel.band) in messages[0].message
  assert messages[1].name == 'workflows.service'
  assert 'without band' in messages[1].message

def test_service_initialization_crashes_are_handled_correctly():
  '''Log messages should be passed to frontend'''
  fe_pipe = mock.Mock()

  class CrashOnInitService(CommonService):
    '''Helper class to test exception handling.
       This service crashes on initialization.'''
    _service_name = "Crashservice 1"
    _logger_name = "workflows.service.crash_on_init"

    @staticmethod
    def initializing():
      '''Crash.'''
      assert False, 'This crash needs to be handled'

  service = CrashOnInitService(frontend=fe_pipe)
  service.start()

  fe_pipe.send.assert_called()

  # Service status should have been set to ERROR
  fe_pipe.send.assert_any_call({ 'band': 'status_update', 'statuscode': service.SERVICE_STATUS_ERROR})

  # Traceback should have been sent to log
  log_msgs = list(filter(lambda c: c[0][0] == { 'band': 'log', 'payload': mock.ANY } and c[0][0]['payload'].levelname == 'CRITICAL', \
                    fe_pipe.send.call_args_list))
  assert log_msgs, 'No critical log message received'
  log = log_msgs[0][0][0]['payload']
  assert 'This crash needs to be handled' in log.exc_text
  assert 'initializing' in log.exc_text
  assert 'test_common_service' in log.exc_text
  assert log.workflows_exc_filename == 'test_common_service.py'
  assert log.workflows_exc_funcName == 'initializing'

def test_service_can_change_name_and_shut_itself_down():
  '''Name changes should be passed to frontend'''
  fe_pipe = mock.Mock()

  class NameChangingService(CommonService):
    '''Helper class to test name changing.'''
    def initializing(self):
      '''Change name.'''
      self._set_name(mock.sentinel.newname)
      self._shutdown()
  service = NameChangingService(frontend=fe_pipe)
  service.start()

  # Check for service name update
  fe_pipe.send.assert_any_call({ 'band': 'set_name', 'name': mock.sentinel.newname })
  # Service should have shut down cleanly
  fe_pipe.send.assert_any_call({ 'band': 'status_update', 'statuscode': service.SERVICE_STATUS_END })

def test_can_pass_environment_to_service():
  '''Test that environment dictionaries can be passed to the service on construction and are available during runtime.'''
  fe_pipe = mock.Mock()

  class EnvironmentPassingService(CommonService):
    '''Helper class to test environment passing.'''
    _service_name = "Environmentservice"
    _logger_name = "workflows.service.environment"

    def get_environment(self):
      '''Make the service environment available to the caller.'''
      return self._environment

  # Initialization without enviroment should still result in an available dictionary
  service = EnvironmentPassingService(frontend=fe_pipe)
  assert service.get_environment().get("non-existing-key-for-environment-passing-test") is None

  # Check that passed environment is available
  sample_environment = { 'environment': mock.sentinel.environment }
  service = EnvironmentPassingService(frontend=fe_pipe, environment=sample_environment)
  assert service.get_environment().get('environment') == mock.sentinel.environment
