from __future__ import absolute_import, division
from workflows.services.common_service import CommonService
import mock
import multiprocessing
import pytest
import workflows.frontend

@mock.patch('workflows.frontend.multiprocessing')
@mock.patch('workflows.frontend.workflows.status.StatusAdvertise')
@mock.patch('workflows.frontend.workflows.transport')
def test_frontend_connects_to_transport_layer(mock_transport, mock_status, mock_mp):
  '''Frontend should call connect method on transport layer module and subscribe to a unique command queue.'''
  workflows.frontend.Frontend()
  mock_transport.lookup.assert_called_once_with(None)
  mock_transport.lookup.return_value.assert_called_once_with()
  mock_transport.lookup.return_value.return_value.connect.assert_called_once_with()

  workflows.frontend.Frontend(transport="some_transport")
  mock_transport.lookup.assert_called_with("some_transport")

  mock_trn = mock.Mock()
  workflows.frontend.Frontend(mock_trn)
  mock_trn.assert_called_once_with()
  mock_trn.return_value.connect.assert_called_once_with()

@mock.patch('workflows.frontend.workflows.status.StatusAdvertise')
@mock.patch('workflows.frontend.workflows.transport')
def test_frontend_subscribes_to_command_channel(mock_transport, mock_status):
  '''Frontend should call connect method on transport layer module and subscribe to a unique command queue.'''
  transport = mock_transport.lookup.return_value.return_value
  fe = workflows.frontend.Frontend()
  mock_transport.lookup.assert_called_once_with(None)
  transport.connect.assert_called_once()
  transport.subscribe.assert_not_called()

  fe = workflows.frontend.Frontend(transport_command_prefix='sentinel.')
  transport.subscribe.assert_called_once()
  assert transport.subscribe.call_args == (('sentinel.' + fe.get_host_id(), mock.ANY), {})
  callbackfn = transport.subscribe.call_args[0][1]

  assert fe.shutdown == False
  callbackfn({}, { 'command': 'shutdown' })
  assert fe.shutdown == True

@mock.patch('workflows.frontend.multiprocessing')
@mock.patch('workflows.frontend.workflows.status.StatusAdvertise')
@mock.patch('workflows.frontend.workflows.transport')
def test_start_service_in_frontend(mock_transport, mock_status, mock_mp):
  '''Check that the service is being run and connected to the frontend properly via the correct pipes,
     and the status advertiser is being started in the background.'''
  mock_service = mock.Mock()
  mock_mp.Pipe.side_effect = [
      (mock.sentinel.pipe1, mock.sentinel.pipe2),
      (mock.sentinel.pipe3, mock.sentinel.pipe4),
      None ]

  # initialize frontend
  fe = workflows.frontend.Frontend()

  # check status information is being broadcast
  mock_status.assert_called_once()
  mock_status.return_value.start.assert_called_once()

  # start service
  fe.switch_service(mock_service)

  # check service was started properly
  mock_service.assert_called_once_with(commands=mock.sentinel.pipe2, frontend=mock.sentinel.pipe3)
  mock_mp.Process.assert_called_once_with(target=mock_service.return_value.start, args=())
  mock_mp.Process.return_value.start.assert_called_once()

@mock.patch('workflows.frontend.workflows.status.StatusAdvertise')
@mock.patch('workflows.frontend.workflows.transport')
def test_get_frontend_status(mock_transport, mock_status):
  '''Check that the get_status-method works and contains the correct host-id.'''
  fe = workflows.frontend.Frontend()
  status = fe.get_status()
  assert status['host'] == fe.get_host_id()

@mock.patch('workflows.frontend.workflows.status.StatusAdvertise')
def test_connect_queue_communication_to_transport_layer(mock_status):
  '''Check that communication messages coming in from the service layer via the Queue are
     passed correctly to the transport layer and vice versa in the other direction.'''
  transport = mock.Mock()
  commqueue = mock.Mock()

  fe = workflows.frontend.Frontend(transport=transport)
  setattr(fe, '_pipe_commands', commqueue)

  transport.assert_called_once()
  transport = transport.return_value
  transport.connect.assert_called_once()

  fe.parse_band_transport( {
      'call': 'send',
      'payload': (
        ( mock.sentinel.channel, mock.sentinel.message ),
        {}
      )
    } )
  transport.send.assert_called_once_with(mock.sentinel.channel,
                                         mock.sentinel.message)

  fe.parse_band_transport( {
      'call': 'subscribe',
      'payload': (
        ( mock.sentinel.subid, mock.sentinel.channel, mock.sentinel.something ),
        { 'kwarg': mock.sentinel.kwarg }
      )
    } )
  transport.subscribe.assert_called_once_with(
    mock.sentinel.channel,
    mock.ANY,
    mock.sentinel.something, kwarg=mock.sentinel.kwarg
    )
  callback_function = transport.subscribe.call_args[0][1]

  callback_function(mock.sentinel.header, mock.sentinel.message)
  commqueue.send.assert_called_once_with( {
      'band': 'transport_message',
      'payload': {
          'subscription_id': mock.sentinel.subid,
          'header': mock.sentinel.header,
          'message': mock.sentinel.message,
        }
    } )

@mock.patch('workflows.frontend.workflows.status.StatusAdvertise')
@pytest.mark.timeout(3)
def test_frontend_can_handle_unhandled_service_initialization_exceptions(mock_status):
  '''When a service crashes on initialization an exception should be thrown.'''
  transport = mock.Mock()

  class CrashServiceNicelyOnInit(CommonService):
    '''A service that crashes python 2.7 with a segmentation fault.'''
    def initializing(self):
      '''Raise AssertionError.
         This should set the error state, kill the service and cause the frontend
         to leave its main loop.'''
      assert False # pragma: no cover
#   self._register_idle(1, self.kill_service)

  fe = workflows.frontend.Frontend(transport=transport, service=CrashServiceNicelyOnInit)
  transport = transport.return_value
  transport.connect.assert_called_once()

  with pytest.raises(workflows.WorkflowsError):
    fe.run()

@mock.patch('workflows.frontend.workflows.status.StatusAdvertise')
@mock.patch('workflows.frontend.multiprocessing')
def test_frontend_can_handle_service_initialization_segfaults(mock_mp, mock_status):
  '''When a service crashes on initialization an exception should be thrown.'''
  transport = mock.Mock()
  service = mock.Mock()
  service_process = mock.Mock()
  dummy_pipe = mock.Mock()
  dummy_pipe.poll.return_value = False
  mock_mp.Pipe.return_value = (dummy_pipe, dummy_pipe)
  mock_mp.Process.return_value = service_process
  service_process.is_alive.return_value = False # Dead on arrival

  fe = workflows.frontend.Frontend(transport=transport, service=service)
  transport = transport.return_value
  transport.connect.assert_called_once()
  mock_mp.Process.assert_called_once_with(target=service.return_value.start, args=())

  with pytest.raises(workflows.WorkflowsError):
    fe.run()
  service_process.start.assert_called()
  service_process.is_alive.assert_called()
  service_process.join.assert_called()

@mock.patch('workflows.frontend.workflows.status.StatusAdvertise')
@mock.patch('workflows.frontend.multiprocessing')
def test_frontend_terminates_on_transport_disconnection(mock_mp, mock_status):
  '''When the transport connection is lost permanently, the frontend should stop.'''
  transport = mock.Mock()
  service = mock.Mock()
  service_process = mock.Mock()
  dummy_pipe = mock.Mock()
  dummy_pipe.poll.side_effect = [ False, Exception('Frontend did not terminate on transport disconnect') ]
  mock_mp.Pipe.return_value = (dummy_pipe, dummy_pipe)
  mock_mp.Process.return_value = service_process
  service_process.is_alive.return_value = True

  fe = workflows.frontend.Frontend(transport=transport, service=service)
  transport = transport.return_value
  transport.connect.assert_called_once()
  transport.is_connected.return_value = False
  mock_mp.Process.assert_called_once_with(target=service.return_value.start, args=())

  with pytest.raises(workflows.WorkflowsError):
    fe.run()

  service_process.terminate.assert_called()
  service_process.join.assert_called()

class mock_pipe(object):
  '''A testing object that behaves like a pipe.'''
  def __init__(self, contents, on_empty=None):
    '''Load up contents into pipe. Set up an optional callback function.'''
    self.contents = contents
    self.on_empty = on_empty

  def poll(self, time=None):
    '''Check if pipe is empty.'''
    if self.contents:
      return True
    return False

  def recv(self):
    '''Return first item off the list or raise exception.
       Call callback function if defined and pipe is emptied.'''
    if not self.contents:
      raise EOFError('Pipe is empty')
    if len(self.contents) == 1 and self.on_empty:
      self.on_empty()
    return self.contents.pop(0)

  def close(self):
    '''This pipe can't be written to anyway. Ignore call.'''

  def assert_empty(self):
    '''Pipe must have been read out completely.'''
    assert not self.contents

@mock.patch('workflows.frontend.workflows.status.StatusAdvertise')
@mock.patch('workflows.frontend.multiprocessing')
def test_frontend_parses_status_updates(mock_mp, mock_status):
  '''The frontend should forward status updates to the advertiser thread when appropriate.'''
  transport = mock.Mock()
  service_process = mock.Mock()
  service_process.is_alive.return_value = True
  def end_service_process():
    service_process.is_alive.return_value = False
  mock_mp.Process.return_value = service_process
  status_thread = mock_status.return_value

  dummy_pipe = mock_pipe([{'band': 'status_update', 'statuscode': 1},
                          {'band': 'status_update', 'statuscode': 2},
                          {'band': 'status_update', 'statuscode': 3, 'trigger_update': False}],
                         on_empty=end_service_process)
  mock_mp.Pipe.return_value = (dummy_pipe, dummy_pipe)

  fe = workflows.frontend.Frontend(transport=transport, service=mock.Mock())
  mock_status.assert_called_once()
  status_callback = mock_status.call_args[1]['status_callback']
  # Record status on trigger call
  trigger_status = []
  mock_status.return_value.trigger = lambda: trigger_status.append(status_callback())

  fe.run()

  dummy_pipe.assert_empty()
  # Status 0 is not reported via trigger(), status 3 must not be reported via trigger()
  # -2 status via trigger() expected due to shutdown.
  assert list(s['status'] for s in trigger_status) == [1, 2, -2]
