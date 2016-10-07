from __future__ import division
import workflows.transport
from workflows.transport.stomp_transport import StompTransport
import json
import mock
import optparse
import os

def test_lookup_and_initialize_stomp_transport_layer():
  '''Find the stomp transport layer via the lookup mechanism and run
     its constructor with default settings.'''
  stomp = workflows.transport.lookup("StompTransport")
  assert stomp == StompTransport
  stomp()

def test_add_command_line_help():
  '''Check that command line parameters are registered in the parser.'''
  parser = mock.MagicMock()

  StompTransport().add_command_line_options(parser)

  assert parser.add_option.called
  assert parser.add_option.call_count > 4
  for call in parser.add_option.call_args_list:
    assert call[1]['action'] == 'callback'

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_check_config_file_behaviour(mockstomp):
  '''Check that a specified configuration file is read, that command line
     parameters have precedence and are passed on to the stomp layer.'''
  cfgfile = os.path.join(os.path.dirname(os.path.realpath(__file__)), \
                         'test_stomp.cfg')
  mockconn = mock.Mock()
  mockstomp.Connection.return_value = mockconn
  parser = optparse.OptionParser()
  stomp = StompTransport()
  stomp.add_command_line_options(parser)

  parser.parse_args([
    '--stomp-conf', cfgfile,
    '--stomp-user', mock.sentinel.user])

  stomp.connect()

  mockstomp.Connection.assert_called_once_with([('localhost', 1234)])
  mockconn.connect.assert_called_once_with(mock.sentinel.user, 'somesecret', wait=True)
  assert stomp.get_namespace() == 'namespace'

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_instantiate_link_and_connect_to_broker(mockstomp):
  '''Test the Stomp connection routine.'''
  mockconn = mock.Mock()
  mockstomp.Connection.return_value = mockconn

  stomp = StompTransport()
  assert not stomp.is_connected()

  stomp.connect()

  mockstomp.Connection.assert_called_once()
  mockconn.start.assert_called_once()
  mockconn.connect.assert_called_once()
  assert stomp.is_connected()

  stomp.connect()

  mockstomp.Connection.assert_called_once()
  mockconn.start.assert_called_once()
  mockconn.connect.assert_called_once()
  assert stomp.is_connected()

  stomp.disconnect()

  # TODO

@mock.patch('workflows.transport.stomp_transport.time')
@mock.patch('workflows.transport.stomp_transport.stomp')
def test_broadcast_status(mockstomp, mocktime):
  '''Test the status broadcast function.'''
  mockconn = mock.Mock()
  mockstomp.Connection.return_value = mockconn
  mocktime.time.return_value = 20000
  stomp = StompTransport()
  stomp.connect()

  stomp.broadcast_status({ 'status': str(mock.sentinel.status) })

  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  # expiration should be 90 seconds in the future
  assert int(kwargs['headers']['expires']) == 1000 * (20000 + 90)
  assert kwargs['destination'].startswith('/topic/transient.status')
  statusdict = json.loads(kwargs['body'])
  assert statusdict['status'] == str(mock.sentinel.status)
  assert 'stomp.cmdchan' in statusdict

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_send_message(mockstomp):
  '''Test the message sending function.'''
  mockconn = mock.Mock()
  mockstomp.Connection.return_value = mockconn
  stomp = StompTransport()
  stomp.connect()

  stomp.send_message( mock.sentinel.message, channel=str(mock.sentinel.channel) )

  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  assert kwargs['destination'] == str(mock.sentinel.channel)
  assert kwargs['body'] == mock.sentinel.message

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_subscribe_to_channel(mockstomp):
  '''Test subscribing to a channel.'''
  mock_cb1 = mock.Mock()
  mock_cb2 = mock.Mock()
  mockconn = mock.Mock()
  mockstomp.Connection.return_value = mockconn
  stomp = StompTransport()
  stomp.connect()

  mockconn.set_listener.assert_called_once()
  listener = mockconn.set_listener.call_args[0][1]
  assert listener is not None

  stomp.subscribe(mock.sentinel.channel1, mock_cb1, client_id='A')

  mockconn.subscribe.assert_called_once()
  args, kwargs = mockconn.subscribe.call_args
  assert args, kwargs == ((mock.sentinel.channel1, mock.ANY), { 'headers': mock.ANY })
  headers1 = kwargs['headers']
  subscription_id1 = args[1]
  assert headers1 == { }

  stomp.subscribe(mock.sentinel.channel2, mock_cb2, client_id='B', retroactive=True)

  assert mockconn.subscribe.call_count == 2
  args, kwargs = mockconn.subscribe.call_args
  assert args, kwargs == ((mock.sentinel.channel2, mock.ANY), { 'headers': mock.ANY })
  headers2 = kwargs['headers']
  subscription_id2 = args[1]
  assert subscription_id1 != subscription_id2
  assert headers2 == { 'activemq.retroactive':'true' }

  assert mock_cb1.call_count == 0
  listener.on_message({'subscription': subscription_id1}, '{"some": "message" }')
  mock_cb1.assert_called_once_with({'subscription': subscription_id1}, { 'some': 'message' })

  assert mock_cb2.call_count == 0
  listener.on_message({'subscription': subscription_id2}, '{"other": ["mess", "age"] }')
  mock_cb2.assert_called_once_with({'subscription': subscription_id2}, { 'other': ['mess', 'age'] })
