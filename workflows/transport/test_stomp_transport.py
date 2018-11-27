from __future__ import absolute_import, division, print_function

import json
import optparse
import os
import tempfile
from imp import reload

import mock
import pytest
import stomp as stomppy
import workflows
import workflows.transport
from workflows.transport.stomp_transport import StompTransport

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

  parser.add_option.assert_called()
  assert parser.add_option.call_count > 4
  for call in parser.add_option.call_args_list:
    assert call[1]['action'] == 'callback'

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_check_config_file_behaviour(mockstomp):
  '''Check that a specified configuration file is read, that command line
     parameters have precedence and are passed on to the stomp layer.'''
  mockconn = mock.Mock()
  mockstomp.Connection.return_value = mockconn
  parser = optparse.OptionParser()
  stomp = StompTransport()
  stomp.add_command_line_options(parser)

  # Temporarily create an example stomp configuration file
  cfgfile = tempfile.NamedTemporaryFile(delete=False)
  try:
    cfgfile.write('''
# An example stomp configuration file
# Only lines in the [stomp] block will be interpreted

[stomp]
#host = 127.0.0.1
port = 1234
username = someuser
password = somesecret
prefix = namespace
'''.encode('utf-8'))
    cfgfile.close()

    parser.parse_args([
      '--stomp-conf', cfgfile.name,
      '--stomp-user', mock.sentinel.user])

    # Command line parameters are shared for all instances
    stomp = StompTransport()
    stomp.connect()

    # Reset configuration for subsequent tests by reloading StompTransport
    reload(workflows.transport.stomp_transport)
    globals()['StompTransport'] = workflows.transport.stomp_transport.StompTransport

    mockstomp.Connection.assert_called_once_with([('localhost', 1234)])
    mockconn.connect.assert_called_once_with(mock.sentinel.user, 'somesecret', wait=False)
    assert stomp.get_namespace() == 'namespace'

  finally:
    os.remove(cfgfile.name)

  # Loading a non-existing configuration file
  with pytest.raises(workflows.Error):
    parser.parse_args(['--stomp-conf', ''])

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_anonymous_connection(mockstomp):
  '''Check that a specified configuration file is read, that command line
     parameters have precedence and are passed on to the stomp layer.'''
  mockconn = mock.Mock()
  mockstomp.Connection.return_value = mockconn
  parser = optparse.OptionParser()
  stomp = StompTransport()
  stomp.add_command_line_options(parser)

  parser.parse_args([ '--stomp-user=', '--stomp-pass=' ])

  # Command line parameters are shared for all instances
  stomp = StompTransport()
  stomp.connect()

  # Reset configuration for subsequent tests by reloading StompTransport
  reload(workflows.transport.stomp_transport)
  globals()['StompTransport'] = workflows.transport.stomp_transport.StompTransport

  mockconn.connect.assert_called_once_with(wait=False)

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_instantiate_link_and_connect_to_broker(mockstomp):
  '''Test the Stomp connection routine.'''
  stomp = StompTransport()
  mockconn = mockstomp.Connection.return_value

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

  mockstomp.Connection.assert_called_once()
  mockconn.start.assert_called_once()
  mockconn.connect.assert_called_once()
  mockconn.disconnect.assert_called_once()
  assert not stomp.is_connected()

  stomp.disconnect()

  mockstomp.Connection.assert_called_once()
  mockconn.start.assert_called_once()
  mockconn.connect.assert_called_once()
  mockconn.disconnect.assert_called_once()
  assert not stomp.is_connected()

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_error_handling_when_connecting_to_broker(mockstomp):
  '''Test the Stomp connection routine.'''
  stomp = StompTransport()
  mockconn = mockstomp.Connection.return_value
  mockconn.start.side_effect = stomppy.exception.ConnectFailedException()
  mockstomp.exception = stomppy.exception

  with pytest.raises(workflows.Disconnected):
    stomp.connect()

  assert not stomp.is_connected()

  mockconn.start.side_effect = None
  mockconn.connect.side_effect = stomppy.exception.ConnectFailedException()

  with pytest.raises(workflows.AuthenticationFailed):
    stomp.connect()

  assert not stomp.is_connected()

@mock.patch('workflows.transport.stomp_transport.time')
@mock.patch('workflows.transport.stomp_transport.stomp')
def test_broadcast_status(mockstomp, mocktime):
  '''Test the status broadcast function.'''
  mocktime.time.return_value = 20000
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  stomp.broadcast_status({ 'status': str(mock.sentinel.status) })

  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  # expiration should be 15 seconds in the future
  assert int(kwargs['headers']['expires']) == 1000 * (20000 + 15)
  destination, message = args
  assert destination.startswith('/topic/transient.status')
  statusdict = json.loads(message)
  assert statusdict['status'] == str(mock.sentinel.status)

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_send_message(mockstomp):
  '''Test the message sending function.'''
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  stomp._send( str(mock.sentinel.channel), mock.sentinel.message )

  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel), mock.sentinel.message)
  assert kwargs.get('headers') == { 'persistent': 'true' }

  stomp._send( str(mock.sentinel.channel), mock.sentinel.message,
               headers={ 'hdr': mock.sentinel.header }, delay=123 )
  assert mockconn.send.call_count == 2
  args, kwargs = mockconn.send.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel), mock.sentinel.message)
  assert kwargs == { 'headers': { 'hdr': mock.sentinel.header,
                                  'persistent': 'true',
                                  'AMQ_SCHEDULED_DELAY': 123000 } }

@mock.patch('workflows.transport.stomp_transport.stomp')
@mock.patch('workflows.transport.stomp_transport.time')
def test_sending_message_with_expiration(time, mockstomp):
  '''Test sending a message that expires some time in the future.'''
  system_time = 1234567.1234567
  message_lifetime = 120
  expiration_time = int((system_time + message_lifetime) * 1000)
  time.time.return_value = system_time

  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  stomp._send( str(mock.sentinel.channel), mock.sentinel.message,
               expiration=120 )

  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel), mock.sentinel.message)
  assert kwargs.get('headers') == { 'persistent': 'true', 'expires': expiration_time }

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_error_handling_on_send(mockstomp):
  '''Unrecoverable errors during sending should mark the connection as disconnected.'''
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value
  mockconn.send.side_effect = stomppy.exception.NotConnectedException()
  mockstomp.exception = stomppy.exception

  with pytest.raises(workflows.Disconnected):
    stomp._send( str(mock.sentinel.channel), mock.sentinel.message )
  assert not stomp.is_connected()

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_send_broadcast(mockstomp):
  '''Test the broadcast sending function.'''
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  stomp._broadcast( str(mock.sentinel.channel), mock.sentinel.message )

  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  assert args == ('/topic/' + str(mock.sentinel.channel), mock.sentinel.message)
  assert kwargs.get('headers') in (None, {})

  stomp._broadcast( str(mock.sentinel.channel), mock.sentinel.message, headers=mock.sentinel.headers )
  assert mockconn.send.call_count == 2
  args, kwargs = mockconn.send.call_args
  assert args == ('/topic/' + str(mock.sentinel.channel), mock.sentinel.message)
  assert kwargs == { 'headers': mock.sentinel.headers }

  stomp._broadcast( str(mock.sentinel.channel), mock.sentinel.message, delay=123 )
  assert mockconn.send.call_count == 3
  args, kwargs = mockconn.send.call_args
  assert args == ('/topic/' + str(mock.sentinel.channel), mock.sentinel.message)
  assert kwargs['headers'].get('AMQ_SCHEDULED_DELAY') == 123000


@mock.patch('workflows.transport.stomp_transport.stomp')
@mock.patch('workflows.transport.stomp_transport.time')
def test_broadcasting_message_with_expiration(time, mockstomp):
  '''Test sending a message that expires some time in the future.'''
  system_time = 1234567.1234567
  message_lifetime = 120
  expiration_time = int((system_time + message_lifetime) * 1000)
  time.time.return_value = system_time

  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  stomp._broadcast( str(mock.sentinel.channel), mock.sentinel.message,
                    expiration=120 )

  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  assert args == ('/topic/' + str(mock.sentinel.channel), mock.sentinel.message)
  assert kwargs.get('headers') == { 'expires': expiration_time }


@mock.patch('workflows.transport.stomp_transport.stomp')
def test_error_handling_on_broadcast(mockstomp):
  '''Unrecoverable errors during broadcasting should mark the connection as disconnected.'''
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value
  mockconn.send.side_effect = stomppy.exception.NotConnectedException()
  mockstomp.exception = stomppy.exception

  with pytest.raises(stomppy.exception.NotConnectedException):
    stomp._broadcast( str(mock.sentinel.channel), mock.sentinel.message )
  assert not stomp.is_connected()

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_messages_are_serialized_for_transport(mockstomp):
  '''Test the message serialization.'''
  banana = { 'entry': [ 0, 'banana' ] }
  banana_str = '{"entry": [0, "banana"]}'
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  stomp.send(str(mock.sentinel.channel1), banana)
  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel1), banana_str)

  stomp.broadcast(str(mock.sentinel.channel2), banana)
  args, kwargs = mockconn.send.call_args
  assert args == ('/topic/' + str(mock.sentinel.channel2), banana_str)

  with pytest.raises(Exception):
    stomp.send(str(mock.sentinel.channel), mock.sentinel.unserializable)

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_messages_are_not_serialized_for_raw_transport(mockstomp):
  '''Test the raw sending methods.'''
  banana = '{"entry": [0, "banana"]}'
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  stomp.raw_send(str(mock.sentinel.channel1), banana)
  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel1), banana)

  mockconn.send.reset_mock()
  stomp.raw_broadcast(str(mock.sentinel.channel2), banana)
  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  assert args == ('/topic/' + str(mock.sentinel.channel2), banana)

  mockconn.send.reset_mock()
  stomp.raw_send(str(mock.sentinel.channel), mock.sentinel.unserializable)
  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel), mock.sentinel.unserializable)

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_messages_are_deserialized_after_transport(mockstomp):
  '''Test the message serialization.'''
  banana = { 'entry': [ 0, 'banana' ] }
  banana_str = '{"entry": [0, "banana"]}'
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value
  message_handler = mockconn.set_listener.call_args[0][1].on_message

  # Test subscriptions
  callback = mock.Mock()
  stomp.subscribe('channel', callback)
  subscription_id = mockconn.subscribe.call_args[0][1]
  message_handler({'subscription': subscription_id}, banana_str)
  callback.assert_called_once_with({'subscription': subscription_id}, banana)

  message_handler({'subscription': subscription_id}, mock.sentinel.undeserializable)
  callback.assert_called_with({'subscription': subscription_id}, mock.sentinel.undeserializable)

  # Test broadcast subscriptions
  callback = mock.Mock()
  stomp.subscribe_broadcast('channel', callback)
  subscription_id = mockconn.subscribe.call_args[0][1]
  message_handler({'subscription': subscription_id}, banana_str)
  callback.assert_called_once_with({'subscription': subscription_id}, banana)

  message_handler({'subscription': subscription_id}, mock.sentinel.undeserializable)
  callback.assert_called_with({'subscription': subscription_id}, mock.sentinel.undeserializable)

  # Test subscriptions with mangling disabled
  callback = mock.Mock()
  stomp.subscribe('channel', callback, disable_mangling=True)
  subscription_id = mockconn.subscribe.call_args[0][1]
  message_handler({'subscription': subscription_id}, banana_str)
  callback.assert_called_once_with({'subscription': subscription_id}, banana_str)

  # Test broadcast subscriptions with mangling disabled
  callback = mock.Mock()
  stomp.subscribe_broadcast('channel', callback, disable_mangling=True)
  subscription_id = mockconn.subscribe.call_args[0][1]
  message_handler({'subscription': subscription_id}, banana_str)
  callback.assert_called_once_with({'subscription': subscription_id}, banana_str)

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_subscribe_to_queue(mockstomp):
  '''Test subscribing to a queue (producer-consumer), callback functions and unsubscribe.'''
  mock_cb1 = mock.Mock()
  mock_cb2 = mock.Mock()
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  def callback_resolver(cbid):
    if cbid == 1: return mock_cb1
    if cbid == 2: return mock_cb2
    raise ValueError('Unknown subscription ID %r' % cbid)
  stomp.subscription_callback = callback_resolver

  mockconn.set_listener.assert_called_once()
  listener = mockconn.set_listener.call_args[0][1]
  assert listener is not None

  stomp._subscribe(1, str(mock.sentinel.channel1), mock_cb1, transformation=mock.sentinel.transformation)

  mockconn.subscribe.assert_called_once()
  args, kwargs = mockconn.subscribe.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel1), 1)
  assert kwargs == { 'headers': {'transformation': mock.sentinel.transformation}, 'ack': 'auto' }

  stomp._subscribe(2, str(mock.sentinel.channel2), mock_cb2, retroactive=True, selector=mock.sentinel.selector, exclusive=True, transformation=True, priority=42)
  assert mockconn.subscribe.call_count == 2
  args, kwargs = mockconn.subscribe.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel2), 2)
  assert kwargs == { 'headers': {'activemq.retroactive':'true', 'selector':mock.sentinel.selector, 'activemq.exclusive':'true', 'transformation': 'jms-object-json', 'activemq.priority':42}, 'ack': 'auto' }

  assert mock_cb1.call_count == 0
  listener.on_message({'subscription': 1}, mock.sentinel.message1)
  mock_cb1.assert_called_once_with({'subscription': 1}, mock.sentinel.message1)

  assert mock_cb2.call_count == 0
  listener.on_message({'subscription': 2}, mock.sentinel.message2)
  mock_cb2.assert_called_once_with({'subscription': 2}, mock.sentinel.message2)

  stomp._subscribe(3, str(mock.sentinel.channel3), mock_cb2, acknowledgement=True)
  assert mockconn.subscribe.call_count == 3
  args, kwargs = mockconn.subscribe.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel3), 3)
  assert kwargs == { 'headers': {}, 'ack': 'client-individual' }

  stomp._unsubscribe(1)
  mockconn.unsubscribe.assert_called_once_with(id=1)
  stomp._unsubscribe(2)
  mockconn.unsubscribe.assert_called_with(id=2)

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_subscribe_to_broadcast(mockstomp):
  '''Test subscribing to a topic (publish-subscribe) and callback functions.'''
  mock_cb1 = mock.Mock()
  mock_cb2 = mock.Mock()
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  def callback_resolver(cbid):
    if cbid == 1: return mock_cb1
    if cbid == 2: return mock_cb2
    raise ValueError('Unknown subscription ID %r' % cbid)
  stomp.subscription_callback = callback_resolver

  mockconn.set_listener.assert_called_once()
  listener = mockconn.set_listener.call_args[0][1]
  assert listener is not None

  stomp._subscribe_broadcast(1, str(mock.sentinel.channel1), mock_cb1, transformation=mock.sentinel.transformation)

  mockconn.subscribe.assert_called_once()
  args, kwargs = mockconn.subscribe.call_args
  assert args == ('/topic/' + str(mock.sentinel.channel1), 1)
  assert kwargs == { 'headers': {'transformation': mock.sentinel.transformation} }

  stomp._subscribe_broadcast(2, str(mock.sentinel.channel2), mock_cb2, retroactive=True, transformation=True)
  assert mockconn.subscribe.call_count == 2
  args, kwargs = mockconn.subscribe.call_args
  assert args == ('/topic/' + str(mock.sentinel.channel2), 2)
  assert kwargs == { 'headers': {'activemq.retroactive':'true', 'transformation': 'jms-object-json'} }

  assert mock_cb1.call_count == 0
  listener.on_message({'subscription': 1}, mock.sentinel.message1)
  mock_cb1.assert_called_once_with({'subscription': 1}, mock.sentinel.message1)

  assert mock_cb2.call_count == 0
  listener.on_message({'subscription': 2}, mock.sentinel.message2)
  mock_cb2.assert_called_once_with({'subscription': 2}, mock.sentinel.message2)

  stomp._unsubscribe(1)
  mockconn.unsubscribe.assert_called_once_with(id=1)
  stomp._unsubscribe(2)
  mockconn.unsubscribe.assert_called_with(id=2)

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_transaction_calls(mockstomp):
  '''Test that calls to create, commit, abort transactions are passed to stomp properly.'''
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  stomp._transaction_begin(mock.sentinel.txid)
  mockconn.begin.assert_called_once_with(transaction=mock.sentinel.txid)

  stomp._send('destination', mock.sentinel.message, transaction=mock.sentinel.txid)
  mockconn.send.assert_called_once_with('/queue/destination', mock.sentinel.message, headers={'persistent': 'true'}, transaction=mock.sentinel.txid)

  stomp._transaction_abort(mock.sentinel.txid)
  mockconn.abort.assert_called_once_with(mock.sentinel.txid)

  stomp._transaction_commit(mock.sentinel.txid)
  mockconn.commit.assert_called_once_with(mock.sentinel.txid)

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_ack_message(mockstomp):
  '''Test that the _ack function is properly forwarded to stomp.'''
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  subid = stomp._subscribe(1, str(mock.sentinel.channel3), None, acknowledgement=True)
  stomp._ack(mock.sentinel.messageid, subid)
  mockconn.ack.assert_called_once_with(mock.sentinel.messageid, subid)

  stomp._ack(mock.sentinel.messageid, subid, transaction=mock.sentinel.txn)
  mockconn.ack.assert_called_with(mock.sentinel.messageid, subid,
                                  transaction=mock.sentinel.txn)

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_nack_message(mockstomp):
  '''Test that the _nack function is properly forwarded to stomp.'''
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  subid = stomp._subscribe(1, str(mock.sentinel.channel3), None, acknowledgement=True)
  stomp._nack(mock.sentinel.messageid, subid)
  mockconn.nack.assert_called_once_with(mock.sentinel.messageid, subid)

  stomp._nack(mock.sentinel.messageid, subid, transaction=mock.sentinel.txn)
  mockconn.nack.assert_called_with(mock.sentinel.messageid, subid,
                                   transaction=mock.sentinel.txn)

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_namespace_is_used_correctly(mockstomp):
  '''Test that a configured namespace is correctly used when subscribing and sending messages.'''
  mockconn = mockstomp.Connection.return_value
  StompTransport.defaults['--stomp-prfx'] = ''
  stomp = StompTransport()
  stomp.connect()
  assert stomp.get_namespace() == ''

  StompTransport.defaults['--stomp-prfx'] = 'ns.'
  stomp = StompTransport()
  stomp.connect()
  assert stomp.get_namespace() == 'ns'

  stomp._send( 'some_queue', mock.sentinel.message1 )
  mockconn.send.assert_called_once()
  assert mockconn.send.call_args[0] == ('/queue/ns.some_queue', mock.sentinel.message1)

  stomp._send( 'some_queue', mock.sentinel.message2, ignore_namespace=True )
  assert mockconn.send.call_args[0] == ('/queue/some_queue', mock.sentinel.message2)

  StompTransport.defaults['--stomp-prfx'] = 'ns'
  stomp = StompTransport()
  stomp.connect()
  assert stomp.get_namespace() == 'ns'

  stomp._send( 'some_queue', mock.sentinel.message1 )
  assert mockconn.send.call_args[0] == ('/queue/ns.some_queue', mock.sentinel.message1)

  stomp._broadcast( 'some_topic', mock.sentinel.message2 )
  assert mockconn.send.call_args[0] == ('/topic/ns.some_topic', mock.sentinel.message2)

  stomp._broadcast( 'some_topic', mock.sentinel.message3, ignore_namespace=True )
  assert mockconn.send.call_args[0] == ('/topic/some_topic', mock.sentinel.message3)

  stomp._subscribe( 1, 'sub_queue', None )
  mockconn.subscribe.assert_called_once()
  assert mockconn.subscribe.call_args[0] == ('/queue/ns.sub_queue', 1)

  stomp._subscribe( 2, 'sub_queue', None, ignore_namespace=True )
  assert mockconn.subscribe.call_args[0] == ('/queue/sub_queue', 2)

  stomp._subscribe_broadcast( 3, 'sub_topic', None )
  assert mockconn.subscribe.call_args[0] == ('/topic/ns.sub_topic', 3)

  stomp._subscribe_broadcast( 4, 'sub_topic', None, ignore_namespace=True )
  assert mockconn.subscribe.call_args[0] == ('/topic/sub_topic', 4)

  stomp.broadcast_status('some status')
  assert mockconn.send.call_args[0] == ('/topic/ns.transient.status', '"some status"')
