from __future__ import absolute_import, division
import workflows.transport
from workflows.transport.stomp_transport import StompTransport
import json
import mock
import optparse
import os
import pytest

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

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_send_message(mockstomp):
  '''Test the message sending function.'''
  mockconn = mock.Mock()
  mockstomp.Connection.return_value = mockconn
  stomp = StompTransport()
  stomp.connect()

  stomp._send( str(mock.sentinel.channel), mock.sentinel.message )

  mockconn.send.assert_called_once()
  args, kwargs = mockconn.send.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel), mock.sentinel.message)
  assert kwargs.get('headers') in (None, {})

  stomp._send( str(mock.sentinel.channel), mock.sentinel.message, headers=mock.sentinel.headers )
  assert mockconn.send.call_count == 2
  args, kwargs = mockconn.send.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel), mock.sentinel.message)
  assert kwargs == { 'headers': mock.sentinel.headers }

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_subscribe_to_channel(mockstomp):
  '''Test subscribing to a channel and callback functions.'''
  mock_cb1 = mock.Mock()
  mock_cb2 = mock.Mock()
  mockconn = mock.Mock()
  mockstomp.Connection.return_value = mockconn
  stomp = StompTransport()
  stomp.connect()

  mockconn.set_listener.assert_called_once()
  listener = mockconn.set_listener.call_args[0][1]
  assert listener is not None

  stomp._subscribe(1, str(mock.sentinel.channel1), mock_cb1)

  mockconn.subscribe.assert_called_once()
  args, kwargs = mockconn.subscribe.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel1), 1)
  assert kwargs == { 'headers': {}, 'ack': 'auto' }

  stomp._subscribe(2, str(mock.sentinel.channel2), mock_cb2, retroactive=True)
  assert mockconn.subscribe.call_count == 2
  args, kwargs = mockconn.subscribe.call_args
  assert args == ('/queue/' + str(mock.sentinel.channel2), 2)
  assert kwargs == { 'headers': {'activemq.retroactive':'true'}, 'ack': 'auto' }

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

@pytest.mark.skip(reason="TODO")
def test_incoming_messages_are_registered_properly():
  pass

@mock.patch('workflows.transport.stomp_transport.stomp')
def test_transaction_calls(mockstomp):
  '''Test that calls to create, commit, abort transactions are passed to stomp properly.'''
  stomp = StompTransport()
  stomp.connect()
  mockconn = mockstomp.Connection.return_value

  stomp._transaction_begin(mock.sentinel.txid)
  mockconn.begin.assert_called_once_with(transaction=mock.sentinel.txid)

  stomp._send('destination', mock.sentinel.message, transaction=mock.sentinel.txid)
  mockconn.send.assert_called_once_with('/queue/destination', mock.sentinel.message, headers={}, transaction=mock.sentinel.txid)

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
