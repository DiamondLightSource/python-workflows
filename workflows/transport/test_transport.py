from workflows.transport import CommonTransport
import mock
import pytest
import workflows

def test_subscribe_unsubscribe_a_channel():
  """Public subscribe()-call should be routed to specific _subscribe().
     Unsubscribes should be routed to _unsubscribe() and handled properly."""
  ct = CommonTransport()
  ct._subscribe = mock.Mock()
  ct._unsubscribe = mock.Mock()

  subid = ct.subscribe(mock.sentinel.channel, mock.sentinel.callback,
        exclusive=mock.sentinel.exclusive, acknowledgement=mock.sentinel.ack)

  assert subid
  ct._subscribe.assert_called_once_with(subid, mock.sentinel.channel,
        mock.sentinel.callback, mock.sentinel.exclusive, mock.sentinel.ack)

  ct.unsubscribe(subid)

  ct._unsubscribe.assert_called_once_with(subid)

def test_simple_subscribe_unsubscribe_a_broadcast():
  """Public subscribe_bc()-call should be routed to specific _subscribe_bc().
     Unsubscribes should be routed to _unsubscribe() and handled properly."""
  ct = CommonTransport()
  ct._subscribe_broadcast = mock.Mock()
  ct._unsubscribe = mock.Mock()

  subid = ct.subscribe_broadcast(mock.sentinel.channel, mock.sentinel.callback,
        retroactive=mock.sentinel.retro)

  assert subid
  ct._subscribe_broadcast.assert_called_once_with(subid, mock.sentinel.channel,
        mock.sentinel.callback, mock.sentinel.retro)

  ct.unsubscribe(subid)

  ct._unsubscribe.assert_called_once_with(subid)

def test_simple_send_message():
  """Pass string and object messages to send(), should be serialized and
     routed to specific _send()"""
  ct = CommonTransport()
  ct._send = mock.Mock()

  ct.send(mock.sentinel.destination, str(mock.sentinel.message),
        headers=mock.sentinel.headers, expiration=mock.sentinel.expiration,
        transaction=mock.sentinel.transaction)

  ct._send.assert_called_once_with(mock.sentinel.destination,
        str(mock.sentinel.message), mock.sentinel.headers,
        mock.sentinel.expiration,
        mock.sentinel.transaction)

  ct.send(mock.sentinel.destination, { 'entry': [ 0, 'banana' ] })

  ct._send.assert_called_with(mock.sentinel.destination,
        '{"entry": [0, "banana"]}',
        None, None, None)

def test_simple_broadcast_message():
  """Pass string and object messages to broadcast(), should be serialized and
     routed to specific _broadcast()"""

  ct = CommonTransport()
  ct._broadcast = mock.Mock()

  ct.broadcast(mock.sentinel.destination, str(mock.sentinel.message),
        headers=mock.sentinel.headers, expiration=mock.sentinel.expiration,
        transaction=mock.sentinel.transaction)

  ct._broadcast.assert_called_once_with(mock.sentinel.destination,
        str(mock.sentinel.message), mock.sentinel.headers,
        mock.sentinel.expiration,
        mock.sentinel.transaction)

  ct.broadcast(mock.sentinel.destination, { 'entry': [ 0, 'banana' ] })

  ct._broadcast.assert_called_with(mock.sentinel.destination,
        '{"entry": [0, "banana"]}',
        None, None, None)

def test_register_and_drop_clients():
  "Register clients, should get unique IDs, unregister clients."
  ct = CommonTransport()

  client = ct.register_client()
  assert client

  client2 = ct.register_client()
  assert client2
  assert client2 != client

  ct.drop_client(client)
  ct.drop_client(client2)

def test_dropping_an_unregistered_client_should_fail():
  "Get error when unregistering unregistered client."
  ct = CommonTransport()

  with pytest.raises(workflows.WorkflowsError):
    ct.drop_client(mock.sentinel.unregistered_client)

def test_dropping_subscriptions_when_dropping_client():
  "Subscriptions associated with a client should be cancelled when client is dropped."
  ct = CommonTransport()
  ct._subscribe = mock.Mock()
  ct._unsubscribe = mock.Mock()

  client = ct.register_client()
  subid = ct.subscribe(mock.sentinel.channel, mock.sentinel.callback, client_id=client)
  ct.drop_client(client)

  ct._unsubscribe.assert_called_once_with(subid)

