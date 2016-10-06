from workflows.transport import CommonTransport
import mock
import pytest
import workflows

def test_simple_subscribe_to_a_channel():
  ct = CommonTransport()
  ct._subscribe = mock.Mock()

  ct.subscribe(mock.sentinel.channel, mock.sentinel.callback,
        exclusive=mock.sentinel.exclusive, acknowledgement=mock.sentinel.ack)

  ct._subscribe.assert_called_once_with(mock.sentinel.channel,
        mock.sentinel.callback, mock.sentinel.exclusive, mock.sentinel.ack)

def test_simple_unsubscribe_from_a_channel():
  ct = CommonTransport()
  ct._unsubscribe = mock.Mock()

  ct.unsubscribe(mock.sentinel.channel)

  ct._unsubscribe.assert_called_once_with(mock.sentinel.channel)

def test_simple_subscribe_to_a_broadcast():
  ct = CommonTransport()
  ct._subscribe_broadcast = mock.Mock()

  ct.subscribe_broadcast(mock.sentinel.channel, mock.sentinel.callback,
        retroactive=mock.sentinel.retro)

  ct._subscribe_broadcast.assert_called_once_with(mock.sentinel.channel,
        mock.sentinel.callback, mock.sentinel.retro)

def test_simple_unsubscribe_from_a_broadcast():
  ct = CommonTransport()
  ct._unsubscribe_broadcast = mock.Mock()

  ct.unsubscribe_broadcast(mock.sentinel.channel)

  ct._unsubscribe_broadcast.assert_called_once_with(mock.sentinel.channel)

def test_simple_send_message():
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
  ct = CommonTransport()

  client = ct.register_client()
  assert client

  client2 = ct.register_client()
  assert client2
  assert client2 != client

  ct.drop_client(client)
  ct.drop_client(client2)

def test_dropping_an_unregistered_client_should_fail():
  ct = CommonTransport()

  with pytest.raises(workflows.WorkflowsError):
    ct.drop_client(mock.sentinel.unregistered_client)

def test_dropping_subscriptions_when_dropping_client():
  ct = CommonTransport()
  ct._subscribe = mock.Mock()
  ct._unsubscribe = mock.Mock()

  client = ct.register_client()
  ct.subscribe(mock.sentinel.channel, mock.sentinel.callback, client_id=client)
  ct.drop_client(client)

  ct._unsubscribe.assert_called_once_with(mock.sentinel.channel)
