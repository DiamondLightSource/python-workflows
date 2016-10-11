from __future__ import absolute_import, division
from workflows.transport.common_transport import CommonTransport
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
  assert ct.subscription_callback(subid) == mock.sentinel.callback
  ct._subscribe.assert_called_once_with(subid, mock.sentinel.channel,
        mock.sentinel.callback,
        exclusive=mock.sentinel.exclusive,
        acknowledgement=mock.sentinel.ack)

  ct.unsubscribe(subid)

  ct._unsubscribe.assert_called_once_with(subid)
  with pytest.raises(workflows.WorkflowsError):
    ct.subscription_callback(subid)

def test_simple_subscribe_unsubscribe_a_broadcast():
  """Public subscribe_bc()-call should be routed to specific _subscribe_bc().
     Unsubscribes should be routed to _unsubscribe() and handled properly."""
  ct = CommonTransport()
  ct._subscribe_broadcast = mock.Mock()
  ct._unsubscribe = mock.Mock()

  subid = ct.subscribe_broadcast(mock.sentinel.channel, mock.sentinel.callback,
        retroactive=mock.sentinel.retro)

  assert subid
  assert ct.subscription_callback(subid) == mock.sentinel.callback
  ct._subscribe_broadcast.assert_called_once_with(subid, mock.sentinel.channel,
        mock.sentinel.callback, retroactive=mock.sentinel.retro)

  ct.unsubscribe(subid)

  ct._unsubscribe.assert_called_once_with(subid)
  with pytest.raises(workflows.WorkflowsError):
    ct.subscription_callback(subid)

def test_simple_send_message():
  """Pass string and object messages to send(), should be serialized and
     routed to specific _send()"""
  ct = CommonTransport()
  ct._send = mock.Mock()

  ct.send(mock.sentinel.destination, str(mock.sentinel.message),
        headers=mock.sentinel.headers,
        expiration=mock.sentinel.expiration,
        transaction=mock.sentinel.transaction,
        something=mock.sentinel.something)

  ct._send.assert_called_once_with(mock.sentinel.destination,
        str(mock.sentinel.message), headers=mock.sentinel.headers,
        expiration=mock.sentinel.expiration,
        transaction=mock.sentinel.transaction,
        something=mock.sentinel.something)

  ct.send(mock.sentinel.destination, { 'entry': [ 0, 'banana' ] })

  ct._send.assert_called_with(mock.sentinel.destination,
        '{"entry": [0, "banana"]}')

def test_simple_broadcast_message():
  """Pass string and object messages to broadcast(), should be serialized and
     routed to specific _broadcast()"""

  ct = CommonTransport()
  ct._broadcast = mock.Mock()

  ct.broadcast(mock.sentinel.destination, str(mock.sentinel.message),
        headers=mock.sentinel.headers, expiration=mock.sentinel.expiration,
        transaction=mock.sentinel.transaction)

  ct._broadcast.assert_called_once_with(mock.sentinel.destination,
        str(mock.sentinel.message), headers=mock.sentinel.headers,
        expiration=mock.sentinel.expiration,
        transaction=mock.sentinel.transaction)

  ct.broadcast(mock.sentinel.destination, { 'entry': [ 0, 'banana' ] })

  ct._broadcast.assert_called_with(mock.sentinel.destination,
        '{"entry": [0, "banana"]}')

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
  with pytest.raises(workflows.WorkflowsError):
    ct.subscription_callback(subid)
  with pytest.raises(workflows.WorkflowsError):
    ct.unsubscribe(subid)

  client = ct.register_client()
  subid = ct.subscribe(mock.sentinel.channel, mock.sentinel.callback, client_id=client)
  assert ct.subscription_callback(subid) == mock.sentinel.callback
  ct.unsubscribe(subid)
  ct.drop_client(client)

  assert ct._unsubscribe.call_count == 2

  with pytest.raises(workflows.WorkflowsError):
    ct.unsubscribe(subid)

def test_create_and_destroy_transactions():
  "Create, commit and abort transactions."
  ct = CommonTransport()
  ct._transaction_begin = mock.Mock()
  ct._transaction_commit = mock.Mock()
  ct._transaction_abort = mock.Mock()

  t = ct.transaction_begin()

  assert t
  ct._transaction_begin.assert_called_once_with(t)

  ct.transaction_abort(t)
  with pytest.raises(workflows.WorkflowsError):
    ct.transaction_commit(t)
  ct._transaction_abort.assert_called_once_with(t)

  t2 = ct.transaction_begin()
  assert t2
  assert t != t2
  ct.transaction_commit(t2)
  with pytest.raises(workflows.WorkflowsError):
    ct.transaction_abort(t2)
  ct._transaction_commit.assert_called_once_with(t2)

def test_dropping_transactions_when_dropping_client():
  "Transactions associated with a client should be aborted when client is dropped."
  ct = CommonTransport()
  ct._transaction_begin = mock.Mock()
  ct._transaction_commit = mock.Mock()
  ct._transaction_abort = mock.Mock()

  client = ct.register_client()
  t = ct.transaction_begin(client_id=client)
  ct.drop_client(client)
  ct._transaction_abort.assert_called_once_with(t)
  with pytest.raises(workflows.WorkflowsError):
    ct.transaction_abort(t)
  with pytest.raises(workflows.WorkflowsError):
    ct.transaction_commit(t)

  client = ct.register_client()
  t = ct.transaction_begin(client_id=client)
  ct.transaction_commit(t)
  ct.drop_client(client)
  ct._transaction_abort.assert_called_once
  with pytest.raises(workflows.WorkflowsError):
    ct.transaction_abort(t)
  with pytest.raises(workflows.WorkflowsError):
    ct.transaction_commit(t)

  client = ct.register_client()
  t = ct.transaction_begin(client_id=client)
  ct.transaction_abort(t)
  ct.drop_client(client)
  assert ct._transaction_abort.call_count == 2
  with pytest.raises(workflows.WorkflowsError):
    ct.transaction_abort(t)
  with pytest.raises(workflows.WorkflowsError):
    ct.transaction_commit(t)

def mock_transport_factory_with_client():
  '''Set up a CommonTransport with mocked internals and one connected client.'''
  ct = CommonTransport()
  ct._ack, ct._nack, ct._transaction_begin, ct._transaction_abort, ct._transaction_commit, ct._subscribe, ct._subscribe_broadcast, ct._unsubscribe = \
    mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock()
  client = ct.register_client()
  return ct, client

def test_message_acknowledgements_are_only_available_for_relevant_subscriptions():
  '''Test messages can not be registered for acknowledgement when the subscription is not set up to do so.'''
  ct, client = mock_transport_factory_with_client()
  with pytest.raises(workflows.WorkflowsError):
    ct.register_message(0, mock.sentinel.message_id)

  subid = ct.subscribe(mock.sentinel.channel, mock.sentinel.callback, client_id=client)
  with pytest.raises(workflows.WorkflowsError):
    ct.register_message(subid, mock.sentinel.message_id)

  ct, client = mock_transport_factory_with_client()
  subid = ct.subscribe(mock.sentinel.channel, mock.sentinel.callback, client_id=client, acknowledgement=False)
  with pytest.raises(workflows.WorkflowsError):
    ct.register_message(subid, mock.sentinel.message_id)

  ct, client = mock_transport_factory_with_client()
  subid = ct.subscribe_broadcast(mock.sentinel.channel, mock.sentinel.callback, client_id=client)
  with pytest.raises(workflows.WorkflowsError):
    ct.register_message(subid, mock.sentinel.message_id)

  ct, client = mock_transport_factory_with_client()
  subid = ct.subscribe_broadcast(mock.sentinel.channel, mock.sentinel.callback, client_id=client, acknowledgement=False)
  with pytest.raises(workflows.WorkflowsError):
    ct.register_message(subid, mock.sentinel.message_id)

def test_messages_are_rejected_when_client_goes_away():
  '''Test ack/nack functions. Automatically NACK messages if client goes away.'''
  ct, client = mock_transport_factory_with_client()
  subid = ct.subscribe(mock.sentinel.channel, mock.sentinel.callback, client_id=client, acknowledgement=True)
  ct.register_message(subid, mock.sentinel.message_id)
  ct.unsubscribe(subid) # this must have no effect

  assert not ct._ack.called
  assert not ct._nack.called

  ct.drop_client(client)
  ct._nack.assert_called_once_with(mock.sentinel.message_id, subid)

def test_messages_are_not_rejected_when_client_goes_away_after_acking_message():
  '''Test ack/nack functions. Do not NACK messages that are already ACKed.'''
  ct, client = mock_transport_factory_with_client()
  subid = ct.subscribe(mock.sentinel.channel, mock.sentinel.callback, client_id=client, acknowledgement=True)
  ct.register_message(subid, mock.sentinel.message_id)
  ct.ack(mock.sentinel.message_id)

  ct._ack.assert_called_once_with(mock.sentinel.message_id, subid)
  assert not ct._nack.called

  ct.drop_client(client)

  ct._ack.assert_called_once()
  assert not ct._nack.called

def test_messages_are_not_rejected_when_client_goes_away_after_nacking_message():
  '''Test ack/nack functions. Do not NACK messages that are already NACKed.'''
  ct, client = mock_transport_factory_with_client()
  subid = ct.subscribe(mock.sentinel.channel, mock.sentinel.callback, client_id=client, acknowledgement=True)
  ct.register_message(subid, mock.sentinel.message_id)
  ct.nack(mock.sentinel.message_id)

  assert not ct._ack.called
  ct._nack.assert_called_once_with(mock.sentinel.message_id, subid)

  ct.drop_client(client)

  assert not ct._ack.called
  ct._nack.assert_called_once()

# TODO:
# In a subscription with client-id && ack each acked packet with uncommitted transaction-id must be nacked when client goes away (not when txn is aborted)
@pytest.mark.skip(reason="TODO")
def test_messages_are_rejected_when_client_goes_away_with_uncommitted_ack():
  pass
@pytest.mark.skip(reason="TODO")
def test_messages_are_rejected_when_client_goes_away_with_uncommitted_nack():
  pass
@pytest.mark.skip(reason="TODO")
def test_messages_are_rejected_when_client_goes_away_with_aborted_ack():
  pass
@pytest.mark.skip(reason="TODO")
def test_messages_are_rejected_when_client_goes_away_with_aborted_nack():
  pass
@pytest.mark.skip(reason="TODO")
def test_messages_are_not_rejected_when_client_goes_away_with_committed_ack():
  pass
@pytest.mark.skip(reason="TODO")
def test_messages_are_not_rejected_when_client_goes_away_with_committed_nack():
  pass

def test_unimplemented_communication_methods_should_fail():
  '''Check that low-level communication calls raise WorkflowsError when not
     overridden.'''
  ct = CommonTransport()
  assert not ct.connect()
  for function, argcount in [
      ('_subscribe', 3),
      ('_subscribe_broadcast', 3),
      ('_unsubscribe', 1),
      ('_send', 2),
      ('_broadcast', 2),
      ('_ack', 2),
      ('_nack', 2),
      ('_transaction_begin', 1),
      ('_transaction_abort', 1),
      ('_transaction_commit', 1),
      ]:
    with pytest.raises(workflows.WorkflowsError):
      getattr(ct, function)(*([mock.Mock()] * argcount))
