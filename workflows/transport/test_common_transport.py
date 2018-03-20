from __future__ import absolute_import, division, print_function

import mock
import pytest
import workflows
from workflows.transport.common_transport import CommonTransport

def test_subscribe_unsubscribe_a_channel():
  """Public subscribe()-call should be routed to specific _subscribe().
     Unsubscribes should be routed to _unsubscribe() and handled properly."""
  ct = CommonTransport()
  ct._subscribe = mock.Mock()
  ct._unsubscribe = mock.Mock()
  mock_callback = mock.Mock()

  subid = ct.subscribe(mock.sentinel.channel, mock_callback,
        exclusive=mock.sentinel.exclusive, acknowledgement=mock.sentinel.ack,
        disable_mangling=True)

  assert subid
  assert ct.subscription_callback(subid) == mock_callback
  ct._subscribe.assert_called_once_with(subid, mock.sentinel.channel,
        mock.ANY,
        exclusive=mock.sentinel.exclusive,
        acknowledgement=mock.sentinel.ack)
  callback = ct._subscribe.call_args[0][2]
  callback(mock.sentinel.header, mock.sentinel.message)
  mock_callback.assert_called_once_with(mock.sentinel.header, mock.sentinel.message)
  # Should not be able to drop callback reference for live subscription
  with pytest.raises(workflows.Error):
    ct.drop_callback_reference(subid)

  ct.unsubscribe(subid)

  ct._unsubscribe.assert_called_once_with(subid)

  # Multiple unsubscribes should not work
  with pytest.raises(workflows.Error):
    ct.unsubscribe(subid)
  ct._unsubscribe.assert_called_once()

  # Callback should still be connected
  ct.subscription_callback(subid)

  ct.drop_callback_reference(subid)
  with pytest.raises(workflows.Error):
    ct.subscription_callback(subid)

  # Should not be able to double-drop reference
  with pytest.raises(workflows.Error):
    ct.drop_callback_reference(subid)

def test_subscription_messages_pass_mangling_function():
  """A message received via a subscription must pass through the mangle function."""
  ct = CommonTransport()
  ct._subscribe = mock.Mock()
  mock_callback = mock.Mock()

  subid = ct.subscribe(mock.sentinel.channel, mock_callback)
  assert subid
  assert ct.subscription_callback(subid) != mock_callback
  ct.subscription_callback(subid)(mock.sentinel.header, mock.sentinel.message)
  mock_callback.assert_called_once_with(mock.sentinel.header, mock.sentinel.message)

def test_simple_subscribe_unsubscribe_a_broadcast():
  """Public subscribe_bc()-call should be routed to specific _subscribe_bc().
     Unsubscribes should be routed to _unsubscribe() and handled properly."""
  ct = CommonTransport()
  ct._subscribe_broadcast = mock.Mock()
  ct._unsubscribe = mock.Mock()
  mock_callback = mock.Mock()

  subid = ct.subscribe_broadcast(mock.sentinel.channel, mock_callback,
        retroactive=mock.sentinel.retro, disable_mangling=True)

  assert subid
  assert ct.subscription_callback(subid) == mock_callback
  ct._subscribe_broadcast.assert_called_once_with(subid, mock.sentinel.channel,
        mock.ANY, retroactive=mock.sentinel.retro)
  callback = ct._subscribe_broadcast.call_args[0][2]
  callback(mock.sentinel.header, mock.sentinel.message)
  mock_callback.assert_called_once_with(mock.sentinel.header, mock.sentinel.message)
  # Should not be able to drop callback reference for live subscription
  with pytest.raises(workflows.Error):
    ct.drop_callback_reference(subid)

  ct.unsubscribe(subid)

  ct._unsubscribe.assert_called_once_with(subid)
  # Multiple unsubscribes should not work
  with pytest.raises(workflows.Error):
    ct.unsubscribe(subid)
  ct._unsubscribe.assert_called_once()

  # Callback should still be connected
  ct.subscription_callback(subid)

  ct.drop_callback_reference(subid)
  with pytest.raises(workflows.Error):
    ct.subscription_callback(subid)

  # Should not be able to double-drop reference
  with pytest.raises(workflows.Error):
    ct.drop_callback_reference(subid)

def test_broadcast_subscription_messages_pass_mangling_function():
  """A message received via a broadcast subscription must pass through the mangle function."""
  ct = CommonTransport()
  ct._subscribe_broadcast = mock.Mock()
  mock_callback = mock.Mock()

  subid = ct.subscribe_broadcast(mock.sentinel.channel, mock_callback)
  assert subid
  assert ct.subscription_callback(subid) != mock_callback
  ct.subscription_callback(subid)(mock.sentinel.header, mock.sentinel.message)
  mock_callback.assert_called_once_with(mock.sentinel.header, mock.sentinel.message)

@pytest.mark.parametrize('mangling', [None, True, False])
def test_callbacks_can_be_intercepted(mangling):
  """The function called on message receipt must be interceptable."""
  ct = CommonTransport()
  ct._subscribe = mock.Mock()
  mock_true_callback = mock.Mock()
  intercept = mock.Mock()

  if mangling is None:
    subid = ct.subscribe(mock.sentinel.channel, mock_true_callback)
  else:
    subid = ct.subscribe(mock.sentinel.channel, mock_true_callback, disable_mangling=mangling)

  # Original code path
  ct.subscription_callback(subid)(mock.sentinel.header, mock.sentinel.message)
  mock_true_callback.assert_called_once_with(mock.sentinel.header, mock.sentinel.message)

  # Intercept and don't pass on
  mock_true_callback.reset_mock()
  ct.subscription_callback_set_intercept(intercept)

  ct.subscription_callback(subid)(mock.sentinel.header, mock.sentinel.message)
  assert not mock_true_callback.called
  intercept.assert_called_once_with(mock.ANY)
  intercept.return_value.assert_called_once_with(mock.sentinel.header, mock.sentinel.message)

  # Pass through (tests the value passed to the interceptor function is sensible)
  intercept = lambda x: x
  ct.subscription_callback_set_intercept(intercept)

  ct.subscription_callback(subid)(mock.sentinel.header, mock.sentinel.message)
  mock_true_callback.assert_called_once_with(mock.sentinel.header, mock.sentinel.message)

  # Disable interception
  mock_true_callback.reset_mock()
  ct.subscription_callback_set_intercept(None)

  ct.subscription_callback(subid)(mock.sentinel.header, mock.sentinel.message)
  mock_true_callback.assert_called_once_with(mock.sentinel.header, mock.sentinel.message)


def test_simple_send_message():
  """Pass messages to send(), should be routed to specific _send()"""
  ct = CommonTransport()
  ct._send = mock.Mock()

  ct.send(mock.sentinel.destination, mock.sentinel.message,
        headers=mock.sentinel.headers,
        expiration=mock.sentinel.expiration,
        transaction=mock.sentinel.transaction,
        something=mock.sentinel.something)

  ct._send.assert_called_once_with(mock.sentinel.destination,
        mock.sentinel.message, headers=mock.sentinel.headers,
        expiration=mock.sentinel.expiration,
        transaction=mock.sentinel.transaction,
        something=mock.sentinel.something)

  ct.send(mock.sentinel.destination, mock.sentinel.message)

  ct._send.assert_called_with(mock.sentinel.destination, mock.sentinel.message)

def test_simple_broadcast_message():
  """Pass messages to broadcast(), should be routed to specific _broadcast()"""

  ct = CommonTransport()
  ct._broadcast = mock.Mock()

  ct.broadcast(mock.sentinel.destination, mock.sentinel.message,
        headers=mock.sentinel.headers, expiration=mock.sentinel.expiration,
        transaction=mock.sentinel.transaction)

  ct._broadcast.assert_called_once_with(mock.sentinel.destination,
        mock.sentinel.message, headers=mock.sentinel.headers,
        expiration=mock.sentinel.expiration,
        transaction=mock.sentinel.transaction)

  ct.broadcast(mock.sentinel.destination, mock.sentinel.message)

  ct._broadcast.assert_called_with(mock.sentinel.destination, mock.sentinel.message)

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
  with pytest.raises(workflows.Error):
    ct.transaction_commit(t)
  ct._transaction_abort.assert_called_once_with(t)

  t2 = ct.transaction_begin()
  assert t2
  assert t != t2
  ct.transaction_commit(t2)
  with pytest.raises(workflows.Error):
    ct.transaction_abort(t2)
  ct._transaction_commit.assert_called_once_with(t2)

def test_messages_can_be_acknowledged_and_rejected():
  '''Check that ack() and nack() calls are passed to implementations.'''
  ct = CommonTransport()
  ct._ack = mock.Mock()
  ct._nack = mock.Mock()

  ct.ack(mock.sentinel.message_id1, mock.sentinel.subscription_id1)
  ct.nack(mock.sentinel.message_id2, mock.sentinel.subscription_id2)
  ct.ack({'message-id': mock.sentinel.message_id3}, mock.sentinel.subscription_id3)
  ct.nack({'message-id': mock.sentinel.message_id4}, mock.sentinel.subscription_id4)
  ct.ack({'message-id': mock.sentinel.message_id5, 'subscription': mock.sentinel.subscription_id5})
  ct.nack({'message-id': mock.sentinel.message_id6, 'subscription': mock.sentinel.subscription_id6})

  ct._ack.assert_any_call(mock.sentinel.message_id1, mock.sentinel.subscription_id1)
  ct._ack.assert_any_call(mock.sentinel.message_id3, mock.sentinel.subscription_id3)
  ct._ack.assert_any_call(mock.sentinel.message_id5, mock.sentinel.subscription_id5)
  ct._nack.assert_any_call(mock.sentinel.message_id2, mock.sentinel.subscription_id2)
  ct._nack.assert_any_call(mock.sentinel.message_id4, mock.sentinel.subscription_id4)
  ct._nack.assert_any_call(mock.sentinel.message_id6, mock.sentinel.subscription_id6)

  with pytest.raises(workflows.Error):
    ct.ack(None)
  with pytest.raises(workflows.Error):
    ct.ack(mock.sentinel.crash)
  with pytest.raises(workflows.Error):
    ct.ack({'message-id': mock.sentinel.crash})
  with pytest.raises(workflows.Error):
    ct.ack({'message-id': None, 'subscription': mock.sentinel.crash})
  with pytest.raises(workflows.Error):
    ct.nack(None)
  with pytest.raises(workflows.Error):
    ct.nack(mock.sentinel.crash)
  with pytest.raises(workflows.Error):
    ct.nack({'message-id': mock.sentinel.crash})
  with pytest.raises(workflows.Error):
    ct.nack({'message-id': None, 'subscription': mock.sentinel.crash})

def test_unimplemented_communication_methods_should_fail():
  '''Check that low-level communication calls raise NotImplementedError when not
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
    with pytest.raises(NotImplementedError):
      getattr(ct, function)(*([mock.Mock()] * argcount))
