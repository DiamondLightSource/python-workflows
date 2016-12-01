from __future__ import absolute_import, division
import mock
import pytest
import workflows.transport
from workflows.transport.queue_transport import QueueTransport

def test_lookup_and_initialize_queue_transport_layer():
  '''Find the queue transport layer via the lookup mechanism and run
     its constructor with default settings.'''
  qt = workflows.transport.lookup("QueueTransport")
  assert qt == QueueTransport
  qt()

def test_add_command_line_help():
  '''Check that trying to add command line parameters does nothing of consequence.'''
  QueueTransport().add_command_line_options(None)

def test_connect_to_a_send_function():
  '''Test the Stomp connection routine.'''
  mockfunc = mock.Mock()

  qt = QueueTransport()
  assert not qt.is_connected()

  assert not qt.connect()
  assert not qt.is_connected()

  with pytest.raises(workflows.WorkflowsError):
    qt.send('', '')

  qt.set_send_function(mockfunc)
  assert not qt.is_connected()

  assert qt.connect()
  assert qt.is_connected()

def setup_qt():
  '''Helper function to create a send function and a QueueTransport()-instance using it.'''
  func = mock.Mock()
  qt = QueueTransport()
  qt.set_send_function(func)
  qt.connect()
  return func, qt

def test_forward_send_call():
  '''Test translation of a send() call to Queue message.'''
  mockfunc, qt = setup_qt()

  qt.send(destination=mock.sentinel.destination,
             message=str(mock.sentinel.message),
             headers=mock.sentinel.header)

  mockfunc.assert_called_with({
    'band': 'transport',
    'call': 'send',
    'payload': (
      ( mock.sentinel.destination, str(mock.sentinel.message) ),
      { 'headers': mock.sentinel.header }
    )
  })

def test_forward_broadcast_call():
  '''Test translation of a broadcast() call to Queue message.'''
  mockfunc, qt = setup_qt()

  qt.broadcast(destination=mock.sentinel.destination,
             message=str(mock.sentinel.message),
             headers=mock.sentinel.header)

  mockfunc.assert_called_once_with({
    'band': 'transport',
    'call': 'broadcast',
    'payload': (
      (mock.sentinel.destination, str(mock.sentinel.message)),
      { 'headers': mock.sentinel.header }
    )
  })

def test_forward_transaction_begin_call():
  '''Test translation of a transaction_begin() call to Queue message.'''
  mockfunc, qt = setup_qt()

  tid = qt.transaction_begin(kwarg=mock.sentinel.kwarg)

  mockfunc.assert_called_once_with({
    'band': 'transport',
    'call': 'transaction_begin',
    'payload': (
      (tid,), { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_transaction_abort_call():
  '''Test translation of a transaction_abort() call to Queue message.'''
  mockfunc, qt = setup_qt()

  tid = qt.transaction_begin()
  qt.transaction_abort(tid, kwarg=mock.sentinel.kwarg)

  assert mockfunc.call_count == 2
  mockfunc.assert_called_with({
    'band': 'transport',
    'call': 'transaction_abort',
    'payload': (
      (tid,), { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_transaction_commit_call():
  '''Test translation of a transaction_commit() call to Queue message.'''
  mockfunc, qt = setup_qt()

  tid = qt.transaction_begin()
  qt.transaction_commit(tid, kwarg=mock.sentinel.kwarg)

  assert mockfunc.call_count == 2
  mockfunc.assert_called_with({
    'band': 'transport',
    'call': 'transaction_commit',
    'payload': (
      (tid,), { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_subscribe_call():
  '''Test translation of a subscribe() call to Queue message.'''
  mockfunc, qt = setup_qt()

  subid = qt.subscribe(mock.sentinel.channel, mock.sentinel.callback,
    kwarg=mock.sentinel.kwarg)

  mockfunc.assert_called_once_with({
    'band': 'transport',
    'call': 'subscribe',
    'payload': (
      ( subid, mock.sentinel.channel ), # callback can't be transported
      { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_subscribe_broadcast_call():
  '''Test translation of a subscribe_broadcast() call to Queue message.'''
  mockfunc, qt = setup_qt()

  mock_callback = mock.Mock()

  subid = qt.subscribe_broadcast(mock.sentinel.channel,
    mock_callback, kwarg=mock.sentinel.kwarg)

  mockfunc.assert_called_once_with({
    'band': 'transport',
    'call': 'subscribe_broadcast',
    'payload': (
      ( subid, mock.sentinel.channel ), # callback can't be transported
      { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_unsubscribe_call():
  '''Test translation of an unsubscribe() call to Queue message.'''
  mockfunc, qt = setup_qt()

  subid = qt.subscribe(mock.sentinel.channel, mock.sentinel.callback)
  qt.unsubscribe(subid, kwarg=mock.sentinel.kwarg)

  assert mockfunc.call_count == 2
  mockfunc.assert_called_with({
    'band': 'transport',
    'call': 'unsubscribe',
    'payload': (
      ( subid, ),
      { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_ack_call():
  '''Test translation of ack() call to Queue message.'''
  mockfunc, qt = setup_qt()

  subid = qt.subscribe(mock.sentinel.channel, mock.sentinel.callback, acknowledgement=True)
  qt.ack(mock.sentinel.messageid, subid, kwarg=mock.sentinel.kwarg)

  mockfunc.assert_called_with({
    'band': 'transport',
    'call': 'ack',
    'payload': (
      ( mock.sentinel.messageid, subid ),
      { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_nack_call():
  '''Test translation of nack() call to Queue message.'''
  mockfunc, qt = setup_qt()

  subid = qt.subscribe(mock.sentinel.channel, mock.sentinel.callback, acknowledgement=True)
  qt.nack(mock.sentinel.messageid, subid, kwarg=mock.sentinel.kwarg)

  mockfunc.assert_called_with({
    'band': 'transport',
    'call': 'nack',
    'payload': (
      ( mock.sentinel.messageid, subid ),
      { 'kwarg': mock.sentinel.kwarg }
    )
  })
