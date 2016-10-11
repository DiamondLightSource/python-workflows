from __future__ import absolute_import, division
import mock
import pytest
import workflows.transport
from workflows.transport.queue_transport import QueueTransport

def test_lookup_and_initialize_queue_transport_layer():
  '''Find the queue transport layer via the lookup mechanism and run
     its constructor with default settings.'''
  queue = workflows.transport.lookup("QueueTransport")
  assert queue == QueueTransport
  queue()

def test_add_command_line_help():
  '''Check that trying to add command line parameters does nothing of consequence.'''
  QueueTransport().add_command_line_options(None)

def test_connect_to_a_queue():
  '''Test the Stomp connection routine.'''
  mockqueue = mock.Mock()

  queue = QueueTransport()
  assert not queue.is_connected()

  assert not queue.connect()
  assert not queue.is_connected()

  with pytest.raises(workflows.WorkflowsError):
    queue.send('', '')

  queue.set_queue(mockqueue)
  assert not queue.is_connected()

  assert queue.connect()
  assert queue.is_connected()

def setup_queue():
  '''Helper function to create a faux-Queue and a QueueTransport()-instance connected to it.'''
  mockqueue = mock.Mock()
  queue = QueueTransport()
  queue.set_queue(mockqueue)
  queue.connect()
  return mockqueue, queue

def test_forward_send_call():
  '''Test translation of a send() call to Queue message.'''
  mockqueue, queue = setup_queue()

  queue.send(destination=mock.sentinel.destination,
             message=str(mock.sentinel.message),
	     headers=mock.sentinel.header)

  mockqueue.put_nowait.assert_called_with({
    'band': 'transport',
    'call': 'send',
    'payload': (
      ( mock.sentinel.destination, str(mock.sentinel.message) ),
      { 'headers': mock.sentinel.header }
    )
  })

def test_forward_broadcast_call():
  '''Test translation of a broadcast() call to Queue message.'''
  mockqueue, queue = setup_queue()

  queue.broadcast(destination=mock.sentinel.destination,
             message=str(mock.sentinel.message),
	     headers=mock.sentinel.header)

  mockqueue.put_nowait.assert_called_once_with({
    'band': 'transport',
    'call': 'broadcast',
    'payload': (
      (mock.sentinel.destination, str(mock.sentinel.message)),
      { 'headers': mock.sentinel.header }
    )
  })

def test_forward_transaction_begin_call():
  '''Test translation of a transaction_begin() call to Queue message.'''
  mockqueue, queue = setup_queue()

  tid = queue.transaction_begin(kwarg=mock.sentinel.kwarg)

  mockqueue.put_nowait.assert_called_once_with({
    'band': 'transport',
    'call': 'transaction_begin',
    'payload': (
      (tid,), { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_transaction_abort_call():
  '''Test translation of a transaction_abort() call to Queue message.'''
  mockqueue, queue = setup_queue()

  tid = queue.transaction_begin()
  queue.transaction_abort(tid, kwarg=mock.sentinel.kwarg)

  assert mockqueue.put_nowait.call_count == 2
  mockqueue.put_nowait.assert_called_with({
    'band': 'transport',
    'call': 'transaction_abort',
    'payload': (
      (tid,), { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_transaction_commit_call():
  '''Test translation of a transaction_commit() call to Queue message.'''
  mockqueue, queue = setup_queue()

  tid = queue.transaction_begin()
  queue.transaction_commit(tid, kwarg=mock.sentinel.kwarg)

  assert mockqueue.put_nowait.call_count == 2
  mockqueue.put_nowait.assert_called_with({
    'band': 'transport',
    'call': 'transaction_commit',
    'payload': (
      (tid,), { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_subscribe_call():
  '''Test translation of a subscribe() call to Queue message.'''
  mockqueue, queue = setup_queue()

  subid = queue.subscribe(mock.sentinel.channel, mock.sentinel.callback,
    kwarg=mock.sentinel.kwarg)

  mockqueue.put_nowait.assert_called_once_with({
    'band': 'transport',
    'call': 'subscribe',
    'payload': (
      ( subid, mock.sentinel.channel ), # callback can't be transported
      { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_subscribe_broadcast_call():
  '''Test translation of a subscribe_broadcast() call to Queue message.'''
  mockqueue, queue = setup_queue()

  subid = queue.subscribe_broadcast(mock.sentinel.channel,
    mock.sentinel.callback, kwarg=mock.sentinel.kwarg)

  mockqueue.put_nowait.assert_called_once_with({
    'band': 'transport',
    'call': 'subscribe_broadcast',
    'payload': (
      ( subid, mock.sentinel.channel, mock.sentinel.callback ),
      { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_unsubscribe_call():
  '''Test translation of an unsubscribe() call to Queue message.'''
  mockqueue, queue = setup_queue()

  subid = queue.subscribe(mock.sentinel.channel, mock.sentinel.callback)
  queue.unsubscribe(subid, kwarg=mock.sentinel.kwarg)

  assert mockqueue.put_nowait.call_count == 2
  mockqueue.put_nowait.assert_called_with({
    'band': 'transport',
    'call': 'unsubscribe',
    'payload': (
      ( subid, ),
      { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_ack_call():
  '''Test translation of ack() call to Queue message.'''
  mockqueue, queue = setup_queue()

  subid = queue.subscribe(mock.sentinel.channel, mock.sentinel.callback, acknowledgement=True)
  queue.register_message(subid, mock.sentinel.messageid)
  queue.ack(mock.sentinel.messageid,
            kwarg=mock.sentinel.kwarg)

  mockqueue.put_nowait.assert_called_with({
    'band': 'transport',
    'call': 'ack',
    'payload': (
      ( mock.sentinel.messageid, ),
      { 'kwarg': mock.sentinel.kwarg }
    )
  })

def test_forward_nack_call():
  '''Test translation of nack() call to Queue message.'''
  mockqueue, queue = setup_queue()

  subid = queue.subscribe(mock.sentinel.channel, mock.sentinel.callback, acknowledgement=True)
  queue.register_message(subid, mock.sentinel.messageid)
  queue.nack(mock.sentinel.messageid,
            kwarg=mock.sentinel.kwarg)

  mockqueue.put_nowait.assert_called_with({
    'band': 'transport',
    'call': 'nack',
    'payload': (
      ( mock.sentinel.messageid, ),
      { 'kwarg': mock.sentinel.kwarg }
    )
  })
