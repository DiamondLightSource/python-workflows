from __future__ import absolute_import, division
from workflows import WorkflowsError
import workflows.transport
from workflows.transport.common_transport import CommonTransport

class QueueTransport(CommonTransport):
  '''Abstraction layer for messaging infrastructure.
     Here we are using messaging via a Queue to the Frontend.'''

  def __init__(self):
    self._connected = False
    self._queue = None

  def add_command_line_options(self, optparser):
    '''This type of transport does not offer command line options.'''

  def set_queue(self, queue):
    '''Set queue to connect to.'''
    self._queue = queue

  def connect(self):
    '''Connect to the queue.'''
    if self._queue:
      self._connected = True
    return self._connected

  def is_connected(self):
    '''Return connection status'''
    return self._connected

  def assert_connected(self):
    if not self._connected:
      raise WorkflowsError('Transport not connected')

  def _send(self, *args):
    '''Forward message sending command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'send',
      'payload': args
    })

  def _broadcast(self, *args):
    '''Forward message broadcast command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'broadcast',
      'payload': args
    })

  def _transaction_begin(self, *args):
    '''Forward transaction start command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'transaction_begin',
      'payload': args
    })

  def _transaction_abort(self, *args):
    '''Forward transaction abort command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'transaction_abort',
      'payload': args
    })

  def _transaction_commit(self, *args):
    '''Forward transaction commit command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'transaction_commit',
      'payload': args
    })

  def _subscribe(self, sub_id, channel, callback, *args):
    '''Forward subscription command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'subscribe',
      'payload': (
        sub_id,
        channel) + args
    })

  def _subscribe_broadcast(self, sub_id, channel, callback, *args):
    '''Forward broadcast subscription command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'subscribe_broadcast',
      'payload': (
        sub_id,
        channel) + args
    })

  def _unsubscribe(self, sub_id):
    '''Forward unsubscribe command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'unsubscribe',
      'payload': (
        sub_id,
      )
    })
