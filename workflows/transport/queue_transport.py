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

  def _send(self, *args, **kwargs):
    '''Forward message sending command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'send',
      'payload': ( args, kwargs )
    })

  def _broadcast(self, *args, **kwargs):
    '''Forward message broadcast command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'broadcast',
      'payload': ( args, kwargs )
    })

  def _transaction_begin(self, *args, **kwargs):
    '''Forward transaction start command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'transaction_begin',
      'payload': ( args, kwargs )
    })

  def _transaction_abort(self, *args, **kwargs):
    '''Forward transaction abort command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'transaction_abort',
      'payload': ( args, kwargs )
    })

  def _transaction_commit(self, *args, **kwargs):
    '''Forward transaction commit command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'transaction_commit',
      'payload': ( args, kwargs )
    })

  def _subscribe(self, subscription_id, channel, callback, **kwargs):
    '''Forward subscription command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'subscribe',
      'payload': ( (subscription_id, channel), kwargs )
    })

  def _subscribe_broadcast(self, *args, **kwargs):
    '''Forward broadcast subscription command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'subscribe_broadcast',
      'payload': ( args, kwargs )
    })

  def _unsubscribe(self, *args, **kwargs):
    '''Forward unsubscribe command to queue.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'unsubscribe',
      'payload': ( args, kwargs )
    })

  def _ack(self, messageid, subscription, *args, **kwargs):
    '''Forward receipt acknowledgement to queue. Drop subscription id.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'ack',
      'payload': ( (messageid, ) + args, kwargs )
    })

  def _nack(self, messageid, subscription, *args, **kwargs):
    '''Forward receipt rejection to queue. Drop subscription id.'''
    self.assert_connected()
    self._queue.put_nowait({
      'band': 'transport',
      'call': 'nack',
      'payload': ( (messageid, ) + args, kwargs )
    })
