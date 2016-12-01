from __future__ import absolute_import, division
from workflows import WorkflowsError
import workflows.transport
from workflows.transport.common_transport import CommonTransport

class QueueTransport(CommonTransport):
  '''Abstraction layer for messaging infrastructure.
     Here we are using messaging via a simple sending function. This function
     could be backing a Queue or Pipe towards the Frontend.'''

  def __init__(self):
    self._connected = False
    self._send_fun = None

  @staticmethod
  def add_command_line_options(optparser):
    '''This type of transport does not offer command line options.'''

  def set_send_function(self, function):
    '''Set a function to be called to send messages. The function will take a
       single argument, and could be a Queue.put, Pipe.send or similar
       construct. The argument is a dictionary containing the message details.
    '''
    self._send_fun = function

  def connect(self):
    '''Connect to the queue.'''
    if self._send_fun:
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
    self._send_fun({
      'band': 'transport',
      'call': 'send',
      'payload': ( args, kwargs )
    })

  def _broadcast(self, *args, **kwargs):
    '''Forward message broadcast command to queue.'''
    self.assert_connected()
    self._send_fun({
      'band': 'transport',
      'call': 'broadcast',
      'payload': ( args, kwargs )
    })

  def _transaction_begin(self, *args, **kwargs):
    '''Forward transaction start command to queue.'''
    self.assert_connected()
    self._send_fun({
      'band': 'transport',
      'call': 'transaction_begin',
      'payload': ( args, kwargs )
    })

  def _transaction_abort(self, *args, **kwargs):
    '''Forward transaction abort command to queue.'''
    self.assert_connected()
    self._send_fun({
      'band': 'transport',
      'call': 'transaction_abort',
      'payload': ( args, kwargs )
    })

  def _transaction_commit(self, *args, **kwargs):
    '''Forward transaction commit command to queue.'''
    self.assert_connected()
    self._send_fun({
      'band': 'transport',
      'call': 'transaction_commit',
      'payload': ( args, kwargs )
    })

  def _subscribe(self, subscription_id, channel, callback, **kwargs):
    '''Forward subscription command to queue.'''
    self.assert_connected()
    self._send_fun({
      'band': 'transport',
      'call': 'subscribe',
      'payload': ( (subscription_id, channel), kwargs )
    })

  def _subscribe_broadcast(self, subscription_id, channel, callback, **kwargs):
    '''Forward broadcast subscription command to queue.'''
    self.assert_connected()
    self._send_fun({
      'band': 'transport',
      'call': 'subscribe_broadcast',
      'payload': ( (subscription_id, channel), kwargs )
    })

  def _unsubscribe(self, *args, **kwargs):
    '''Forward unsubscribe command to queue.'''
    self.assert_connected()
    self._send_fun({
      'band': 'transport',
      'call': 'unsubscribe',
      'payload': ( args, kwargs )
    })

  def _ack(self, messageid, subscription, *args, **kwargs):
    '''Forward receipt acknowledgement to queue.'''
    self.assert_connected()
    self._send_fun({
      'band': 'transport',
      'call': 'ack',
      'payload': ( (messageid, subscription) + args, kwargs )
    })

  def _nack(self, messageid, subscription, *args, **kwargs):
    '''Forward receipt rejection to queue.'''
    self.assert_connected()
    self._send_fun({
      'band': 'transport',
      'call': 'nack',
      'payload': ( (messageid, subscription) + args, kwargs )
    })
