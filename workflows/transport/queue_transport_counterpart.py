from __future__ import absolute_import, division

import logging

class QueueTransportCounterpart(object):
  '''The counterpart object to a QueueTransport, which can process serialized
     Transport object commands and apply them to another Transport object.
  '''

  def __init__(self, transport=None, send_to_queue=None):
    # TODO: Comment

    self.log = logging.getLogger('workflows.transport.queue_transport_counterpart')
    self._transaction_mapping = {}
    self._transport = transport
    self._send_to_queue = send_to_queue
    self._subscription_mapping = {}

  def handle(self, message, client_id=None):
    '''Treat messages received via a queue. These were encoded by
       transport.queue_transport on the other end. Pass the messages to
       individual functions depending on the 'call' field.
       param: message: The full queue message data structure.
    '''
    try:
      handler = getattr(self, 'handle_' + message['call'])
    except AttributeError:
      self.log.error("Unknown transport handler '%s'.\n%s", str(message.get('call')), str(message)[:1000])
      return
    if 'transaction' in message['payload'][1]:
#     print("Mapping transaction %s to %s for %s" % (
#         str( message['payload'][1]['transaction'] ),
#         str( self._transaction_mapping[message['payload'][1]['transaction']] ),
#         message['call']))
      message['payload'][1]['transaction'] = \
        self._transaction_mapping[message['payload'][1]['transaction']]
    handler(message, client_id=client_id)

  def handle_send(self, message, **kwargs):
    '''Counterpart to the transport.send call from the service.
       Forward the call to the real transport layer.'''
    margs, mkwargs = message['payload']
    self._transport.send(*margs, **mkwargs)

  def handle_broadcast(self, message, **kwargs):
    '''Counterpart to the transport.broadcast call from the service.
       Forward the call to the real transport layer.'''
    margs, mkwargs = message['payload']
    self._transport.broadcast(*margs, **mkwargs)

  def handle_ack(self, message, **kwargs):
    '''Counterpart to the transport.ack call from the service.
       Forward the call to the real transport layer.'''
    margs, mkwargs = message['payload']
    message_id, subscription_id = margs[:2]
    assert subscription_id in self._subscription_mapping
    self._transport.ack(message_id,
                        self._subscription_mapping[subscription_id],
                        *margs[2:], **mkwargs)

  def handle_nack(self, message, **kwargs):
    '''Counterpart to the transport.nack call from the service.
       Forward the call to the real transport layer.'''
    margs, mkwargs = message['payload']
    message_id, subscription_id = margs[:2]
    assert subscription_id in self._subscription_mapping
    self._transport.nack(message_id,
                         self._subscription_mapping[subscription_id],
                         *margs[2:], **mkwargs)

  def handle_transaction_begin(self, message, **kwargs):
    '''Counterpart to the transport.transaction_begin call from the service.
       Forward the call to the real transport layer.'''
    margs, mkwargs = message['payload']
    service_txn_id = margs[0]
    self._transaction_mapping[service_txn_id] = \
      self._transport.transaction_begin(client_id=kwargs.get('client_id'))

  def handle_transaction_commit(self, message, **kwargs):
    '''Counterpart to the transport.transaction_begin call from the service.
       Forward the call to the real transport layer.'''
    margs, mkwargs = message['payload']
    service_txn_id = margs[0]
    assert service_txn_id in self._transaction_mapping
    self._transport.transaction_commit(self._transaction_mapping[service_txn_id])
    del self._transaction_mapping[service_txn_id]

  def handle_transaction_abort(self, message, **kwargs):
    '''Counterpart to the transport.transaction_abort call from the service.
       Forward the call to the real transport layer.'''
    margs, mkwargs = message['payload']
    service_txn_id = margs[0]
    assert service_txn_id in self._transaction_mapping
    self._transport.transaction_abort(self._transaction_mapping[service_txn_id])
    del self._transaction_mapping[service_txn_id]

  def handle_subscribe(self, message, **kwargs):
    '''Counterpart to the transport.subscribe call from the service.
       Forward the call to the real transport layer.'''
    subscription_id, channel = message['payload'][0][:2]
    def callback_helper(cb_header, cb_message):
      '''Helper function to rewrite the message header and redirect it back
         towards the queue.'''
      cb_header['subscription'] = subscription_id
      self._send_to_queue( {
        'band': 'transport_message',
        'payload': {
          'subscription_id': subscription_id,
          'header': cb_header,
          'message': cb_message,
        }
      } )
    self._subscription_mapping[subscription_id] = self._transport.subscribe(
      channel,
      callback_helper,
      *message['payload'][0][2:],
      client_id=kwargs.get('client_id'),
      **message['payload'][1]
    )

  def handle_subscribe_broadcast(self, message, **kwargs):
    '''Counterpart to the transport.subscribe_broadcast call from the service.
       Forward the call to the real transport layer.'''
    subscription_id, channel = message['payload'][0][:2]
    def callback_helper(cb_header, cb_message):
      '''Helper function to rewrite the message header and redirect it back
         towards the queue.'''
      cb_header['subscription'] = subscription_id
      self._send_to_queue( {
        'band': 'transport_message',
        'payload': {
          'subscription_id': subscription_id,
          'header': cb_header,
          'message': cb_message,
        }
      } )
    self._subscription_mapping[subscription_id] = self._transport.subscribe_broadcast(
      channel,
      callback_helper,
      *message['payload'][0][2:],
      client_id=kwargs.get('client_id'),
      **message['payload'][1]
    )

  def handle_unsubscribe(self, message, **kwargs):
    '''Counterpart to the transport.unsubscribe call from the service.
       Forward the call to the real transport layer.'''
    subscription_id = message['payload'][0][0]
    self._transport.unsubscribe(self._subscription_mapping[subscription_id])
    del self._subscription_mapping[subscription_id]
