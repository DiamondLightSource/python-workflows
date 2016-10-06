from __future__ import division
import json
import workflows
import workflows.transport

all_transports = {
  'stomp': lambda: workflows.transport.stomp.Transport
}
default_transport = 'stomp'

def lookup(transport):
  '''Get a transport layer class based on its name.'''
  return all_transports.get(transport, all_transports[default_transport])()

def add_command_line_options(parser):
  '''Add command line options for all available transport layer classes.'''
  for transport in all_transports.itervalues():
    transport().add_command_line_options(parser)


class CommonTransport():
  '''A common transport class, containing e.g. the logic to connect clients
     to message subscriptions and transactions, so that these can be cleanly
     terminated when the client goes away.'''

  __clients = {}
  __client_id = 0
  __subscriptions = {}
  __subscription_id = 0
  __transactions = {}
  __transaction_id = 0

  #
  # -- High level communication calls ----------------------------------------
  #

  def subscribe(self, channel, callback, client_id=None, exclusive=False,
                acknowledgement=False):
    '''Listen to a queue, notify via callback function.
       :param channel: Queue name to subscribe to
       :param callback: Function to be called when messages are received
       :param client_id: Value tying a subscription to one client. This allows
                         removing all subscriptions for a client simultaneously
                         when the client goes away.
       :param exclusive: Attempt to become exclusive subscriber to the queue.
       :param acknowledgement: If true receipt of each message needs to be
                               acknowledged.
       :return: A unique subscription ID
    '''
    self.__subscription_id += 1
    self.__subscriptions[self.__subscription_id] = {
      'channel': channel, 'client': client_id
    }
    if client_id:
      self.__clients[client_id]['subscriptions'].add(self.__subscription_id)
    self._subscribe(self.__subscription_id, channel, callback, exclusive,
                    acknowledgement)
    return self.__subscription_id

  def unsubscribe(self, subscription):
    '''Stop listening to a queue or a broadcast
       :param subscription: Subscription ID to cancel
    '''
    if subscription not in self.__subscriptions:
      raise workflows.WorkflowsError \
            ("Attempting to unsubscribe unknown subscription")
    if self.__subscriptions[subscription]['client']:
      self.__clients[self.__subscriptions[subscription]['client']] \
        ['subscriptions'].remove(subscription)
    self._unsubscribe(subscription)
    del(self.__subscriptions[subscription])

  def subscribe_broadcast(self, channel, callback, client_id=None,
                          retroactive=False):
    '''Listen to a broadcast topic, notify via callback function.
       :param channel: Topic name to subscribe to
       :param callback: Function to be called when messages are received
       :param client_id: Value tying a subscription to one client. This allows
                         removing all subscriptions for a client simultaneously
                         when the client goes away.
       :param retroactive: Ask broker to send old messages if possible
    '''
    self.__subscription_id += 1
    self.__subscriptions[self.__subscription_id] = {
      'channel': channel, 'client': client_id
    }
    if client_id:
      self.__clients[client_id]['subscriptions'].add(self.__subscription_id)
    self._subscribe_broadcast(self.__subscription_id, channel, callback,
                              retroactive)
    return self.__subscription_id

  def send(self, destination, message, headers=None, expiration=None,
           transaction=None):
    '''Send a message to a queue.
       :param destination: Queue name to send to
       :param message: Either a string or a serializable object to be sent
       :param headers: Optional dictionary of header entries
       :param expiration: Optional expiration time, relative to sending time
       :param transaction: Transaction ID if message should be part of a
                           transaction
    '''
    if not isinstance(message, basestring):
      message = json.dumps(message)
    self._send(destination, message, headers, expiration, transaction)

  def broadcast(self, destination, message, headers=None, expiration=None,
                transaction=None):
    '''Broadcast a message.
       :param destination: Topic name to send to
       :param message: Either a string or a serializable object to be sent
       :param headers: Optional dictionary of header entries
       :param expiration: Optional expiration time, relative to sending time
       :param transaction: Transaction ID if message should be part of a
                           transaction
    '''
    if not isinstance(message, basestring):
      message = json.dumps(message)
    self._broadcast(destination, message, headers, expiration, transaction)

  def ack(self, message_id, transaction=None):
    '''Acknowledge receipt of a message. This only makes sense when the
       'acknowledgment' flag was set for the relevant subscription.
       :param message_id: ID of the message to be acknowledged
       :param transaction: Transaction ID if acknowledgement should be part of
                           a transaction
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def nack(self, message_id, transaction=None):
    '''Reject receipt of a message. This only makes sense when the
       'acknowledgment' flag was set for the relevant subscription.
       :param message_id: ID of the message to be rejected
       :param transaction: Transaction ID if rejection should be part of a
                           transaction
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def transaction_begin(self, client_id=None):
    '''Start a new transaction.
       :param client_id: Value tying a transaction to one client. This allows
                         aborting all transactions for a client simultaneously
                         when the client goes away.
       :return: A transaction ID that can be passed to other functions.
    '''
    self.__transaction_id += 1
    self.__transactions[self.__transaction_id] = {
      'client': client_id
    }
    if client_id:
      self.__clients[client_id]['transactions'].add(self.__transaction_id)
    self._transaction_begin(self.__transaction_id)
    return self.__transaction_id

  def transaction_abort(self, transaction_id):
    '''Abort a transaction and roll back all operations.
       :param transaction_id: ID of transaction to be aborted.
    '''
    if transaction_id not in self.__transactions:
      raise workflows.WorkflowsError("Attempting to abort unknown transaction")
    if self.__transactions[transaction_id]['client']:
      self.__clients[self.__transactions[transaction_id]['client']] \
        ['transactions'].remove(transaction_id)
    del(self.__transactions[transaction_id])
    self._transaction_abort(transaction_id)

  def transaction_commit(self, transaction_id):
    '''Commit a transaction.
       :param transaction_id: ID of transaction to be committed.
    '''
    if transaction_id not in self.__transactions:
      raise workflows.WorkflowsError("Attempting to commit unknown transaction")
    if self.__transactions[transaction_id]['client']:
      self.__clients[self.__transactions[transaction_id]['client']] \
        ['transactions'].remove(transaction_id)
    del(self.__transactions[transaction_id])
    self._transaction_commit(transaction_id)

  #
  # -- Client management calls -----------------------------------------------
  #

  def register_client(self):
    '''Generates a new unique client ID. Subscriptions and transactions can be
       tied to client IDs, so that they can be collectively dropped when
       clients go away.'''
    self.__client_id += 1
    self.__clients[self.__client_id] = { 'subscriptions': set(),
                                         'transactions': set() }
    return self.__client_id

  def drop_client(self, client_id):
    '''Remove a client ID and all connected subscriptions, transactions and
       unacknowledged messages.
       :param client_id: Client to be dropped.'''
    if client_id not in self.__clients:
      raise workflows.WorkflowsError("Attempting to drop unregistered client")
    channel_subscriptions = list(self.__clients[client_id]['subscriptions'])
    for subscription in channel_subscriptions:
      self.unsubscribe(subscription)
    transactions = list(self.__clients[client_id]['transactions'])
    for transaction in transactions:
      self.transaction_abort(transaction)
    del(self.__clients[client_id])

  #
  # -- Debugging -------------------------------------------------------------
  #

  @staticmethod
  def _debug(debug_info):
    '''An overrideable central debugging function.'''
    print(debug_info)

  #
  # -- Low level communication calls to be implemented by subclass -----------
  #

  def _subscribe(self, sub_id, channel, callback, exclusive, acknowledgement):
    '''Listen to a queue, notify via callback function.
       :param sub_id: ID for this subscription in the transport layer
       :param channel: Queue name to subscribe to
       :param callback: Function to be called when messages are received
       :param exclusive: Attempt to become exclusive subscriber to the queue.
       :param acknowledgement: If true receipt of each message needs to be
                               acknowledged.
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _subscribe_broadcast(self, sub_id, channel, callback, retroactive):
    '''Listen to a broadcast topic, notify via callback function.
       :param sub_id: ID for this subscription in the transport layer
       :param channel: Topic name to subscribe to
       :param callback: Function to be called when messages are received
       :param retroactive: Ask broker to send old messages if possible
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _unsubscribe(self, sub_id):
    '''Stop listening to a queue or a broadcast
       :param sub_id: ID for this subscription in the transport layer
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _send(self, destination, message, headers, expiration, transaction):
    '''Send a message to a queue.
       :param destination: Queue name to send to
       :param message: A string to be sent
       :param headers: Optional dictionary of header entries
       :param expiration: Optional expiration time, relative to sending time
       :param transaction: Transaction ID if message should be part of a
                           transaction
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _broadcast(self, destination, message, headers, expiration,
                 transaction):
    '''Broadcast a message.
       :param destination: Topic name to send to
       :param message: A string to be broadcast
       :param headers: Optional dictionary of header entries
       :param expiration: Optional expiration time, relative to sending time
       :param transaction: Transaction ID if message should be part of a
                           transaction
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _ack(self, message_id, transaction):
    '''Acknowledge receipt of a message. This only makes sense when the
       'acknowledgment' flag was set for the relevant subscription.
       :param message_id: ID of the message to be acknowledged
       :param transaction: Transaction ID if acknowledgement should be part of
                           a transaction
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _nack(self, message_id, transaction):
    '''Reject receipt of a message. This only makes sense when the
       'acknowledgment' flag was set for the relevant subscription.
       :param message_id: ID of the message to be rejected
       :param transaction: Transaction ID if rejection should be part of a
                           transaction
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _transaction_begin(self, transaction_id):
    '''Start a new transaction.
       :param transaction_id: ID for this transaction in the transport layer.
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _transaction_abort(self, transaction_id):
    '''Abort a transaction and roll back all operations.
       :param transaction_id: ID of transaction to be aborted.
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _transaction_commit(self, transaction_id):
    '''Commit a transaction.
       :param transaction_id: ID of transaction to be committed.
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")
