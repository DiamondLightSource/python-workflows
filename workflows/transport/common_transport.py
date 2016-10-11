from __future__ import absolute_import, division
import json
import workflows

class CommonTransport(object):
  '''A common transport class, containing e.g. the logic to connect clients
     to message subscriptions and transactions, so that these can be cleanly
     terminated when the client goes away.'''

  __clients = {}
  __client_id = 0
  __subscriptions = {}
  __subscription_id = 0
  __transactions = {}
  __transaction_id = 0
  __messages = {}

  #
  # -- High level communication calls ----------------------------------------
  #

  def connect(self):
    '''Connect the transport class. This function has to be overridden.
       :return: True-like value when connection successful,
                False-like value otherwise.'''
    return False

  def subscribe(self, channel, callback, **kwargs):
    '''Listen to a queue, notify via callback function.
       :param channel: Queue name to subscribe to
       :param callback: Function to be called when messages are received
       :param **kwargs: Further parameters for the transport layer. For example
              client_id: Value tying a subscription to one client. This allows
                         removing all subscriptions for a client simultaneously
                         when the client goes away.
              exclusive: Attempt to become exclusive subscriber to the queue.
              acknowledgement: If true receipt of each message needs to be
                               acknowledged.
       :return: A unique subscription ID
    '''
    self.__subscription_id += 1
    self.__subscriptions[self.__subscription_id] = {
      'channel': channel,
      'client': kwargs.get('client_id'),
      'callback': callback,
      'ack': kwargs.get('acknowledgement'),
    }
    self._debug('Subscribing to %s for %s with ID %d' % \
        (channel, kwargs.get('client_id'), self.__subscription_id))
    if 'client_id' in kwargs:
      self.__clients[kwargs['client_id']]['subscriptions'].add \
        (self.__subscription_id)
      del(kwargs['client_id'])
    self._subscribe(self.__subscription_id, channel, callback, **kwargs)
    return self.__subscription_id

  def unsubscribe(self, subscription, **kwargs):
    '''Stop listening to a queue or a broadcast
       :param subscription: Subscription ID to cancel
       :param **kwargs: Further parameters for the transport layer.
    '''
    if subscription not in self.__subscriptions:
      raise workflows.WorkflowsError \
            ("Attempting to unsubscribe unknown subscription")
    if self.__subscriptions[subscription]['client']:
      self.__clients[self.__subscriptions[subscription]['client']] \
        ['subscriptions'].remove(subscription)
    self._unsubscribe(subscription, **kwargs)
    del(self.__subscriptions[subscription])

  def subscribe_broadcast(self, channel, callback, **kwargs):
    '''Listen to a broadcast topic, notify via callback function.
       :param channel: Topic name to subscribe to
       :param callback: Function to be called when messages are received
       :param client_id: Value tying a subscription to one client. This allows
                         removing all subscriptions for a client simultaneously
                         when the client goes away.
       :param **kwargs: Further parameters for the transport layer. For example
              retroactive: Ask broker to send old messages if possible
       :return: A unique subscription ID
    '''
    self.__subscription_id += 1
    self.__subscriptions[self.__subscription_id] = {
      'channel': channel,
      'client': kwargs.get('client_id'),
      'callback': callback,
      'ack': False,
    }
    if 'client_id' in kwargs:
      self.__clients[kwargs['client_id']]['subscriptions'].add \
        (self.__subscription_id)
      del(kwargs['client_id'])
    self._subscribe_broadcast(self.__subscription_id, channel, callback, \
        **kwargs)
    return self.__subscription_id

  def subscription_callback(self, subscription):
    '''Retrieve the callback function for a subscription. Raise a
       WorkflowsError if the subscription does not exist.
       :param subscription: Subscription ID to look up
       :return: Callback function
    '''
    if subscription not in self.__subscriptions:
      raise workflows.WorkflowsError \
            ("Attempting to callback on unknown subscription")
    return self.__subscriptions[subscription]['callback']

  def send(self, destination, message, **kwargs):
    '''Send a message to a queue.
       :param destination: Queue name to send to
       :param message: Either a string or a serializable object to be sent
       :param **kwargs: Further parameters for the transport layer. For example
              headers: Optional dictionary of header entries
              expiration: Optional expiration time, relative to sending time
              transaction: Transaction ID if message should be part of a
                           transaction
    '''
    if not isinstance(message, basestring):
      message = json.dumps(message)
    self._send(destination, message, **kwargs)

  def broadcast(self, destination, message, **kwargs):
    '''Broadcast a message.
       :param destination: Topic name to send to
       :param message: Either a string or a serializable object to be sent
       :param **kwargs: Further parameters for the transport layer. For example
              headers: Optional dictionary of header entries
              expiration: Optional expiration time, relative to sending time
              transaction: Transaction ID if message should be part of a
                           transaction
    '''
    if not isinstance(message, basestring):
      message = json.dumps(message)
    self._broadcast(destination, message, **kwargs)

  def ack(self, message_id, **kwargs):
    '''Acknowledge receipt of a message. This only makes sense when the
       'acknowledgement' flag was set for the relevant subscription.
       :param message_id: ID of the message to be acknowledged
       :param **kwargs: Further parameters for the transport layer. For example
              transaction: Transaction ID if acknowledgement should be part of
                           a transaction
    '''
    if message_id not in self.__messages:
      raise workflows.WorkflowsError \
            ("Attempting to ACK unknown message")
    subscription, client = self.__messages[message_id]
    if client:
      self.__clients[client]['messages'].remove(message_id)
    del(self.__messages[message_id])
    self._ack(message_id, subscription, **kwargs)

  def nack(self, message_id, **kwargs):
    '''Reject receipt of a message. This only makes sense when the
       'acknowledgement' flag was set for the relevant subscription.
       :param message_id: ID of the message to be rejected
       :param **kwargs: Further parameters for the transport layer. For example
              transaction: Transaction ID if rejection should be part of a
                           transaction
    '''
    if message_id not in self.__messages:
      raise workflows.WorkflowsError \
            ("Attempting to NACK unknown message")
    subscription, client = self.__messages[message_id]
    if client:
      self.__clients[client]['messages'].remove(message_id)
    del(self.__messages[message_id])
    self._nack(message_id, subscription, **kwargs)

  def subscription_requires_ack(self, subscription):
    '''Report if messages belonging to a particular subscription require to be
       acknowledged manually.
       :param subscription: ID of the subscription to test
       :return: Boolean value, true if messages need to be ACKed/NACKed.
    '''
    if subscription not in self.__subscriptions:
      raise workflows.WorkflowsError \
            ("Unknown subscription")
    return self.__subscriptions[subscription]['ack']

  def register_message(self, subscription, message_id):
    '''Mark an incoming message to be ACKed/NACKed. This is only relevant when
       'acknowledgement' flag was set for the relevant subscription, and will
       raise a WorkflowsError otherwise. The message_id will be tracked, and
       if it is not ACKed/NACKed, the transport layer will NACK it once it is
       clear that the message can no longer be processed (ie. when the
       associated client goes away).
       :param subscription: ID of the subscription that the incoming message
                            belongs to.
       :param message_id: ID of the message to be rejected
    '''
    if not self.subscription_requires_ack(subscription):
      raise workflows.WorkflowsError \
            ("Attempting to register message for subscription without"
             "acknowledgement flag set")
    client = self.__subscriptions[subscription]['client']
    self.__messages[message_id] = (subscription, client)
    if client:
      self.__clients[client]['messages'].add(message_id)

  def transaction_begin(self, **kwargs):
    '''Start a new transaction.
       :param **kwargs: Further parameters for the transport layer. For example
              client_id: Value tying a transaction to one client. This allows
                         aborting all transactions for a client simultaneously
                         when the client goes away.
       :return: A transaction ID that can be passed to other functions.
    '''
    self.__transaction_id += 1
    self.__transactions[self.__transaction_id] = {
      'client': kwargs.get('client_id'),
    }
    if 'client_id' in kwargs:
      self.__clients[kwargs['client_id']]['transactions'].add \
        (self.__transaction_id)
      del(kwargs['client_id'])
    self._transaction_begin(self.__transaction_id, **kwargs)
    return self.__transaction_id

  def transaction_abort(self, transaction_id, **kwargs):
    '''Abort a transaction and roll back all operations.
       :param transaction_id: ID of transaction to be aborted.
       :param **kwargs: Further parameters for the transport layer.
    '''
    if transaction_id not in self.__transactions:
      raise workflows.WorkflowsError("Attempting to abort unknown transaction")
    if self.__transactions[transaction_id]['client']:
      self.__clients[self.__transactions[transaction_id]['client']] \
        ['transactions'].remove(transaction_id)
    del(self.__transactions[transaction_id])
    self._transaction_abort(transaction_id, **kwargs)

  def transaction_commit(self, transaction_id, **kwargs):
    '''Commit a transaction.
       :param transaction_id: ID of transaction to be committed.
       :param **kwargs: Further parameters for the transport layer.
    '''
    if transaction_id not in self.__transactions:
      raise workflows.WorkflowsError("Attempting to commit unknown transaction")
    if self.__transactions[transaction_id]['client']:
      self.__clients[self.__transactions[transaction_id]['client']] \
        ['transactions'].remove(transaction_id)
    del(self.__transactions[transaction_id])
    self._transaction_commit(transaction_id, **kwargs)

  #
  # -- Client management calls -----------------------------------------------
  #

  def register_client(self):
    '''Generates a new unique client ID. Subscriptions and transactions can be
       tied to client IDs, so that they can be collectively dropped when
       clients go away.'''
    self.__client_id += 1
    self.__clients[self.__client_id] = { 'subscriptions': set(),
                                         'transactions': set(),
                                         'messages': set() }
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
    messages = list(self.__clients[client_id]['messages'])
    for message_id in messages:
      self.nack(message_id)
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

  def _subscribe(self, sub_id, channel, callback, **kwargs):
    '''Listen to a queue, notify via callback function.
       :param sub_id: ID for this subscription in the transport layer
       :param channel: Queue name to subscribe to
       :param callback: Function to be called when messages are received
       :param **kwargs: Further parameters for the transport layer. For example
              exclusive: Attempt to become exclusive subscriber to the queue.
              acknowledgement: If true receipt of each message needs to be
                               acknowledged.
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _subscribe_broadcast(self, sub_id, channel, callback, **kwargs):
    '''Listen to a broadcast topic, notify via callback function.
       :param sub_id: ID for this subscription in the transport layer
       :param channel: Topic name to subscribe to
       :param callback: Function to be called when messages are received
       :param **kwargs: Further parameters for the transport layer. For example
              retroactive: Ask broker to send old messages if possible
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _unsubscribe(self, sub_id):
    '''Stop listening to a queue or a broadcast
       :param sub_id: ID for this subscription in the transport layer
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _send(self, destination, message, **kwargs):
    '''Send a message to a queue.
       :param destination: Queue name to send to
       :param message: A string to be sent
       :param **kwargs: Further parameters for the transport layer. For example
              headers: Optional dictionary of header entries
              expiration: Optional expiration time, relative to sending time
              transaction: Transaction ID if message should be part of a
                           transaction
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _broadcast(self, destination, message, **kwargs):
    '''Broadcast a message.
       :param destination: Topic name to send to
       :param message: A string to be broadcast
       :param **kwargs: Further parameters for the transport layer. For example
              headers: Optional dictionary of header entries
              expiration: Optional expiration time, relative to sending time
              transaction: Transaction ID if message should be part of a
                           transaction
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _ack(self, message_id, subscription, **kwargs):
    '''Acknowledge receipt of a message. This only makes sense when the
       'acknowledgement' flag was set for the relevant subscription.
       :param message_id: ID of the message to be acknowledged
       :param subscription: ID of the relevant subscriptiong
       :param **kwargs: Further parameters for the transport layer. For example
              transaction: Transaction ID if acknowledgement should be part of
                           a transaction
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _nack(self, message_id, subscription, **kwargs):
    '''Reject receipt of a message. This only makes sense when the
       'acknowledgement' flag was set for the relevant subscription.
       :param message_id: ID of the message to be rejected
       :param subscription: ID of the relevant subscriptiong
       :param **kwargs: Further parameters for the transport layer. For example
              transaction: Transaction ID if rejection should be part of a
                           transaction
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _transaction_begin(self, transaction_id, **kwargs):
    '''Start a new transaction.
       :param transaction_id: ID for this transaction in the transport layer.
       :param **kwargs: Further parameters for the transport layer.
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _transaction_abort(self, transaction_id, **kwargs):
    '''Abort a transaction and roll back all operations.
       :param transaction_id: ID of transaction to be aborted.
       :param **kwargs: Further parameters for the transport layer.
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  def _transaction_commit(self, transaction_id, **kwargs):
    '''Commit a transaction.
       :param transaction_id: ID of transaction to be committed.
       :param **kwargs: Further parameters for the transport layer.
    '''
    raise workflows.WorkflowsError("Transport interface not implemented")

  #
  # -- Plugin-related functions ----------------------------------------------
  #

  class __metaclass__(type):
    '''Define metaclass function to keep a list of all subclasses. This enables
       looking up transport mechanisms by name.'''
    def __init__(cls, name, base, attrs):
      '''Add new subclass of CommonTransport to list of all known subclasses.'''
      if not hasattr(cls, 'transport_register'):
        cls.transport_register = {}
      else:
        cls.transport_register[name] = cls

  @classmethod
  def load(cls, paths):
    '''Import all python files (except test_*) in directories. This is required
       for registration of subclasses.'''
    import imp, pkgutil
    if isinstance(paths, basestring):
      paths = list(paths)
    cls.registered = []
    for _, name, _ in pkgutil.iter_modules(paths):
      if not name.startswith('test_'):
        fid, pathname, desc = imp.find_module(name, paths)
        imp.load_module(name, fid, pathname, desc)
        if fid:
          fid.close()
