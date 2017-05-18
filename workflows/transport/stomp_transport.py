from __future__ import absolute_import, division
try:
  import ConfigParser
except ImportError: # Python3 compatibility
  import configparser as ConfigParser
import json
import stomp
import threading
import time
from workflows import WorkflowsError, DisconnectedError, AuthenticationError
from workflows.transport.common_transport import CommonTransport

class StompTransport(CommonTransport):
  '''Abstraction layer for messaging infrastructure. Here we are using ActiveMQ
     with STOMP.'''

  # Set some sensible defaults
  defaults = {
    '--stomp-host': 'localhost',
    '--stomp-port': 61613,
    '--stomp-user': 'admin',
    '--stomp-pass': 'password',
    '--stomp-prfx': '',
  }
  # Effective configuration
  config = {}

  def __init__(self):
    self._connected = False
    self._namespace = ''
    self._idcounter = 0
    self._subscription_callbacks = {}
    self._lock = threading.RLock()
    self._stomp_listener = stomp.listener.ConnectionListener()
#   self._stomp_listener = stomp.PrintingListener()
    self._stomp_listener.on_message = self._on_message
    self._stomp_listener.on_before_message = lambda x, y: (x, y)

  def get_namespace(self):
    '''Return the stomp namespace. This is a prefix used for all topic and
       queue names.'''
    if self._namespace.endswith('.'):
      return self._namespace[:-1]
    return self._namespace

  @classmethod
  def _set_parameter(cls, option, opt, value, parser):
    '''callback function for optionparser'''
    cls.config[opt] = value
    if opt == '--stomp-conf':
      cls.load_configuration_file(value)

  @classmethod
  def load_configuration_file(cls, filename):
    cfgparser = ConfigParser.ConfigParser(allow_no_value=True)
    if not cfgparser.read(filename):
      raise WorkflowsError('Could not read from configuration file %s' % filename)
    for cfgoption, target in [
          ('host', '--stomp-host'),
          ('port', '--stomp-port'),
          ('password', '--stomp-pass'),
          ('username', '--stomp-user'),
          ('prefix', '--stomp-prfx'),
          ]:
      try:
        cls.defaults[target] = cfgparser.get('stomp', cfgoption)
      except ConfigParser.NoOptionError:
        pass

  @classmethod
  def add_command_line_options(cls, optparser):
    '''function to inject command line parameters'''
    optparser.add_option('--stomp-host', metavar='HOST',
      default=cls.defaults.get('--stomp-host'),
      help="Stomp broker address, default '%default'",
      type='string', nargs=1,
      action='callback', callback=cls._set_parameter)
    optparser.add_option('--stomp-port', metavar='PORT',
      default=cls.defaults.get('--stomp-port'),
      help="Stomp broker port, default '%default'",
      type='int', nargs=1,
      action='callback', callback=cls._set_parameter)
    optparser.add_option('--stomp-user', metavar='USER',
      default=cls.defaults.get('--stomp-user'),
      help="Stomp user, default '%default'",
      type='string', nargs=1,
      action='callback', callback=cls._set_parameter)
    optparser.add_option('--stomp-pass', metavar='PASS',
      default=cls.defaults.get('--stomp-pass'),
      help="Stomp password",
      type='string', nargs=1,
      action='callback', callback=cls._set_parameter)
    optparser.add_option('--stomp-prfx', metavar='PRE',
      default=cls.defaults.get('--stomp-prfx'),
      help="Stomp namespace prefix, default '%default'",
      type='string', nargs=1,
      action='callback', callback=cls._set_parameter)
    optparser.add_option('--stomp-conf', metavar='CNF',
      default=cls.defaults.get('--stomp-conf'),
      help='Stomp configuration file containing connection information, disables default values',
      type='string', nargs=1,
      action='callback', callback=cls._set_parameter)

  def connect(self):
    with self._lock:
      if self._connected:
        return True
      self._conn = stomp.Connection([(
        self.config.get('--stomp-host', self.defaults.get('--stomp-host')),
        int(self.config.get('--stomp-port', self.defaults.get('--stomp-port'))),
        )])
      self._conn.set_listener('', self._stomp_listener)
      try:
        self._conn.start()
      except stomp.exception.ConnectFailedException:
        raise DisconnectedError('Could not initiate connection to stomp host')
      username = self.config.get('--stomp-user',
                                 self.defaults.get('--stomp-user'))
      password = self.config.get('--stomp-pass',
                                 self.defaults.get('--stomp-pass'))
      try:
        if username or password:
          self._conn.connect(username, password, wait=True)
        else: # anonymous access
          self._conn.connect(wait=True)
      except stomp.exception.ConnectFailedException:
        raise AuthenticationError('Could not connect to stomp host: Authentication error')
      self._namespace = \
        self.config.get('--stomp-prfx', self.defaults.get('--stomp-prfx'))
      if self._namespace and not self._namespace.endswith('.'):
        self._namespace = self._namespace + '.'
      self._connected = True
    return True

  def is_connected(self):
    '''Return connection status'''
    self._connected = self._connected and self._conn.is_connected()
    return self._connected

  def disconnect(self):
    '''Gracefully close connection to stomp server.'''
    with self._lock:
      if self._connected:
        self._conn.disconnect()
        self._connected = False

  def broadcast_status(self, status, channel=None):
    '''Broadcast transient status information to all listeners'''
    destination = ['/topic/' + self._namespace + 'transient.status']
    if channel:
      destination.append(channel)
    destination = '.'.join(destination)
    import json
    message = json.dumps(status)
    with self._lock:
      self._conn.send(
          destination, message,
          headers={
                    'expires': '%d' % int((90 + time.time()) * 1000)
                  })

  def _subscribe(self, sub_id, channel, callback, **kwargs):
    '''Listen to a queue, notify via callback function.
       :param sub_id: ID for this subscription in the transport layer
       :param channel: Queue name to subscribe to
       :param callback: Function to be called when messages are received
       :param **kwargs: Further parameters for the transport layer. For example
         acknowledgement:  If true receipt of each message needs to be
                           acknowledged.
         exclusive:        Attempt to become exclusive subscriber to the queue.
         ignore_namespace: Do not apply namespace to the destination name
         priority:         Consumer priority, messages are sent to higher
                           priority consumers whenever possible.
         selector:         Only receive messages filtered by a selector. See
                           https://activemq.apache.org/activemq-message-properties.html
                           for potential filter criteria. Uses SQL 92 syntax.
         transformation:   Transform messages into different format. If set
                           to True, will use 'jms-object-json' formatting.
    '''
    headers = {}
    if kwargs.get('exclusive'):
      headers['activemq.exclusive'] = 'true'
    if kwargs.get('ignore_namespace'):
      destination = '/queue/' + channel
    else:
      destination = '/queue/' + self._namespace + channel
    if kwargs.get('priority'):
      headers['activemq.priority'] = kwargs['priority']
    if kwargs.get('retroactive'):
      headers['activemq.retroactive'] = 'true'
    if kwargs.get('selector'):
      headers['selector'] = kwargs['selector']
    if kwargs.get('transformation'):
      if kwargs['transformation'] == True:
        headers['transformation'] = 'jms-object-json'
      else:
        headers['transformation'] = kwargs['transformation']
    if kwargs.get('acknowledgement'):
      ack = 'client-individual'
    else:
      ack = 'auto'
    self._subscription_callbacks[sub_id] = callback

    with self._lock:
      self._conn.subscribe(destination, sub_id, headers=headers, ack=ack)

  def _subscribe_broadcast(self, sub_id, channel, callback, **kwargs):
    '''Listen to a broadcast topic, notify via callback function.
       :param sub_id: ID for this subscription in the transport layer
       :param channel: Topic name to subscribe to
       :param callback: Function to be called when messages are received
       :param **kwargs: Further parameters for the transport layer. For example
         ignore_namespace: Do not apply namespace to the destination name
         retroactive:      Ask broker to send old messages if possible
         transformation:   Transform messages into different format. If set
                           to True, will use 'jms-object-json' formatting.
    '''
    headers = {}
    if kwargs.get('ignore_namespace'):
      destination = '/topic/' + channel
    else:
      destination = '/topic/' + self._namespace + channel
    if kwargs.get('retroactive'):
      headers['activemq.retroactive'] = 'true'
    if kwargs.get('transformation'):
      if kwargs['transformation'] == True:
        headers['transformation'] = 'jms-object-json'
      else:
        headers['transformation'] = kwargs['transformation']
    self._subscription_callbacks[sub_id] = callback
    with self._lock:
      self._conn.subscribe(destination, sub_id, headers=headers)

  def _unsubscribe(self, subscription, **kwargs):
    '''Stop listening to a queue or a broadcast
       :param subscription: Subscription ID to cancel
    '''
    self._conn.unsubscribe(id=subscription)
    # Callback reference is kept as further messages may already have been received

  def _send(self, destination, message, headers=None, delay=None, **kwargs):
    '''Send a message to a queue.
       :param destination: Queue name to send to
       :param message: A string to be sent
       :param **kwargs: Further parameters for the transport layer. For example
         delay:            Delay transport of message by this many seconds
         expiration:       Optional expiration time, relative to sending time
         headers:          Optional dictionary of header entries
         ignore_namespace: Do not apply namespace to the destination name
         persistent:       Whether to mark messages as persistent, to be kept
                           between broker restarts. Default is 'true'.
         transaction:      Transaction ID if message should be part of a
                           transaction
    '''
    if not headers:
      headers = {}
    if 'persistent' not in headers:
      headers['persistent'] = 'true'
    if delay:
      # The 'delay' mechanism is only supported when
      # schedulerSupport is enabled on the broker.
      headers['AMQ_SCHEDULED_DELAY'] = 1000 * delay
    if kwargs.get('ignore_namespace'):
      destination = '/queue/' + destination
    else:
      destination = '/queue/' + self._namespace + destination
    with self._lock:
      try:
        self._conn.send(
          destination, message,
          headers=headers, **kwargs)
      except stomp.exception.NotConnectedException:
        self._connected = False
        raise DisconnectedError('No connection to stomp host')

  def _broadcast(self, destination, message, headers=None, delay=None, **kwargs):
    '''Broadcast a message.
       :param destination: Topic name to send to
       :param message: A string to be broadcast
       :param **kwargs: Further parameters for the transport layer. For example
         delay:            Delay transport of message by this many seconds
         expiration:       Optional expiration time, relative to sending time
         headers:          Optional dictionary of header entries
         ignore_namespace: Do not apply namespace to the destination name
         transaction:      Transaction ID if message should be part of a
                           transaction
    '''
    if not headers:
      headers = {}
    if delay:
      headers['AMQ_SCHEDULED_DELAY'] = 1000 * delay
    if kwargs.get('ignore_namespace'):
      destination = '/topic/' + destination
    else:
      destination = '/topic/' + self._namespace + destination
    with self._lock:
      try:
        self._conn.send(
          destination, message,
          headers=headers, **kwargs)
      except stomp.exception.NotConnectedException:
        self._connected = False
        raise

  def _transaction_begin(self, transaction_id, **kwargs):
    '''Start a new transaction.
       :param transaction_id: ID for this transaction in the transport layer.
       :param **kwargs: Further parameters for the transport layer.
    '''
    with self._lock:
      self._conn.begin(transaction=transaction_id)

  def _transaction_abort(self, transaction_id, **kwargs):
    '''Abort a transaction and roll back all operations.
       :param transaction_id: ID of transaction to be aborted.
       :param **kwargs: Further parameters for the transport layer.
    '''
    with self._lock:
      self._conn.abort(transaction_id)

  def _transaction_commit(self, transaction_id, **kwargs):
    '''Commit a transaction.
       :param transaction_id: ID of transaction to be committed.
       :param **kwargs: Further parameters for the transport layer.
    '''
    with self._lock:
      self._conn.commit(transaction_id)

  def _ack(self, message_id, subscription_id, **kwargs):
    '''Acknowledge receipt of a message. This only makes sense when the
       'acknowledgement' flag was set for the relevant subscription.
       :param message_id: ID of the message to be acknowledged
       :param subscription: ID of the relevant subscriptiong
       :param **kwargs: Further parameters for the transport layer. For example
              transaction: Transaction ID if acknowledgement should be part of
                           a transaction
    '''
    with self._lock:
      self._conn.ack(message_id, subscription_id, **kwargs)

  def _nack(self, message_id, subscription_id, **kwargs):
    '''Reject receipt of a message. This only makes sense when the
       'acknowledgement' flag was set for the relevant subscription.
       :param message_id: ID of the message to be rejected
       :param subscription: ID of the relevant subscriptiong
       :param **kwargs: Further parameters for the transport layer. For example
              transaction: Transaction ID if rejection should be part of a
                           transaction
    '''
    with self._lock:
      self._conn.nack(message_id, subscription_id, **kwargs)

  @staticmethod
  def _mangle_for_sending(message):
    '''Function that any message will pass through before it being forwarded to
       the actual _send* functions.
       Stomp only deals with serialized strings, so serialize message as json.
    '''
    return json.dumps(message)

  @staticmethod
  def _mangle_for_receiving(message):
    '''Function that any message will pass through before it being forwarded to
       the receiving subscribed callback functions.
       This transport class only deals with serialized strings, so decode
       message from json. However anything can come into here, so catch any
       deserialization errors.
    '''
    try:
      return json.loads(message)
    except (TypeError, ValueError):
      return message

## Stomp listener methods #####################################################

  def _on_message(self, headers, body):
    subscription_id = int(headers.get('subscription'))
    with self._lock:
      target_function = self._subscription_callbacks.get(subscription_id)
    if target_function is not None:
      target_function(headers, body)
    else:
      raise WorkflowsError('Unhandled message %s %s' % (repr(headers), repr(body)))
