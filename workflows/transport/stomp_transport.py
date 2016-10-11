from __future__ import absolute_import, division
import stomp
import threading
import time
from workflows import WorkflowsError
from workflows.transport.common_transport import CommonTransport

class StompTransport(CommonTransport):
  '''Abstraction layer for messaging infrastructure. Here we are using ActiveMQ
     with STOMP.'''

  def __init__(self):
    # Set some sensible defaults
    self.defaults = {
      '--stomp-host': 'localhost',
      '--stomp-port': 61613,
      '--stomp-user': 'admin',
      '--stomp-pass': 'password',
      '--stomp-prfx': 'demo'
    }
    # Effective configuration
    self.config = {}

    self._connected = False
    self._namespace = None
    self._idcounter = 0
    self._subscription_callbacks = {}
    self._lock = threading.RLock()
    self._stomp_listener = stomp.listener.ConnectionListener()
    self._stomp_listener = stomp.PrintingListener()
    self._stomp_listener.on_message = self._on_message
    self._stomp_listener.on_before_message = self._on_before_message

  def get_namespace(self):
    '''Return the stomp namespace. This is a prefix used for all topic and
       queue names.'''
    return self._namespace

  def _set_parameter(self, option, opt, value, parser):
    '''callback function for optionparser'''
    self.config[opt] = value
    if opt == '--stomp-conf':
      import ConfigParser
      cfgparser = ConfigParser.ConfigParser(allow_no_value=True)
      if not cfgparser.read(value):
        raise WorkflowsError('Could not read from configuration file %s' % value)
      for cfgoption, target in [
          ('host', '--stomp-host'),
          ('port', '--stomp-port'),
          ('password', '--stomp-pass'),
          ('username', '--stomp-user'),
          ('prefix', '--stomp-prfx'),
          ]:
        try:
          self.defaults[target] = cfgparser.get('stomp', cfgoption)
        except ConfigParser.NoOptionError:
          pass

  def add_command_line_options(self, optparser):
    '''function to inject command line parameters'''
    optparser.add_option('--stomp-host', metavar='HOST',
      default=self.defaults.get('--stomp-host'),
      help="Stomp broker address, default '%default'",
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-port', metavar='PORT',
      default=self.defaults.get('--stomp-port'),
      help="Stomp broker port, default '%default'",
      type='int', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-user', metavar='USER',
      default=self.defaults.get('--stomp-user'),
      help="Stomp user, default '%default'",
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-pass', metavar='PASS',
      default=self.defaults.get('--stomp-pass'),
      help="Stomp password, default '%default'",
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-prfx', metavar='PRE',
      default=self.defaults.get('--stomp-prfx'),
      help="Stomp namespace prefix, default '%default'",
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)
    optparser.add_option('--stomp-conf', metavar='CNF',
      default=self.defaults.get('--stomp-conf'),
      help='Stomp configuration file containing connection information, disables default values',
      type='string', nargs=1,
      action='callback', callback=self._set_parameter)

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
        return False
      self._conn.connect(
        self.config.get('--stomp-user', self.defaults.get('--stomp-user')),
        self.config.get('--stomp-pass', self.defaults.get('--stomp-pass')),
        wait=True)
      self._namespace = \
        self.config.get('--stomp-prfx', self.defaults.get('--stomp-prfx'))
      if self._namespace:
        self._namespace + '.'
      self._connected = True
    return True

  def is_connected(self):
    '''Return connection status'''
    return self._connected

  def disconnect(self):
    with self._lock:
      if self._connected:
        pass # TODO

  def broadcast_status(self, status, channel=None):
    '''Broadcast transient status information to all listeners'''
    destination = ['/topic/transient.status']
    if self.get_namespace():
      destination.append(self.get_namespace())
    if channel:
      destination.append(channel)
    destination = '.'.join(destination)
    import json
    message = json.dumps(status)
    with self._lock:
      self._conn.send(
          body=message,
          destination=destination,
          headers={
                    'expires': '%d' % int((90 + time.time()) * 1000)
                  })

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
    headers = {}
    if kwargs.get('retroactive'):
      headers['activemq.retroactive'] = 'true'
    if kwargs.get('acknowledgement'):
      ack = 'client-individual'
      def callback_bounce(header, message):
        self.register_message(sub_id, header['message-id'])
        callback(header, message)
      self._subscription_callbacks[sub_id] = callback_bounce
    else:
      ack = 'auto'
      self._subscription_callbacks[sub_id] = callback

    with self._lock:
      self._conn.subscribe('/queue/' + channel, sub_id, headers=headers, ack=ack)

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
    headers = kwargs.get('headers')
    if not headers:
      headers = {}
#   TODO: Does not take prefix into account
    destination = '/queue/' + destination
    with self._lock:
      self._conn.send(
          body=message,
          destination=destination,
          headers=headers)

  def _ack(self, message_id, subscription_id, **kwargs):
    '''Acknowledge receipt of a message. This only makes sense when the
       'acknowledgement' flag was set for the relevant subscription.
       :param message_id: ID of the message to be acknowledged
       :param subscription: ID of the relevant subscriptiong
       :param **kwargs: Further parameters for the transport layer. For example
              transaction: Transaction ID if acknowledgement should be part of
                           a transaction
    '''
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
    self._conn.nack(message_id, subscription_id, **kwargs)


## Stomp listener methods #####################################################

  def _on_before_message(self, headers, body):
    return headers, body

  def _on_message(self, headers, body):
#   print "on_message", headers, body
    subscription_id = int(headers.get('subscription'))
    with self._lock:
      target_function = self._subscription_callbacks.get(subscription_id)
    if target_function is not None:
      target_function(headers, body)
    else:
      raise WorkflowsError('Unhandled message %s %s' % (repr(headers), repr(body)))
