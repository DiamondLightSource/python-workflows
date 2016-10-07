from __future__ import absolute_import
from __future__ import division
import json
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
    self._client_subscriptions = {}
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
    status['stomp.cmdchan'] = '12345'
    message = json.dumps(status)
    with self._lock:
      self._conn.send(
          body=message,
          destination=destination,
          headers={
                    'expires': '%d' % int((90 + time.time()) * 1000)
                  })

  def send_message(self, message, channel):
    '''Send a message to a topic or queue.'''
#   TODO: Does not take prefix into account
    with self._lock:
      self._conn.send(
          body=message,
          destination=channel)

  def subscribe(self, channel, callback, client_id=None, retroactive=False, debug=True):
    '''Listen to a queue or topic, notify via callback function.
       :param channel: Full name of the topic or queue to subscribe to
       :param callback: Function to be called when messages are received
       :param client_id: Value tying a subscription to one client. This allows
                         removing all subscriptions for a client simultaneously
                         when the client goes away.
       :param retroactive: Ask broker to send old messages if possible
       :param debug: Print debugging information to STDOUT
    '''
    with self._lock:
      self._idcounter = self._idcounter + 1
      subscription_id = str(self._idcounter)
      self._subscription_callbacks[subscription_id] = callback
      if client_id is not None:
        if client_id not in self._client_subscriptions:
          self._client_subscriptions[client_id] = []
        self._client_subscriptions[client_id].append(subscription_id)

    headers = {}
    if retroactive:
      headers['activemq.retroactive'] = 'true'

    if debug:
      print '-'*30
      print subscription_id
      print self._idcounter
      print self._subscription_callbacks
      print self._client_subscriptions
      print '-'*30

    with self._lock:
      self._conn.subscribe(channel, subscription_id, headers=headers)


## Stomp listener methods #####################################################

  def _on_before_message(self, headers, body):
    return headers, body

  def _on_message(self, headers, body):
    subscription_id = headers.get('subscription')
    with self._lock:
      target_function = self._subscription_callbacks.get(subscription_id)
    if target_function is not None:
      target_function(headers, json.loads(body))
    else:
      raise WorkflowsError('Unhandled message %s %s' % (repr(headers), repr(body)))
