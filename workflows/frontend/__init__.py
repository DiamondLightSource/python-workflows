from __future__ import absolute_import, division
import logging
import multiprocessing
import threading
import time
import workflows
import workflows.services
from workflows.services.common_service import CommonService
import workflows.transport

try: # Python3 compatibility
  basestring = basestring
except NameError:
  basestring = (str, bytes)

class Frontend():
  '''The frontend class encapsulates the actual service. It controls the
     service process and keeps the connection to the transport layer. It
     can process control messages directly, or pass messages on to the
     service.
  '''

  def __init__(self, transport=None, service=None,
      transport_command_prefix=None, restart_service=False,
      verbose_service=False, environment=None):
    '''Create a frontend instance. Connect to the transport layer, start any
       requested service, begin broadcasting status information and listen
       for control commands.
       :param restart_service:
           If the service process dies unexpectedly the frontend should start
           a new instance.
       :param service:
           A class or name of the class to be instantiated in a subprocess as
           service.
       :param transport:
           Either the name of a transport class, a transport class, or a
           transport class object.
       :param transport_command_prefix:
           An optional prefix of a transport subscription to be listened to for
           commands.
       :param verbose_service:
           If set, run services with increased logging level (DEBUG).
       :param environment:
           An optional dictionary that is passed to started services.
    '''
    self.__lock = threading.RLock()
    self.__hostid = self._generate_unique_host_id()
    self._service = None         # pointer to the service instance
    self._service_class_name = None
    self._service_factory = None # pointer to the service class
    self._service_name = None
    self._service_transportid = None
    self._service_starttime = None
    self._service_rapidstarts = None
    self._pipe_commands = None # frontend -> service
    self._pipe_service = None  # frontend <- service
    self._service_status = CommonService.SERVICE_STATUS_NONE
    self._service_status_announced = CommonService.SERVICE_STATUS_NONE

    self.restart_service = restart_service
    self.shutdown = False

    # Status broadcast related variables
    self._status_interval = 6
    self._status_last_broadcast = 0
    self._status_idle_since = None

    # Set up logging
    self._verbose_service = verbose_service
    class LogAdapter():
      '''A helper class that acts like a dictionary, but actually reads its
         values from the get_status() function.'''
      status_fn = self.get_status
      status = status_fn()

      def __iter__(self):
        '''Update cached status values, renaming the keys for logging.
           Return a dictionary key iterator.'''
        self.status = { 'workflows_' + k: v \
                        for k, v in self.status_fn().items() }
        return self.status.__iter__()

      def __getitem__(self, key):
        '''Return a value from the status dictionary.'''
        return self.status.__getitem__(key)
    self.log = logging.LoggerAdapter(
                   logging.getLogger('workflows.frontend'),
                   LogAdapter())
    self.log.warn = self.log.warning # LoggerAdapter does not support .warn

    # Connect to the network transport layer
    if transport is None or isinstance(transport, basestring):
      self._transport = workflows.transport.lookup(transport)()
    elif hasattr(transport, '__call__'):
      self._transport = transport()
    else:
      self._transport = transport
    assert self._transport.connect(), "Could not connect to transport layer"
    self._service_transportid = self._transport.register_client()

    if transport_command_prefix:
      self._transport.subscribe(transport_command_prefix + self.__hostid,
                                self.process_transport_command)
      self.log.debug('Listening for commands on transport layer')

    # Save environment for service starts
    self._service_environment = environment

    # Start initial service if one has been requested
    self._service_factory = service
    if service is not None:
      self.update_status(CommonService.SERVICE_STATUS_NEW)
      self.switch_service()
    else:
      self.update_status()

  def update_status(self, status_code=None):
    '''Update the service status kept inside the frontend (_service_status).
       The status is broadcasted over the network immediately. If the status
       changes to IDLE then this message is delayed. The IDLE status is only
       broadcasted if it is held for over 0.5 seconds.
       When the status does not change it is still broadcasted every
       _status_interval seconds.
       :param status_code: Either an integer describing the service status
                           (see workflows.services.common_service), or None
                           if the status is unchanged.
    '''
    if status_code is not None:
      self._service_status = status_code

    # Check whether IDLE status should be delayed
    if self._service_status == CommonService.SERVICE_STATUS_IDLE:
      if self._status_idle_since is None:
        self._status_idle_since = time.time()
        return
      elif self._status_idle_since + 0.5 > time.time():
        return
    else:
      self._status_idle_since = None

    new_status = self._service_status != self._service_status_announced
    if (new_status or self._status_last_broadcast + self._status_interval <= time.time()) \
        and self._transport and self._transport.is_connected():
      self._service_status_announced = self._service_status
      self._transport.broadcast_status(self.get_status())
      self._status_last_broadcast = time.time()

  def run(self):
    '''The main loop of the frontend. Here incoming messages from the service
       are processed and forwarded to the corresponding callback methods.'''
    self.log.debug("Entered main loop")
    while not self.shutdown:
      # If no service is running slow down the main loop
      if not self._pipe_service:
        time.sleep(0.3)
      self.update_status()
      # While a service is running, check for incoming messages from that service
      if self._pipe_service and self._pipe_service.poll(1):
        try:
          message = self._pipe_service.recv()
          if isinstance(message, dict) and 'band' in message:
            # only dictionaries with 'band' entry are valid messages
            try:
              handler = getattr(self, 'parse_band_' + message['band'])
            except AttributeError:
              handler = None
              self.log.warn("Unknown band %s", str(message['band']))
            if handler:
#              try:
                handler(message)
#              except Exception:
#                print('Uh oh. What to do.')
          else:
            self.log.warn("Invalid message received %s", str(message))
        except EOFError:
          # Service has gone away
          error_message = False
          if self._service_status == CommonService.SERVICE_STATUS_END:
            self.log.info("Service terminated")
          elif self._service_status == CommonService.SERVICE_STATUS_ERROR:
            error_message = "Service terminated with error code"
          elif self._service_status in (CommonService.SERVICE_STATUS_NONE,
                                        CommonService.SERVICE_STATUS_NEW,
                                        CommonService.SERVICE_STATUS_STARTING):
            error_message = 'Service may have died unexpectedly in ' \
                          + 'initialization (last known status: %s)' \
                              % CommonService.human_readable_state.get( \
                                    self._service_status, self._service_status)
          else:
            error_message = "Service may have died unexpectedly" \
                            " (last known status: %s" \
                            % CommonService.human_readable_state.get( \
                                    self._service_status, self._service_status)
          if error_message:
            self.log.error(error_message)
          self._terminate_service()
          if self.restart_service:
            self.exponential_backoff()
          else:
            self.shutdown = True
            if error_message:
              raise workflows.WorkflowsError(error_message)

      with self.__lock:
        if self._service is None and self.restart_service and self._service_factory:
          self.update_status(status_code=CommonService.SERVICE_STATUS_NEW)
          self.switch_service()

      # Check that the transport is alive
      if not self._transport.is_connected():
        self._terminate_service()
        raise workflows.WorkflowsError('Lost transport layer connection')
    self.log.debug("Left main loop")
    self.update_status(status_code=CommonService.SERVICE_STATUS_TEARDOWN)
    self._terminate_service()
    self.log.debug("Terminating.")

  def send_command(self, command):
    '''Send command to service via the command queue.'''
#   print("To command queue: " + str(command))
    if self._pipe_commands:
      self._pipe_commands.send(command)

  def process_transport_command(self, header, message):
    '''Parse a command coming in through the transport command subscription'''
    if isinstance(message, dict) and message.get('command'):
      self.log.info('Received command \'%s\' via transport layer', message['command'])
      if message['command'] == 'shutdown':
        self.shutdown = True
    else:
      self.log.warn('Received invalid transport command message')

  def parse_band_log(self, message):
    '''Process incoming logging messages from the service.'''
    if 'payload' in message and hasattr(message['payload'], 'name'):
      record = message['payload']
      for k in dir(record):
        if k.startswith('workflows_exc_'):
          setattr(record, k[14:], getattr(record, k))
          delattr(record, k)
      for k, v in self.get_status().items():
        setattr(record, 'workflows_' + k, v)
      logging.getLogger(record.name).handle(record)
    else:
      self.log.warn("Received broken record on log band\nMessage: %s\nRecord: %s",
                    str(message),
                    str(hasattr(message.get('payload'), '__dict__') and message['payload'].__dict__))

  def parse_band_set_name(self, message):
    '''Process incoming message indicating service name change.'''
    if message.get('name'):
      self._service_name = message['name']
    else:
      self.log.warn("Received broken record on set_name band\nMessage: %s",
                    str(message))

  def parse_band_status_update(self, message):
    '''Process incoming status updates from the service.'''
    self.log.debug("Status update: " + str(message))
    self.update_status(status_code=message['statuscode'])

  def get_host_id(self):
    '''Get a cached copy of the host id.'''
    return self.__hostid

  @staticmethod
  def _generate_unique_host_id():
    '''Generate a unique ID, that is somewhat guaranteed to be unique among all
       instances running at the same time.'''
    import socket
    host = '.'.join(reversed(socket.gethostname().split('.')))
    import os
    pid = os.getpid()
    return "%s.%d" % (host, pid)

  def get_status(self):
    '''Returns a dictionary containing all relevant status information to be
       broadcast across the network.'''
    return { 'host': self.__hostid,
             'status': self._service_status_announced,
             'statustext': CommonService.human_readable_state.get(self._service_status_announced),
             'service': self._service_name,
             'serviceclass': self._service_class_name,
             'workflows': workflows.version(),
           }

  def exponential_backoff(self):
    '''A function that keeps waiting longer and longer the more rapidly it is called.
       It can be used to increasingly slow down service starts when they keep failing.'''
    last_service_switch = self._service_starttime
    if not last_service_switch:
      return
    time_since_last_switch = time.time() - last_service_switch

    if not self._service_rapidstarts:
      self._service_rapidstarts = 0
    minimum_wait = 0.1 * (2 ** self._service_rapidstarts)
    minimum_wait = min(5, minimum_wait)

    if time_since_last_switch > 10:
      self._service_rapidstarts = 0
      return
    self._service_rapidstarts += 1
    self.log.debug("Slowing down service starts (%.1f seconds)", minimum_wait)
    time.sleep(minimum_wait)

  def switch_service(self, new_service=None):
    '''Start a new service in a subprocess.
       :param new_service: Either a service name or a service class. If not set,
                           start up a new instance of the previous class
       :return: True on success, False on failure.
    '''
    if new_service:
      self._service_factory = new_service
    with self.__lock:
      # Terminate existing service if necessary
      if self._service is not None:
        self._terminate_service()

      # Find service class if necessary
      if isinstance(self._service_factory, basestring):
        self._service_factory = workflows.services.lookup(self._service_factory)
      if not self._service_factory:
        return False

      # Set up pipes and connect new service object
      svc_commands, self._pipe_commands = multiprocessing.Pipe(False)
      self._pipe_service, svc_tofrontend = multiprocessing.Pipe(False)
      service_instance = self._service_factory(
        commands=svc_commands,
        frontend=svc_tofrontend,
        environment=self._service_environment)

      # Clean out transport layer for new service
      if self._service_transportid:
        self._transport.drop_client(self._service_transportid)
      self._service_transportid = self._transport.register_client()

      # Start new service in a separate process
      self._service = multiprocessing.Process(
        target=service_instance.start, args=(),
        kwargs={'verbose_log': self._verbose_service})
      self._service_name = service_instance.get_name()
      self._service_class_name = service_instance.__class__.__name__
      self._service.daemon = True
      self._service.start()
      self._service_starttime = time.time()

      # Starting the process copies all file descriptors.
      # At this point (and no sooner!) the passed pipe objects must be closed
      # in this process here.
      svc_commands.close()
      svc_tofrontend.close()
    self.log.info("Started service: %s", self._service_name)
    return True

  def _terminate_service(self):
    '''Force termination of running service.
       Disconnect queues, end queue feeder threads.
       Wait for service process to clear, drop all references.'''
    with self.__lock:
      if self._service:
        self._service.terminate()
      if self._pipe_commands:
        self._pipe_commands.close()
      if self._pipe_service:
        self._pipe_service.close()
      self._pipe_commands = None
      self._pipe_service = None
      self._service_class_name = None
      self._service_name = None
      if self._service_status != CommonService.SERVICE_STATUS_TEARDOWN:
        self.update_status(status_code=CommonService.SERVICE_STATUS_END)
      if self._service_transportid:
        self._transport.drop_client(self._service_transportid)
      self._service_transportid = None
      if self._service:
        self._service.join() # must wait for process to be actually destroyed
      self._service = None

# ---- Transport calls -----------------------------------------------------

  _transaction_mapping = {}
  _subscription_mapping = {}

  def parse_band_transport(self, message):
    '''Treat messages sent from service via queue. These were encoded by
       transport.queue_transport on the other end. Forward the messages to
       individual functions parse_band_transport_${call}() depending on the
       'call' field.
       param: message: The full queue message data structure.
    '''
    try:
      handler = getattr(self, 'parse_band_transport_' + message['call'])
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
    handler(message)

  def parse_band_transport_send(self, message):
    '''Counterpart to the transport.send call from the service.
       Forward the call to the real transport layer.'''
    args, kwargs = message['payload']
    self._transport.send(*args, **kwargs)

  def parse_band_transport_broadcast(self, message):
    '''Counterpart to the transport.broadcast call from the service.
       Forward the call to the real transport layer.'''
    args, kwargs = message['payload']
    self._transport.broadcast(*args, **kwargs)

  def parse_band_transport_ack(self, message):
    '''Counterpart to the transport.ack call from the service.
       Forward the call to the real transport layer.'''
    args, kwargs = message['payload']
    message_id, subscription_id = args[:2]
    assert subscription_id in self._subscription_mapping
    self._transport.ack(message_id,
                        self._subscription_mapping[subscription_id],
                        *args[2:], **kwargs)

  def parse_band_transport_nack(self, message):
    '''Counterpart to the transport.nack call from the service.
       Forward the call to the real transport layer.'''
    args, kwargs = message['payload']
    message_id, subscription_id = args[:2]
    assert subscription_id in self._subscription_mapping
    self._transport.nack(message_id,
                         self._subscription_mapping[subscription_id],
                         *args[2:], **kwargs)

  def parse_band_transport_transaction_begin(self, message):
    '''Counterpart to the transport.transaction_begin call from the service.
       Forward the call to the real transport layer.'''
    args, kwargs = message['payload']
    service_txn_id = args[0]
    self._transaction_mapping[service_txn_id] = \
      self._transport.transaction_begin(client_id=self._service_transportid)

  def parse_band_transport_transaction_commit(self, message):
    '''Counterpart to the transport.transaction_begin call from the service.
       Forward the call to the real transport layer.'''
    args, kwargs = message['payload']
    service_txn_id = args[0]
    assert service_txn_id in self._transaction_mapping
    self._transport.transaction_commit(self._transaction_mapping[service_txn_id])
    del self._transaction_mapping[service_txn_id]

  def parse_band_transport_transaction_abort(self, message):
    '''Counterpart to the transport.transaction_abort call from the service.
       Forward the call to the real transport layer.'''
    args, kwargs = message['payload']
    service_txn_id = args[0]
    assert service_txn_id in self._transaction_mapping
    self._transport.transaction_abort(self._transaction_mapping[service_txn_id])
    del self._transaction_mapping[service_txn_id]

  def parse_band_transport_subscribe(self, message):
    '''Counterpart to the transport.subscribe call from the service.
       Forward the call to the real transport layer.'''
    subscription_id, channel = message['payload'][0][:2]
    def callback_helper(cb_header, cb_message):
      '''Helper function to rewrite the message header and redirect it towards
         the service.'''
      cb_header['subscription'] = subscription_id
      self.send_command( {
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
      client_id=self._service_transportid,
      **message['payload'][1]
    )

  def parse_band_transport_subscribe_broadcast(self, message):
    '''Counterpart to the transport.subscribe_broadcast call from the service.
       Forward the call to the real transport layer.'''
    subscription_id, channel = message['payload'][0][:2]
    def callback_helper(cb_header, cb_message):
      '''Helper function to rewrite the message header and redirect it towards
         the service.'''
      cb_header['subscription'] = subscription_id
      self.send_command( {
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
      client_id=self._service_transportid,
      **message['payload'][1]
    )

  def parse_band_transport_unsubscribe(self, message):
    '''Counterpart to the transport.unsubscribe call from the service.
       Forward the call to the real transport layer.'''
    subscription_id = message['payload'][0][0]
    self._transport.unsubscribe(self._subscription_mapping[subscription_id])
    del self._subscription_mapping[subscription_id]
