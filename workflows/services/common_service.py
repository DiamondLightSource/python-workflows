from __future__ import absolute_import, division
import Queue
import workflows
from workflows.transport.queue_transport import QueueTransport

class CommonService(object):
  '''
  Base class for workflow services. A service is a piece of software that runs
  in an isolated environment, communicating only via queues with the outside
  world. Units of work are injected via a queue. Results, status and log
  messages, etc. are written out via a queue. Any task can be encapsulated
  as a service, for example a service that counts spots on an image passed
  as a filename, and returns the number of counts.

  To instantiate a service two Queue-like objects should be passed to the
  constructors, one to communicate from the service to the frontend, one to
  communicate from the frontend to the service.
  '''

  # Human readable service name -----------------------------------------------

  _service_name = 'unnamed service'

  # Overrideable functions ----------------------------------------------------

  def initializing(self):
    '''Service initialization. This function is run before any commands are
       received from the frontend. This is the place to request channel
       subscriptions with the messaging layer, and register callbacks.
       This function can be overridden by specific service implementations.'''
    pass

  def log(self, logmessage):
    '''Pass a log message to the frontend.
       This function can be overridden by specific service implementations.'''
    self._log_send(logmessage)

  def update_status(self, status):
    '''Pass a status update to the frontend.
       This function can be overridden by specific service implementations.'''
    self._update_status(status)

  def in_shutdown(self):
    '''Service shutdown. This function is run before the service is terminated.
       No more commands are received, but communications can still be sent.
       This function can be overridden by specific service implementations.'''
    pass


  # Internal service status codes ---------------------------------------------
  # These codes will be sent to the frontend to indicate the current state of
  # the main loop regardless of the status text, which can be set freely by
  # the specific service.

  # The state transitions are: (see definition of start() below)
  #  constructor() -> NEW
  #            NEW -> start() being called -> STARTING
  #       STARTING -> self.initializing() -> IDLE
  #           IDLE -> wait for messages on command queue -> PROCESSING
  #              \--> optionally: idle timer elapsed -> TIMER
  #     PROCESSING -> process command -> IDLE
  #              \--> shutdown command received -> SHUTDOWN
  #          TIMER -> process event -> IDLE
  #       SHUTDOWN -> self.in_shutdown() -> END
  #  unhandled exception -> ERROR

  SERVICE_STATUS_NEW, SERVICE_STATUS_STARTING, SERVICE_STATUS_IDLE, \
    SERVICE_STATUS_TIMER, SERVICE_STATUS_PROCESSING, SERVICE_STATUS_SHUTDOWN, \
    SERVICE_STATUS_END, SERVICE_STATUS_ERROR = range(8)

  # Extra states that are not used by services themselves but may be used
  # externally:

  SERVICE_STATUS_NONE = -1     # Node has no service instance loaded
  SERVICE_STATUS_TEARDOWN = -2 # Node is shutting down

  # Not so overrideable functions ---------------------------------------------

  def __init__(self, *args, **kwargs):
    '''Service constructor. Parameters include optional references to two
       queues: frontend=Queue for messages from the service to the frontend,
       and commands=Queue for messages from the frontend to the service.'''
    self.__queue_frontend = kwargs.get('frontend')
    self.__queue_commands = kwargs.get('commands')
    self._transport = workflows.transport.queue_transport.QueueTransport()
    self._transport.set_queue(self.__queue_frontend)
    self._transport.connect()
    self.__shutdown = False
    self.__callback_register = {}
    self.__update_service_status(self.SERVICE_STATUS_NEW)
    self._idle_callback = None
    self._idle_time = None

  def _log_send(self, data_structure):
    '''Internal function to format and send log messages.'''
    self.__log_send_full({'log': data_structure, 'source': 'other'})

  def __log_send_full(self, data_structure):
    '''Internal function to actually send log messages.'''
    if self.__queue_frontend:
      self.__queue_frontend.put_nowait({
        'band': 'log',
        'payload': data_structure
      })

  def _register(self, message_band, callback):
    '''Register a callback function for a specific message band.'''
    self.__callback_register[message_band] = callback

  def _register_idle(self, idle_time, callback):
    '''Register a callback function that is run when idling for a given
       time span (in seconds).'''
    self._idle_callback = callback
    self._idle_time = idle_time

  def _update_status(self, status):
    '''Internal function to actually send status update.'''
    if self.__queue_frontend:
      self.__queue_frontend.put_nowait({
        'band': 'status_update',
        'status': status
      })

  def __update_service_status(self, statuscode):
    '''Set the internal status of the service object, and notify frontend.'''
    self.__service_status = statuscode
    if self.__queue_frontend:
      self.__queue_frontend.put_nowait({
        'band': 'status_update',
        'statuscode': self.__service_status
      })

  def get_name(self):
    '''Return a name for this service.
       Change the name by overwriting self._service_name.'''
    return self._service_name

  def start(self):
    '''Start listening to command queue, process commands in main loop,
       set status, etc...
       This function is most likely called by the frontend in a separate
       process.'''
    self.__update_service_status(self.SERVICE_STATUS_STARTING)

    self.initializing()
    self._register('command', self.__process_command)
    self._register('transport_message', self.__process_transport)

    if self.__queue_commands is None:
      # can only listen to commands if command queue is defined
      self.__shutdown = True

    while not self.__shutdown: # main loop

      self.__update_service_status(self.SERVICE_STATUS_IDLE)

      if self._idle_time is None:
        message = self.__queue_commands.get()
      else:
        try:
          message = self.__queue_commands.get(True, self._idle_time)
        except Queue.Empty:
          self.__update_service_status(self.SERVICE_STATUS_TIMER)
          if self._idle_callback is not None:
            self._idle_callback()
          continue

      self.__update_service_status(self.SERVICE_STATUS_PROCESSING)

      if message and 'band' in message:
        processor = self.__callback_register.get(message['band'])
        if processor is None:
          self.__log_send_full({
              'source': 'service',
              'cause': 'received message on unregistered band',
              'log': message})
        else:
          processor(message.get('payload'))
      else:
        self.__log_send_full({
            'source': 'service',
            'cause': 'received message without band information',
            'log': message})

    self.__update_service_status(self.SERVICE_STATUS_SHUTDOWN)

    self.in_shutdown()

    self.__update_service_status(self.SERVICE_STATUS_END)

  def __process_command(self, command):
    '''Process an incoming command message from the frontend.'''
    if command == Commands.SHUTDOWN:
      self.__shutdown = True

  def __process_transport(self, message):
    '''Process an incoming transport message from the frontend.'''
    if self._transport.subscription_requires_ack(message['subscription_id']):
      self._transport.register_message(message['subscription_id'],
                                       message['header']['message-id'])
    self._transport.subscription_callback(message['subscription_id']) \
      ( message['header'], message['message'] )

  #
  # -- Plugin-related functions ----------------------------------------------
  #

  class __metaclass__(type):
    '''Define metaclass function to keep a list of all subclasses. This enables
       looking up service mechanisms by name.'''
    def __init__(cls, name, base, attrs):
      '''Add new subclass of CommonService to list of all known subclasses.'''
      if not hasattr(cls, 'service_register'):
        cls.service_register = {}
      else:
        cls.service_register[name] = cls

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

class Commands():
  SHUTDOWN = 'shutdown'
  SUBSCRIBE = 'subscribe'
  UNSUBSCRIBE = 'unsubscribe'
