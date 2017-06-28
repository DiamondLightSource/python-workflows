from __future__ import absolute_import, division
import contextlib
import enum
import logging
import workflows
import workflows.logging
from workflows.transport.queue_transport import QueueTransport

class Status(enum.Enum):
  '''
  Internal service status codes
  ---------------------------------------------
  These codes will be sent to the frontend to indicate the current state of
  the main loop regardless of the status text, which can be set freely by
  the specific service.
  '''

  # The state transitions are: (see definition of CommonService.start() below)
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

  NEW        = (0, 'constructing')
  STARTING   = (1, 'starting')
  IDLE       = (2, 'idle')
  TIMER      = (3, 'timer event')
  PROCESSING = (4, 'processing')
  SHUTDOWN   = (5, 'shutting down')
  END        = (6, 'shutdown')
  ERROR      = (7, 'error')

  # Extra states that are not used by services themselves but may be used
  # externally:

  NONE       = (-1, 'no service loaded') # Node has no service instance loaded
  TEARDOWN   = (-2, 'shutdown')          # Node is shutting down

  def __init__(self, intval, description):
    '''
    Each status is defined as a tuple of a unique integer value and a
    descriptive string. These are available via enum properties
    '''
    self.intval = intval
    self.description = description

class CommonService(workflows.add_plugin_register_to_class(object)):
  '''
  Base class for workflow services. A service is a piece of software that runs
  in an isolated environment, communicating only via pipes with the outside
  world. Units of work are injected via a pipe. Results, status and log
  messages, etc. are written out via a pipe. Any task can be encapsulated
  as a service, for example a service that counts spots on an image passed
  as a filename, and returns the number of counts.

  To instantiate a service two Pipe-like objects should be passed to the
  constructors, one to communicate from the service to the frontend, one to
  communicate from the frontend to the service.
  '''

  # Human readable service name -----------------------------------------------

  _service_name = 'unnamed service'

  # Logger name ---------------------------------------------------------------

  _logger_name = 'workflows.service'  # The logger can be accessed via self.log

  # Overrideable functions ----------------------------------------------------

  def initializing(self):
    '''Service initialization. This function is run before any commands are
       received from the frontend. This is the place to request channel
       subscriptions with the messaging layer, and register callbacks.
       This function can be overridden by specific service implementations.'''
    pass

  def in_shutdown(self):
    '''Service shutdown. This function is run before the service is terminated.
       No more commands are received, but communications can still be sent.
       This function can be overridden by specific service implementations.'''
    pass

  SERVICE_STATUS_NEW = Status.NEW.intval
  SERVICE_STATUS_STARTING = Status.STARTING.intval
  SERVICE_STATUS_IDLE = Status.IDLE.intval
  SERVICE_STATUS_TIMER = Status.TIMER.intval
  SERVICE_STATUS_PROCESSING = Status.PROCESSING.intval
  SERVICE_STATUS_SHUTDOWN = Status.SHUTDOWN.intval
  SERVICE_STATUS_END = Status.END.intval
  SERVICE_STATUS_ERROR = Status.ERROR.intval
  SERVICE_STATUS_NONE = Status.NONE.intval
  SERVICE_STATUS_TEARDOWN = Status.TEARDOWN.intval

  # Number to short string conversion

  human_readable_state = { e.intval: e.description for e in Status }

  # Default logging level for log messages from this service

  log_verbosity = logging.INFO

  # Any keyword arguments set on service invocation

  start_kwargs = { }

  # Not so overrideable functions ---------------------------------------------

  def __init__(self, *args, **kwargs):
    '''Service constructor. Parameters include optional references to two
       pipes: frontend= for messages from the service to the frontend,
       and commands= for messages from the frontend to the service.
       A dictionary can optionally be passed with environment=, which is then
       available to the service during runtime.'''
    self.__pipe_frontend = kwargs.get('frontend')
    self.__pipe_commands = kwargs.get('commands')
    self._environment = kwargs.get('environment', {})
    self._transport = workflows.transport.queue_transport.QueueTransport()
    self._transport.set_send_function(self.__send_to_frontend)
    self._transport.connect()
    self.__callback_register = {}
    self.__log_extensions = []
    self.__service_status = None
    self.__shutdown = False
    self.__update_service_status(self.SERVICE_STATUS_NEW)
    self._idle_callback = None
    self._idle_time = None

    # Logger will be overwritten in start() function
    self.log = logging.getLogger(self._logger_name)

  def __send_to_frontend(self, data_structure):
    '''Put a message in the pipe for the frontend.'''
    if self.__pipe_frontend:
      self.__pipe_frontend.send(data_structure)

  @contextlib.contextmanager
  def extend_log(self, field, value):
    '''A context wherein a specified extra field in log messages is populated
       with a fixed value. This affects all log messages within the context.'''
    self.__log_extensions.append((field, value))
    try:
      yield
    except Exception as e:
      setattr(e, 'workflows_log_' + field, value)
      raise
    finally:
      self.__log_extensions.remove((field, value))

  def _log_send(self, logrecord):
    '''Forward log records to the frontend.'''
    for field, value in self.__log_extensions:
      setattr(logrecord, field, value)
    self.__send_to_frontend({
      'band': 'log',
      'payload': logrecord
    })

  def _register(self, message_band, callback):
    '''Register a callback function for a specific message band.'''
    self.__callback_register[message_band] = callback

  def _register_idle(self, idle_time, callback):
    '''Register a callback function that is run when idling for a given
       time span (in seconds).'''
    self._idle_callback = callback
    self._idle_time = idle_time

  def __update_service_status(self, statuscode):
    '''Set the internal status of the service object, and notify frontend.'''
    if self.__service_status != statuscode:
      self.__service_status = statuscode
      self.__send_to_frontend({
        'band': 'status_update',
        'statuscode': self.__service_status
      })

  def get_name(self):
    '''Get the name for this service.'''
    return self._service_name

  def _set_name(self, name):
    '''Set a new name for this service, and notify the frontend accordingly.'''
    self._service_name = name
    self.__send_to_frontend({ 'band': 'set_name', 'name': self._service_name })

  def _shutdown(self):
    '''Terminate the service.'''
    self.__shutdown = True

  def initialize_logging(self):
    '''Reset the logging for the service process. All logged messages are
       forwarded to the frontend. If any filtering is desired, then this must
       take place on the service side.'''
    # Reset logging to pass logrecords into the queue to the frontend only.
    # Existing handlers may be broken as they were copied into a new process,
    # so should be discarded.
    for loggername in [None] + list(logging.Logger.manager.loggerDict.keys()):
      logger = logging.getLogger(loggername)
      while logger.handlers:
        logger.removeHandler(logger.handlers[0])

    # Re-enable logging to console
    root_logger = logging.getLogger()

    # By default pass all warning (and higher) level messages to the frontend
    root_logger.setLevel(logging.WARN)
    root_logger.addHandler(workflows.logging.CallbackHandler(self._log_send))

    # Set up the service logger and pass all info (and higher) level messages
    # (or other level if set differently)
    self.log = logging.getLogger(self._logger_name)
    if self.start_kwargs.get('verbose_log'):
      self.log_verbosity = logging.DEBUG
    self.log.setLevel(self.log_verbosity)

    # Additionally, write all critical messages directly to console
    console = logging.StreamHandler()
    console.setLevel(logging.CRITICAL)
    root_logger.addHandler(console)

  def start(self, **kwargs):
    '''Start listening to command queue, process commands in main loop,
       set status, etc...
       This function is most likely called by the frontend in a separate
       process.'''

    # Keep a copy of keyword arguments for use in subclasses
    self.start_kwargs.update(kwargs)
    try:
      self.initialize_logging()

      self.__update_service_status(self.SERVICE_STATUS_STARTING)

      self.initializing()
      self._register('command', self.__process_command)
      self._register('transport_message', self.__process_transport)

      if self.__pipe_commands is None:
        # can only listen to commands if command queue is defined
        self.__shutdown = True

      while not self.__shutdown: # main loop
        self.__update_service_status(self.SERVICE_STATUS_IDLE)

        if self._idle_time is None \
            or self.__pipe_commands.poll(self._idle_time):
          message = self.__pipe_commands.recv()
        else:
          self.__update_service_status(self.SERVICE_STATUS_TIMER)
          if self._idle_callback is not None:
            self._idle_callback()
          continue

        self.__update_service_status(self.SERVICE_STATUS_PROCESSING)

        if message and 'band' in message:
          processor = self.__callback_register.get(message['band'])
          if processor is None:
            self.log.warning('received message on unregistered band\n%s',
                             message)
          else:
            processor(message.get('payload'))
        else:
          self.log.warning('received message without band information\n%s',
                           message)

      self.__update_service_status(self.SERVICE_STATUS_SHUTDOWN)

      self.in_shutdown()

      self.__update_service_status(self.SERVICE_STATUS_END)

    except KeyboardInterrupt:
      self.log.warning('Ctrl+C detected. Shutting down.')
      return

    except Exception as e:
      self.process_uncaught_exception(e)
      self.__update_service_status(self.SERVICE_STATUS_ERROR)
      return

  def process_uncaught_exception(self, e):
    '''This is called to handle otherwise uncaught exceptions from the service.
       The service will terminate either way, but here we can do things such as
       gathering useful environment information and logging for posterity.'''
    # Add information about the actual exception to the log message
    # This includes the file, line and piece of code causing the exception.
    # exc_info=True adds the full stack trace to the log message.
    exc_file_fullpath, exc_file, exc_lineno, exc_func, exc_line = \
        workflows.logging.get_exception_source()
    added_information = {
        'workflows_exc_lineno': exc_lineno,
        'workflows_exc_funcName': exc_func,
        'workflows_exc_line': exc_line,
        'workflows_exc_pathname': exc_file_fullpath,
        'workflows_exc_filename': exc_file,
    }
    for field in filter(lambda x: x.startswith('workflows_log_'), dir(e)):
      added_information[field[14:]] = getattr(e, field, None)
    self.log.critical('Unhandled service exception: %s', e, exc_info=True,
        extra=added_information)

  def __process_command(self, command):
    '''Process an incoming command message from the frontend.'''
    if command == Commands.SHUTDOWN:
      self.__shutdown = True

  def __process_transport(self, message):
    '''Process an incoming transport message from the frontend.'''
    self._transport.subscription_callback(message['subscription_id']) \
      ( message['header'], message['message'] )

class Commands():
  '''A list of command strings used for communicating with the frontend.'''
  SHUTDOWN = 'shutdown'
