from __future__ import annotations

import contextlib
import enum
import itertools
import logging
import multiprocessing.connection
import queue
import threading
import time
from collections.abc import Callable, Generator, Mapping
from typing import Any

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

import workflows
import workflows.logging
from workflows.transport.common_transport import CommonTransport, MessageCallback
from workflows.transport.middleware.otel_tracing import OTELTracingMiddleware


class Status(enum.Enum):
    """
    Internal service status codes
    ---------------------------------------------
    These codes will be sent to the frontend to indicate the current state of
    the main loop regardless of the status text, which can be set freely by
    the specific service.
    """

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

    NEW = (0, "constructing")
    STARTING = (1, "starting")
    IDLE = (2, "idle")
    TIMER = (3, "timer event")
    PROCESSING = (4, "processing")
    SHUTDOWN = (5, "shutting down")
    END = (6, "shutdown")
    ERROR = (7, "error")

    # Extra states that are not used by services themselves but may be used
    # externally:

    NONE = (-1, "no service loaded")  # Node has no service instance loaded
    TEARDOWN = (-2, "shutdown")  # Node is shutting down

    def __init__(self, intval: int, description: str):
        """
        Each status is defined as a tuple of a unique integer value and a
        descriptive string. These are available via enum properties
        """
        self.intval = intval
        self.description = description


class Priority(enum.IntEnum):
    """
    Priorities for the service-internal priority queue. This ensures that eg.
    frontend commands are always processed before timer events.
    """

    COMMAND = 1
    TIMER = 2
    TRANSPORT = 3
    IDLE = 4


class CommonService:
    """
    Base class for workflow services. A service is a piece of software that runs
    in an isolated environment, communicating only via pipes with the outside
    world. Units of work are injected via a pipe. Results, status and log
    messages, etc. are written out via a pipe. Any task can be encapsulated
    as a service, for example a service that counts spots on an image passed
    as a filename, and returns the number of counts.

    To instantiate a service two Pipe-like objects should be passed to the
    constructors, one to communicate from the service to the frontend, one to
    communicate from the frontend to the service.
    """

    # Human readable service name -----------------------------------------------

    _service_name = "unnamed service"

    # Logger name ---------------------------------------------------------------

    #: The logger can be accessed via self.log
    _logger_name = "workflows.service"

    # Overrideable functions ----------------------------------------------------

    def initializing(self) -> None:
        """Service initialization. This function is run before any commands are
        received from the frontend. This is the place to request channel
        subscriptions with the messaging layer, and register callbacks.
        This function can be overridden by specific service implementations."""
        pass

    def in_shutdown(self) -> None:
        """Service shutdown. This function is run before the service is terminated.
        No more commands are received, but communications can still be sent.
        This function can be overridden by specific service implementations."""
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

    human_readable_state = {e.intval: e.description for e in Status}  # lgtm

    # Default logging level for log messages from this service

    log_verbosity = logging.INFO

    # Any keyword arguments set on service invocation

    start_kwargs: dict[str, Any]
    _transport_interceptor_counter: itertools.count[int]

    # Not so overrideable functions ---------------------------------------------

    def __init__(self, *, environment: dict[str, Any] | None = None):
        """
        Service constructor.

        Args:
            environment:
                Optional dictionary made available to the service at runtime
                via ``self._environment``. Typically used by the frontend to
                pass configuration (e.g. ``config``, ``metrics``, ``liveness``)
                into the spawned service process.
        """
        self.__pipe_frontend: multiprocessing.connection.Connection | None = None
        self.__pipe_commands: multiprocessing.connection.Connection | None = None
        self._environment = environment if environment is not None else {}
        self._transport: CommonTransport | None = None
        self.__callback_register: dict[str, Callable[[Any], None]] = {}
        self.__log_extensions: list[tuple[str, Any]] = []
        self.__service_status: int = self.SERVICE_STATUS_NEW
        self.__shutdown: bool = False
        self.__queue: queue.PriorityQueue[tuple[Priority, int, Any]] = (
            queue.PriorityQueue()
        )
        self._idle_callback: Callable[[], None] | None = None
        self._idle_time: float | None = None
        self.start_kwargs = {}

        # Logger will be overwritten in start() function
        self.log = logging.getLogger(self._logger_name)

    def __send_to_frontend(self, data_structure: Any) -> None:
        """Put a message in the pipe for the frontend."""
        if self.__pipe_frontend:
            self.__pipe_frontend.send(data_structure)

    @property
    def config(self) -> Any:
        return self._environment.get("config")

    @property
    def transport(self) -> CommonTransport:
        # Handle the fact that we apparently allow missing transport layers
        if self._transport is None:
            raise RuntimeError("Transport layer has not yet been defined")
        return self._transport

    @transport.setter
    def transport(self, value: CommonTransport) -> None:
        if self._transport:
            raise AttributeError("Transport already defined")
        self._transport = value

    def start_transport(self) -> None:
        """If a transport object has been defined, then connect it."""
        if self._transport:
            if self.transport.connect():
                self.log.debug("Service successfully connected to transport layer")
            else:
                raise RuntimeError("Service could not connect to transport layer")
            # direct all transport callbacks into the main queue
            self._transport_interceptor_counter = itertools.count()
            self.transport.subscription_callback_set_intercept(
                self._transport_interceptor
            )

            # Configure OTELTracing if configuration is available
            otel_config = getattr(self.config, "_opentelemetry", None)
            if otel_config:
                # Configure OTELTracing
                resource = Resource.create(
                    {
                        SERVICE_NAME: self._service_name,
                    }
                )

                self.log.debug("Configuring OTELTracing")
                provider = TracerProvider(resource=resource)
                trace.set_tracer_provider(provider)

                # Configure BatchProcessor and OTLPSpanExporter using config values
                otlp_exporter = OTLPSpanExporter(
                    endpoint=otel_config["endpoint"],
                    timeout=otel_config.get("timeout", 10),
                )
                span_processor = BatchSpanProcessor(otlp_exporter)
                provider.add_span_processor(span_processor)

                # Add OTELTracingMiddleware to the transport layer
                tracer = trace.get_tracer(__name__)
                otel_middleware = OTELTracingMiddleware(
                    tracer, service_name=self._service_name
                )
                self.transport.add_middleware(otel_middleware)

            metrics = self._environment.get("metrics")
            if metrics:
                import prometheus_client

                from workflows.transport.middleware.prometheus import (
                    PrometheusMiddleware,
                )

                self.log.debug("Instrumenting transport")
                source = f"{self.__module__}:{self.__class__.__name__}"
                instrument = PrometheusMiddleware(source=source)
                self.transport.add_middleware(instrument)
                port = metrics["port"]
                self.log.debug(f"Starting metrics endpoint on port {port}")
                prometheus_client.start_http_server(port=port)
        else:
            self.log.debug("No transport layer defined for service. Skipping.")

    def stop_transport(self) -> None:
        """If a transport object has been defined then tear it down."""
        if self._transport:
            self.log.debug("Stopping transport object")
            self.transport.disconnect()

    def _transport_interceptor(self, callback: MessageCallback) -> MessageCallback:
        """Takes a callback function and returns a function that takes headers and
        messages and places them on the main service queue."""

        def add_item_to_queue(header: Mapping[str, Any], message: Any) -> None:
            queue_item = (
                Priority.TRANSPORT,
                next(
                    self._transport_interceptor_counter
                ),  # insertion sequence to keep messages in order
                (callback, header, message),
            )
            self.__queue.put(
                queue_item
            )  # Block incoming transport until insertion completes

        return add_item_to_queue

    def connect(
        self,
        frontend: multiprocessing.connection.Connection | None = None,
        commands: multiprocessing.connection.Connection | None = None,
    ) -> None:
        """Inject the pipes connecting this service to the frontend.

        Injection should happen before :meth:`start` is called, otherwise
        the underlying file descriptor references may not be handled
        correctly across the process boundary.

        Args:
            frontend: Write end of the pipe used to send messages from the
                service to the frontend (status updates, log records, etc.).
                Setting this also triggers an immediate status broadcast.
            commands: Read end of the pipe used to receive command messages
                from the frontend. If left as ``None`` the service has no
                way to receive commands and will shut itself down shortly
                after :meth:`start`.
        """
        if frontend is not None:
            self.__pipe_frontend = frontend
            self.__send_service_status_to_frontend()
        if commands is not None:
            self.__pipe_commands = commands

    @contextlib.contextmanager
    def extend_log(self, field: str, value: Any) -> Generator[None, None, None]:
        """Annotate log records emitted within the context with an extra field.

        The ``(field, value)`` pair is attached to every log record produced
        while the context is active, and removed on exit. If an exception
        propagates out of the block, the field is also stashed on the
        exception as ``workflows_log_<field>`` so downstream handlers
        (notably :meth:`process_uncaught_exception`) can surface it.

        Args:
            field: Name of the extra field to attach to log records. Must be
                a valid Python identifier suffix, as it is also used to
                build the attribute name on any escaping exception.
            value: Value to associate with ``field``. Anything that the
                log handler can serialize is acceptable.

        Yields:
            Control to the wrapped block. No value is yielded.
        """
        self.__log_extensions.append((field, value))
        try:
            yield
        except Exception as e:
            setattr(e, "workflows_log_" + field, value)
            raise
        finally:
            self.__log_extensions.remove((field, value))

    def __command_queue_listener(self) -> None:
        """Function to continuously retrieve data from the frontend. Commands are
        sent to the central priority queue. If the pipe from the frontend is
        closed the service shutdown is initiated. Check every second if service
        has shut down, then terminate.
        This function is run by a separate daemon thread, which is started by
        the __start_command_queue_listener function.
        """
        assert self.__pipe_commands is not None, (
            "Listener started without command queue connection"
        )
        self.log.debug("Queue listener thread started")
        counter = itertools.count()  # insertion sequence to keep messages in order
        while not self.__shutdown:
            if self.__pipe_commands.poll(1):
                try:
                    message = self.__pipe_commands.recv()
                except EOFError:
                    # Pipe was closed by frontend. Shut down service.
                    self.__shutdown = True
                    self.log.error(
                        "Pipe closed by frontend, shutting down service", exc_info=True
                    )
                    break
                queue_item = (Priority.COMMAND, next(counter), message)
                try:
                    self.__queue.put(queue_item, True, 60)
                except queue.Full:
                    # If the message can't be stored within 60 seconds then the service is
                    # operating outside normal parameters. Try to shut it down.
                    self.__shutdown = True
                    self.log.error(
                        "Write to service priority queue failed, shutting down service",
                        exc_info=True,
                    )
                    break
                time.sleep(0.05)
        self.log.debug("Queue listener thread terminating")

    def __start_command_queue_listener(self) -> None:
        """Start the function __command_queue_listener in a separate thread. This
        function continuously listens to the pipe connected to the frontend.
        """
        thread_function = self.__command_queue_listener

        class QueueListenerThread(threading.Thread):
            def run(self) -> None:
                thread_function()

        assert not hasattr(self, "__queue_listener_thread")
        self.log.debug("Starting queue listener thread")
        self.__queue_listener_thread = QueueListenerThread()
        self.__queue_listener_thread.daemon = True
        self.__queue_listener_thread.name = "Command Queue Listener"
        self.__queue_listener_thread.start()

    def _log_send(self, logrecord: logging.LogRecord) -> None:
        """Forward log records to the frontend."""
        for field, value in self.__log_extensions:
            setattr(logrecord, field, value)
        self.__send_to_frontend({"band": "log", "payload": logrecord})

    def _register(self, message_band: str, callback: Callable[[Any], None]) -> None:
        """Register a callback function for a specific message band."""
        self.__callback_register[message_band] = callback

    def _register_idle(self, idle_time: float, callback: Callable[[], None]) -> None:
        """Register a callback function that is run when idling for a given
        time span (in seconds)."""
        self._idle_callback = callback
        self._idle_time = idle_time

    def __update_service_status(self, statuscode: int) -> None:
        """Set the internal status of the service object, and notify frontend."""
        if self.__service_status != statuscode:
            self.__service_status = statuscode
            self.__send_service_status_to_frontend()

    def __send_service_status_to_frontend(self) -> None:
        """Actually send the internal status of the service object to the frontend."""
        self.__send_to_frontend(
            {"band": "status_update", "statuscode": self.__service_status}
        )

    def get_name(self) -> str:
        """Get the name for this service."""
        return self._service_name

    def _set_name(self, name: str) -> None:
        """Set a new name for this service, and notify the frontend accordingly."""
        self._service_name = name
        self.__send_to_frontend({"band": "set_name", "name": self._service_name})

    def _request_termination(self) -> None:
        """Terminate the service from the frontend side"""
        self.__send_to_frontend({"band": "request_termination"})

    def _shutdown(self) -> None:
        """Terminate the service from the service side."""
        self.__shutdown = True

    def initialize_logging(self) -> None:
        """Reset the logging for the service process. All logged messages are
        forwarded to the frontend. If any filtering is desired, then this must
        take place on the service side."""
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

        if self.start_kwargs.get("verbose_log"):
            self.log_verbosity = logging.DEBUG
        self.log.setLevel(self.log_verbosity)

        # Additionally, write all critical messages directly to console
        console = logging.StreamHandler()
        console.setLevel(logging.CRITICAL)
        root_logger.addHandler(console)

    def start(self, *, verbose_log: bool = False, **kwargs: Any) -> None:
        """Run the service main loop until shutdown.

        This is the entry point invoked by the frontend in the spawned service
        process. It sets up logging and transport, calls :meth:`initializing`,
        then enters the main loop, dispatching command-band and transport-band
        messages off the internal priority queue and emitting status updates as
        the service state changes. On shutdown - ``clean``, or via an unhandled
        exception - :meth:`in_shutdown`, is invoked and the transport is torn
        down.

        Args:
            verbose_log:
                If set, initialises the service logger level to ``DEBUG``.
            **kwargs:
                Other arbitrary keyword arguments, forwarded by the frontend.
                Stored on :attr:`start_kwargs` for use by subclasses.
        """
        # Keep a copy of keyword arguments for use in subclasses
        self.start_kwargs.update(kwargs)
        if verbose_log:
            self.start_kwargs["verbose_log"] = verbose_log
        try:
            self.initialize_logging()

            self.__update_service_status(self.SERVICE_STATUS_STARTING)

            self.start_transport()

            self.initializing()
            self._register("command", self.__process_command)

            if self.__pipe_commands is None:
                # can only listen to commands if command queue is defined
                self.__shutdown = True
            else:
                # start listening to command queue in separate thread
                self.__start_command_queue_listener()

            while not self.__shutdown:  # main loop
                self.__update_service_status(self.SERVICE_STATUS_IDLE)

                try:
                    task = self.__queue.get(True, self._idle_time or 2)
                except queue.Empty:
                    task = None

                if self._transport and not self.transport.is_connected():
                    raise workflows.Disconnected("Connection lost")

                if task is None:
                    # Run the idle task
                    if self._idle_time:
                        # run this outside the 'except' to avoid exception chaining
                        self.__update_service_status(self.SERVICE_STATUS_TIMER)
                        if self._idle_callback:
                            self._idle_callback()
                    continue

                self.__update_service_status(self.SERVICE_STATUS_PROCESSING)

                if task[0] == Priority.COMMAND:
                    message = task[2]
                    if message and "band" in message:
                        processor = self.__callback_register.get(message["band"])
                        if processor is None:
                            self.log.warning(
                                "received message on unregistered band\n%s", message
                            )
                        else:
                            processor(message.get("payload"))
                    else:
                        self.log.warning(
                            "received message without band information\n%s", message
                        )
                elif task[0] == Priority.TRANSPORT:
                    callback, header, message = task[2]
                    callback(header, message)
                else:
                    self.log.warning("Unknown item on main service queue\n%r", task)

        except KeyboardInterrupt:
            self.log.warning("Ctrl+C detected. Shutting down.")

        except Exception as e:
            self.process_uncaught_exception(e)
            self.__update_service_status(self.SERVICE_STATUS_ERROR)
            self.in_shutdown()
            self.stop_transport()
            return

        try:
            self.__update_service_status(self.SERVICE_STATUS_SHUTDOWN)
            self.in_shutdown()
            self.__update_service_status(self.SERVICE_STATUS_END)
            self.stop_transport()
        except Exception as e:
            self.process_uncaught_exception(e)
            self.__update_service_status(self.SERVICE_STATUS_ERROR)

    def process_uncaught_exception(self, e: BaseException) -> None:
        """This is called to handle otherwise uncaught exceptions from the service.
        The service will terminate either way, but here we can do things such as
        gathering useful environment information and logging for posterity."""
        # Add information about the actual exception to the log message
        # This includes the file, line and piece of code causing the exception.
        # exc_info=True adds the full stack trace to the log message.
        (
            exc_file_fullpath,
            exc_file,
            exc_lineno,
            exc_func,
            exc_line,
        ) = workflows.logging.get_exception_source()
        added_information = {
            "workflows_exc_lineno": exc_lineno,
            "workflows_exc_funcName": exc_func,
            "workflows_exc_line": exc_line,
            "workflows_exc_pathname": exc_file_fullpath,
            "workflows_exc_filename": exc_file,
        }
        for field in filter(lambda x: x.startswith("workflows_log_"), dir(e)):
            added_information[field[14:]] = getattr(e, field, None)
        self.log.critical(
            "Unhandled service exception: %s", e, exc_info=True, extra=added_information
        )

    def __process_command(self, command: str) -> None:
        """Process an incoming command message from the frontend."""
        if command == Commands.SHUTDOWN:
            self.__shutdown = True
        elif command == Commands.LIVENESS_CHECK:
            self.__send_to_frontend({"band": "liveness_check", "payload": "alive"})


class Commands:
    """A list of command strings used for communicating with the frontend."""

    SHUTDOWN = "shutdown"
    LIVENESS_CHECK = "liveness_check"
