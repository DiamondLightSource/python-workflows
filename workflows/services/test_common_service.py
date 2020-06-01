import queue
from multiprocessing import Pipe

import mock
import pytest
from workflows.services.common_service import Commands, CommonService, Priority


def test_instantiate_basic_service():
    """Create a basic service object"""
    service = CommonService()

    assert service.get_name() is not None


def test_logging_to_frontend():
    """Log messages should be passed to frontend"""
    fe_pipe = mock.Mock()
    service = CommonService()

    # Connect service instance to pipe
    service.connect(frontend=fe_pipe)

    # Start service to initialize logging
    service.start()

    # Note that by default only warning and higher are passed to frontend
    service.log.warning(mock.sentinel.logmessage)

    fe_pipe.send.assert_called()
    assert fe_pipe.send.call_args == (({"band": "log", "payload": mock.ANY},), {})
    logrec = fe_pipe.send.call_args[0][0]["payload"]
    assert logrec.levelname == "WARNING"
    assert str(mock.sentinel.logmessage) in logrec.message


def test_adding_fieldvalue_pairs_to_log_messages():
    """Add a custom field to all outgoing log messages using the extend_log mechanism."""
    fe_pipe = mock.Mock()
    logmsg = "Test message for extension"
    service = CommonService()
    service.connect(frontend=fe_pipe)
    service.start()

    fe_pipe.send.reset_mock()
    service.log.warning(logmsg)
    fe_pipe.send.assert_called_once()
    record = fe_pipe.send.call_args[0][0]["payload"]
    assert record.message == logmsg
    assert getattr(record, "something", mock.sentinel.NA) == mock.sentinel.NA
    assert getattr(record, "furtherthing", mock.sentinel.NA) == mock.sentinel.NA

    with service.extend_log("something", "otherthing"):
        fe_pipe.send.reset_mock()
        service.log.warning(logmsg)
        fe_pipe.send.assert_called_once()
        record = fe_pipe.send.call_args[0][0]["payload"]
        assert record.message == logmsg
        assert getattr(record, "something", mock.sentinel.NA) == "otherthing"
        assert getattr(record, "furtherthing", mock.sentinel.NA) == mock.sentinel.NA

        with service.extend_log("furtherthing", "morething"):
            fe_pipe.send.reset_mock()
            service.log.warning(logmsg)
            fe_pipe.send.assert_called_once()
            record = fe_pipe.send.call_args[0][0]["payload"]
            assert record.message == logmsg
            assert getattr(record, "something", mock.sentinel.NA) == "otherthing"
            assert getattr(record, "furtherthing", mock.sentinel.NA) == "morething"

    fe_pipe.send.reset_mock()
    service.log.warning(logmsg)
    fe_pipe.send.assert_called_once()
    record = fe_pipe.send.call_args[0][0]["payload"]
    assert record.message == logmsg
    assert getattr(record, "something", mock.sentinel.NA) == mock.sentinel.NA
    assert getattr(record, "furtherthing", mock.sentinel.NA) == mock.sentinel.NA


def test_log_message_fieldvalue_pairs_are_removed_outside_their_context():
    """Custom fields added to outgoing log messages don't live on outside their extend_log context."""
    fe_pipe = mock.Mock()
    logmsg = "Test message for extension"
    service = CommonService()
    service.connect(frontend=fe_pipe)
    service.start()

    fe_pipe.send.reset_mock()
    service.log.warning(logmsg)
    fe_pipe.send.assert_called_once()
    record = fe_pipe.send.call_args[0][0]["payload"]
    assert record.message == logmsg
    assert getattr(record, "something", mock.sentinel.NA) == mock.sentinel.NA

    trace_exception = ZeroDivisionError(mock.sentinel.trace)
    try:
        with service.extend_log("something", "otherthing"):
            fe_pipe.send.reset_mock()
            service.log.warning(logmsg)
            fe_pipe.send.assert_called_once()
            record = fe_pipe.send.call_args[0][0]["payload"]
            assert record.message == logmsg
            assert getattr(record, "something", mock.sentinel.NA) == "otherthing"

            raise trace_exception
        assert False  # Getting to this point means something went horribly wrong.

    except ZeroDivisionError as e:
        assert e == trace_exception

        fe_pipe.send.reset_mock()
        service.log.warning(logmsg)
        fe_pipe.send.assert_called_once()
        record = fe_pipe.send.call_args[0][0]["payload"]
        assert record.message == logmsg
        assert getattr(record, "something", mock.sentinel.NA) == mock.sentinel.NA


def test_log_message_fieldvalue_pairs_are_attached_to_unhandled_exceptions_and_logged_properly():
    """When an exception falls through the extend_log context handler the fields are removed from future log messages,
    but they are also attached to the exception object, as they may contain valuable information for debugging."""
    fe_pipe = mock.Mock()
    service = CommonService()
    service.connect(frontend=fe_pipe)
    service.start()

    try:
        with service.extend_log("something", "otherthing"):
            nonsense = 7 / 0  # noqa
        assert False  # Getting to this point means something went horribly wrong.
    except ZeroDivisionError as e:
        assert getattr(e, "workflows_log_something", None) == "otherthing"

        # Now check that the default process_uncaught_exception handler adds that
        # field to the resulting log message
        fe_pipe.send.reset_mock()
        service.process_uncaught_exception(e)
        fe_pipe.send.assert_called_once()
        record = fe_pipe.send.call_args[0][0]["payload"]
        import pprint

        pprint.pprint(dir(record))
        assert "Unhandled service exception" in record.message
        assert "zero" in record.message.lower()
        assert getattr(record, "something", mock.sentinel.NA) == "otherthing"


@mock.patch("workflows.services.common_service.threading.Thread")
def test_receive_frontend_commands(mock_thread):
    """Check that messages via the command pipe land on the main queue."""
    cmd_pipe = mock.Mock()
    cmd_pipe.poll.side_effect = [
        True,
        True,
        True,
        AssertionError("Read past pipe lifetime"),
    ]
    cmd_pipe.recv.side_effect = [
        mock.sentinel.msg1,
        mock.sentinel.msg2,
        EOFError(),
        AssertionError("Read past pipe lifetime"),
    ]
    main_queue = mock.Mock()

    # Create service
    service = CommonService()
    service._CommonService__queue = main_queue
    service.connect(commands=cmd_pipe, frontend=mock.Mock())

    # Check that thread would be started
    service._CommonService__start_command_queue_listener()
    mock_thread.start.assert_called_once()
    assert mock_thread.daemon is True
    # Can't make assertions about the run() method, limitations of mocking

    # Now start listener in main thread
    service._CommonService__command_queue_listener()
    main_queue.put.assert_called()
    expected = [
        mock.call((Priority.COMMAND, 0, mock.sentinel.msg1), True, mock.ANY),
        mock.call((Priority.COMMAND, 1, mock.sentinel.msg2), True, mock.ANY),
    ]
    assert main_queue.put.call_args_list == expected


def test_observe_shutdown_command():
    """Receive a shutdown message via the command pipe and act on it.
    Check that status codes are updated properly."""
    start_cmd_queue_listener = mock.Mock()
    main_queue = mock.Mock()
    main_queue.get.side_effect = [
        (Priority.COMMAND, 1, {"band": "command", "payload": Commands.SHUTDOWN}),
        AssertionError("Not observing commands"),
    ]
    fe_pipe, fe_pipe_out = Pipe()

    # Create service
    service = CommonService()
    service.connect(commands=mock.Mock(), frontend=fe_pipe)
    # override class API to ensure overidden functions are called
    service.initializing = mock.Mock()
    service.in_shutdown = mock.Mock()
    # override service queue management
    service._CommonService__start_command_queue_listener = start_cmd_queue_listener
    service._CommonService__queue = main_queue

    # Check new status
    messages = []
    while fe_pipe_out.poll():
        message = fe_pipe_out.recv()
        if "statuscode" in message:
            messages.append(message["statuscode"])
    assert messages == [service.SERVICE_STATUS_NEW]

    # Start service
    service.start()

    # Check startup/shutdown sequence
    service.initializing.assert_called_once()
    service.in_shutdown.assert_called_once()
    main_queue.get.assert_called_once_with()
    messages = []
    while fe_pipe_out.poll():
        message = fe_pipe_out.recv()
        if "statuscode" in message:
            messages.append(message["statuscode"])
    assert messages == [
        service.SERVICE_STATUS_STARTING,
        service.SERVICE_STATUS_IDLE,
        service.SERVICE_STATUS_PROCESSING,
        service.SERVICE_STATUS_SHUTDOWN,
        service.SERVICE_STATUS_END,
    ]


def test_idle_timer_is_triggered():
    """Check that the idle timer callback is run if set."""
    start_cmd_queue_listener = mock.Mock()
    main_queue = mock.Mock()
    main_queue.get.side_effect = [
        queue.Empty(),
        (Priority.COMMAND, None, {"band": "command", "payload": Commands.SHUTDOWN}),
        AssertionError("Not observing commands"),
    ]
    fe_pipe, fe_pipe_out = Pipe()
    idle_trigger = mock.Mock()

    # Create service
    service = CommonService()
    service.connect(commands=mock.Mock(), frontend=fe_pipe)
    service._register_idle(mock.sentinel.idle_time, idle_trigger)

    # Override service queue management
    service._CommonService__start_command_queue_listener = start_cmd_queue_listener
    service._CommonService__queue = main_queue

    # Start service
    service.start()

    # Check trigger has been called after correct time
    idle_trigger.assert_called_once_with()
    main_queue.get.assert_called_with(True, mock.sentinel.idle_time)
    assert main_queue.get.call_count == 2

    # Check startup/shutdown sequence
    start_cmd_queue_listener.assert_called_once_with()
    messages = []
    while fe_pipe_out.poll():
        message = fe_pipe_out.recv()
        if "statuscode" in message:
            messages.append(message["statuscode"])
    assert messages == [
        service.SERVICE_STATUS_NEW,
        service.SERVICE_STATUS_STARTING,
        service.SERVICE_STATUS_IDLE,
        service.SERVICE_STATUS_TIMER,
        service.SERVICE_STATUS_IDLE,
        service.SERVICE_STATUS_PROCESSING,
        service.SERVICE_STATUS_SHUTDOWN,
        service.SERVICE_STATUS_END,
    ]


def test_callbacks_are_routed_correctly():
    """Incoming messages are routed to the correct callback functions"""
    cmd_pipe = mock.Mock()
    cmd_pipe.poll.return_value = True
    cmd_pipe.recv.side_effect = [
        {"band": mock.sentinel.band, "payload": mock.sentinel.payload},
        {"band": "command", "payload": Commands.SHUTDOWN},
        AssertionError("Stop queue reading thread"),
    ]
    fe_pipe, _ = Pipe()
    callback = mock.Mock()

    # Create service
    service = CommonService()
    service.connect(commands=cmd_pipe, frontend=fe_pipe)
    service._register(mock.sentinel.band, callback)

    # Start service
    service.start()

    # Check callback occured
    callback.assert_called_with(mock.sentinel.payload)


def test_log_unknown_band_data():
    """All unidentified messages should be logged to the frondend."""
    cmd_pipe = mock.Mock()
    cmd_pipe.poll.return_value = True
    cmd_pipe.recv.side_effect = [
        {"band": mock.sentinel.band, "payload": mock.sentinel.failure1},
        {"payload": mock.sentinel.failure2},
        {"band": "command", "payload": Commands.SHUTDOWN},
        AssertionError("Not observing commands"),
    ]
    fe_pipe, fe_pipe_out = Pipe()

    # Create service
    service = CommonService()
    service.connect(commands=cmd_pipe, frontend=fe_pipe)

    # Start service
    service.start()

    # Check startup/shutdown sequence
    messages = []
    while fe_pipe_out.poll():
        message = fe_pipe_out.recv()
        if message.get("band") == "log":
            messages.append(message.get("payload"))
    assert len(messages) == 2
    assert messages[0].name == "workflows.service"
    assert "unregistered band" in messages[0].message
    assert str(mock.sentinel.band) in messages[0].message
    assert messages[1].name == "workflows.service"
    assert "without band" in messages[1].message


def test_service_initialization_crashes_are_handled_correctly():
    """Log messages should be passed to frontend"""
    fe_pipe = mock.Mock()

    class CrashOnInitService(CommonService):
        """Helper class to test exception handling.
        This service crashes on initialization."""

        _service_name = "Crashservice 1"
        _logger_name = "workflows.service.crash_on_init"

        @staticmethod
        def initializing():
            """Crash."""
            assert False, "This crash needs to be handled"

    service = CrashOnInitService()
    service.connect(frontend=fe_pipe)
    service.start()

    fe_pipe.send.assert_called()

    # Service status should have been set to ERROR
    fe_pipe.send.assert_any_call(
        {"band": "status_update", "statuscode": service.SERVICE_STATUS_ERROR}
    )

    # Traceback should have been sent to log
    log_msgs = list(
        filter(
            lambda c: c[0][0] == {"band": "log", "payload": mock.ANY}
            and c[0][0]["payload"].levelname == "CRITICAL",
            fe_pipe.send.call_args_list,
        )
    )
    assert log_msgs, "No critical log message received"
    log = log_msgs[0][0][0]["payload"]
    assert "This crash needs to be handled" in log.exc_text
    assert "initializing" in log.exc_text
    assert "test_common_service" in log.exc_text
    assert log.workflows_exc_filename == "test_common_service.py"
    assert log.workflows_exc_funcName == "initializing"


def test_service_can_change_name_and_shut_itself_down():
    """Name changes should be passed to frontend"""
    fe_pipe = mock.Mock()

    class NameChangingService(CommonService):
        """Helper class to test name changing."""

        def initializing(self):
            """Change name."""
            self._set_name(mock.sentinel.newname)
            self._shutdown()

    service = NameChangingService()
    service.connect(frontend=fe_pipe)
    service.start()

    # Check for service name update
    fe_pipe.send.assert_any_call({"band": "set_name", "name": mock.sentinel.newname})
    # Service should have shut down cleanly
    fe_pipe.send.assert_any_call(
        {"band": "status_update", "statuscode": service.SERVICE_STATUS_END}
    )


def test_service_can_mark_itself_as_unstable():
    """A termination request should be passed to the frontend."""
    fe_pipe = mock.Mock()

    class UnstableService(CommonService):
        """Helper class to test self-termination requests."""

        def initializing(self):
            """Change name."""
            self._request_termination()

    service = UnstableService()
    service.connect(frontend=fe_pipe)
    service.start()

    # Check for service name update
    fe_pipe.send.assert_any_call({"band": "request_termination"})


def test_can_pass_environment_to_service():
    """Test that environment dictionaries can be passed to the service on construction and are available during runtime."""
    fe_pipe = mock.Mock()

    class EnvironmentPassingService(CommonService):
        """Helper class to test environment passing."""

        _service_name = "Environmentservice"
        _logger_name = "workflows.service.environment"

        def get_environment(self):
            """Make the service environment available to the caller."""
            return self._environment

    # Initialization without enviroment should still result in an available dictionary
    service = EnvironmentPassingService()
    service.connect(frontend=fe_pipe)
    assert (
        service.get_environment().get("non-existing-key-for-environment-passing-test")
        is None
    )

    # Check that passed environment is available
    sample_environment = {"environment": mock.sentinel.environment}
    service = EnvironmentPassingService(environment=sample_environment)
    service.connect(frontend=fe_pipe)
    assert service.get_environment().get("environment") == mock.sentinel.environment


def test_transport_object_can_be_injected():
    """Transport object must be stored, but not connected immediately."""
    service = CommonService()

    transport = mock.Mock()
    service.transport = transport

    assert service.transport == transport
    transport.connect.assert_not_called()


def test_transport_object_can_not_be_overwritten():
    """Transport object must be stored, but not connected immediately."""
    service = CommonService()

    transport1 = mock.Mock()
    transport2 = mock.Mock()
    service.transport = transport1
    with pytest.raises(AttributeError):
        service.transport = transport2

    assert service.transport == transport1


def test_transport_connection_is_started_on_initialization():
    """Transport object must be stored, but not connected immediately."""
    service = CommonService()
    transport = mock.Mock()
    service.transport = transport

    service.start()

    transport.connect.assert_called_once_with()


def test_main_queue_can_deal_with_being_empty():
    """Checks that the main service queue can be empty without crashing the service."""
    start_cmd_queue_listener = mock.Mock()
    main_queue = mock.Mock()
    main_queue.poll.side_effect = [False, True]
    main_queue.get.side_effect = [
        (Priority.COMMAND, None, {"band": "command", "payload": Commands.SHUTDOWN}),
        AssertionError("Not observing commands"),
    ]
    fe_pipe, fe_pipe_out = Pipe()

    # Create service
    service = CommonService()
    service.connect(commands=mock.Mock(), frontend=fe_pipe)

    # Override service queue management
    service._CommonService__start_command_queue_listener = start_cmd_queue_listener
    service._CommonService__queue = main_queue

    # Start service
    service.start()

    # Check startup/shutdown sequence
    start_cmd_queue_listener.assert_called_once_with()
    messages = []
    while fe_pipe_out.poll():
        message = fe_pipe_out.recv()
        if "statuscode" in message:
            messages.append(message["statuscode"])
    assert messages == [
        service.SERVICE_STATUS_NEW,
        service.SERVICE_STATUS_STARTING,
        service.SERVICE_STATUS_IDLE,
        service.SERVICE_STATUS_PROCESSING,
        service.SERVICE_STATUS_SHUTDOWN,
        service.SERVICE_STATUS_END,
    ]


def test_commands_are_processed_from_main_queue_before_transport():
    """Checks that the priority queue behaves as such."""
    start_cmd_queue_listener = mock.Mock()
    fe_pipe, fe_pipe_out = Pipe()

    mock_low_priority_callback = mock.Mock()

    # Create service
    service = CommonService()
    service.connect(commands=mock.Mock(), frontend=fe_pipe)
    main_queue = service._CommonService__queue
    main_queue.put((Priority.TRANSPORT, 1, (mock_low_priority_callback, None, None)))
    main_queue.put(
        (Priority.COMMAND, 1, {"band": "command", "payload": Commands.SHUTDOWN})
    )

    # Override service queue management
    service._CommonService__start_command_queue_listener = start_cmd_queue_listener

    # Start service
    service.start()

    # Service should terminate before processing the transport callback
    assert mock_low_priority_callback.called is False

    # Check startup/shutdown sequence
    start_cmd_queue_listener.assert_called_once_with()
    messages = []
    while fe_pipe_out.poll():
        message = fe_pipe_out.recv()
        if "statuscode" in message:
            messages.append(message["statuscode"])
    assert messages == [
        service.SERVICE_STATUS_NEW,
        service.SERVICE_STATUS_STARTING,
        service.SERVICE_STATUS_IDLE,
        service.SERVICE_STATUS_PROCESSING,
        service.SERVICE_STATUS_SHUTDOWN,
        service.SERVICE_STATUS_END,
    ]


def test_transport_callbacks_are_processed_from_main_queue():
    """Checks that the transport callbacks are properly processed from the main queue."""
    start_cmd_queue_listener = mock.Mock()
    mock_transport_callback = mock.Mock()
    main_queue = mock.Mock()
    main_queue.get.side_effect = [
        (
            Priority.TRANSPORT,
            1,
            (mock_transport_callback, mock.sentinel.header, mock.sentinel.message),
        ),
        (Priority.COMMAND, None, {"band": "command", "payload": Commands.SHUTDOWN}),
        AssertionError("Not observing commands"),
    ]
    fe_pipe, fe_pipe_out = Pipe()

    # Create service
    service = CommonService()
    service.connect(commands=mock.Mock(), frontend=fe_pipe)

    # Override service queue management
    service._CommonService__start_command_queue_listener = start_cmd_queue_listener
    service._CommonService__queue = main_queue

    # Start service
    service.start()

    # Service must call transport callback
    mock_transport_callback.assert_called_once_with(
        mock.sentinel.header, mock.sentinel.message
    )

    # Check startup/shutdown sequence
    start_cmd_queue_listener.assert_called_once_with()
    messages = []
    while fe_pipe_out.poll():
        message = fe_pipe_out.recv()
        if "statuscode" in message:
            messages.append(message["statuscode"])
    assert messages == [
        service.SERVICE_STATUS_NEW,
        service.SERVICE_STATUS_STARTING,
        service.SERVICE_STATUS_IDLE,
        service.SERVICE_STATUS_PROCESSING,
        service.SERVICE_STATUS_IDLE,
        service.SERVICE_STATUS_PROCESSING,
        service.SERVICE_STATUS_SHUTDOWN,
        service.SERVICE_STATUS_END,
    ]
