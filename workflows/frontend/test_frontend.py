import mock
import pytest
import workflows.frontend
from workflows.services.common_service import CommonService

### Helper classes used in tests ##############################################


class ServiceCrashingOnInit(CommonService):
    """A service that raises an unhandled exception."""

    @staticmethod
    def initializing():
        """Raise AssertionError.
        This should set the error state, kill the service and cause the frontend
        to leave its main loop."""
        assert False  # pragma: no cover


class MockPipe:
    """An object that behaves like a pipe."""

    def __init__(self, contents):
        """Load up contents into pipe."""
        self.contents = contents

    @staticmethod
    def poll(time=None):
        """Check if pipe is empty. There is always something there, either
        some content or an EOFError."""
        return True

    def recv(self):
        """Return first item off the list or raise exception.
        Call callback function if defined and pipe is emptied."""
        if not self.contents:
            raise EOFError("Pipe is empty")
        return_value = self.contents.pop(0)
        if isinstance(return_value, Exception):
            raise return_value
        return return_value

    @staticmethod
    def close():
        """This pipe can't be written to anyway. Ignore call."""

    def assert_empty(self):
        """Pipe must not contain any valid messages. Exceptions may remain."""
        assert all(isinstance(m, Exception) for m in self.contents)


def assert_single_call_only(target):
    def wrapper(*args, **kwargs):
        if hasattr(wrapper, "called"):
            raise Exception("Only a single call to object %s is allowed" % str(target))
        setattr(wrapper, "called", True)
        return target(*args, **kwargs)

    return wrapper


###############################################################################


@mock.patch("workflows.frontend.multiprocessing")
@mock.patch("workflows.frontend.workflows.transport")
def test_frontend_connects_to_transport_layer(mock_transport, mock_mp):
    """Frontend should call connect method on transport layer module and subscribe to a unique command queue."""
    workflows.frontend.Frontend()
    mock_transport.lookup.assert_called_once_with(None)
    mock_transport.lookup.return_value.assert_called_once_with()
    mock_transport.lookup.return_value.return_value.connect.assert_called_once_with()

    workflows.frontend.Frontend(transport="some_transport")
    mock_transport.lookup.assert_called_with("some_transport")

    mock_trn = mock.Mock()
    workflows.frontend.Frontend(mock_trn)
    mock_trn.assert_called_once_with()
    mock_trn.return_value.connect.assert_called_once_with()


@mock.patch("workflows.frontend.workflows.transport")
def test_frontend_subscribes_to_command_channel(mock_transport):
    """Frontend should call connect method on transport layer module and subscribe to a unique command queue."""
    transport = mock_transport.lookup.return_value.return_value
    fe = workflows.frontend.Frontend()
    mock_transport.lookup.assert_called_once_with(None)
    transport.connect.assert_called_once()
    transport.subscribe.assert_not_called()

    def frontend_with_message(message):
        transport.reset_mock()
        fe = workflows.frontend.Frontend(
            transport_command_channel=mock.sentinel.command
        )
        transport.subscribe_broadcast.assert_called_once_with(
            mock.sentinel.command, mock.ANY
        )
        transport.subscribe_broadcast.call_args[0][1]({}, message)
        return fe

    assert frontend_with_message({}).shutdown is False
    assert frontend_with_message({"command": "shutdown"}).shutdown is False
    assert (
        frontend_with_message(
            {"command": "shutdown", "host": fe.get_host_id()}
        ).shutdown
        is True
    )
    assert (
        frontend_with_message(
            {"command": "shutdown", "host": mock.sentinel.different_host}
        ).shutdown
        is False
    )
    assert (
        frontend_with_message(
            {"command": "shutdown", "service": fe.get_status()["serviceclass"]}
        ).shutdown
        is True
    )
    assert (
        frontend_with_message(
            {"command": "shutdown", "service": mock.sentinel.different_service}
        ).shutdown
        is False
    )


@mock.patch("workflows.frontend.multiprocessing")
@mock.patch("workflows.frontend.workflows.transport")
def test_start_service_in_frontend(mock_transport, mock_mp):
    """Check that the service is being run and connected to the frontend via the correct pipes."""
    mock_service = mock.Mock()
    pipes = [mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock()]
    mock_mp.Pipe.side_effect = [(pipes[0], pipes[1]), (pipes[2], pipes[3]), None]

    # initialize frontend
    fe = workflows.frontend.Frontend(environment=mock.sentinel.environment)

    # start service
    fe.switch_service(mock_service)

    # check service was started properly
    mock_service.assert_called_once_with(environment=mock.sentinel.environment)
    mock_service.return_value.connect.assert_called_once_with(
        commands=pipes[0], frontend=pipes[3]
    )
    mock_mp.Process.assert_called_once_with(
        target=mock_service.return_value.start, args=(), kwargs=mock.ANY
    )
    mock_mp.Process.return_value.start.assert_called_once()

    # Fun with python multiprocessing:
    # Because the pipe file descriptors are copied they must be closed in the process not using them
    pipes[0].close.assert_called_once_with()
    pipes[3].close.assert_called_once_with()


@mock.patch("workflows.frontend.workflows.transport")
def test_get_frontend_status(mock_transport):
    """Check that the get_status-method works and contains the correct host-id."""
    fe = workflows.frontend.Frontend()
    status = fe.get_status()
    assert status["host"] == fe.get_host_id()


@pytest.mark.timeout(3)
def test_frontend_can_handle_unhandled_service_initialization_exceptions():
    """When a service crashes on initialization an exception should be thrown."""
    transport = mock.Mock()

    fe = workflows.frontend.Frontend(
        transport=transport, service=assert_single_call_only(ServiceCrashingOnInit)
    )
    transport = transport.return_value
    transport.connect.assert_called_once()

    with pytest.raises(workflows.Error):
        fe.run()

    status_list = [
        args[0].get("status")
        for args, kwargs in transport.broadcast_status.call_args_list
        if args
    ]
    assert status_list == [
        CommonService.SERVICE_STATUS_NEW,
        CommonService.SERVICE_STATUS_STARTING,
        CommonService.SERVICE_STATUS_ERROR,
        CommonService.SERVICE_STATUS_END,
    ]


@mock.patch("workflows.frontend.multiprocessing")
def test_frontend_can_handle_service_initialization_segfaults(mock_mp):
    """When a service crashes on initialization an exception should be thrown."""
    transport = mock.Mock()
    service = mock.Mock()
    service_process = mock.Mock()
    dummy_pipe = mock.Mock()
    dummy_pipe.recv.side_effect = EOFError()  # Dead on arrival
    mock_mp.Pipe.return_value = (dummy_pipe, dummy_pipe)
    mock_mp.Process.return_value = service_process

    fe = workflows.frontend.Frontend(transport=transport, service=service)
    transport = transport.return_value
    transport.connect.assert_called_once()
    mock_mp.Process.assert_called_once_with(
        target=service.return_value.start, args=(), kwargs=mock.ANY
    )

    with pytest.raises(workflows.Error):
        fe.run()
    service_process.start.assert_called()
    service_process.join.assert_called()


@mock.patch("workflows.frontend.multiprocessing")
def test_frontend_terminates_on_transport_disconnection(mock_mp):
    """When the transport connection is lost permanently, the frontend should stop."""
    transport = mock.Mock()
    service = mock.Mock()
    service_process = mock.Mock()
    dummy_pipe = mock.Mock()
    dummy_pipe.poll.side_effect = [
        False,
        Exception("Frontend did not terminate on transport disconnect"),
    ]
    mock_mp.Pipe.return_value = (dummy_pipe, dummy_pipe)
    mock_mp.Process.return_value = service_process
    service_process.is_alive.return_value = True

    fe = workflows.frontend.Frontend(transport=transport, service=service)
    transport = transport.return_value
    transport.connect.assert_called_once()
    transport.is_connected.return_value = False
    mock_mp.Process.assert_called_once_with(
        target=service.return_value.start, args=(), kwargs=mock.ANY
    )

    with pytest.raises(workflows.Error):
        fe.run()

    service_process.terminate.assert_called()
    service_process.join.assert_called()


@mock.patch("workflows.frontend.multiprocessing")
def test_frontend_parses_status_updates(mock_mp):
    """The frontend should forward status updates to the advertiser thread when appropriate."""
    transport = mock.Mock()
    service_process = mock.Mock()
    mock_mp.Process.return_value = service_process

    dummy_pipe = MockPipe(
        [
            {"band": "status_update", "statuscode": CommonService.SERVICE_STATUS_NEW},
            {
                "band": "status_update",
                "statuscode": CommonService.SERVICE_STATUS_STARTING,
            },
            {
                "band": "status_update",
                "statuscode": CommonService.SERVICE_STATUS_PROCESSING,
            },
            {
                "band": "status_update",
                "statuscode": CommonService.SERVICE_STATUS_SHUTDOWN,
            },
            {"band": "status_update", "statuscode": CommonService.SERVICE_STATUS_END},
        ]
    )
    mock_mp.Pipe.return_value = (dummy_pipe, dummy_pipe)

    fe = workflows.frontend.Frontend(transport=transport, service=mock.Mock())

    # intercept status code updates
    status_list = []
    original_status_update_fn = fe.update_status

    def intercept(*args, **kwargs):
        if "status_code" in kwargs:
            status_list.append(kwargs["status_code"])
        return original_status_update_fn(*args, **kwargs)

    fe.update_status = intercept

    fe.run()

    dummy_pipe.assert_empty()
    assert status_list == [
        CommonService.SERVICE_STATUS_NEW,
        CommonService.SERVICE_STATUS_STARTING,
        CommonService.SERVICE_STATUS_PROCESSING,
        CommonService.SERVICE_STATUS_SHUTDOWN,
        CommonService.SERVICE_STATUS_END,  # Following updates caused by frontend
        CommonService.SERVICE_STATUS_END,
        CommonService.SERVICE_STATUS_TEARDOWN,
    ]


@mock.patch("workflows.frontend.multiprocessing")
def test_frontend_parses_termination_requests(mock_mp):
    """The frontend should observe termination requests from the server."""
    transport = mock.Mock()
    service_process = mock.Mock()
    mock_mp.Process.return_value = service_process

    dummy_pipe = MockPipe(
        [
            {"band": "status_update", "statuscode": CommonService.SERVICE_STATUS_NEW},
            {
                "band": "status_update",
                "statuscode": CommonService.SERVICE_STATUS_STARTING,
            },
            {
                "band": "status_update",
                "statuscode": CommonService.SERVICE_STATUS_PROCESSING,
            },
            {"band": "request_termination"},
            RuntimeError("Service termination request not observed"),
        ]
    )
    mock_mp.Pipe.return_value = (dummy_pipe, dummy_pipe)

    fe = workflows.frontend.Frontend(transport=transport, service=mock.Mock())

    # intercept status code updates
    status_list = []
    original_status_update_fn = fe.update_status

    def intercept(*args, **kwargs):
        if "status_code" in kwargs:
            status_list.append(kwargs["status_code"])
        return original_status_update_fn(*args, **kwargs)

    fe.update_status = intercept

    fe.run()

    dummy_pipe.assert_empty()
    assert status_list == [
        CommonService.SERVICE_STATUS_NEW,
        CommonService.SERVICE_STATUS_STARTING,
        CommonService.SERVICE_STATUS_PROCESSING,
        CommonService.SERVICE_STATUS_END,
        CommonService.SERVICE_STATUS_TEARDOWN,
    ]


@mock.patch("workflows.frontend.time")
@mock.patch("workflows.frontend.multiprocessing")
def test_frontend_sends_status_updates(mock_mp, mock_time):
    """The frontend should send status updates on update_status() calls.
    Some sensible rate limiting must be applied."""
    transport = mock.Mock()
    mock_time.time.return_value = 10

    fe = workflows.frontend.Frontend(transport=transport)

    transport = transport.return_value
    transport.broadcast_status.assert_called_once()
    assert (
        transport.broadcast_status.call_args[0][0]["status"]
        == CommonService.SERVICE_STATUS_NONE
    )
    transport.broadcast_status.reset_mock()

    fe.update_status(status_code=CommonService.SERVICE_STATUS_STARTING)

    transport.broadcast_status.assert_called_once()
    assert (
        transport.broadcast_status.call_args[0][0]["status"]
        == CommonService.SERVICE_STATUS_STARTING
    )
    transport.broadcast_status.reset_mock()

    # subsequent update call that does not change anything should be ignored
    fe.update_status()
    transport.broadcast_status.assert_not_called()

    # time passes. Update call should cause broadcast.
    mock_time.time.return_value = 20
    fe.update_status()
    transport.broadcast_status.assert_called_once()
    assert (
        transport.broadcast_status.call_args[0][0]["status"]
        == CommonService.SERVICE_STATUS_STARTING
    )
    transport.broadcast_status.reset_mock()

    # not much time passes. Update call should still cause broadcast, because status has not been seen before
    mock_time.time.return_value = 20.1
    fe.update_status(status_code=CommonService.SERVICE_STATUS_PROCESSING)
    transport.broadcast_status.assert_called_once()
    assert (
        transport.broadcast_status.call_args[0][0]["status"]
        == CommonService.SERVICE_STATUS_PROCESSING
    )
    transport.broadcast_status.reset_mock()

    # not much time passes. Update call should not broadcast, because this is a recent IDLE status
    mock_time.time.return_value = 20.2
    fe.update_status(status_code=CommonService.SERVICE_STATUS_IDLE)
    transport.broadcast_status.assert_not_called()

    # not much time passes. Update call should not broadcast, because the announced status has not changed
    mock_time.time.return_value = 20.3
    fe.update_status(status_code=CommonService.SERVICE_STATUS_PROCESSING)
    transport.broadcast_status.assert_not_called()
    transport.broadcast_status.reset_mock()

    # not much time passes. Update call should still not broadcast, because this is again a recent IDLE status
    mock_time.time.return_value = 20.4
    fe.update_status(status_code=CommonService.SERVICE_STATUS_IDLE)
    transport.broadcast_status.assert_not_called()

    # not much time passes. Update call should still not broadcast, because this IDLE is still too recent
    mock_time.time.return_value = 20.8
    fe.update_status(status_code=CommonService.SERVICE_STATUS_IDLE)
    transport.broadcast_status.assert_not_called()

    # however as time passes the update call should cause a late broadcast
    mock_time.time.return_value = 21
    fe.update_status()
    transport.broadcast_status.assert_called_once()
    assert (
        transport.broadcast_status.call_args[0][0]["status"]
        == CommonService.SERVICE_STATUS_IDLE
    )


@mock.patch("workflows.frontend.multiprocessing")
def test_frontend_does_not_restart_nonrestartable_service_on_segfault(mock_mp):
    """When the frontend is constructed with restart_service=False failing services must not be restarted."""
    service_factory = mock.Mock()
    service_process = mock.Mock()
    dummy_pipe = mock.Mock()
    dummy_pipe.recv.side_effect = EOFError()  # Dead on arrival
    mock_mp.Pipe.return_value = (dummy_pipe, dummy_pipe)
    mock_mp.Process.return_value = service_process

    service_instances = [mock.Mock()]
    service_factory.side_effect = service_instances + [
        Exception("More than one service object instantiated")
    ]

    fe = workflows.frontend.Frontend(
        transport=mock.Mock(), service=service_factory, restart_service=False
    )
    with pytest.raises(workflows.Error):
        fe.run()

    service_factory.assert_called_once()
    service_process.start.assert_called_once()
    service_process.join.assert_called_once()
    mock_mp.Process.assert_called_once_with(
        target=service_instances[0].start, args=(), kwargs=mock.ANY
    )


@mock.patch("workflows.frontend.multiprocessing")
def test_frontend_does_not_restart_nonrestartable_service_on_error(mock_mp):
    """When the frontend is constructed with restart_service=False failing services must not be restarted."""
    service_instances = [mock.Mock()]
    service_factory = mock.Mock()
    service_factory.side_effect = service_instances + [
        Exception("More than one service object instantiated")
    ]
    service_process = mock.Mock()
    mock_mp.Process.return_value = service_process

    dummy_pipe = MockPipe(
        [
            {"band": "status_update", "statuscode": CommonService.SERVICE_STATUS_NEW},
            {
                "band": "status_update",
                "statuscode": CommonService.SERVICE_STATUS_STARTING,
            },
            {
                "band": "status_update",
                "statuscode": CommonService.SERVICE_STATUS_PROCESSING,
            },
            {"band": "status_update", "statuscode": CommonService.SERVICE_STATUS_ERROR},
        ]
    )
    mock_mp.Pipe.return_value = (dummy_pipe, dummy_pipe)

    fe = workflows.frontend.Frontend(
        transport=mock.Mock(), service=service_factory, restart_service=False
    )
    with pytest.raises(workflows.Error):
        fe.run()

    service_factory.assert_called_once()
    service_process.start.assert_called_once()
    service_process.join.assert_called_once()


@mock.patch("workflows.frontend.multiprocessing")
def test_frontend_does_restart_restartable_service_on_segfault(mock_mp):
    """When the frontend is constructed with restart_service=True failing services must be restarted."""
    transport_factory = mock.Mock()
    transport1 = mock.Mock()
    transport2 = mock.Mock()
    transport3 = mock.Mock()
    transport1.return_value.connect.return_value = True
    transport2.return_value.connect.return_value = True
    transport3.return_value.connect.return_value = True
    transport_factory.side_effect = [transport1, transport2, transport3, None]
    service_factory = mock.Mock()
    service_process = mock.Mock()
    dummy_pipe = mock.Mock()
    dummy_pipe.recv.side_effect = EOFError()  # Dead on arrival
    mock_mp.Pipe.return_value = (dummy_pipe, dummy_pipe)
    mock_mp.Process.return_value = service_process

    sentinel_exception = Exception("break loop")

    service_instances = [mock.Mock(), mock.Mock()]
    service_factory.side_effect = service_instances + [sentinel_exception]

    fe = workflows.frontend.Frontend(
        transport=transport_factory, service=service_factory, restart_service=True
    )
    with pytest.raises(Exception) as e:
        fe.run()
    assert e.value == sentinel_exception

    assert service_factory.call_count == 3
    assert service_process.start.call_count == 2
    assert service_process.join.call_count == 2
    mock_mp.Process.assert_has_calls(
        [
            mock.call(args=(), kwargs=mock.ANY, target=service_instances[0].start),
            mock.call(args=(), kwargs=mock.ANY, target=service_instances[1].start),
        ],
        any_order=True,
    )
    assert service_instances[0].transport == transport2
    assert service_instances[1].transport == transport3
    assert transport1.connect.call_count == 1
    assert transport2.connect.call_count == 0
    assert transport3.connect.call_count == 0


@mock.patch("workflows.frontend.multiprocessing")
def test_frontend_does_restart_restartable_service_on_error(mock_mp):
    """When the frontend is constructed with restart_service=True failing services must be restarted."""
    sentinel_exception = Exception("break loop")
    service_instances = [mock.Mock(), mock.Mock()]
    service_factory = mock.Mock()
    service_factory.side_effect = service_instances + [sentinel_exception]
    service_process = mock.Mock()
    mock_mp.Process.return_value = service_process

    def pipe_creator(*args, **kwargs):
        """Pipe creator creates pipes."""
        dummy_pipe = MockPipe(
            [
                {
                    "band": "status_update",
                    "statuscode": CommonService.SERVICE_STATUS_NEW,
                },
                {
                    "band": "status_update",
                    "statuscode": CommonService.SERVICE_STATUS_STARTING,
                },
                {
                    "band": "status_update",
                    "statuscode": CommonService.SERVICE_STATUS_PROCESSING,
                },
                {
                    "band": "status_update",
                    "statuscode": CommonService.SERVICE_STATUS_ERROR,
                },
            ]
        )
        return (dummy_pipe, dummy_pipe)

    mock_mp.Pipe.side_effect = pipe_creator
    fe = workflows.frontend.Frontend(
        transport=mock.Mock(), service=service_factory, restart_service=True
    )
    try:
        fe.run()
        assert False, "Exception should have been raised"
    except Exception as e:
        if e != sentinel_exception:
            raise

    assert service_factory.call_count == 3
    assert service_process.start.call_count == 2
    assert service_process.join.call_count == 2
    mock_mp.Process.assert_has_calls(
        [
            mock.call(args=(), kwargs=mock.ANY, target=service_instances[0].start),
            mock.call(args=(), kwargs=mock.ANY, target=service_instances[1].start),
        ],
        any_order=True,
    )
