from __future__ import annotations

from unittest import mock

import pytest

import workflows.contrib.start_service


def test_get_command_line_help(capsys):
    """Running the start_service script with --help should display command line help and exit."""
    with pytest.raises(SystemExit):
        workflows.contrib.start_service.ServiceStarter().run(
            ["--help"], program_name="sentinelvalue"
        )
    out, err = capsys.readouterr()
    assert "Usage: sentinelvalue" in out


@mock.patch("workflows.contrib.start_service.OptionParser")
@mock.patch("workflows.contrib.start_service.workflows.transport.lookup")
@mock.patch("workflows.contrib.start_service.workflows.frontend")
@mock.patch("workflows.contrib.start_service.workflows.services")
def test_script_initialises_transport_and_starts_frontend(
    mock_services, mock_frontend, mock_tlookup, mock_parser
):
    """Check that the start_service script sets up the transport mechanism and the frontend properly.
    Correct service should be selected and the frontend started."""
    mock_options = mock.Mock()
    mock_options.service = "someservice"
    mock_options.transport = mock.sentinel.transport
    mock_options.liveness = False
    mock_options.workflows_queue = []
    mock_parser.return_value.parse_args.return_value = (mock_options, mock.Mock())
    mock_services.get_known_services.return_value = {"SomeService": None}

    workflows.contrib.start_service.ServiceStarter().run(
        cmdline_args=["-s", "some"], version=mock.sentinel.version
    )

    mock_tlookup.assert_called_once_with(mock.sentinel.transport)
    mock_parser.assert_called_once_with(
        usage=mock.ANY,
        version=f"{mock.sentinel.version} (workflows {workflows.version()})",
    )
    mock_frontend.Frontend.assert_called_once_with(
        service="SomeService",
        transport=mock.ANY,
        environment={},
    )
    mock_frontend.Frontend.return_value.run.assert_called_once_with()


@mock.patch("workflows.contrib.start_service.OptionParser")
@mock.patch("workflows.contrib.start_service.workflows.transport.lookup")
@mock.patch("workflows.contrib.start_service.workflows.frontend")
@mock.patch("workflows.contrib.start_service.workflows.services")
def test_add_metrics_option(mock_services, mock_frontend, mock_tlookup, mock_parser):
    mock_options = mock.Mock()
    mock_options.service = "someservice"
    mock_options.transport = mock.sentinel.transport
    mock_options.liveness = False
    mock_options.workflows_queue = []
    mock_options.metrics = False
    mock_options.metrics_port = 4242
    mock_parser.return_value.parse_args.return_value = (mock_options, mock.Mock())
    mock_services.get_known_services.return_value = {"SomeService": None}

    workflows.contrib.start_service.ServiceStarter().run(
        cmdline_args=["-s", "some"],
        version=mock.sentinel.version,
        add_metrics_option=True,
    )
    mock_tlookup.assert_called_once_with(mock.sentinel.transport)
    mock_frontend.Frontend.assert_called_once_with(
        service="SomeService",
        transport=mock.ANY,
        environment={},
    )

    mock_tlookup.reset_mock()
    mock_options.metrics = True
    workflows.contrib.start_service.ServiceStarter().run(
        cmdline_args=["-s", "some", "--metrics"],
        version=mock.sentinel.version,
        add_metrics_option=True,
    )
    mock_tlookup.assert_called_once_with(mock.sentinel.transport)
    mock_frontend.Frontend.assert_called_with(
        service="SomeService",
        transport=mock.ANY,
        environment={"metrics": {"port": 4242}},
    )


@pytest.mark.parametrize(
    "args,environment",
    [
        ([], {}),
        (["-q", "other"], {"queue": "other"}),
        (
            ["--queue", "a=a.test", "-q", "b=b.test"],
            {"queues": {"a": "a.test", "b": "b.test"}},
        ),
        (
            ["-q", "a=a.test", "-q", "other"],
            {"queue": "other", "queues": {"a": "a.test"}},
        ),
    ],
)
@mock.patch("workflows.contrib.start_service.workflows.transport.lookup")
@mock.patch("workflows.contrib.start_service.workflows.frontend")
@mock.patch("workflows.contrib.start_service.workflows.services")
def test_queue_option(mock_services, mock_frontend, mock_tlookup, args, environment):
    mock_services.get_known_services.return_value = {"SomeService": None}
    workflows.contrib.start_service.ServiceStarter().run(
        cmdline_args=["-s", "SomeService", *args]
    )
    assert mock_frontend.Frontend.call_args.kwargs["environment"] == environment


@mock.patch("workflows.contrib.start_service.workflows.transport.lookup")
@mock.patch("workflows.contrib.start_service.workflows.frontend")
@mock.patch("workflows.contrib.start_service.workflows.services")
def test_queue_option_allows_one_unnamed_queue(
    mock_services, mock_frontend, mock_tlookup, capsys
):
    mock_services.get_known_services.return_value = {"SomeService": None}
    with pytest.raises(SystemExit):
        workflows.contrib.start_service.ServiceStarter().run(
            cmdline_args=["-s", "SomeService", "-q", "a", "-q", "b"]
        )
    assert "Only one --queue entry" in capsys.readouterr().err
    mock_frontend.Frontend.assert_not_called()


@pytest.mark.parametrize(
    "variable,args,environment",
    [
        ("other", [], {"queue": "other"}),
        ("a=a.env, b=b.env", [], {"queues": {"a": "a.env", "b": "b.env"}}),
        (
            "a=a.env b=b.env other",
            [],
            {"queue": "other", "queues": {"a": "a.env", "b": "b.env"}},
        ),
        # Command line options override the environment variable
        (
            "a=a.env,b=b.env",
            ["-q", "a=a.cli"],
            {"queues": {"a": "a.cli", "b": "b.env"}},
        ),
        ("env", ["-q", "cli"], {"queue": "cli"}),
        ("env", ["-q", "a=a.cli"], {"queue": "env", "queues": {"a": "a.cli"}}),
        ("", [], {}),
    ],
)
@mock.patch("workflows.contrib.start_service.workflows.transport.lookup")
@mock.patch("workflows.contrib.start_service.workflows.frontend")
@mock.patch("workflows.contrib.start_service.workflows.services")
def test_queue_environment_variable(
    mock_services, mock_frontend, mock_tlookup, monkeypatch, variable, args, environment
):
    monkeypatch.setenv("WORKFLOWS_QUEUE", variable)
    mock_services.get_known_services.return_value = {"SomeService": None}
    workflows.contrib.start_service.ServiceStarter().run(
        cmdline_args=["-s", "SomeService", *args]
    )
    assert mock_frontend.Frontend.call_args.kwargs["environment"] == environment


@pytest.mark.parametrize(
    "variable,args,error",
    [
        ("a b", [], "Only one WORKFLOWS_QUEUE entry"),
        ("a=", [], "Missing queue name in WORKFLOWS_QUEUE entry 'a='"),
        ("", ["-q", "a="], "Missing queue name in --queue entry 'a='"),
    ],
)
@mock.patch("workflows.contrib.start_service.workflows.transport.lookup")
@mock.patch("workflows.contrib.start_service.workflows.frontend")
@mock.patch("workflows.contrib.start_service.workflows.services")
def test_queue_overrides_rejected(
    mock_services,
    mock_frontend,
    mock_tlookup,
    monkeypatch,
    capsys,
    variable,
    args,
    error,
):
    monkeypatch.setenv("WORKFLOWS_QUEUE", variable)
    mock_services.get_known_services.return_value = {"SomeService": None}
    with pytest.raises(SystemExit):
        workflows.contrib.start_service.ServiceStarter().run(
            cmdline_args=["-s", "SomeService", *args]
        )
    assert error in capsys.readouterr().err
    mock_frontend.Frontend.assert_not_called()


class StarterWithOwnQueueOption(workflows.contrib.start_service.ServiceStarter):
    """A subclass defining its own queue option, as dlstbx did before workflows"""

    def __init__(self, *option_strings: str):
        self.option_strings = option_strings or ("-q", "--queue")

    def on_parser_preparation(self, parser):
        parser.add_option(*self.option_strings, dest="queue", default=None)

    def on_parsing(self, options, args):
        self.options = options

    def before_frontend_construction(self, kwargs):
        if self.options.queue:
            kwargs["environment"]["queue"] = self.options.queue
        return kwargs


@pytest.mark.parametrize("option_strings", [(), ("--queue",), ("-q", "--quiet")])
@mock.patch("workflows.contrib.start_service.workflows.transport.lookup")
@mock.patch("workflows.contrib.start_service.workflows.frontend")
@mock.patch("workflows.contrib.start_service.workflows.services")
def test_queue_option_skipped_if_already_defined(
    mock_services, mock_frontend, mock_tlookup, monkeypatch, option_strings
):
    monkeypatch.setenv("WORKFLOWS_QUEUE", "a=a.env")
    mock_services.get_known_services.return_value = {"SomeService": None}
    starter = StarterWithOwnQueueOption(*option_strings)
    flag = (option_strings or ("-q",))[0]
    starter.run(cmdline_args=["-s", "SomeService", flag, "own"])

    # The subclass option is parsed as before, as a single value
    assert starter.options.queue == "own"
    assert not hasattr(starter.options, "workflows_queue")
    # The environment variable still applies
    assert mock_frontend.Frontend.call_args.kwargs["environment"] == {
        "queue": "own",
        "queues": {"a": "a.env"},
    }
