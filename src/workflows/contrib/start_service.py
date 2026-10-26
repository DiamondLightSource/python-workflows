from __future__ import annotations

import optparse
import os
import sys
from collections.abc import Callable
from optparse import SUPPRESS_HELP, OptionParser
from typing import Any

import workflows
import workflows.frontend
import workflows.services
import workflows.transport
from workflows.transport.common_transport import CommonTransport

# Environment variable holding queue overrides, as entries separated by commas
# or whitespace. Each entry is NAME=QUEUE, to consume QUEUE in place of the
# default queue NAME, or a bare QUEUE for a service consuming a single queue,
# which may be given at most once. This is the same format as the --queue option.
QUEUE_ENVIRONMENT_VARIABLE = "WORKFLOWS_QUEUE"


def _parse_queue_overrides(
    entries: list[str], source: str
) -> tuple[str | None, dict[str, str]]:
    """Split [NAME=]QUEUE entries into the unnamed queue and named overrides"""
    queue: str | None = None
    queues: dict[str, str] = {}
    for entry in entries:
        name, _, queue_name = entry.rpartition("=")
        if not queue_name:
            raise ValueError(f"Missing queue name in {source} entry {entry!r}")
        if name:
            queues[name] = queue_name
        elif queue is not None:
            raise ValueError(f"Only one {source} entry may be given without a NAME")
        else:
            queue = queue_name
    return queue, queues


class ServiceStarter:
    """A helper class to start a workflows service from the command line.
    A number of hooks are provided so that this class can be subclassed and
    used in a number of scenarios."""

    @staticmethod
    def on_parser_preparation(parser: OptionParser) -> OptionParser | None:
        """Plugin hook to manipulate the OptionParser object before command line
        parsing. If a value is returned here it will replace the OptionParser
        object."""

    @staticmethod
    def on_parsing(
        options: optparse.Values, args: list[str]
    ) -> tuple[optparse.Values, list[str]] | None:
        """Plugin hook to manipulate the command line parsing results.
        A tuple of values can be returned, which will replace (options, args).
        """

    @staticmethod
    def on_transport_factory_preparation(
        transport_factory: Callable[[], CommonTransport],
    ) -> Callable[[], CommonTransport] | None:
        """Plugin hook to intercept/manipulate newly created Transport factories
        before first invocation."""

    @staticmethod
    def on_transport_preparation(transport: CommonTransport) -> CommonTransport | None:
        """Plugin hook to intercept/manipulate newly created Transport objects
        before connecting."""

    @staticmethod
    def before_frontend_construction(kwargs: dict[str, Any]) -> dict[str, Any] | None:
        """Plugin hook to manipulate the Frontend object constructor arguments. If
        a value is returned here it will replace the keyword arguments
        dictionary passed to the constructor."""

    @staticmethod
    def on_frontend_preparation(
        frontend: workflows.frontend.Frontend,
    ) -> workflows.frontend.Frontend | None:
        """Plugin hook to manipulate the Frontend object before starting it. If a
        value is returned here it will replace the Frontend object."""

    def run(
        self,
        cmdline_args: list[str] | None = None,
        program_name: str = "start_service",
        version: str | None = None,
        add_metrics_option: bool = False,
        **kwargs: Any,
    ) -> None:
        """Example command line interface to start services.

        Args:
            cmdline_args: List of command line arguments to pass to parser
            program_name: Name of the command line tool to display in help
            version: Version number to print when run with '--version'
        """

        # Enumerate all known services
        known_services = sorted(workflows.services.get_known_services())
        known_services_help = "Known services: " + ", ".join(known_services)

        if version:
            version = f"{version} (workflows {workflows.version()})"
        else:
            version = workflows.version()

        # Set up parser
        parser = OptionParser(
            usage=program_name + " [options]" if program_name else None, version=version
        )
        parser.add_option("-?", action="help", help=SUPPRESS_HELP)
        parser.add_option(
            "-s",
            "--service",
            metavar="SVC",
            help=f"Name of the service to start. {known_services_help}",
        )
        parser.add_option(
            "--liveness",
            action="store_true",
            default=False,
            help=(
                "Expose a liveness check endpoint for this service."
                "A return code of 200 indicates success."
                "A return code of 408 indicates failure."
            ),
        )
        parser.add_option(
            "--liveness-port",
            default=8000,
            type="int",
            help="Expose liveness check endpoint on this port.",
        )
        parser.add_option(
            "--liveness-timeout",
            default=30,
            type="float",
            help="Timeout for the liveness check (in seconds).",
        )
        if add_metrics_option:
            parser.add_option(
                "-m",
                "--metrics",
                action="store_true",
                default=False,
                help=(
                    "Record metrics for this service and expose them on the port "
                    "defined by the --metrics-port option."
                ),
            )
            parser.add_option(
                "--metrics-port",
                default=8080,
                type="int",
                help="Expose metrics via a prometheus endpoint on this port.",
            )
        workflows.transport.add_command_line_options(parser, transport_argument=True)

        # Call on_parser_preparation hook
        parser = self.on_parser_preparation(parser) or parser

        # Add the queue option after the hook, and only if a subclass has not
        # already defined its own -q or --queue option
        if not (parser.has_option("-q") or parser.has_option("--queue")):
            parser.add_option(
                "-q",
                "--queue",
                action="append",
                dest="workflows_queue",
                default=[],
                metavar="[NAME=]QUEUE",
                help=(
                    "Consume messages from QUEUE instead of the default queue NAME. "
                    "NAME may be omitted for services that consume a single queue. "
                    "May be specified multiple times. Defaults can be set in the "
                    f"{QUEUE_ENVIRONMENT_VARIABLE} environment variable, as a comma or "
                    "space separated list."
                ),
            )

        # Parse command line options
        (options, args) = parser.parse_args(cmdline_args)

        # Call on_parsing hook
        (options, args) = self.on_parsing(options, args) or (options, args)

        # Exit with error if no service has been specified.
        if not options.service:
            parser.error(f"Please specify a service name. {known_services_help}")

        # Create Transport factory
        transport_factory: Callable[[], CommonTransport] = workflows.transport.lookup(
            options.transport
        )

        # Call on_transport_factory_preparation hook
        transport_factory = (
            self.on_transport_factory_preparation(transport_factory)
            or transport_factory
        )

        # Set up on_transport_preparation hook to affect newly created transport objects
        true_transport_factory = transport_factory

        def on_transport_preparation_hook() -> CommonTransport:
            transport_object = true_transport_factory()
            return self.on_transport_preparation(transport_object) or transport_object

        transport_factory = on_transport_preparation_hook

        # When service name is specified, check if service exists or can be derived.
        if options.service not in known_services:
            # First check whether the provided service name is a case-insensitive match.
            service_lower = options.service.lower()
            exact_match = {s.lower(): s for s in known_services}.get(
                service_lower, None
            )
            match: list[str] = (
                [exact_match]
                if exact_match
                # Next, check whether the provided service name is a partial
                # case-sensitive match.
                else [s for s in known_services if s.startswith(options.service)]
                # Next check whether the provided service name is a partial
                # case-insensitive match.
                or [s for s in known_services if s.lower().startswith(service_lower)]
            )

            # Catch ambiguous partial matches and exit with an error.
            if len(match) > 1:
                sys.exit(
                    f"Specified service name {options.service} is ambiguous, partially "
                    f"matching each of these known services: " + ", ".join(match)
                )
            # Otherwise, set the derived service name, if there's a unique match.
            elif match:
                (options.service,) = match
            # Otherwise, exit with an error.
            else:
                sys.exit(f"Please specify a valid service name. {known_services_help}")

        kwargs.update(
            {
                "service": options.service,
                "transport": transport_factory,
            },
        )
        kwargs.setdefault("environment", {})
        if add_metrics_option and options.metrics:
            kwargs["environment"]["metrics"] = {"port": options.metrics_port}

        # Queue overrides from the command line take precedence over those
        # from the environment variable
        try:
            env_queue, env_queues = _parse_queue_overrides(
                os.environ.get(QUEUE_ENVIRONMENT_VARIABLE, "")
                .replace(",", " ")
                .split(),
                QUEUE_ENVIRONMENT_VARIABLE,
            )
            cli_queue, cli_queues = _parse_queue_overrides(
                getattr(options, "workflows_queue", []), "--queue"
            )
        except ValueError as e:
            parser.error(str(e))
        if queue := cli_queue or env_queue:
            kwargs["environment"]["queue"] = queue
        if queues := {**env_queues, **cli_queues}:
            kwargs["environment"]["queues"] = queues

        if options.liveness:
            kwargs["environment"]["liveness"] = {
                "port": options.liveness_port,
                "timeout": options.liveness_timeout,
            }

        # Call before_frontend_construction hook
        kwargs = self.before_frontend_construction(kwargs) or kwargs

        # Create Frontend object
        frontend = workflows.frontend.Frontend(**kwargs)

        # Call on_frontend_preparation hook
        frontend = self.on_frontend_preparation(frontend) or frontend

        # Start Frontend
        try:
            frontend.run()
        except KeyboardInterrupt:
            print("\nShutdown via Ctrl+C")


if __name__ == "__main__":  # pragma: no cover
    ServiceStarter().run()
