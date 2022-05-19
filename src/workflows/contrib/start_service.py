from __future__ import annotations

from optparse import SUPPRESS_HELP, OptionParser

import workflows
import workflows.frontend
import workflows.services
import workflows.transport


class ServiceStarter:
    """A helper class to start a workflows service from the command line.
    A number of hooks are provided so that this class can be subclassed and
    used in a number of scenarios."""

    @staticmethod
    def on_parser_preparation(parser):
        """Plugin hook to manipulate the OptionParser object before command line
        parsing. If a value is returned here it will replace the OptionParser
        object."""

    @staticmethod
    def on_parsing(options, args):
        """Plugin hook to manipulate the command line parsing results.
        A tuple of values can be returned, which will replace (options, args).
        """

    @staticmethod
    def on_transport_factory_preparation(transport_factory):
        """Plugin hook to intercept/manipulate newly created Transport factories
        before first invocation."""

    @staticmethod
    def on_transport_preparation(transport):
        """Plugin hook to intercept/manipulate newly created Transport objects
        before connecting."""

    @staticmethod
    def before_frontend_construction(kwargs):
        """Plugin hook to manipulate the Frontend object constructor arguments. If
        a value is returned here it will replace the keyword arguments
        dictionary passed to the constructor."""

    @staticmethod
    def on_frontend_preparation(frontend):
        """Plugin hook to manipulate the Frontend object before starting it. If a
        value is returned here it will replace the Frontend object."""

    def run(
        self,
        cmdline_args=None,
        program_name="start_service",
        version=None,
        add_metrics_option: bool = False,
        **kwargs,
    ):
        """Example command line interface to start services.
        :param cmdline_args: List of command line arguments to pass to parser
        :param program_name: Name of the command line tool to display in help
        :param version: Version number to print when run with '--version'
        """

        # Enumerate all known services
        known_services = sorted(workflows.services.get_known_services())

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
            dest="service",
            metavar="SVC",
            default=None,
            help="Name of the service to start. Known services: "
            + ", ".join(known_services),
        )
        if add_metrics_option:
            parser.add_option(
                "-m",
                "--metrics",
                dest="metrics",
                action="store_true",
                default=False,
                help=(
                    "Record metrics for this service and expose them on the port defined by"
                    "the --metrics-port option."
                ),
            )
            parser.add_option(
                "--metrics-port",
                dest="metrics_port",
                default=8080,
                type="int",
                help="Expose metrics via a prometheus endpoint on this port.",
            )
        workflows.transport.add_command_line_options(parser, transport_argument=True)

        # Call on_parser_preparation hook
        parser = self.on_parser_preparation(parser) or parser

        # Parse command line options
        (options, args) = parser.parse_args(cmdline_args)

        # Call on_parsing hook
        (options, args) = self.on_parsing(options, args) or (options, args)

        # Create Transport factory
        transport_factory = workflows.transport.lookup(options.transport)

        # Call on_transport_factory_preparation hook
        transport_factory = (
            self.on_transport_factory_preparation(transport_factory)
            or transport_factory
        )

        # Set up on_transport_preparation hook to affect newly created transport objects
        true_transport_factory_call = transport_factory.__call__

        def on_transport_preparation_hook():
            transport_object = true_transport_factory_call()
            return self.on_transport_preparation(transport_object) or transport_object

        transport_factory.__call__ = on_transport_preparation_hook

        # When service name is specified, check if service exists or can be derived
        if options.service and options.service not in known_services:
            matching = [s for s in known_services if s.startswith(options.service)]
            if not matching:
                matching = [
                    s
                    for s in known_services
                    if s.lower().startswith(options.service.lower())
                ]
            if matching and len(matching) == 1:
                options.service = matching[0]

        kwargs.update(
            {
                "service": options.service,
                "transport": transport_factory,
            },
        )
        kwargs.setdefault("environment", {})
        if add_metrics_option and options.metrics:
            kwargs["environment"]["metrics"] = {"port": options.metrics_port}

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
