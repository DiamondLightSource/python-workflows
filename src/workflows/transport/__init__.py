from __future__ import annotations

import argparse
import optparse
from typing import TYPE_CHECKING, Type

import pkg_resources

if TYPE_CHECKING:
    from .common_transport import CommonTransport

default_transport = "PikaTransport"


def lookup(transport: str) -> Type[CommonTransport]:
    """Get a transport layer class based on its name."""
    return get_known_transports().get(
        transport, get_known_transports()[default_transport]
    )


def add_command_line_options(
    parser: argparse.ArgumentParser | optparse.OptionParser,
    transport_argument: bool = False,
) -> None:
    """Add command line options for all available transport layer classes."""
    if transport_argument:
        known_transports = list(get_known_transports())
        if isinstance(parser, argparse.ArgumentParser):
            parser.add_argument(
                "-t",
                "--transport",
                dest="transport",
                metavar="TRN",
                default=default_transport,
                help="Transport mechanism. Known mechanisms: "
                + ", ".join(known_transports)
                + f" (default: {default_transport})",
                choices=known_transports,
            )
        else:
            parser.add_option(
                "-t",
                "--transport",
                dest="transport",
                metavar="TRN",
                default=default_transport,
                help="Transport mechanism. Known mechanisms: "
                + ", ".join(known_transports)
                + " (default: %default)",
                type="choice",
                choices=known_transports,
            )
    for transport in get_known_transports().values():
        transport().add_command_line_options(parser)


def get_known_transports() -> dict[str, Type[CommonTransport]]:
    """Return a dictionary of all known transport mechanisms."""
    if not hasattr(get_known_transports, "cache"):
        setattr(
            get_known_transports,
            "cache",
            {
                e.name: e.load()
                for e in pkg_resources.iter_entry_points("workflows.transport")
            },
        )
    return get_known_transports.cache.copy()  # type: ignore
