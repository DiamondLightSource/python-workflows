from __future__ import annotations

__version__ = "2.24.1"


def version():
    """Returns the version number of the installed workflows package."""
    return __version__


class Error(Exception):
    """Common class for exceptions deliberately raised by workflows package."""


class Disconnected(Error):
    """Indicates the connection could not be established or has been lost."""
