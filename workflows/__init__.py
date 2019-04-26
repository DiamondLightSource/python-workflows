from __future__ import absolute_import, division, print_function

__version__ = "1.5"


def version():
    """Returns the version number of the installed workflows package."""
    return __version__


class Error(Exception):
    """Common class for exceptions deliberately raised by workflows package."""


class Disconnected(Error):
    """Indicates the connection could not be established or has been lost."""


class AuthenticationFailed(Error):
    """Indicates the connection could not be established due to incorrect credentials."""


# Legacy error names, 20180319
WorkflowsError = Error
DisconnectedError = Disconnected
AuthenticationError = AuthenticationFailed
