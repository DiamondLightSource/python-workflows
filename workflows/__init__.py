import pathlib

__version__ = "2.2"

exit(
    """
The 'master' branch of the workflows repository has been renamed to 'main'.
Please run the following commands to update your local repository:
    cd {path}
    git branch -m master main
    git fetch origin
    git branch -u origin/main main
""".format(
        path=pathlib.Path.cwd().joinpath(__file__).parent.parent
    )
)


def version():
    """Returns the version number of the installed workflows package."""
    return __version__


class Error(Exception):
    """Common class for exceptions deliberately raised by workflows package."""


class Disconnected(Error):
    """Indicates the connection could not be established or has been lost."""
