from __future__ import annotations

import linecache
import logging
import os.path
import sys


def get_exception_source():
    """Returns full file path, file name, line number, function name, and line contents
    causing the last exception."""
    _, _, tb = sys.exc_info()
    while tb.tb_next:
        tb = tb.tb_next
    f = tb.tb_frame
    lineno = tb.tb_lineno
    co = f.f_code
    filefullpath = co.co_filename
    filename = os.path.basename(filefullpath)
    name = co.co_name
    linecache.checkcache(filefullpath)
    line = linecache.getline(filefullpath, lineno, f.f_globals)
    if line:
        line = line.strip()
    else:
        line = None
    return filefullpath, filename, lineno, name, line


class CallbackHandler(logging.Handler):
    """This handler sends logrecords to a callback function."""

    def __init__(self, callback):
        """Set up a handler instance, record the callback function."""
        super().__init__()
        self._callback = callback

    def prepare(self, record):
        # Function taken from Python 3.6 QueueHandler
        """
        Prepares a record for queuing. The object returned by this method is
        enqueued.
        The base implementation formats the record to merge the message
        and arguments, and removes unpickleable items from the record
        in-place.
        You might want to override this method if you want to convert
        the record to a dict or JSON string, or send a modified copy
        of the record while leaving the original intact.
        """
        # The format operation gets traceback text into record.exc_text
        # (if there's exception data), and also puts the message into
        # record.message. We can then use this to replace the original
        # msg + args, as these might be unpickleable. We also zap the
        # exc_info attribute, as it's no longer needed and, if not None,
        # will typically not be pickleable.
        self.format(record)
        record.msg = record.message
        record.args = None
        record.exc_info = None
        return record

    def emit(self, record):
        """Send a LogRecord to the callback function, after preparing it
        for serialization."""
        try:
            self._callback(self.prepare(record))
        except Exception:
            self.handleError(record)

    def handleError(self, record):
        t, v, _ = sys.exc_info()
        try:
            sys.stderr.write(
                f"--- Logging error --- {t.__name__}: {v}\n"
                "Could not forward log message from service to frontend process\n"
                f"Message: {record.msg}\n"
                f"Level: {record.levelno} - Thread: {record.threadName} - Arguments: {record.args}\n"
            )
        except Exception as e:
            sys.stderr.write(
                "--- Logging error ---\n"
                f"Encountered exception {e!r} during exception handling\n"
            )
