from __future__ import absolute_import, division
import logging

class CallbackHandler(logging.Handler):
  '''This handler sends logrecords to a callback function.'''

  def __init__(self, callback):
    '''Set up a handler instance, record the callback function.'''
    super(CallbackHandler, self).__init__()
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
    '''Send a LogRecord to the callback function, after preparing it
       for serialization.'''
    try:
      self._callback(self.prepare(record))
    except Exception:
      self.handleError(record)
