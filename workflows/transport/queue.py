from __future__ import absolute_import
from __future__ import division
from workflows import WorkflowsError
import workflows.transport

class Transport(workflows.transport.CommonTransport):
  '''Abstraction layer for messaging infrastructure.
     Here we are using messaging via a Queue to the Frontend.'''

  def __init__(self):
    self._connected = False
    self._queue = None

  def add_command_line_options(self, optparser):
    '''This type of transport does not offer command line options.'''

  def connect(self):
    '''Connect to the queue.'''
    if self._queue:
      self._connected = True

  def is_connected(self):
    '''Return connection status'''
    return self._connected

  def disconnect(self):
    if self._connected:
      pass # TODO

