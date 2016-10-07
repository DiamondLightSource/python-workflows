from __future__ import absolute_import, division
from workflows.services.common_service import CommonService
import time

class Producer(CommonService):
  '''An example service building on top of the workflow.services architecture,
     demonstrating how this architecture can be used.
     This service generates messages into a queue.'''

  def initializing(self):
    '''Service initialization. This function is run before any commands are
       received from the frontend. This is the place to request channel
       subscriptions with the messaging layer, and register callbacks.
       This function can be overridden by specific service implementations.'''
    self._register_idle(3, self.create_message)

  def log(self, logmessage):
    '''Pass a log message to the frontend.
       This function can be overridden by specific service implementations.'''
    self._log_send(logmessage)

  def update_status(self, status):
    '''Pass a status update to the frontend.
       This function can be overridden by specific service implementations.'''
    self._update_status(status)

  def in_shutdown(self):
    '''Service shutdown. This function is run before the service is terminated.
       No more commands are received, but communications can still be sent.
       This function can be overridden by specific service implementations.'''
    pass

  def create_message(self):
    print "Creating message."
