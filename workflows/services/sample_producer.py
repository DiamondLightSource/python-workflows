from __future__ import absolute_import, division
from workflows.services.common_service import CommonService
import time

class Producer(CommonService):
  '''An example service building on top of the workflow.services architecture,
     demonstrating how this architecture can be used.
     This service generates messages into a queue.'''

  counter = 0

  def initializing(self):
    '''Service initialization. This function is run before any commands are
       received from the frontend. This is the place to request channel
       subscriptions with the messaging layer, and register callbacks.
       This function can be overridden by specific service implementations.'''
    self._register_idle(3, self.create_message)

  def create_message(self):
    self.counter += 1
    self._transport.send("transient.destination", "Message #%d" % self.counter)
