from __future__ import absolute_import, division
from workflows.services.common_service import CommonService
import time

class SampleProducer(CommonService):
  '''An example service building on top of the workflow.services architecture,
     demonstrating how this architecture can be used.
     This service generates messages into a queue.'''

  # Human readable service name
  _service_name = "Message Producer"

  counter = 0

  def initializing(self):
    '''Service initialization. This function is run before any commands are
       received from the frontend. This is the place to request channel
       subscriptions with the messaging layer, and register callbacks.
       This function can be overridden by specific service implementations.'''
    self._register_idle(3, self.create_message)

  def create_message(self):
    '''Create and send a unique message for this service.'''
    self.counter += 1
    print "Sending message #%d" % self.counter
    self._transport.send("transient.destination",
                         "Message #%d\n++++++++Produced@ %f" % (
                           self.counter,
                           (time.time() % 1000) * 1000
                         ))
