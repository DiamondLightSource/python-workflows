from __future__ import absolute_import, division
from workflows.services.common_service import CommonService
import json
import time

class SampleConsumer(CommonService):
  '''An example service building on top of the workflow.services architecture,
     demonstrating how this architecture can be used.
     This service consumes messages off a queue.'''

  # Human readable service name
  _service_name = "Message Consumer"

  def initializing(self):
    '''Subscribe to a channel.'''
    self._transport.subscribe('transient.destination', self.consume_message)

  def consume_message(self, header, message):
    '''Consume a message'''
    print "=== Consume ===="
    if header:
      print json.dumps(header, indent=2)
      print "----------------"
    print message
    print "========Received@", (time.time() % 1000) * 1000
