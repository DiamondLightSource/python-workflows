from __future__ import absolute_import, division
from workflows.services.common_service import CommonService

class Consumer(CommonService):
  '''An example service building on top of the workflow.services architecture,
     demonstrating how this architecture can be used.
     This service consumes messages off a queue.'''

  def initializing(self):
    '''Subscribe to a channel.'''
    self._transport.subscribe('transient.destination', self.consume_message)

  def consume_message(self, header, message):
    '''Consume a message'''
    print "=== Consume ===="
    print header
    print "----------------"
    print message
    print "================"
