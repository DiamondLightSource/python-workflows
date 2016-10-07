from __future__ import division
import workflows.transport
from workflows.transport.queue_transport import QueueTransport
import mock

def test_lookup_and_initialize_queue_transport_layer():
  '''Find the queue transport layer via the lookup mechanism and run
     its constructor with default settings.'''
  queue = workflows.transport.lookup("QueueTransport")
  assert queue == QueueTransport
  queue()

