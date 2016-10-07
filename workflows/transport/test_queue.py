from __future__ import division
import workflows.transport
from workflows.transport.queue import Transport
import mock

def test_lookup_and_initialize_queue_transport_layer():
  '''Find the queue transport layer via the lookup mechanism and run
     its constructor with default settings.'''
  queue = workflows.transport.lookup("queue")
  assert queue == Transport
  queue()

