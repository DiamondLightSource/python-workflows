from __future__ import division
import workflows.services

class Consumer(workflows.services.Service):
  '''An example service building on top of the workflow.services architecture,
     demonstrating how this architecture can be used.
     This service consumes messages off a queue.'''

