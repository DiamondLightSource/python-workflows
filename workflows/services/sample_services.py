from __future__ import division
import workflows.services
import time

class Waiter(workflows.services.Service):
  '''An example service building on top of the workflow.services architecture,
     demonstrating how this architecture can be used.
     This service receives work, waits a while and generates some 'results'.'''

  def __init__(self, *args, **kwargs):
    '''Pass on arguments to baseclass constructor.'''
    super(Waiter, self).__init__(*args, **kwargs)
    self._service_name = 'waiting service'

  def initializing(self):
    '''Register handling function for 'stuff' messages.'''
    self._register('stuff', self.stuff_handler)

  def stuff_handler(self, *args, **kwargs):
    '''Pretend processing of "data"'''
    self.update_status('Processing stuff [1/3]')
    time.sleep(3)
    self.update_status('Processing stuff [2/3]')
    time.sleep(8)
    self.update_status('Processing stuff [3/3]')
    time.sleep(4)
    self.update_status('Completed processing stuff')

class Consumer(workflows.services.Service):
  '''An example service building on top of the workflow.services architecture,
     demonstrating how this architecture can be used.
     This service consumes messages off a queue.'''

class Producer(workflows.services.Service):
  '''An example service building on top of the workflow.services architecture,
     demonstrating how this architecture can be used.
     This service generates messages into a queue.'''

