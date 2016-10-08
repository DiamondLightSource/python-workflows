from __future__ import absolute_import, division
import Queue
import sys
import threading
import time
import traceback

class StatusAdvertise():
  '''Background thread that advertises the current node status, obtained by
     calling a specified function, to a specified transport layer at regular
     intervals.'''

  def __init__(self, interval=60, status_callback=None, transport=None):
    self._advertise_lock = threading.Lock()
    self._interval = interval
    self._status_function = status_callback
    self._transport = transport
    self._background_thread = threading.Thread(
        target=self._timer,
        name='heartbeat')
    self._background_thread.daemon = True
    self._shutdown = False
    self._notification = Queue.Queue()

  def start(self):
    '''Start a background thread to broadcast the current node status at
       regular intervals.'''
    self._background_thread.start()

  def stop(self):
    '''Stop the background thread.'''
    self._shutdown = True

  def stop_and_wait(self):
    '''Stop the background thread and wait for any pending broadcasts.'''
    self.stop()
    self._background_thread.join()

  def trigger(self):
    '''Trigger an immediate status update.'''
    self._notification.put(None)

  def _timer(self):
    '''Advertise current frontend and service status to transport layer, and
       broadcast useful information about this node.
       This runs in a separate thread.'''
    while not self._shutdown:
      waitperiod = self._interval + time.time()

      try:
        with self._advertise_lock:
          status = None
          if self._status_function is not None:
            status = self._status_function()
          if self._transport is not None:
            if status is None:
              self._transport.broadcast_status()
            else:
              self._transport.broadcast_status(status)
      except Exception, e:
        # should pass these to a logging function
        print "Exception in status thread:"
        print '-'*60
        traceback.print_exc(file=sys.stdout)
        print '-'*60

      waitperiod = waitperiod - time.time()
      try:
        self._notification.get(True, waitperiod)
      except Queue.Empty:
        pass # intentional

    # Thread is stopping. Check if one last notification should be sent
    with self._advertise_lock:
      status = None
      if self._status_function is not None:
        status = self._status_function()
      if self._transport is not None:
        if status is None:
          self._transport.broadcast_status()
        else:
          self._transport.broadcast_status(status)
