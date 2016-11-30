from __future__ import absolute_import, division
import logging
import Queue
import threading
import time

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
    self.log = logging.getLogger('workflows.status')

  def start(self):
    '''Start a background thread to broadcast the current node status at
       regular intervals.'''
    self._background_thread.start()

  def stop(self):
    '''Stop the background thread.'''
    self.log.debug('Notifying status advertising thread to shut down')
    self._shutdown = True

  def stop_and_wait(self):
    '''Stop the background thread and wait for any pending broadcasts.'''
    self.stop()
    self.log.debug('Waiting for status advertising thread to shut down')
    self._background_thread.join()

  def trigger(self):
    '''Trigger an immediate status update.'''
    self._notification.put(None)

  def _timer(self):
    '''Advertise current frontend and service status to transport layer, and
       broadcast useful information about this node.
       This runs in a separate thread.'''
    self.log.debug('Status advertising thread started')
    try:
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
          self.log.error('Exception in status advertising thread', exc_info=True)

        waitperiod = waitperiod - time.time()
        try:
          self._notification.get(True, waitperiod)
        except Queue.Empty:
          pass # intentional
      self.log.debug('Stopping status advertising thread')

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
      self.log.debug('Status advertising stopped')
    except Exception, e:
      self.log.critical('Unhandled exception in status advertising thread', exc_info=True)
      raise
