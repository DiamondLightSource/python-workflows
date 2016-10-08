from __future__ import absolute_import, division
import time
import workflows.transport
import threading

class Terminal():
  def __init__(self, transport=None):
    # Connect to the network transport layer
    if transport is None or isinstance(transport, basestring):
      self._transport = workflows.transport.lookup(transport)()
    else:
      self._transport = transport()
    if not self._transport.connect():
      print "Could not connect to transport layer"
      self._transport = None
    self._lock = threading.RLock()
    self._node_status = {}

  def update_status(self, header, body):
    print header, body
    with self._lock:
      if body['host'] not in self._node_status or \
          int(header['timestamp']) >= self._node_status[body['host']]['last_seen']:
        self._node_status[body['host']] = body
        self._node_status[body['host']]['last_seen'] = int(header['timestamp'])

  def run(self):
    print "\n", '='*47
    print "service_monitor started. End with Ctrl+C\n", '='*47
    self._transport.subscribe('/topic/transient.status.demo', self.update_status, retroactive=True)
    try:
      while True:
        print ""
        now = int(time.time())
        with self._lock:
          overview = self._node_status.copy()
        for host, status in overview.iteritems():
          age = (now - int(status['last_seen'] / 1000))
          if age > 90:
            with self._lock:
              del(self._node_status[host])
          else:
            print "%s: %s in state %d (last seen %d seconds ago)" % \
                (host, status['service'], status['status'], age)
        time.sleep(3)
    except KeyboardInterrupt:
      print
    self._transport.disconnect()
    self._transport = None
