from __future__ import absolute_import, division
import multiprocessing
import Queue
import threading
import time
import workflows.transport
import workflows.services
from workflows.services.common_service import CommonService
import workflows.status

class Frontend():
  def __init__(self, transport=None, service=None):
    '''Create a frontend instance. Connect to the transport layer, start any
       requested service and begin broadcasting status information.'''
    self.__lock = threading.RLock()
    self.__hostid = self._generate_unique_host_id()
    self._service = None
    self._service_name = None
    self._service_transportid = None
    self._queue_commands = None
    self._queue_frontend = None
    self._service_status = CommonService.SERVICE_STATUS_NONE

    # Connect to the network transport layer
    if transport is None or isinstance(transport, basestring):
      self._transport = workflows.transport.lookup(transport)()
    else:
      self._transport = transport()
    assert self._transport.connect(), "Could not connect to transport layer"
    self._service_transportid = self._transport.register_client()

    # Start initial service if one has been requested
    if service is not None:
      self._service_status = CommonService.SERVICE_STATUS_NEW
      self.switch_service(service)

    # Start broadcasting node information
    self._status_advertiser = workflows.status.StatusAdvertise(
        interval=6,
        status_callback=self.get_status,
        transport=self._transport)
    self._status_advertiser.start()

  def run(self):
    print "Current service:", self._service
    n = 3600
    while n > 0:
      if self._queue_frontend is not None:
        try:
          message = self._queue_frontend.get(True, 1)
          if isinstance(message, dict) and 'band' in message:
            # only dictionaries with 'band' entry are valid messages
            try:
              handler = getattr(self, 'parse_band_' + message['band'])
            except AttributeError:
              handler = None
              print 'Unknown band %s' % message['band']
            if handler:
#              try:
                handler(message)
#              except Exception:
#                print 'Uh oh. What to do.'
          else:
            print 'Invalid message received'
        except Queue.Empty:
          pass
      n = n - 1

    self._service_status = CommonService.SERVICE_STATUS_TEARDOWN
    self._status_advertiser.trigger()
    self._status_advertiser.stop_and_wait()
    print "Fin."

  def send_command(self, command):
    '''Send command to service via the command queue.'''
#   print "To command queue: ", command
    if self._queue_commands:
      self._queue_commands.put(command)

  def parse_band_log(self, message):
    print "LOG:", message

  def parse_band_status(self, message):
    print "STT:", message

  def parse_band_status_update(self, message):
#   print "STU:", message
    self._service_status = message['statuscode']
    self._status_advertiser.trigger()

  def get_host_id(self):
    '''Get a cached copy of the host id.'''
    return self.__hostid

  def _generate_unique_host_id(self):
    '''Generate a unique ID, that is somewhat guaranteed to be unique among all
       instances running at the same time.'''
    import socket
    host = '.'.join(reversed(socket.gethostname().split('.')))
    import os
    pid = os.getpid()
    return "%s.%d" % (host, pid)

  def get_status(self):
    '''Returns a dictionary containing all relevant status information to be
       broadcast across the network.'''
    return { 'host': self.__hostid,
             'status': self._service_status,
             'service': self._service_name }

  def switch_service(self, new_service):
    '''Start a new service in a subprocess.
       :param new_service: Either a service name or a service class.
       :return: True on success, False on failure.
    '''
    with self.__lock:
      # Terminate existing service if necessary
      if self._service is not None:
        self._terminate_service()

      # Find service class if necessary
      if isinstance(new_service, basestring):
        service_class = workflows.services.lookup(new_service)
      else:
        service_class = new_service

      if not service_class:
        return False

      # Set up queues and connect new service object
      self._queue_commands = multiprocessing.Queue()
      self._queue_frontend = multiprocessing.Queue()
      service_instance = service_class(
        commands=self._queue_commands,
        frontend=self._queue_frontend)

      # Clean out transport layer for new service
      if self._service_transportid:
        self._transport.drop_client(self._service_transportid)
      self._service_transportid = self._transport.register_client()

      # Start new service in a separate process
      self._service = multiprocessing.Process(
        target=service_instance.start, args=())
      self._service_name = service_instance.get_name()
      self._service.daemon = True
      self._service.start()
    return True

  def _terminate_service(self):
    '''Force termination of running service.
       Disconnect queues as they may get corrupted'''
    with self.__lock:
      self._service.terminate()
      self._service = None
      self._service_name = None
      self._service_status = CommonService.SERVICE_STATUS_END
      self._queue_commands = None
      self._queue_frontend = None
      if self._service_transportid:
        self._transport.drop_client(self._service_transportid)
      self._service_transportid = None

# ---- Transport calls -----------------------------------------------------

  _transaction_mapping = {}

  def parse_band_transport(self, message):
    '''Treat messages sent from service via queue. These were encoded by
       transport.queue_transport on the other end. Forward the messages to
       individual functions parse_band_transport_${call}() depending on the
       'call' field.
       param: message: The full queue message data structure.
    '''
    try:
      handler = getattr(self, 'parse_band_transport_' + message['call'])
    except AttributeError:
      # TODO: This should go to a log
      print 'Unknown transport handler for message', message
      return
    if 'transaction' in message['payload'][1]:
#     print "Mapping transaction %s to %s for %s" % (
#         str( message['payload'][1]['transaction'] ),
#         str( self._transaction_mapping[message['payload'][1]['transaction']] ),
#         message['call'])
      message['payload'][1]['transaction'] = \
        self._transaction_mapping[message['payload'][1]['transaction']]
    handler(message)

  def parse_band_transport_send(self, message):
    args, kwargs = message['payload']
    self._transport.send(*args, **kwargs)

  def parse_band_transport_ack(self, message):
    args, kwargs = message['payload']
    self._transport.ack(*args, **kwargs)

  def parse_band_transport_nack(self, message):
    args, kwargs = message['payload']
    self._transport.nack(*args, **kwargs)

  def parse_band_transport_transaction_begin(self, message):
    args, kwargs = message['payload']
    service_txn_id = args[0]
    self._transaction_mapping[service_txn_id] = \
      self._transport.transaction_begin(clientid=self._service_transportid)

  def parse_band_transport_transaction_commit(self, message):
    args, kwargs = message['payload']
    service_txn_id = args[0]
    assert service_txn_id in self._transaction_mapping
    self._transport.transaction_commit(self._transaction_mapping[service_txn_id])
    del(self._transaction_mapping[service_txn_id])

  def parse_band_transport_transaction_abort(self, message):
    args, kwargs = message['payload']
    service_txn_id = args[0]
    assert service_txn_id in self._transaction_mapping
    self._transport.transaction_abort(self._transaction_mapping[service_txn_id])
    del(self._transaction_mapping[service_txn_id])

  def parse_band_transport_subscribe(self, message):
    subscription_id, channel = message['payload'][0][:2]
    self._transport.subscribe(channel,
      lambda cb_header, cb_message:
      self.send_command( {
          'band': 'transport_message',
          'payload': {
            'subscription_id': subscription_id,
            'header': cb_header,
            'message': cb_message,
          },
        } ),
       *message['payload'][0][2:],
       **message['payload'][1]
    )
