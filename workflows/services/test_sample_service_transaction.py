from __future__ import absolute_import, division

import workflows.services
import workflows.services.sample_transaction
import mock

def test_services_can_be_looked_up():
  '''Attempt to look up the services by their names'''
  service_class = workflows.services.lookup('SampleTxn')
  assert service_class == workflows.services.sample_transaction.SampleTxn
  service_class = workflows.services.lookup('SampleTxnProducer')
  assert service_class == workflows.services.sample_transaction.SampleTxnProducer

def test_txnproducer_registers_idle_timer():
  '''Check that the TXN producer registers an idle event handler.'''
  p = workflows.services.sample_transaction.SampleTxnProducer()
  mock_idlereg = mock.Mock()
  setattr(p, '_register_idle', mock_idlereg)

  p.initializing()

  mock_idlereg.assert_called_once_with(mock.ANY, p.create_message)

def test_txnproducer_produces_messages():
  '''Check that the TXN producer produces messages in the idle event handler.'''
  p = workflows.services.sample_transaction.SampleTxnProducer()
  mock_transport = mock.Mock()
  setattr(p, '_transport', mock_transport)

  p.initializing()
  assert not mock_transport.send.called
  p.create_message()

  mock_transport.send.assert_called_once()

  p.create_message()

  assert mock_transport.send.call_count == 2
  calls = mock_transport.send.call_args_list
  assert calls[0][0][0] == calls[1][0][0] # same destination
  assert calls[0][0][1] != calls[1][0][1] # different message
