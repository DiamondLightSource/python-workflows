from __future__ import division
import frontend
import mock

@mock.patch('workflows.frontend.multiprocessing')
@mock.patch('workflows.frontend.status.StatusAdvertise')
@mock.patch('workflows.frontend.workflows.transport')
def test_frontend_connects_to_transport_layer(mock_transport, mock_status, mock_mp):
  frontend.Frontend()
  mock_transport.lookup.assert_called_once_with(None)
  mock_transport.lookup.return_value.assert_called_once_with()
  mock_transport.lookup.return_value.return_value.connect.assert_called_once_with()

  frontend.Frontend(transport="some_transport")
  mock_transport.lookup.assert_called_with("some_transport")

  mock_trn = mock.Mock()
  frontend.Frontend(mock_trn)
  mock_trn.assert_called_once_with()
  mock_trn.return_value.connect.assert_called_once_with()

@mock.patch('workflows.frontend.multiprocessing')
@mock.patch('workflows.frontend.status.StatusAdvertise')
@mock.patch('workflows.frontend.workflows.transport')
def test_start_service_in_frontend(mock_transport, mock_status, mock_mp):
  mock_service = mock.Mock()
  mock_mp.Queue.return_value = mock.sentinel.Queue

  # initialize frontend
  fe = frontend.Frontend()

  # check status information is being broadcast
  mock_status.assert_called_once()
  mock_status.return_value.start.assert_called_once()

  # start service
  fe.switch_service(mock_service)

  # check service was started properly
  mock_service.assert_called_once_with(commands=mock.sentinel.Queue, frontend=mock.sentinel.Queue)
  mock_mp.Process.assert_called_once_with(target=mock_service.return_value.start, args=())
  mock_mp.Process.return_value.start.assert_called_once()

@mock.patch('workflows.frontend.status.StatusAdvertise')
@mock.patch('workflows.frontend.workflows.transport')
def test_get_frontend_status(mock_transport, mock_status):
  fe = frontend.Frontend()
  status = fe.get_status()
  assert status['host'] == fe.get_host_id()
