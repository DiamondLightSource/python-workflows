from __future__ import absolute_import, division
import workflows.contrib.status_monitor as status_monitor
import mock

@mock.patch('workflows.contrib.status_monitor.time')
@mock.patch('workflows.contrib.status_monitor.workflows.transport')
def test_status_monitor_connects_to_transport_layer(mock_transport, mock_time):
  mock_time.sleep.side_effect = KeyboardInterrupt()

  status_monitor.Terminal()
  mock_transport.lookup.assert_called_once_with(None)
  mock_transport.lookup.return_value.assert_called_once_with()
  mock_transport.lookup.return_value.return_value.connect.assert_called_once_with()

  status_monitor.Terminal(transport="some_transport")
  mock_transport.lookup.assert_called_with("some_transport")

  mock_trn = mock.Mock()
  mon = status_monitor.Terminal(mock_trn)
  mock_trn.assert_called_once_with()
  mock_trn.return_value.connect.assert_called_once_with()

  mon.run()
  mock_trn.return_value.subscribe.assert_called_once_with(mock.ANY, mon.update_status, retroactive=True)

  mock_trn.return_value.disconnect.assert_called_once()
