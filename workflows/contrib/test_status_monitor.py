from __future__ import absolute_import, division, print_function

import mock
import workflows.contrib.status_monitor as status_monitor


@mock.patch("workflows.contrib.status_monitor.curses")
@mock.patch("workflows.contrib.status_monitor.time")
@mock.patch("workflows.contrib.status_monitor.workflows.transport")
def test_status_monitor_connects_to_transport_layer(
    mock_transport, mock_time, mock_curses
):
    """Check that the status monitor properly connects to the transport layer and sets up relevant subscriptions."""
    mock_time.sleep.side_effect = KeyboardInterrupt()

    status_monitor.Monitor()
    mock_transport.lookup.assert_called_once_with(None)
    mock_transport.lookup.return_value.assert_called_once_with()
    mock_transport.lookup.return_value.return_value.connect.assert_called_once_with()

    status_monitor.Monitor(transport="some_transport")
    mock_transport.lookup.assert_called_with("some_transport")

    mock_trn = mock.Mock()
    mon = status_monitor.Monitor(mock_trn)
    mock_trn.assert_called_once_with()
    mock_trn.return_value.connect.assert_called_once_with()
    mock_trn.return_value.subscribe_broadcast.assert_called_once_with(
        mock.ANY, mon.update_status, retroactive=True
    )

    mon.run()

    mock_curses.wrapper.assert_called_once()
    run_method = mock_curses.wrapper.call_args[0][0]
    run_method(mock.Mock())

    mock_trn.return_value.disconnect.assert_called_once()
