from __future__ import absolute_import, division
import workflows.status
import mock
import pytest

@mock.patch('workflows.status.threading')
def test_status_advertiser_starts_and_stops_threads(mock_threading):
  s = workflows.status.StatusAdvertise()
  mock_threading.Thread.assert_called_once()
  s.start()
  mock_threading.Thread.return_value.start.assert_called_once()
  s.stop_and_wait()
  mock_threading.Thread.return_value.join.assert_called_once()

@mock.patch('workflows.status.Queue')
@mock.patch('workflows.status.threading')
@mock.patch('workflows.status.time')
def test_status_advertiser_regularly_passes_status(mock_time, mock_threading, mock_queue):
  sm = mock.Mock() # status mock
  tm = mock.Mock() # transport mock
  qm = mock_queue.Queue.return_value
  s = workflows.status.StatusAdvertise(interval=120, status_callback=sm, transport=tm)
  t = mock_threading.Thread.call_args[1]['target'] # target function
  mock_time.time.return_value = 100
  qm.get.side_effect = RuntimeError(mock.sentinel.pause)
  sm.return_value = mock.sentinel.status1

  # Run with a failing status function
  sm.side_effect = RuntimeError(mock.sentinel.status_error)
  with pytest.raises(RuntimeError) as excinfo:
    t()
  assert excinfo.value.message == mock.sentinel.pause

  qm.get.assert_called_once_with(True, 120)
  sm.assert_called_once()

  # Run with a working status function
  sm.side_effect = None
  with pytest.raises(RuntimeError) as excinfo:
    t()
  assert excinfo.value.message == mock.sentinel.pause

  assert qm.get.call_count == 2
  tm.broadcast_status.assert_called_once_with(mock.sentinel.status1)

  # Run after being stopped
  s.stop_and_wait()
  t() # this must no longer throw an exception

  assert sm.call_count == 3

@mock.patch('workflows.status.Queue')
@mock.patch('workflows.status.threading')
@mock.patch('workflows.status.time')
def test_status_advertiser_external_triggering(mock_time, mock_threading, mock_queue):
  sm = mock.Mock() # status mock
  tm = mock.Mock() # transport mock
  qm = mock_queue.Queue.return_value
  mock_threading.Queue.return_value = qm
  s = workflows.status.StatusAdvertise(interval=120, status_callback=sm, transport=tm)
  t = mock_threading.Thread.call_args[1]['target'] # target function

  s.trigger()
  qm.put.assert_called_once()

@mock.patch('workflows.status.Queue')
@mock.patch('workflows.status.threading')
def test_status_advertiser_sends_last_update_when_stopping(mock_threading, mock_queue):
  tm = mock.Mock() # transport mock
  qm = mock_queue.Queue.return_value
  mock_threading.Queue.return_value = qm
  s = workflows.status.StatusAdvertise(transport=tm)
  t = mock_threading.Thread.call_args[1]['target'] # target function
  qm.get.side_effect = RuntimeError(mock.sentinel.queue_read)
  qm.empty.return_value = False

  s.stop()

  t()
  tm.broadcast_status.assert_called_once()
