from __future__ import annotations

from unittest import mock

import pytest

import workflows.services
import workflows.services.sample_producer
from workflows.transport.offline_transport import OfflineTransport


def test_service_can_be_looked_up():
    """Attempt to look up the service by its name"""
    service_class = workflows.services.lookup("SampleProducer")
    assert service_class == workflows.services.sample_producer.SampleProducer


def test_service_registers_idle_timer():
    """Check that the service registers an idle event handler."""
    p = workflows.services.sample_producer.SampleProducer()
    mock_idlereg = mock.Mock()
    setattr(p, "_register_idle", mock_idlereg)

    p.initializing()

    mock_idlereg.assert_called_once_with(mock.ANY, p.create_message)


def test_service_produces_messages():
    """Check that the producer produces messages in the idle event handler."""
    p = workflows.services.sample_producer.SampleProducer()
    mock_transport = mock.Mock()
    setattr(p, "_transport", mock_transport)

    p.initializing()
    assert not mock_transport.send.called
    p.create_message()

    mock_transport.send.assert_called_once()

    p.create_message()

    assert mock_transport.send.call_count == 2
    calls = mock_transport.send.call_args_list
    assert calls[0][0][0] == calls[1][0][0]  # same destination
    assert calls[0][0][1] != calls[1][0][1]  # different message


def test_service_with_metrics(mocker):
    prometheus_client = pytest.importorskip("prometheus_client")
    t = OfflineTransport()
    mock_start_http_server = mocker.patch("prometheus_client.start_http_server")

    p = workflows.services.sample_producer.SampleProducer(
        environment={"metrics": {"port": 4242}}
    )
    p.transport = t
    p.start()

    mock_start_http_server.assert_called_with(port=4242)

    p.create_message()
    p.create_message()

    data = prometheus_client.generate_latest().decode("ascii")
    assert (
        'workflows_transport_send_total{source="workflows.services.sample_producer:SampleProducer"} 2.0'
        in data
    )
