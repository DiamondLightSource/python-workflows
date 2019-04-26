from __future__ import absolute_import, division, print_function

import mock
import workflows.services
import workflows.services.sample_producer


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
