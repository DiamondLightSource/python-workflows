from __future__ import absolute_import, division, print_function

import mock
import workflows.services
import workflows.services.sample_consumer


def test_service_can_be_looked_up():
    """Attempt to look up the service by its name"""
    service_class = workflows.services.lookup("SampleConsumer")
    assert service_class == workflows.services.sample_consumer.SampleConsumer


def test_service_subscribes_to_channel():
    """Check that the service subscribes to some location."""
    p = workflows.services.sample_consumer.SampleConsumer()
    mock_transport = mock.Mock()
    setattr(p, "_transport", mock_transport)

    p.initializing()

    mock_transport.subscribe.assert_called_once_with(mock.ANY, p.consume_message)


def test_service_can_consume_messages():
    """Check that the service registers an idle event handler."""
    p = workflows.services.sample_consumer.SampleConsumer()
    p.consume_message(None, mock.sentinel.message)
    p.consume_message({"some": "header"}, mock.sentinel.message)
