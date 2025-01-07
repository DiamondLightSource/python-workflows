from __future__ import annotations

import decimal
import logging
from unittest import mock

import pytest

import workflows.transport
from workflows.transport.offline_transport import OfflineTransport


def test_lookup_and_initialize_offline_transport_layer():
    """Find the offline transport layer via the lookup mechanism and run
    its constructor with default settings."""
    offline = workflows.transport.lookup("OfflineTransport")
    assert offline == OfflineTransport
    offline()


def test_instantiate_link_and_connect_to_broker():
    """Test the Offline connection routine."""
    offline = OfflineTransport()
    assert not offline.is_connected()

    offline.connect()
    assert offline.is_connected()

    offline.disconnect()
    assert not offline.is_connected()


def test_broadcast_status(caplog):
    """Test the status broadcast function."""
    offline = OfflineTransport()
    offline.connect()
    message = "Writing status message"
    with caplog.at_level(logging.INFO):
        offline.broadcast_status({"status": str(mock.sentinel.status)})
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            )
        ]
    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        offline.broadcast_status({"status": str(mock.sentinel.status)})
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            ),
            (
                "workflows.transport.offline_transport",
                logging.DEBUG,
                f"{{'status': '{str(mock.sentinel.status)}'}}",
            ),
        ]


def test_send_message(caplog):
    """Test the message sending function."""
    offline = OfflineTransport()
    offline.connect()

    with caplog.at_level(logging.DEBUG):
        offline._send(str(mock.sentinel.channel), str(mock.sentinel.message))
        message = f"Sending {len(str(mock.sentinel.message))} bytes to {str(mock.sentinel.channel)}"
        debug = f"{str(mock.sentinel.message)}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            ),
            (
                "workflows.transport.offline_transport",
                logging.DEBUG,
                debug,
            ),
        ]


def test_send_broadcast(caplog):
    """Test the broadcast sending function."""
    offline = OfflineTransport()
    offline.connect()

    with caplog.at_level(logging.DEBUG):
        offline._broadcast(str(mock.sentinel.channel), str(mock.sentinel.message))
        message = f"Broadcasting {len(str(mock.sentinel.message))} bytes to {str(mock.sentinel.channel)}"
        debug = f"{str(mock.sentinel.message)}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            ),
            (
                "workflows.transport.offline_transport",
                logging.DEBUG,
                debug,
            ),
        ]


def test_messages_are_serialized_for_transport(caplog):
    """Test the message serialization."""
    banana = {"entry": [1, 2.0, decimal.Decimal(3), "banana"]}
    banana_str = '{"entry": [1, 2.0, 3.0, "banana"]}'
    offline = OfflineTransport()
    offline.connect()

    with caplog.at_level(logging.DEBUG):
        offline.send(str(mock.sentinel.channel1), banana)
        message = f"Sending {len(banana_str)} bytes to {str(mock.sentinel.channel1)}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            ),
            (
                "workflows.transport.offline_transport",
                logging.DEBUG,
                banana_str,
            ),
        ]
    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        offline.broadcast(str(mock.sentinel.channel2), banana)
        message = (
            f"Broadcasting {len(banana_str)} bytes to {str(mock.sentinel.channel2)}"
        )
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            ),
            (
                "workflows.transport.offline_transport",
                logging.DEBUG,
                banana_str,
            ),
        ]

    with pytest.raises(Exception):
        offline.send(str(mock.sentinel.channel), mock.sentinel.unserializable)


def test_subscribe_to_queue(caplog):
    """Test subscribing to a queue (producer-consumer), callback functions and unsubscribe."""
    mock_cb1 = mock.Mock()
    mock_cb2 = mock.Mock()
    offline = OfflineTransport()
    offline.connect()

    with caplog.at_level(logging.INFO):
        offline._subscribe(
            1,
            str(mock.sentinel.channel1),
            mock_cb1,
            transformation=mock.sentinel.transformation,
        )
        message = f"Subscribing to messages on {str(mock.sentinel.channel1)}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            )
        ]

    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        offline._subscribe(
            1,
            str(mock.sentinel.channel1),
            mock_cb1,
            transformation=mock.sentinel.transformation,
        )
        message = f"Subscribing to messages on {str(mock.sentinel.channel1)}"
        debug = f"subscription ID 1, callback function {str(mock_cb1)}, further keywords: {{'transformation': {str(mock.sentinel.transformation)}}}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            ),
            ("workflows.transport.offline_transport", logging.DEBUG, f"{debug}"),
        ]

    caplog.clear()
    with caplog.at_level(logging.INFO):
        offline._subscribe(
            2,
            str(mock.sentinel.channel2),
            mock_cb2,
            retroactive=True,
            selector=mock.sentinel.selector,
            exclusive=True,
            transformation=True,
            priority=42,
        )
        message = f"Subscribing to messages on {str(mock.sentinel.channel2)}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            )
        ]

    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        offline._subscribe(
            2,
            str(mock.sentinel.channel2),
            mock_cb2,
            retroactive=True,
            selector=mock.sentinel.selector,
            exclusive=True,
            transformation=True,
            priority=42,
        )
        message = f"Subscribing to messages on {str(mock.sentinel.channel2)}"
        debug = f"subscription ID 2, callback function {str(mock_cb2)}, further keywords: {{'retroactive': True, 'selector': {str(mock.sentinel.selector)}, 'exclusive': True, 'transformation': True, 'priority': 42}}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            ),
            ("workflows.transport.offline_transport", logging.DEBUG, f"{debug}"),
        ]


def test_subscribe_to_broadcast(caplog):
    """Test subscribing to a topic (publish-subscribe) and callback functions."""
    mock_cb1 = mock.Mock()
    mock_cb2 = mock.Mock()
    offline = OfflineTransport()
    offline.connect()

    with caplog.at_level(logging.INFO):
        offline._subscribe_broadcast(
            1,
            str(mock.sentinel.channel1),
            mock_cb1,
            transformation=mock.sentinel.transformation,
        )
        message = f"Subscribing to broadcasts on {str(mock.sentinel.channel1)}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            )
        ]

    caplog.clear()
    caplog.at_level(logging.DEBUG)
    with caplog.at_level(logging.DEBUG):
        offline._subscribe_broadcast(
            1,
            str(mock.sentinel.channel1),
            mock_cb1,
            transformation=mock.sentinel.transformation,
        )
        message = f"Subscribing to broadcasts on {str(mock.sentinel.channel1)}"
        debug = f"subscription ID 1, callback function {str(mock_cb1)}, further keywords: {{'transformation': {str(mock.sentinel.transformation)}}}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            ),
            ("workflows.transport.offline_transport", logging.DEBUG, f"{debug}"),
        ]

    caplog.clear()
    with caplog.at_level(logging.INFO):
        offline._subscribe_broadcast(
            2,
            str(mock.sentinel.channel2),
            mock_cb2,
            retroactive=True,
            transformation=True,
        )
        message = f"Subscribing to broadcasts on {str(mock.sentinel.channel2)}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            )
        ]

    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        offline._subscribe_broadcast(
            2,
            str(mock.sentinel.channel2),
            mock_cb2,
            retroactive=True,
            transformation=True,
        )
        message = f"Subscribing to broadcasts on {str(mock.sentinel.channel2)}"
        debug = f"subscription ID 2, callback function {str(mock_cb2)}, further keywords: {{'retroactive': True, 'transformation': True}}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            ),
            ("workflows.transport.offline_transport", logging.DEBUG, f"{debug}"),
        ]

    caplog.clear()
    with caplog.at_level(logging.INFO):
        offline._unsubscribe(1)
        message = "Ending subscription #1"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            )
        ]

    caplog.clear()
    with caplog.at_level(logging.INFO):
        offline._unsubscribe(2)
        message = "Ending subscription #2"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            )
        ]


def test_subscribe_temporary(caplog):
    """Test subscribing to a temporary queue and callback functions."""
    mock_cb1 = mock.Mock()
    channel_hint = "foo"
    offline = OfflineTransport()
    offline.connect()

    with caplog.at_level(logging.INFO):
        channel = offline._subscribe_temporary(
            1,
            channel_hint,
            mock_cb1,
            transformation=mock.sentinel.transformation,
        )
        message = f"Subscribing to messages on {channel}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            )
        ]

    caplog.clear()
    with caplog.at_level(logging.INFO):
        offline._unsubscribe(1)
        message = "Ending subscription #1"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message}",
            )
        ]


def test_transaction_calls(caplog):
    """Test that calls to create, commit, abort transactions are properly logged."""
    offline = OfflineTransport()
    offline.connect()

    with caplog.at_level(logging.INFO):
        offline._transaction_begin(mock.sentinel.txid)
        offline._send(
            "destination", str(mock.sentinel.message), transaction=mock.sentinel.txid
        )
        offline._transaction_abort(mock.sentinel.txid)
        offline._transaction_commit(mock.sentinel.txid)
        message_start = f"Starting transaction {str(mock.sentinel.txid)}"
        message_send = f"Sending {len(str(mock.sentinel.message))} bytes to destination"
        message_abort = f"Rolling back transaction {str(mock.sentinel.txid)}"
        message_commit = f"Committing transaction {str(mock.sentinel.txid)}"
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message_start}",
            ),
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message_send}",
            ),
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message_abort}",
            ),
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message_commit}",
            ),
        ]


def test_ack_message(caplog):
    """Test that the _ack function is properly logged."""
    offline = OfflineTransport()
    offline.connect()

    with caplog.at_level(logging.DEBUG):
        offline._subscribe(1, str(mock.sentinel.channel3), None, acknowledgement=True)
        offline._ack(str(mock.sentinel.messageid), 1)
        message_subscribe = f"Subscribing to messages on {str(mock.sentinel.channel3)}"
        debug = "subscription ID 1, callback function None, further keywords: {'acknowledgement': True}"
        message_ack = (
            f"Acknowledging message {str(mock.sentinel.messageid)} in subscription 1"
        )
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message_subscribe}",
            ),
            ("workflows.transport.offline_transport", logging.DEBUG, f"{debug}"),
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message_ack}",
            ),
        ]


def test_nack_message(caplog):
    """Test that the _nack function is properly logged."""
    offline = OfflineTransport()
    offline.connect()

    with caplog.at_level(logging.DEBUG):
        offline._subscribe(1, str(mock.sentinel.channel3), None, acknowledgement=True)
        offline._nack(mock.sentinel.messageid, 1)
        message_subscribe = f"Subscribing to messages on {str(mock.sentinel.channel3)}"
        debug = "subscription ID 1, callback function None, further keywords: {'acknowledgement': True}"
        message_nack = (
            f"Rejecting message {str(mock.sentinel.messageid)} in subscription 1"
        )
        assert caplog.record_tuples == [
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message_subscribe}",
            ),
            ("workflows.transport.offline_transport", logging.DEBUG, f"{debug}"),
            (
                "workflows.transport.offline_transport",
                logging.INFO,
                f"Offline Transport: {message_nack}",
            ),
        ]
