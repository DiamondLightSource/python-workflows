# A workflows transport that doesn't actually transport anything

from __future__ import annotations

import json
import logging
import pprint
import uuid
from typing import Any, Dict, Optional, Type

import workflows.util
from workflows.transport import middleware
from workflows.transport.common_transport import (
    CommonTransport,
    MessageCallback,
    json_serializer,
)

_offlog = logging.getLogger("workflows.transport.offline_transport")


class OfflineTransport(CommonTransport):
    """Abstraction layer for messaging infrastructure. Here we.. do nothing."""

    # Add for compatibility
    defaults: Dict[Any, Any] = {}
    # Effective configuration
    config: Dict[Any, Any] = {}

    def __init__(
        self, middleware: list[Type[middleware.BaseTransportMiddleware]] = None
    ):
        self._connected = False
        super().__init__(middleware=middleware)

    def connect(self):
        self._connected = True
        return True

    def is_connected(self):
        return self._connected

    def disconnect(self):
        self._connected = False

    def _output(self, message, details=None):
        _offlog.info(f"Offline Transport: {message}")
        if details:
            _offlog.debug(details)

    def broadcast_status(self, status):
        self._output("Writing status message", pprint.pformat(status))

    def _subscribe(self, sub_id, channel, callback, **kwargs):
        self._output(
            f"Subscribing to messages on {channel}",
            f"subscription ID {sub_id}, callback function {callback}, further keywords: {kwargs}",
        )

    def _subscribe_temporary(
        self,
        sub_id: int,
        channel_hint: Optional[str],
        callback: MessageCallback,
        **kwargs,
    ) -> str:
        channel = channel_hint or workflows.util.generate_unique_host_id()
        channel = channel + "." + str(uuid.uuid4())
        if not channel.startswith("transient."):
            channel = "transient." + channel

        self._subscribe(sub_id, channel, callback, **kwargs)
        return channel

    def _subscribe_broadcast(self, sub_id, channel, callback, **kwargs):
        self._output(
            f"Subscribing to broadcasts on {channel}",
            f"subscription ID {sub_id}, callback function {callback}, further keywords: {kwargs}",
        )

    def _unsubscribe(self, subscription, **kwargs):
        self._output(
            f"Ending subscription #{subscription}", f"further keywords: {kwargs}"
        )

    def _send(
        self, destination, message, headers=None, delay=None, expiration=None, **kwargs
    ):
        self._output(f"Sending {len(message)} bytes to {destination}", message)

    def _broadcast(
        self, destination, message, headers=None, delay=None, expiration=None, **kwargs
    ):
        self._output(f"Broadcasting {len(message)} bytes to {destination}", message)

    def _transaction_begin(self, transaction_id, **kwargs):
        self._output(f"Starting transaction {transaction_id}")

    def _transaction_abort(self, transaction_id, **kwargs):
        self._output(f"Rolling back transaction {transaction_id}")

    def _transaction_commit(self, transaction_id, **kwargs):
        self._output(f"Committing transaction {transaction_id}")

    def _ack(self, message_id, subscription_id, **kwargs):
        self._output(
            f"Acknowledging message {message_id} in subscription {subscription_id}"
        )

    def _nack(self, message_id, subscription_id, **kwargs):
        self._output(
            f"Rejecting message {message_id} in subscription {subscription_id}"
        )

    @staticmethod
    def _mangle_for_sending(message):
        return json.dumps(message, default=json_serializer)
