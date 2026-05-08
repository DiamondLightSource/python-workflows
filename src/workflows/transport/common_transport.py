from __future__ import annotations

import decimal
import logging
from collections.abc import Callable, Mapping
from typing import Any, NamedTuple

import workflows
from workflows.transport import middleware

MessageCallback = Callable[[Mapping[str, Any], Any], None]


class TemporarySubscription(NamedTuple):
    subscription_id: int
    queue_name: str


class CommonTransport:
    """A common transport class.

    Contains e.g. the logic to manage subscriptions and transactions.
    """

    __callback_interceptor = None
    __subscriptions: dict[int, dict[str, Any]] = {}
    __subscription_id: int = 0
    __transactions: set[int] = set()
    __transaction_id: int = 0

    log = logging.getLogger("workflows.transport")

    #
    # -- High level communication calls ----------------------------------------
    #

    def __init__(
        self, middleware: list[middleware.BaseTransportMiddleware] | None = None
    ):
        if middleware is None:
            self.middleware = []
        else:
            self.middleware = middleware

    def add_middleware(self, middleware: middleware.BaseTransportMiddleware):
        self.middleware.insert(0, middleware)

    @classmethod
    def add_command_line_options(cls, parser):
        """Inject command line parameters."""
        pass

    def connect(self) -> bool:
        """Connect the transport class. This function must be overridden.

        Returns:
            True-like value when connection successful, False-like value
            otherwise.
        """
        return False

    def is_connected(self) -> bool:
        """Return the current connection status. This function must be overridden.

        Returns:
            True-like value when connection is available, False-like value
            otherwise.
        """
        return False

    def disconnect(self):
        """Gracefully disconnect the transport class.

        This function should be overridden.
        """

    @middleware.wrap
    def subscribe(self, channel, callback, **kwargs) -> int:
        """Listen to a queue, notify via callback function.

        Args:
            channel: Queue name to subscribe to.
            callback: Function to be called when messages are received.
                The callback will pass two arguments, the header as a
                dictionary structure, and the message.
            **kwargs: Further parameters for the transport layer. For example:
                disable_mangling: Receive messages as unprocessed strings.
                exclusive: Attempt to become exclusive subscriber to the queue.
                acknowledgement: If true receipt of each message needs to be
                acknowledged.

        Returns:
            A unique subscription ID.
        """

        self.__subscription_id += 1

        def mangled_callback(header, message):
            return callback(header, self._mangle_for_receiving(message))

        if "disable_mangling" in kwargs:
            if kwargs["disable_mangling"]:
                mangled_callback = callback  # noqa:F811
            del kwargs["disable_mangling"]
        self.__subscriptions[self.__subscription_id] = {
            "channel": channel,
            "callback": mangled_callback,
            "ack": kwargs.get("acknowledgement"),
            "unsubscribed": False,
        }
        self.log.debug("Subscribing to %s with ID %d", channel, self.__subscription_id)
        self._subscribe(self.__subscription_id, channel, mangled_callback, **kwargs)
        return self.__subscription_id

    @middleware.wrap
    def subscribe_temporary(
        self, channel_hint: str | None, callback: MessageCallback, **kwargs
    ) -> TemporarySubscription:
        """Listen to a new queue specifically created for this connection.

        The queue has a limited lifetime. Notify for messages via callback
        function.

        Args:
            channel_hint: Suggested queue name to subscribe to, the actual
                queue name will be decided by both transport layer and server.
            callback: Function to be called when messages are received.
                The callback will pass two arguments, the header as a
                dictionary structure, and the message.
            **kwargs: Further parameters for the transport layer. For example:
                disable_mangling: Receive messages as unprocessed strings.
                acknowledgement: If true receipt of each message needs to be
                acknowledged.

        Returns:
            A named tuple containing a unique subscription ID and the actual
            queue name which can then be referenced by other senders.
        """

        self.__subscription_id += 1

        def _(header: Mapping[str, Any], message: Any) -> None:
            callback(header, self._mangle_for_receiving(message))

        mangled_callback: MessageCallback = _

        if "disable_mangling" in kwargs:
            if kwargs["disable_mangling"]:
                mangled_callback = callback  # noqa:F811
            del kwargs["disable_mangling"]
        self.__subscriptions[self.__subscription_id] = {
            # "channel": channel,
            "callback": mangled_callback,
            "ack": kwargs.get("acknowledgement"),
            "unsubscribed": False,
        }
        self.log.debug(
            "Subscribing to temporary queue (name hint: %r) with ID %d",
            channel_hint,
            self.__subscription_id,
        )
        queue_name = self._subscribe_temporary(
            self.__subscription_id, channel_hint, mangled_callback, **kwargs
        )

        return TemporarySubscription(
            subscription_id=self.__subscription_id, queue_name=queue_name
        )

    @middleware.wrap
    def unsubscribe(self, subscription: int, drop_callback_reference=False, **kwargs):
        """Stop listening to a queue or a broadcast.

        Args:
            subscription: Subscription ID to cancel.
            drop_callback_reference: Drop the reference to the registered
                callback function immediately. This means any buffered
                messages still in flight will not arrive at the intended
                destination and cause exceptions to be raised instead.
            **kwargs: Further parameters for the transport layer.
        """

        if subscription not in self.__subscriptions:
            raise workflows.Error("Attempting to unsubscribe unknown subscription")
        if self.__subscriptions[subscription]["unsubscribed"]:
            raise workflows.Error(
                "Attempting to unsubscribe already unsubscribed subscription"
            )
        self._unsubscribe(subscription, **kwargs)
        self.__subscriptions[subscription]["unsubscribed"] = True
        if drop_callback_reference:
            self.drop_callback_reference(subscription)

    def drop_callback_reference(self, subscription: int):
        """Drop reference to the callback function after unsubscribing.

        Any future messages arriving for that subscription will result in
        exceptions being raised.

        Args:
            subscription: Subscription ID to delete callback reference for.
        """
        if subscription not in self.__subscriptions:
            raise workflows.Error(
                "Attempting to drop callback reference for unknown subscription"
            )
        if not self.__subscriptions[subscription]["unsubscribed"]:
            raise workflows.Error(
                "Attempting to drop callback reference for live subscription"
            )
        del self.__subscriptions[subscription]

    @middleware.wrap
    def subscribe_broadcast(self, channel, callback, **kwargs) -> int:
        """Listen to a broadcast topic, notify via callback function.

        Args:
            channel: Topic name to subscribe to.
            callback: Function to be called when messages are received.
                The callback will pass two arguments, the header as a
                dictionary structure, and the message.
            **kwargs: Further parameters for the transport layer. For example:
                disable_mangling: Receive messages as unprocessed strings.
                retroactive: Ask broker to send old messages if possible.

        Returns:
            A unique subscription ID.
        """

        self.__subscription_id += 1

        def mangled_callback(header, message):
            return callback(header, self._mangle_for_receiving(message))

        if "disable_mangling" in kwargs:
            if kwargs["disable_mangling"]:
                mangled_callback = callback  # noqa:F811
            del kwargs["disable_mangling"]
        self.__subscriptions[self.__subscription_id] = {
            "channel": channel,
            "callback": mangled_callback,
            "ack": False,
            "unsubscribed": False,
        }
        self.log.debug(
            "Subscribing to broadcasts on %s with ID %d",
            channel,
            self.__subscription_id,
        )
        self._subscribe_broadcast(
            self.__subscription_id, channel, mangled_callback, **kwargs
        )
        return self.__subscription_id

    def subscription_callback(self, subscription: int) -> MessageCallback:
        """Retrieve the callback function for a subscription.

        All transport callbacks can be intercepted by setting an interceptor
        function with subscription_callback_intercept().

        Args:
            subscription: Subscription ID to look up.

        Returns:
            Callback function.

        Raises:
            workflows.Error: If the subscription does not exist.
        """
        subscription_record = self.__subscriptions.get(subscription)
        if not subscription_record:
            raise workflows.Error("Attempting to callback on unknown subscription")
        callback = subscription_record["callback"]
        if self.__callback_interceptor:
            return self.__callback_interceptor(callback)
        return callback

    def subscription_callback_set_intercept(self, interceptor):
        """Set a function to intercept all callbacks.

        This is useful to, for example, keep a thread barrier between the
        transport related functions and processing functions.

        Args:
            interceptor: A function that takes the original callback function
                and returns a modified callback function. Or None to disable
                interception.
        """
        self.__callback_interceptor = interceptor

    @middleware.wrap
    def send(
        self, destination: str, message: Any, *, headers: dict | None = None, **kwargs
    ):
        """Send a message to a queue.

        Args:
            destination: Queue name to send to.
            message: The message. Usually string-like or json-serializable but
                exact specification depends on the concrete transport.
            headers: Optional dictionary of header entries to set.
            **kwargs: Further parameters for the transport layer. For example:
                delay: Delay transport of message by this many seconds.
                expiration: Optional expiration time, relative to sending time.
                transaction: Transaction ID if message should be part of a
                transaction.
        """

        message = self._mangle_for_sending(message)
        self._send(destination, message, headers=headers, **kwargs)

    @middleware.wrap
    def raw_send(self, destination, message, **kwargs):
        """Send a raw (unmangled) message to a queue.

        This may cause errors if the receiver expects a mangled message.

        Args:
            destination: Queue name to send to.
            message: Either a string or a serializable object to be sent.
            **kwargs: Further parameters for the transport layer. For example:
                delay: Delay transport of message by this many seconds.
                headers: Optional dictionary of header entries.
                expiration: Optional expiration time, relative to sending time.
                transaction: Transaction ID if message should be part of a
                transaction.
        """

        self._send(destination, message, **kwargs)

    @middleware.wrap
    def broadcast(self, destination, message, **kwargs):
        """Broadcast a message.

        Args:
            destination: Topic name to send to.
            message: Either a string or a serializable object to be sent.
            **kwargs: Further parameters for the transport layer. For example:
                delay: Delay transport of message by this many seconds.
                headers: Optional dictionary of header entries.
                expiration: Optional expiration time, relative to sending time.
                transaction: Transaction ID if message should be part of a
                transaction.
        """

        message = self._mangle_for_sending(message)
        self._broadcast(destination, message, **kwargs)

    @middleware.wrap
    def raw_broadcast(self, destination, message, **kwargs):
        """Broadcast a raw (unmangled) message.

        This may cause errors if the receiver expects a mangled message.

        Args:
            destination: Topic name to send to.
            message: Either a string or a serializable object to be sent.
            **kwargs: Further parameters for the transport layer. For example:
                delay: Delay transport of message by this many seconds.
                headers: Optional dictionary of header entries.
                expiration: Optional expiration time, relative to sending time.
                transaction: Transaction ID if message should be part of a
                transaction.
        """

        self._broadcast(destination, message, **kwargs)

    def broadcast_status(self, status: dict) -> None:
        """Broadcast transient status information to all listeners."""
        raise NotImplementedError

    @middleware.wrap
    def ack(self, message, subscription_id: int | None = None, **kwargs):
        """Acknowledge receipt of a message.

        This only makes sense when the 'acknowledgement' flag was set for the
        relevant subscription.

        Args:
            message: ID of the message to be acknowledged, OR a dictionary
                containing a field 'message-id'.
            subscription_id: ID of the associated subscription. Optional when
                a dictionary is passed as first parameter and that dictionary
                contains field 'subscription'.
            **kwargs: Further parameters for the transport layer. For example:
                transaction: Transaction ID if acknowledgement should be part
                of a transaction.
        """

        if isinstance(message, dict):
            message_id = message.get("message-id")
            if not subscription_id:
                subscription_id = message.get("subscription")
        else:
            message_id = message
        if not message_id:
            raise workflows.Error("Cannot acknowledge message without message ID")
        if not subscription_id:
            raise workflows.Error("Cannot acknowledge message without subscription ID")
        self.log.debug(
            "Acknowledging message %s on subscription %s",
            message_id,
            subscription_id,
        )
        self._ack(message_id, subscription_id=subscription_id, **kwargs)

    @middleware.wrap
    def nack(self, message, subscription_id: int | None = None, **kwargs):
        """Reject receipt of a message.

        This only makes sense when the 'acknowledgement' flag was set for the
        relevant subscription.

        Args:
            message: ID of the message to be rejected, OR a dictionary
                containing a field 'message-id'.
            subscription_id: ID of the associated subscription. Optional when
                a dictionary is passed as first parameter and that dictionary
                contains field 'subscription'.
            **kwargs: Further parameters for the transport layer. For example:
                transaction: Transaction ID if rejection should be part of a
                transaction.
        """

        if isinstance(message, dict):
            message_id = message.get("message-id")
            if not subscription_id:
                subscription_id = message.get("subscription")
        else:
            message_id = message
        if not message_id:
            raise workflows.Error("Cannot reject message without message ID")
        if not subscription_id:
            raise workflows.Error("Cannot reject message without subscription ID")
        self.log.debug(
            "Rejecting message %s on subscription %d", message_id, subscription_id
        )
        self._nack(message_id, subscription_id=subscription_id, **kwargs)

    @middleware.wrap
    def transaction_begin(self, subscription_id: int | None = None, **kwargs) -> int:
        """Start a new transaction.

        Args:
            subscription_id: ID of the subscription to scope this transaction to.
            **kwargs: Further parameters for the transport layer.

        Returns:
            A transaction ID that can be passed to other functions.
        """

        self.__transaction_id += 1
        self.__transactions.add(self.__transaction_id)
        if subscription_id:
            self.log.debug(
                "Starting transaction with ID %d on subscription %d",
                self.__transaction_id,
                subscription_id,
            )
        else:
            self.log.debug("Starting transaction with ID %d", self.__transaction_id)
        self._transaction_begin(
            self.__transaction_id, subscription_id=subscription_id, **kwargs
        )
        return self.__transaction_id

    @middleware.wrap
    def transaction_abort(self, transaction_id: int, **kwargs):
        """Abort a transaction and roll back all operations.

        Args:
            transaction_id: ID of transaction to be aborted.
            **kwargs: Further parameters for the transport layer.
        """

        if transaction_id not in self.__transactions:
            raise workflows.Error("Attempting to abort unknown transaction")
        self.log.debug("Aborting transaction %s", transaction_id)
        self.__transactions.remove(transaction_id)
        self._transaction_abort(transaction_id, **kwargs)

    @middleware.wrap
    def transaction_commit(self, transaction_id: int, **kwargs):
        """Commit a transaction.

        Args:
            transaction_id: ID of transaction to be committed.
            **kwargs: Further parameters for the transport layer.
        """

        if transaction_id not in self.__transactions:
            raise workflows.Error("Attempting to commit unknown transaction")
        self.log.debug("Committing transaction %s", transaction_id)
        self.__transactions.remove(transaction_id)
        self._transaction_commit(transaction_id, **kwargs)

    @property
    def is_reconnectable(self):
        """Check if the transport object is in a status where reconnecting is supported.

        There must not be any active subscriptions or transactions.
        """
        return not self.__subscriptions and not self.__transactions

    #
    # -- Low level communication calls to be implemented by subclass -----------
    #

    def _subscribe(self, sub_id: int, channel, callback, **kwargs):
        """Listen to a queue, notify via callback function.

        Args:
            sub_id: ID for this subscription in the transport layer.
            channel: Queue name to subscribe to.
            callback: Function to be called when messages are received.
            **kwargs: Further parameters for the transport layer. For example:
                exclusive: Attempt to become exclusive subscriber to the queue.
                acknowledgement: If true receipt of each message needs to be
                acknowledged.
        """
        raise NotImplementedError("Transport interface not implemented")

    def _subscribe_broadcast(self, sub_id: int, channel, callback, **kwargs):
        """Listen to a broadcast topic, notify via callback function.

        Args:
            sub_id: ID for this subscription in the transport layer.
            channel: Topic name to subscribe to.
            callback: Function to be called when messages are received.
            **kwargs: Further parameters for the transport layer. For example:
                retroactive: Ask broker to send old messages if possible.
        """
        raise NotImplementedError("Transport interface not implemented")

    def _subscribe_temporary(
        self,
        sub_id: int,
        channel_hint: str | None,
        callback: MessageCallback,
        **kwargs,
    ) -> str:
        """Create and then listen to a temporary queue, notify via callback function.

        Args:
            sub_id: ID for this subscription in the transport layer.
            channel_hint: Name suggestion for the temporary queue.
            callback: Function to be called when messages are received.
            **kwargs: Further parameters for the transport layer. For example:
                acknowledgement: If true receipt of each message needs to be
                acknowledged.

        Returns:
            The name of the temporary queue.
        """
        raise NotImplementedError("Transport interface not implemented")

    def _unsubscribe(self, sub_id: int, **kwargs):
        """Stop listening to a queue or a broadcast.

        Args:
            sub_id: ID for this subscription in the transport layer.
            **kwargs: Further parameters for the transport layer.
        """
        raise NotImplementedError("Transport interface not implemented")

    def _send(self, destination, message, **kwargs):
        """Send a message to a queue.

        Args:
            destination: Queue name to send to.
            message: A string to be sent.
            **kwargs: Further parameters for the transport layer. For example:
                headers: Optional dictionary of header entries.
                expiration: Optional expiration time, relative to sending time.
                transaction: Transaction ID if message should be part of a
                transaction.
        """
        raise NotImplementedError("Transport interface not implemented")

    def _broadcast(self, destination, message, **kwargs):
        """Broadcast a message.

        Args:
            destination: Topic name to send to.
            message: A string to be broadcast.
            **kwargs: Further parameters for the transport layer. For example:
                headers: Optional dictionary of header entries.
                expiration: Optional expiration time, relative to sending time.
                transaction: Transaction ID if message should be part of a
                transaction.
        """
        raise NotImplementedError("Transport interface not implemented")

    def _ack(self, message_id, subscription_id, **kwargs):
        """Acknowledge receipt of a message.

        This only makes sense when the 'acknowledgement' flag was set for the
        relevant subscription.

        Args:
            message_id: ID of the message to be acknowledged.
            subscription_id: ID of the associated subscription.
            **kwargs: Further parameters for the transport layer. For example:
                transaction: Transaction ID if acknowledgement should be part
                of a transaction.
        """
        raise NotImplementedError("Transport interface not implemented")

    def _nack(self, message_id, subscription_id, **kwargs):
        """Reject receipt of a message.

        This only makes sense when the 'acknowledgement' flag was set for the
        relevant subscription.

        Args:
            message_id: ID of the message to be rejected.
            subscription_id: ID of the associated subscription.
            **kwargs: Further parameters for the transport layer. For example:
                transaction: Transaction ID if rejection should be part of a
                transaction.
        """
        raise NotImplementedError("Transport interface not implemented")

    def _transaction_begin(
        self, transaction_id: int, *, subscription_id: int | None = None, **kwargs
    ) -> None:
        """Start a new transaction.

        Args:
            transaction_id: ID for this transaction in the transport layer.
            subscription_id: ID of the subscription to scope this transaction to.
            **kwargs: Further parameters for the transport layer.
        """
        raise NotImplementedError("Transport interface not implemented")

    def _transaction_abort(self, transaction_id: int, **kwargs) -> None:
        """Abort a transaction and roll back all operations.

        Args:
            transaction_id: ID of transaction to be aborted.
            **kwargs: Further parameters for the transport layer.
        """
        raise NotImplementedError("Transport interface not implemented")

    def _transaction_commit(self, transaction_id: int, **kwargs) -> None:
        """Commit a transaction.

        Args:
            transaction_id: ID of transaction to be committed.
            **kwargs: Further parameters for the transport layer.
        """
        raise NotImplementedError("Transport interface not implemented")

    #
    # -- Internal message mangling functions -----------------------------------
    #

    # Some transport mechanisms will not be able to work with arbitrary objects,
    # so these functions are used to prepare a message for sending/receiving.
    # The canonical example is serialization/deserialization, see stomp_transport

    @staticmethod
    def _mangle_for_sending(message):
        """Pass any message through this before forwarding to the actual _send* functions."""
        return message

    @staticmethod
    def _mangle_for_receiving(message):
        """Pass any message through this before forwarding to the receiving subscribed callback functions."""
        return message


def json_serializer(obj):
    """Helper function for JSON serialization, usable as the ``default=`` argument.

    This function helps the serializer to translate objects that otherwise
    would not be understood. Note that this is one-way only - these objects
    are not restored on the receiving end.

    Args:
        obj: The object to serialize.

    Returns:
        A JSON-serializable representation of obj.

    Raises:
        TypeError: If obj is not JSON serializable.
    """

    if isinstance(obj, decimal.Decimal):
        # turn all Decimals into floats
        return float(obj)

    raise TypeError(repr(obj) + " is not JSON serializable")
