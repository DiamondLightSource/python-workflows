import decimal
import logging

import workflows


class CommonTransport:
    """A common transport class, containing e.g. the logic to manage
    subscriptions and transactions."""

    __callback_interceptor = None
    __subscriptions = {}
    __subscription_id = 0
    __transactions = set()
    __transaction_id = 0

    log = logging.getLogger("workflows.transport")

    #
    # -- High level communication calls ----------------------------------------
    #

    @staticmethod
    def connect():
        """Connect the transport class. This function must be overridden.
        :return: True-like value when connection successful,
                 False-like value otherwise."""
        return False

    @staticmethod
    def is_connected():
        """Returns the current connection status. This function must be overridden.
        :return: True-like value when connection is available,
                 False-like value otherwise."""
        return False

    @staticmethod
    def disconnect():
        """Gracefully disconnect the transport class. This function should be
        overridden."""

    def subscribe(self, channel, callback, **kwargs):
        """Listen to a queue, notify via callback function.
        :param channel: Queue name to subscribe to
        :param callback: Function to be called when messages are received.
                         The callback will pass two arguments, the header as a
                         dictionary structure, and the message.
        :param **kwargs: Further parameters for the transport layer. For example
               disable_mangling: Receive messages as unprocessed strings.
               exclusive: Attempt to become exclusive subscriber to the queue.
               acknowledgement: If true receipt of each message needs to be
                                acknowledged.
        :return: A unique subscription ID
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

    def unsubscribe(self, subscription, drop_callback_reference=False, **kwargs):
        """Stop listening to a queue or a broadcast
        :param subscription: Subscription ID to cancel
        :param drop_callback_reference: Drop the reference to the registered
                                        callback function immediately. This
                                        means any buffered messages still in
                                        flight will not arrive at the intended
                                        destination and cause exceptions to be
                                        raised instead.
        :param **kwargs: Further parameters for the transport layer.
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

    def drop_callback_reference(self, subscription):
        """Drop reference to the callback function after unsubscribing.
        Any future messages arriving for that subscription will result in
        exceptions being raised.
        :param subscription: Subscription ID to delete callback reference for.
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

    def subscribe_broadcast(self, channel, callback, **kwargs):
        """Listen to a broadcast topic, notify via callback function.
        :param channel: Topic name to subscribe to
        :param callback: Function to be called when messages are received.
                         The callback will pass two arguments, the header as a
                         dictionary structure, and the message.
        :param **kwargs: Further parameters for the transport layer. For example
               disable_mangling: Receive messages as unprocessed strings.
               retroactive: Ask broker to send old messages if possible
        :return: A unique subscription ID
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

    def subscription_callback(self, subscription):
        """Retrieve the callback function for a subscription. Raise a
        workflows.Error if the subscription does not exist.
        All transport callbacks can be intercepted by setting an
        interceptor function with subscription_callback_intercept().
        :param subscription: Subscription ID to look up
        :return: Callback function
        """
        subscription_record = self.__subscriptions.get(subscription)
        if not subscription_record:
            raise workflows.Error("Attempting to callback on unknown subscription")
        callback = subscription_record["callback"]
        if self.__callback_interceptor:
            return self.__callback_interceptor(callback)
        return callback

    def subscription_callback_set_intercept(self, interceptor):
        """Set a function to intercept all callbacks. This is useful to, for
        example, keep a thread barrier between the transport related functions
        and processing functions.
        :param interceptor: A function that takes the original callback function
                            and returns a modified callback function. Or None to
                            disable interception.
        """
        self.__callback_interceptor = interceptor

    def send(self, destination, message, **kwargs):
        """Send a message to a queue.
        :param destination: Queue name to send to
        :param message: Either a string or a serializable object to be sent
        :param **kwargs: Further parameters for the transport layer. For example
               delay: Delay transport of message by this many seconds
               headers: Optional dictionary of header entries
               expiration: Optional expiration time, relative to sending time
               transaction: Transaction ID if message should be part of a
                            transaction
        """
        message = self._mangle_for_sending(message)
        self._send(destination, message, **kwargs)

    def raw_send(self, destination, message, **kwargs):
        """Send a raw (unmangled) message to a queue.
        This may cause errors if the receiver expects a mangled message.
        :param destination: Queue name to send to
        :param message: Either a string or a serializable object to be sent
        :param **kwargs: Further parameters for the transport layer. For example
               delay: Delay transport of message by this many seconds
               headers: Optional dictionary of header entries
               expiration: Optional expiration time, relative to sending time
               transaction: Transaction ID if message should be part of a
                            transaction
        """
        self._send(destination, message, **kwargs)

    def broadcast(self, destination, message, **kwargs):
        """Broadcast a message.
        :param destination: Topic name to send to
        :param message: Either a string or a serializable object to be sent
        :param **kwargs: Further parameters for the transport layer. For example
               delay: Delay transport of message by this many seconds
               headers: Optional dictionary of header entries
               expiration: Optional expiration time, relative to sending time
               transaction: Transaction ID if message should be part of a
                            transaction
        """
        message = self._mangle_for_sending(message)
        self._broadcast(destination, message, **kwargs)

    def raw_broadcast(self, destination, message, **kwargs):
        """Broadcast a raw (unmangled) message.
        This may cause errors if the receiver expects a mangled message.
        :param destination: Topic name to send to
        :param message: Either a string or a serializable object to be sent
        :param **kwargs: Further parameters for the transport layer. For example
               delay: Delay transport of message by this many seconds
               headers: Optional dictionary of header entries
               expiration: Optional expiration time, relative to sending time
               transaction: Transaction ID if message should be part of a
                            transaction
        """
        self._broadcast(destination, message, **kwargs)

    def ack(self, message, subscription_id=None, **kwargs):
        """Acknowledge receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        :param message: ID of the message to be acknowledged, OR a dictionary
                        containing a field 'message-id'.
        :param subscription_id: ID of the associated subscription. Optional when
                                a dictionary is passed as first parameter and
                                that dictionary contains field 'subscription'.
        :param **kwargs: Further parameters for the transport layer. For example
               transaction: Transaction ID if acknowledgement should be part of
                            a transaction
        """
        if isinstance(message, dict):
            message_id = message.get("message-id")
            if not subscription_id:
                subscription_id = message.get("subscription")
        else:
            message_id = message
        if not message_id:
            raise workflows.Error("Cannot acknowledge message without " + "message ID")
        if not subscription_id:
            raise workflows.Error(
                "Cannot acknowledge message without " + "subscription ID"
            )
        self.log.debug(
            "Acknowledging message %s on subscription %s", message_id, subscription_id
        )
        self._ack(message_id, subscription_id, **kwargs)

    def nack(self, message, subscription_id=None, **kwargs):
        """Reject receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        :param message: ID of the message to be rejected, OR a dictionary
                        containing a field 'message-id'.
        :param subscription_id: ID of the associated subscription. Optional when
                                a dictionary is passed as first parameter and
                                that dictionary contains field 'subscription'.
        :param **kwargs: Further parameters for the transport layer. For example
               transaction: Transaction ID if rejection should be part of a
                            transaction
        """
        if isinstance(message, dict):
            message_id = message.get("message-id")
            if not subscription_id:
                subscription_id = message.get("subscription")
        else:
            message_id = message
        if not message_id:
            raise workflows.Error("Cannot reject message without " + "message ID")
        if not subscription_id:
            raise workflows.Error("Cannot reject message without " + "subscription ID")
        self.log.debug(
            "Rejecting message %s on subscription %s", message_id, subscription_id
        )
        self._nack(message_id, subscription_id, **kwargs)

    def transaction_begin(self, **kwargs):
        """Start a new transaction.
        :param **kwargs: Further parameters for the transport layer. For example
        :return: A transaction ID that can be passed to other functions.
        """
        self.__transaction_id += 1
        self.__transactions.add(self.__transaction_id)
        self.log.debug("Starting transaction with ID %d", self.__subscription_id)
        self._transaction_begin(self.__transaction_id, **kwargs)
        return self.__transaction_id

    def transaction_abort(self, transaction_id, **kwargs):
        """Abort a transaction and roll back all operations.
        :param transaction_id: ID of transaction to be aborted.
        :param **kwargs: Further parameters for the transport layer.
        """
        if transaction_id not in self.__transactions:
            raise workflows.Error("Attempting to abort unknown transaction")
        self.log.debug("Aborting transaction %s", transaction_id)
        self.__transactions.remove(transaction_id)
        self._transaction_abort(transaction_id, **kwargs)

    def transaction_commit(self, transaction_id, **kwargs):
        """Commit a transaction.
        :param transaction_id: ID of transaction to be committed.
        :param **kwargs: Further parameters for the transport layer.
        """
        if transaction_id not in self.__transactions:
            raise workflows.Error("Attempting to commit unknown transaction")
        self.log.debug("Committing transaction %s", transaction_id)
        self.__transactions.remove(transaction_id)
        self._transaction_commit(transaction_id, **kwargs)

    #
    # -- Low level communication calls to be implemented by subclass -----------
    #

    @staticmethod
    def _subscribe(sub_id, channel, callback, **kwargs):
        """Listen to a queue, notify via callback function.
        :param sub_id: ID for this subscription in the transport layer
        :param channel: Queue name to subscribe to
        :param callback: Function to be called when messages are received
        :param **kwargs: Further parameters for the transport layer. For example
               exclusive: Attempt to become exclusive subscriber to the queue.
               acknowledgement: If true receipt of each message needs to be
                                acknowledged.
        """
        raise NotImplementedError("Transport interface not implemented")

    @staticmethod
    def _subscribe_broadcast(sub_id, channel, callback, **kwargs):
        """Listen to a broadcast topic, notify via callback function.
        :param sub_id: ID for this subscription in the transport layer
        :param channel: Topic name to subscribe to
        :param callback: Function to be called when messages are received
        :param **kwargs: Further parameters for the transport layer. For example
               retroactive: Ask broker to send old messages if possible
        """
        raise NotImplementedError("Transport interface not implemented")

    @staticmethod
    def _unsubscribe(sub_id):
        """Stop listening to a queue or a broadcast
        :param sub_id: ID for this subscription in the transport layer
        """
        raise NotImplementedError("Transport interface not implemented")

    @staticmethod
    def _send(destination, message, **kwargs):
        """Send a message to a queue.
        :param destination: Queue name to send to
        :param message: A string to be sent
        :param **kwargs: Further parameters for the transport layer. For example
               headers: Optional dictionary of header entries
               expiration: Optional expiration time, relative to sending time
               transaction: Transaction ID if message should be part of a
                            transaction
        """
        raise NotImplementedError("Transport interface not implemented")

    @staticmethod
    def _broadcast(destination, message, **kwargs):
        """Broadcast a message.
        :param destination: Topic name to send to
        :param message: A string to be broadcast
        :param **kwargs: Further parameters for the transport layer. For example
               headers: Optional dictionary of header entries
               expiration: Optional expiration time, relative to sending time
               transaction: Transaction ID if message should be part of a
                            transaction
        """
        raise NotImplementedError("Transport interface not implemented")

    @staticmethod
    def _ack(message_id, subscription_id, **kwargs):
        """Acknowledge receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        :param message_id: ID of the message to be acknowledged.
        :param subscription_id: ID of the associated subscription.
        :param **kwargs: Further parameters for the transport layer. For example
               transaction: Transaction ID if acknowledgement should be part of
                            a transaction
        """
        raise NotImplementedError("Transport interface not implemented")

    @staticmethod
    def _nack(message_id, subscription_id, **kwargs):
        """Reject receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        :param message_id: ID of the message to be rejected.
        :param subscription_id: ID of the associated subscription.
        :param **kwargs: Further parameters for the transport layer. For example
               transaction: Transaction ID if rejection should be part of a
                            transaction
        """
        raise NotImplementedError("Transport interface not implemented")

    @staticmethod
    def _transaction_begin(transaction_id, **kwargs):
        """Start a new transaction.
        :param transaction_id: ID for this transaction in the transport layer.
        :param **kwargs: Further parameters for the transport layer.
        """
        raise NotImplementedError("Transport interface not implemented")

    @staticmethod
    def _transaction_abort(transaction_id, **kwargs):
        """Abort a transaction and roll back all operations.
        :param transaction_id: ID of transaction to be aborted.
        :param **kwargs: Further parameters for the transport layer.
        """
        raise NotImplementedError("Transport interface not implemented")

    @staticmethod
    def _transaction_commit(transaction_id, **kwargs):
        """Commit a transaction.
        :param transaction_id: ID of transaction to be committed.
        :param **kwargs: Further parameters for the transport layer.
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
        """Function that any message will pass through before it being forwarded to
        the actual _send* functions."""
        return message

    @staticmethod
    def _mangle_for_receiving(message):
        """Function that any message will pass through before it being forwarded to
        the receiving subscribed callback functions."""
        return message


def json_serializer(obj):
    """A helper function for JSON serialization, where it can be used as
    the default= argument. This function helps the serializer to translate
    objects that otherwise would not be understood. Note that this is
    one-way only - these objects are not restored on the receiving end."""

    if isinstance(obj, decimal.Decimal):
        # turn all Decimals into floats
        return float(obj)

    raise TypeError(repr(obj) + " is not JSON serializable")
