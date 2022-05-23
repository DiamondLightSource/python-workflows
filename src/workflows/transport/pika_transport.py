from __future__ import annotations

import configparser
import dataclasses
import functools
import json
import logging
import random
import sys
import threading
import time
import uuid
from concurrent.futures import Future
from enum import Enum, auto
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type, Union

import pika.exceptions
from bidict import bidict
from pika.adapters.blocking_connection import BlockingChannel

import workflows.util
from workflows.transport import middleware
from workflows.transport.common_transport import (
    CommonTransport,
    MessageCallback,
    json_serializer,
)

logger = logging.getLogger("workflows.transport.pika_transport")

# The form pika expects callbacks in
PikaCallback = Callable[
    [
        BlockingChannel,
        pika.spec.Basic.Deliver,
        pika.spec.BasicProperties,
        bytes,
    ],
    None,
]


class PikaTransport(CommonTransport):
    """Abstraction layer for messaging infrastructure for RabbitMQ via pika.

    Pika methods with both the BlockingConnection and SelectConnection adapters
    take control over the thread, and temporarily release it via callbacks.
    So to make pika work with the workflows model we need a thread that only
    runs pika.
    """

    # Set some sensible defaults
    defaults = {
        "--rabbit-host": "localhost",
        "--rabbit-port": 5672,
        "--rabbit-user": "guest",
        "--rabbit-pass": "guest",
        "--rabbit-vhost": "/",
    }

    # Effective configuration
    config: Dict[Any, Any] = {}

    def __init__(
        self, middleware: list[Type[middleware.BaseTransportMiddleware]] = None
    ):
        self._channel = None
        self._conn = None
        self._connected = False
        self._lock = threading.RLock()
        self._pika_thread: _PikaThread
        self._vhost = "/"
        super().__init__(middleware=middleware)

    def get_namespace(self) -> str:
        """Return the RabbitMQ virtual host"""
        if self._vhost.endswith("."):
            return self._vhost[:-1]
        return self._vhost

    @classmethod
    def load_configuration_file(cls, filename):
        cfgparser = configparser.ConfigParser(allow_no_value=True)
        if not cfgparser.read(filename):
            raise workflows.Error(
                "Could not read from configuration file %s" % filename
            )
        for cfgoption, target in [
            ("host", "--rabbit-host"),
            ("port", "--rabbit-port"),
            ("password", "--rabbit-pass"),
            ("username", "--rabbit-user"),
            ("vhost", "--rabbit-vhost"),
        ]:
            try:
                cls.defaults[target] = cfgparser.get("rabbit", cfgoption)
            except configparser.NoOptionError:
                pass

    @classmethod
    def add_command_line_options(cls, parser):
        """Function to inject command line parameters"""
        if "add_argument" in dir(parser):
            return cls.add_command_line_options_argparse(parser)
        else:
            return cls.add_command_line_options_optparse(parser)

    @classmethod
    def add_command_line_options_argparse(cls, argparser):
        """Function to inject command line parameters into
        a Python ArgumentParser."""
        import argparse

        class SetParameter(argparse.Action):
            """callback object for ArgumentParser"""

            def __call__(self, parser, namespace, value, option_string=None):
                cls.config[option_string] = value
                if option_string == "--rabbit-conf":
                    cls.load_configuration_file(value)

        argparser.add_argument(
            "--rabbit-host",
            metavar="HOST",
            default=cls.defaults.get("--rabbit-host"),
            help="RabbitMQ broker address, default '%(default)s'",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-port",
            metavar="PORT",
            default=cls.defaults.get("--rabbit-port"),
            help="RabbitMQ broker port, default '%(default)s",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-user",
            metavar="USER",
            default=cls.defaults.get("--rabbit-user"),
            help="RabbitMQ user, default '%(default)s'",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-pass",
            metavar="PASS",
            default=cls.defaults.get("--rabbit-pass"),
            help="RabbitMQ password",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-vhost",
            metavar="VHST",
            default=cls.defaults.get("--rabbit-vhost"),
            help="RabbitMQ virtual host, default '%(default)s'",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-conf",
            metavar="CNF",
            default=cls.defaults.get("--rabbit-conf"),
            help="Rabbit configuration file containing connection information, disables default values",
            type=str,
            action=SetParameter,
        )

    @classmethod
    def add_command_line_options_optparse(cls, optparser):
        """function to inject command line parameters into
        a Python OptionParser."""

        def set_parameter(option, opt, value, parser):
            """callback function for OptionParser"""
            cls.config[opt] = value
            if opt == "--rabbit-conf":
                cls.load_configuration_file(value)

        optparser.add_option(
            "--rabbit-host",
            metavar="HOST",
            default=cls.defaults.get("--rabbit-host"),
            help="RabbitMQ broker address, default '%default'",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-port",
            metavar="PORT",
            default=cls.defaults.get("--rabbit-port"),
            help="RabbitMQ broker port, default '%default'",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-user",
            metavar="USER",
            default=cls.defaults.get("--rabbit-user"),
            help="RabbitMQ user, default '%default'",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-pass",
            metavar="PASS",
            default=cls.defaults.get("--rabbit-pass"),
            help="RabbitMQ password",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-vhost",
            metavar="VHST",
            default=cls.defaults.get("--rabbit-vhost"),
            help="RabbitMQ virtual host, default '%default'",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-conf",
            metavar="CNF",
            default=cls.defaults.get("--rabbit-conf"),
            help="RabbitMQ configuration file containing connection information, disables default values",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )

    def _generate_connection_parameters(self) -> List[pika.ConnectionParameters]:
        username = self.config.get("--rabbit-user", self.defaults.get("--rabbit-user"))
        password = self.config.get("--rabbit-pass", self.defaults.get("--rabbit-pass"))
        credentials = pika.PlainCredentials(username, password)

        host_string = self.config.get(
            "--rabbit-host", self.defaults.get("--rabbit-host")
        )
        port_string = str(
            self.config.get("--rabbit-port", self.defaults.get("--rabbit-port"))
        )
        vhost = self.config.get("--rabbit-vhost", self.defaults.get("--rabbit-vhost"))
        if "," in host_string:
            host = host_string.split(",")
        else:
            host = [host_string]
        if "," in port_string:
            port = [int(p) for p in port_string.split(",")]
        else:
            port = [int(port_string)]
        if len(port) == 1:
            port = port * len(host)
        elif len(port) > len(host):
            raise workflows.Disconnected(
                "Invalid hostname/port specifications: cannot specify more ports than hosts"
            )
        elif len(port) < len(host):
            raise workflows.Disconnected(
                "Invalid hostname/port specifications: must either specify a single port, or one port per host"
            )
        connection_parameters = [
            pika.ConnectionParameters(
                host=h,
                port=p,
                virtual_host=vhost,
                credentials=credentials,
            )
            for h, p in zip(host, port)
        ]

        # Randomize connection order once to spread connection attempts equally across all servers
        random.shuffle(connection_parameters)
        return connection_parameters

    def connect(self) -> bool:
        """Ensure both connection and channel to the RabbitMQ server are open."""
        with self._lock:
            if not hasattr(self, "_pika_thread"):
                self._pika_thread = _PikaThread(self._generate_connection_parameters())
        try:
            self._pika_thread.start()
            self._pika_thread.wait_for_connection()
        except pika.exceptions.AMQPConnectionError as e:
            raise workflows.Disconnected(e)

        # This either succeeded or the entire connection failed irreversably
        return self._pika_thread.connection_alive

    def is_connected(self) -> bool:
        """Return connection status."""
        # TODO: Does this question even make sense with reconnection?
        #       Surely .connection_alive is (slightly) better?
        return hasattr(self, "_pika_thread") and self._pika_thread.connection_alive

    def disconnect(self):
        """Gracefully close connection to pika server"""
        self._pika_thread.join(stop=True)

    def broadcast_status(self, status):
        """Broadcast transient status information to all listeners"""

        # Basic status checks - this is based on behaviour of status_monitor
        # We must have: host, workflows version, status
        missing_fields = {"host", "workflows", "status"} - status.keys()
        if missing_fields:
            raise ValueError(
                f"Missing required field(s) for status broadcast: {', '.join(missing_fields)}"
            )
        self._broadcast("transient.status", json.dumps(status), expiration=15)

    def _call_message_callback(
        self,
        subscription_id: int,
        _channel: pika.channel.Channel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ):
        """Rewrite and redirect a pika callback to the subscription function"""
        merged_headers = dict(properties.headers)
        merged_headers.update(
            {
                "consumer_tag": str(method.consumer_tag),
                "delivery_mode": properties.delivery_mode,
                "exchange": method.exchange,
                "message-id": method.delivery_tag,
                "redelivered": method.redelivered,
                "routing_key": method.routing_key,
                "subscription": subscription_id,
            }
        )
        self.subscription_callback(subscription_id)(
            merged_headers,
            body,
        )

    def _subscribe(
        self,
        sub_id: int,
        channel: str,
        callback: MessageCallback,
        *,
        acknowledgement: bool = False,
        prefetch_count: int = 1,
        reconnectable: bool = False,
        **_kwargs,
    ):
        """
        Listen to a queue, notify via callback function.

        Waits until subscription is complete.

        Args:
            sub_id: Internal ID to keep track of this subscription.
            channel: Queue name to subscribe to
            callback: Function to be called when messages are received
            acknowledgement:
                Each message will need to be explicitly acknowledged.
                Not compatible with reconnectable.
            reconnectable:
                Allow automatic re-establishing of the subscription over
                a new connection, in case of connection failure. This
                cannot be used if acknowledgement is also set.
            prefetch_count:
                How many messages will be prefetched for the subscription.
                This makes little difference unless acknowledgement is
                also set.
        """
        if acknowledgement and reconnectable:
            raise ValueError(
                "Acknowledgements can't be reliably used on reconnectable connections"
            )

        # Technically this makes sense - we might want to do later. But not
        # for the initial version of the transport
        if not channel:
            raise NotImplementedError(
                "Although RabbitMQ allows anonymous subscriptions, not implemented in Transport yet"
            )

        try:
            return self._pika_thread.subscribe_queue(
                queue=channel,
                callback=functools.partial(self._call_message_callback, sub_id),
                auto_ack=not acknowledgement,
                subscription_id=sub_id,
                reconnectable=reconnectable,
                prefetch_count=prefetch_count,
            ).result()
        except (
            pika.exceptions.AMQPChannelError,
            pika.exceptions.AMQPConnectionError,
        ) as e:
            raise workflows.Disconnected(repr(e)) from None

    def _subscribe_broadcast(
        self,
        sub_id: int,
        channel: str,
        callback: MessageCallback,
        *,
        reconnectable: bool = False,
        **_kwargs,
    ):
        """
        Listen to a FANOUT exchange, notify via callback function.

        Waits until subscription is complete.

        Args:
            sub_id: Internal ID for this subscription
            channel: Name of the exchange to bind to
            callback: Function to be called when messages are received
            reconnectable:
                Can we reconnect to this exchange if the connection is
                lost. Currently, this means that messages can be missed
                if they are sent while we are reconnecting.
        """

        self._pika_thread.subscribe_broadcast(
            exchange=channel,
            callback=functools.partial(self._call_message_callback, sub_id),
            subscription_id=sub_id,
            reconnectable=reconnectable,
        ).result()

    def _subscribe_temporary(
        self,
        sub_id: int,
        channel_hint: Optional[str],
        callback: MessageCallback,
        *,
        acknowledgement: bool = False,
        **kwargs,
    ) -> str:
        """
        Create and then listen to a temporary queue, notify via callback function.

        Wait until subscription is complete.

        Args:
            sub_id: Internal ID for this subscription
            channel_hint: Name suggestion for the temporary queue
            callback: Function to be called when messages are received
            acknowledgement:
                Each message will need to be explicitly acknowledged.
        Returns:
            The name of the temporary queue
        """
        queue: str = channel_hint or ""
        if queue:
            if not queue.startswith("transient."):
                queue = "transient." + queue
            queue = queue + "." + str(uuid.uuid4())

        try:
            return self._pika_thread.subscribe_temporary(
                queue=queue,
                callback=functools.partial(self._call_message_callback, sub_id),
                auto_ack=not acknowledgement,
                subscription_id=sub_id,
            ).result()
        except (
            pika.exceptions.AMQPChannelError,
            pika.exceptions.AMQPConnectionError,
        ) as e:
            raise workflows.Disconnected(e)

    def _unsubscribe(self, sub_id: int, **kwargs):
        """Stop listening to a queue
        :param sub_id: Consumer Tag to cancel
        """
        self._pika_thread.unsubscribe(sub_id)
        # self._channel.basic_cancel(consumer_tag=consumer_tag, callback=None)
        # Callback reference is kept as further messages may already have been received

    def _send(
        self,
        destination,
        message,
        headers=None,
        delay=None,
        expiration=None,
        transaction: Optional[int] = None,
        exchange: Optional[str] = None,
        **kwargs,
    ):
        """
        Send a message to a queue.

        Args:
            destination: Queue name to send to
            message: A string or bytes to be sent
            headers: Further arbitrary headers to pass to pika
            delay: Delay transport of message by this many seconds
            expiration: Optional TTL expiration time, relative to sending time
            transaction: Transaction ID if message should be part of a transaction
        """
        if not headers:
            headers = {}

        if delay:
            headers["x-delay"] = int(1000 * delay)
            exchange = "delayed"
        else:
            exchange = exchange or ""

        properties = pika.BasicProperties(
            headers=headers,
            delivery_mode=2,
        )
        if expiration:
            properties.expiration = str(expiration * 1000)

        self._pika_thread.send(
            exchange=exchange,
            routing_key=str(destination),
            body=message,
            properties=properties,
            mandatory=True,
            transaction_id=transaction,
        ).result()

    def _broadcast(
        self,
        destination,
        message,
        headers=None,
        delay=None,
        expiration: Optional[int] = None,
        transaction: Optional[int] = None,
        **kwargs,
    ):
        """Send a message to a fanout exchange.

        Args:
            destination: Exchange to post to
            message: A string to be broadcast
            headers: Further arbitrary headers to pass to pika
            delay: Delay transport of message by this many seconds
            expiration: Optional TTL expiration time, in seconds, relative to sending time
            transaction: Transaction ID if message should be part of a transaction
            kwargs: Arbitrary arguments for other transports. Ignored.
        """
        assert delay is None, "Delay Not implemented"

        if not headers:
            headers = {}
        # if delay:
        #     headers["x-delay"] = 1000 * delay

        properties = pika.BasicProperties(
            headers=headers,
            delivery_mode=2,
        )
        if expiration is not None:
            properties.expiration = str(expiration * 1000)

        self._pika_thread.send(
            exchange=destination,
            routing_key="",
            body=message,
            properties=properties,
            mandatory=False,
            transaction_id=transaction,
        ).result()

    def _transaction_begin(
        self, transaction_id: int, *, subscription_id: Optional[int] = None, **kwargs
    ) -> None:
        """Start a new transaction.
        :param transaction_id: ID for this transaction in the transport layer.
        :param subscription_id: Tie the transaction to a specific channel containing this subscription.
        """
        self._pika_thread.tx_select(transaction_id, subscription_id).result()

    def _transaction_abort(self, transaction_id: int, **kwargs) -> None:
        """Abort a transaction and roll back all operations.
        :param transaction_id: ID of transaction to be aborted.
        """
        self._pika_thread.tx_rollback(transaction_id).result()

    def _transaction_commit(self, transaction_id: int, **kwargs) -> None:
        """Commit a transaction.
        :param transaction_id: ID of transaction to be committed.
        """
        self._pika_thread.tx_commit(transaction_id).result()

    def _ack(
        self, message_id, subscription_id: int, *, multiple: bool = False, **_kwargs
    ):
        """
        Acknowledge receipt of a message.

        This only makes sense when the 'acknowledgement' flag was set
        for the relevant subscription.

        Args:
            message_id: ID of the message to be acknowledged
            subscription_id:
                Internal id for the subscription this message came from
            multiple: Should multiple messages be acknowledged?

        :param **kwargs: Further parameters for the transport layer.
        """
        self._pika_thread.ack(
            message_id,
            subscription_id,
            multiple=multiple,
            transaction_id=_kwargs.get("transaction"),
        )

    def _nack(
        self,
        message_id,
        subscription_id: int,
        *,
        multiple: bool = False,
        requeue: bool = True,
        **_kwargs,
    ):
        """
        Reject receipt of a message.

        This only makes sense when the 'acknowledgement' flag was set
        for the relevant subscription.

        Args:
            message_id: ID of the message to be rejected
            subscription_id:
                Internal id for the subscription this message came from
            multiple: Nack multiple messages. See AMQP basic.nack
            requeue: Attempt to requeue. see AMQP basic.nack.
        """
        self._pika_thread.nack(
            message_id,
            subscription_id,
            multiple=multiple,
            requeue=requeue,
            transaction_id=_kwargs.get("transaction"),
        )

    @staticmethod
    def _mangle_for_sending(message):
        """Function that any message will pass through before it being forwarded to
        the actual _send* functions.
        Pika only deals with serialized strings, so serialize message as json.
        """
        return json.dumps(message, default=json_serializer)

    @staticmethod
    def _mangle_for_receiving(message):
        """Function that any message will pass through before it being forwarded to
        the receiving subscribed callback functions.
        This transport class only deals with serialized strings, so decode
        message from json. However anything can come into here, so catch any
        deserialization errors.
        """
        try:
            return json.loads(message)
        except (TypeError, ValueError):
            return message


class _PikaThreadStatus(Enum):
    NEW = auto()  # The thread has not started connecting yet
    CONNECTING = auto()
    CONNECTED = auto()
    STOPPING = auto()
    STOPPED = auto()

    @property
    def is_new(self):
        return self is self.NEW

    @property
    def is_end_of_life(self):
        return self in {self.STOPPING, self.STOPPED}


class _PikaSubscriptionKind(Enum):
    """
    Represents a kind of subscription, for reconnection

    Attributes:
        DIRECT: A subscription to a named exchange and queue.
        FANOUT: A subscription to a fanout exchange. An exclusive queue is created.
    """

    DIRECT = auto()
    FANOUT = auto()


@dataclasses.dataclass
class _PikaSubscription:
    """
    Persistently represent a subscription

    Attributes:
        arguments: Any extra arguments to pass through to pika
        auto_ack: Should this subscription auto-acknowledge messages?
        destination:
            The target for subscription. This is the exchange name if
            subscribing to broadcasts with an ephemeral queue, or the
            queue name if subscribing to a queue directly.
        kind: What type of subscription this is.
        on_message_callback: The function called by Pika on messages
        prefetch_count: How many messages are we allowed to prefetch
        queue:
            The queue this subscription is actually physically bound to
        reconnectable: Are we allowed to reconnect to this subscription
    """

    arguments: Dict[str, Any]
    auto_ack: bool
    destination: str
    kind: _PikaSubscriptionKind
    on_message_callback: PikaCallback = dataclasses.field(repr=False)
    prefetch_count: int
    queue: Optional[str] = dataclasses.field(init=False, default=None)
    reconnectable: bool


class _PikaThread(threading.Thread):
    """
    Asynchronously handle a pika connection

    This will automatically reconnect (if appropriate), and manage
    persistent subscriptions across reconnections.

    Args:
        connection_parameters:
            How to connect to the AMPQ server. This is an iterable of
            possible connections, which will be attempted in a random
            order.
        reconnection_attempts:
            How many times to consecutively attempt connection before
            giving up. Connecting successfully (and reattaching any
            reconnectable queues) resets this counter. Successive
            attempts will wait longer before reattempting.
    """

    def __init__(
        self,
        connection_parameters: Iterable[pika.ConnectionParameters],
        reconnection_attempts=5,
    ):
        super().__init__(name="workflows pika_transport", daemon=True, target=self._run)
        self._state: _PikaThreadStatus = _PikaThreadStatus.NEW
        # Internal store of subscriptions, to resubscribe if necessary. Keys are
        # unique and auto-generated, and known as subscription IDs or consumer tags
        # (strictly: pika/AMQP consumer tags are strings, not integers)
        self._subscriptions: Dict[int, _PikaSubscription] = {}
        # The pika connection object
        self._connection: Optional[pika.BlockingConnection] = None
        # Index of per-subscription channels.
        self._pika_channels: bidict[int, BlockingChannel] = bidict()
        # Bidirectional index of all ongoing transactions. May include the shared channel
        self._transaction_on_channel: bidict[BlockingChannel, int] = bidict()
        # Information on whether a channel has uncommitted messages
        self._channel_has_active_tx: Dict[BlockingChannel, bool] = {}
        # Information on whether a channel is transactional
        self._channel_is_transactional: Dict[BlockingChannel, bool] = {}
        # A common, shared channel, used for sending messages outside of transactions.
        self._pika_shared_channel: Optional[BlockingChannel]
        # Are we allowed to reconnect. Can only be turned off, never on
        self._reconnection_allowed: bool = True
        # Our list of connection parameters, so we know where to connect to
        self._connection_parameters = list(connection_parameters)
        # If we failed with an unexpected exception
        self._exc_info: Optional[Tuple[Any, Any, Any]] = None
        self._reconnection_attempt_limit = reconnection_attempts
        # General bookkeeping events

        self._started: threading.Event

        # When set, requests the main loop to stop when convenient
        self._please_stop = threading.Event()
        # Have we had an initial connection - after this, we assume reconnection or failure
        # This is also set on complete failure, to prevent clients blocking forever
        self._connected = threading.Event()

        # Internal list of all live future objects that have been returned to
        # external callers, and that have not been resolved yet.
        self._future_tracker: dict[Future[None], Callable[[], None]] = {}

    @property
    def state(self) -> _PikaThreadStatus:
        """Read the current connection state"""
        return self._state

    def stop(self):
        """
        Request termination, including disconnection and cleanup if necessary.

        This will not block. Please call join() to wait until the thread
        has terminated.
        """
        # Wanted to check this hadn't happened yet - but hard to do
        # without race conditions in a thread-safe way. Check if we
        # already failed to stop properly.
        if not self.is_alive() and self._state not in (
            _PikaThreadStatus.NEW,
            _PikaThreadStatus.STOPPED,
        ):
            raise RuntimeError(
                "abnormal shutdown; pika thread has terminated, but not marked itself as stopped"
            )

        self._please_stop.set()
        # We might be waiting for an event, so give the event loop one...
        # We might already be closed or shutting down, so ignore errors for that
        try:
            if self._connection:
                self._connection.add_callback_threadsafe(lambda: None)
        except pika.exceptions.ConnectionWrongStateError:
            pass

    def join(
        self, timeout: Optional[float] = None, *, re_raise: bool = False, stop=False
    ):
        """Wait until the thread terminates.

        Args:
            timeout:
                When not None, a floating point number specifying a
                timeout for the operation in seconds (or fractions thereof).
            re_raise:
                If the thread terminated due to an exception, then raise
                this exception in the callers thread. Equivalent to calling
                'raise_if_exception' after 'join()'.
            stop:
                Call .stop() to request termination.
        """
        if stop:
            self.stop()
        super().join(timeout)
        for f in list(self._future_tracker):
            f.set_exception(workflows.Disconnected())
        if re_raise:
            self.raise_if_exception()

    def wait_for_connection(self, timeout=None):
        """
        Safely wait until the thread has connected and is communicating with the server.

        When the timeout argument is present and not None, it should be
        a floating point number specifying a timeout for the operation
        in seconds (or fractions thereof).
        """
        if not self.connection_alive:
            raise RuntimeError("Connection stopped or failed")
        self._connected.wait(timeout)
        self.raise_if_exception()

    def raise_if_exception(self):
        """If the thread has failed with an exception, raise it in the callers thread."""
        exception = self._exc_info
        if exception:
            raise exception[0] from exception[0].with_traceback(
                exception[1], exception[2]
            )

    def subscribe_queue(
        self,
        queue: str,
        callback: PikaCallback,
        subscription_id: int,
        *,
        auto_ack: bool = True,
        prefetch_count: int = 1,
        reconnectable: bool = False,
    ) -> Future[None]:
        """
        Subscribe to a queue. Thread-safe.

        Args:
            queue: The queue to listen for messages on
            callback: The function to call when receiving messages on this queue
            subscription_id: Internal ID representing this subscription.
            auto_ack: Should this subscription auto-acknowledge messages?
            prefetch_count: How many messages are we allowed to prefetch
            reconnectable: Are we allowed to reconnect to this subscription

        Returns:
            A Future representing the subscription state. It will be set
            to an empty value upon subscription success.
        """

        if not self._connection:
            raise RuntimeError("Cannot subscribe to unstarted connection")

        new_sub = _PikaSubscription(
            arguments={},
            auto_ack=auto_ack,
            destination=queue,
            kind=_PikaSubscriptionKind.DIRECT,
            on_message_callback=callback,
            prefetch_count=prefetch_count,
            reconnectable=reconnectable,
        )
        result: Future[None] = Future()
        self._register_future_and_callback(
            result,
            functools.partial(
                self._add_subscription_in_thread, subscription_id, new_sub, result
            ),
        )
        return result

    def subscribe_broadcast(
        self,
        exchange: str,
        callback: PikaCallback,
        subscription_id: int,
        *,
        auto_ack: bool = True,
        reconnectable: bool = False,
        prefetch_count: int = 0,
    ) -> Future[None]:
        """
        Subscribe to a broadcast exchange. Thread-safe.

        Args:
            exchange: The queue to listen for messages on
            callback: The function to call when receiving messages on this queue
            auto_ack: Should this subscription auto-acknowledge messages?
            subscription_id: Internal ID representing this subscription.
            prefetch_count: How many messages are we allowed to prefetch
            reconnectable:
                Are we allowed to reconnect to this subscription?
                **Warning**: Reconnecting to broadcast exchanges has the
                    potential for dropping messages sent while disconnected.

        Returns:
            A Future representing the subscription state. It will be set
            to an empty value upon subscription success.
        """
        if not self._connection:
            raise RuntimeError("Cannot subscribe to unstarted connection")

        new_sub = _PikaSubscription(
            arguments={},
            auto_ack=auto_ack,
            destination=exchange,
            kind=_PikaSubscriptionKind.FANOUT,
            on_message_callback=callback,
            prefetch_count=prefetch_count,
            reconnectable=reconnectable,
        )
        result: Future[None] = Future()
        self._register_future_and_callback(
            result,
            functools.partial(
                self._add_subscription_in_thread, subscription_id, new_sub, result
            ),
        )
        return result

    def subscribe_temporary(
        self,
        queue: str,
        callback: PikaCallback,
        subscription_id: int,
        *,
        auto_ack: bool = True,
    ) -> Future[str]:
        """
        Create and then subscribe to a temporary queue. Thread-safe.

        Args:
            queue: The queue to listen for messages on, may be empty
            callback: The function to call when receiving messages on this queue
            subscription_id: Internal ID representing this subscription.
            auto_ack: Should this subscription auto-acknowledge messages?

        Returns:
            A Future representing the resulting queue name.
            It will be set upon subscription success.
        """

        if not self._connection:
            raise RuntimeError("Cannot subscribe to unstarted connection")

        result: Future[str] = Future()

        def _declare_subscribe_queue_in_thread():
            try:
                if result.set_running_or_notify_cancel():
                    assert (
                        subscription_id not in self._subscriptions
                    ), f"Subscription request {subscription_id} rejected due to existing subscription {self._subscriptions[subscription_id]}"
                    temporary_queue_name = (
                        self._get_shared_channel()
                        .queue_declare(
                            queue, auto_delete=True, exclusive=True, durable=False
                        )
                        .method.queue
                    )
                    temporary_subscription = _PikaSubscription(
                        arguments={},
                        auto_ack=auto_ack,
                        destination=temporary_queue_name,
                        kind=_PikaSubscriptionKind.DIRECT,
                        on_message_callback=callback,
                        prefetch_count=0,
                        reconnectable=False,
                    )
                    self._add_subscription(subscription_id, temporary_subscription)
                    result.set_result(temporary_queue_name)
            except BaseException as e:
                result.set_exception(e)
                raise

        self._register_future_and_callback(result, _declare_subscribe_queue_in_thread)

        return result

    def unsubscribe(self, subscription_id: int) -> Future[None]:
        if subscription_id not in self._subscriptions:
            raise KeyError(
                f"No subscription with ID {subscription_id} to unsubscribe from"
            )

        assert self._connection is not None

        result: Future[None] = Future()

        def _unsubscribe():
            try:
                if result.set_running_or_notify_cancel():
                    logger.debug("Unsubscribing from subscription %d", subscription_id)
                    del self._subscriptions[subscription_id]
                    channel = self._pika_channels.pop(subscription_id)
                    channel.basic_cancel(str(subscription_id))

                    # Close the channel if nobody else is using it
                    if channel not in self._pika_channels.values():
                        logger.debug("Closing channel that is now unused")
                        channel.close()

                        # Forget about any ongoing transactions on the channel
                        self._transaction_on_channel.pop(channel, None)

                    result.set_result(None)
            except BaseException as e:
                result.set_exception(e)

        self._register_future_and_callback(result, _unsubscribe)

        return result

    def send(
        self,
        exchange: str,
        routing_key: str,
        body: Union[str, bytes],
        properties: pika.spec.BasicProperties = None,
        mandatory: bool = True,
        transaction_id: Optional[int] = None,
    ) -> Future[None]:
        """Send a message. Thread-safe."""

        if not self._connection:
            raise RuntimeError("Cannot send on unstarted connection")

        future: Future[None] = Future()

        def _send():
            if future.set_running_or_notify_cancel():
                try:
                    if transaction_id:
                        channel = self._transaction_on_channel.inverse[transaction_id]
                        self._channel_has_active_tx[channel] = True
                    else:
                        channel = self._get_shared_channel()
                    channel.basic_publish(
                        exchange=exchange,
                        routing_key=routing_key,
                        body=body,
                        properties=properties,
                        mandatory=mandatory,
                    )
                    future.set_result(None)
                except BaseException as e:
                    future.set_exception(e)
                    raise

        self._register_future_and_callback(future, _send)
        return future

    def ack(
        self,
        delivery_tag: int,
        subscription_id: int,
        *,
        multiple=False,
        transaction_id: Optional[int],
    ):
        if subscription_id not in self._subscriptions:
            raise KeyError(f"Could not find subscription {subscription_id} to ACK")

        channel = self._pika_channels[subscription_id]

        assert self._connection is not None

        # Check if channel is in tx mode
        transaction = self._transaction_on_channel.get(channel)
        if transaction_id != transaction:
            raise workflows.Error(
                "Transaction state mismatch. "
                f"Call assumes transaction {transaction_id}, channel has transaction {transaction}"
            )
        if transaction_id is None and not self._channel_has_active_tx.get(channel):

            def _ack_callback():
                channel.basic_ack(delivery_tag, multiple=multiple)
                if self._channel_is_transactional.get(channel):
                    channel.tx_commit()

            self._connection.add_callback_threadsafe(_ack_callback)
        else:
            # Matching transaction IDs - perfect
            # We are in a transaction so make a note that it is now being used
            self._channel_has_active_tx[channel] = True
            self._connection.add_callback_threadsafe(
                lambda: channel.basic_ack(delivery_tag, multiple=multiple)
            )

    def nack(
        self,
        delivery_tag: int,
        subscription_id: int,
        *,
        multiple=False,
        requeue=True,
        transaction_id: Optional[int],
    ):
        if subscription_id not in self._subscriptions:
            raise KeyError(f"Could not find subscription {subscription_id} to NACK")

        channel = self._pika_channels[subscription_id]

        assert self._connection is not None

        # Check if channel is in tx mode
        transaction = self._transaction_on_channel.get(channel)
        if transaction_id != transaction:
            raise workflows.Error(
                "Transaction state mismatch. "
                f"Call assumes transaction {transaction_id}, channel has transaction {transaction}"
            )
        if transaction_id is None and not self._channel_has_active_tx.get(channel):

            def _nack_callback():
                channel.basic_nack(delivery_tag, multiple=multiple, requeue=requeue)
                if self._channel_is_transactional.get(channel):
                    channel.tx_commit()

            self._connection.add_callback_threadsafe(_nack_callback)
        else:
            # Matching transaction IDs - perfect
            # We are in a transaction so make a note that it is now being used
            self._channel_has_active_tx[channel] = True
            self._connection.add_callback_threadsafe(
                lambda: channel.basic_nack(
                    delivery_tag, multiple=multiple, requeue=requeue
                )
            )

    def tx_select(
        self, transaction_id: int, subscription_id: Optional[int]
    ) -> Future[None]:
        """Set a channel to transaction mode. Thread-safe.
        :param transaction_id: ID for this transaction in the transport layer.
        :param subscription_id: Tie the transaction to a specific channel containing this subscription.
        """

        if not self._connection:
            raise RuntimeError("Cannot transact on unstarted connection")

        future: Future[None] = Future()

        def _tx_select():
            if future.set_running_or_notify_cancel():
                try:
                    if subscription_id:
                        if subscription_id not in self._pika_channels:
                            raise KeyError(
                                f"Could not find subscription {subscription_id} to begin transaction"
                            )
                        channel = self._pika_channels[subscription_id]
                        if self._subscriptions[subscription_id].reconnectable:
                            raise KeyError(
                                f"Subscription {subscription_id} on channel {channel} is reconnectable; "
                                "Transactions are unsupported on reconnectable channels"
                            )
                    else:
                        channel = self._get_shared_channel()
                    if channel in self._transaction_on_channel:
                        raise KeyError(
                            f"Channel {channel} is already running transaction {self._transaction_on_channel[channel]}, so can't start transaction {transaction_id}"
                        )
                    channel.tx_select()
                    self._transaction_on_channel[channel] = transaction_id
                    self._channel_has_active_tx.setdefault(channel, False)
                    self._channel_is_transactional[channel] = True

                    future.set_result(None)
                except BaseException as e:
                    future.set_exception(e)
                    raise

        self._register_future_and_callback(future, _tx_select)
        return future

    def tx_rollback(self, transaction_id: int) -> Future[None]:
        """Abort a transaction and roll back all operations. Thread-safe.
        :param transaction_id: ID of transaction to be aborted.
        """
        if not self._connection:
            raise RuntimeError("Cannot transact on unstarted connection")

        future: Future[None] = Future()

        def _tx_rollback():
            if future.set_running_or_notify_cancel():
                try:
                    channel = self._transaction_on_channel.inverse.pop(
                        transaction_id, None
                    )
                    if not channel:
                        raise KeyError(
                            f"Could not find transaction {transaction_id} to roll back"
                        )
                    channel.tx_rollback()
                    self._channel_has_active_tx.pop(channel, None)
                    future.set_result(None)
                except BaseException as e:
                    future.set_exception(e)
                    raise

        self._register_future_and_callback(future, _tx_rollback)
        return future

    def tx_commit(self, transaction_id: int) -> Future[None]:
        """Commit a transaction.
        :param transaction_id: ID of transaction to be committed. Thread-safe..
        """
        if not self._connection:
            raise RuntimeError("Cannot transact on unstarted connection")

        future: Future[None] = Future()

        def _tx_commit():
            if future.set_running_or_notify_cancel():
                try:
                    channel = self._transaction_on_channel.inverse.pop(
                        transaction_id, None
                    )
                    if not channel:
                        raise KeyError(
                            f"Could not find transaction {transaction_id} to commit"
                        )
                    channel.tx_commit()
                    self._channel_has_active_tx.pop(channel, None)
                    future.set_result(None)
                except BaseException as e:
                    future.set_exception(e)
                    raise

        self._register_future_and_callback(future, _tx_commit)
        return future

    @property
    def connection_alive(self) -> bool:
        """
        Is the connection object connected, or in the process of reconnecting?

        This will return True if the object is connected right now, or there is
        a potential that this object will be reconnected in the future, even if
        the connection is lost at this point.
        """
        return self._started.is_set() and self._state not in (
            _PikaThreadStatus.STOPPED,
            _PikaThreadStatus.STOPPING,
        )

    # NOTE: With reconnection lifecycle this probably doesn't make sense
    #       on its own. It might make sense to add this returning a
    #       connection-specific 'token' - presumably the user might want
    #       to ensure that a connection is still the same connection
    #       and thus adhering to various within-connection guarantees.
    #       Remove this until we understand where it might be necessary.
    # @property
    # def connected(self):
    #     """
    #     Has a connection been successfully started at least once, and isn't in the process of shutting down.

    #     Note that this might have changed by the time that you make a
    #     decision based on this property.
    #     """
    #     return self.connection_alive and self._connected.is_set()

    ####################################################################
    # PikaThread Internal methods

    def _debug_close_connection(self):
        self._connection.add_callback_threadsafe(lambda: self._connection.close())

    def _get_shared_channel(self) -> BlockingChannel:
        """Get the shared (no prefetch) channel. Create if necessary."""

        assert self._connection is not None

        if not self._pika_shared_channel:
            self._pika_shared_channel = self._connection.channel()
            ##### self._pika_shared_channel.confirm_delivery()
        return self._pika_shared_channel

    def _recreate_subscriptions(self):
        """Resubscribe all existing subscriptions"""
        old_subscriptions = self._subscriptions
        self._subscriptions = {}

        logger.debug("Setting up %d subscriptions", len(old_subscriptions))
        try:
            for subscription_id, subscription in old_subscriptions.items():
                self._add_subscription(subscription_id, subscription)
        except BaseException:
            # If something goes (temporarily) wrong recreating, then we
            # don't want to only partially resubscribe next time
            self._subscriptions = old_subscriptions
            raise

        logger.debug(
            f"Subscriptions recreated. Reconnections allowed? - {'Yes' if self._reconnection_allowed else 'No.'}"
        )

    def _add_subscription(self, subscription_id: int, subscription: _PikaSubscription):
        assert self._connection is not None
        assert subscription_id not in self._subscriptions

        # We flip reconnection to False if any subscription is not reconnectable
        if self._reconnection_allowed and not subscription.reconnectable:
            self._reconnection_allowed = False
            logger.debug(
                f"Subscription {subscription_id} to '{subscription.destination}' is not reconnectable. Turning reconnection off."
            )

        # Open a dedicated channel for this subscription
        channel = self._connection.channel()
        channel.basic_qos(prefetch_count=subscription.prefetch_count)

        if subscription.kind is _PikaSubscriptionKind.FANOUT:
            # If a FANOUT subscription, then we need to create and bind
            # a temporary queue to receive messages from the exchange
            queue = channel.queue_declare("", exclusive=True).method.queue
            assert queue is not None
            channel.queue_bind(queue, subscription.destination)
            subscription.queue = queue
        elif subscription.kind is _PikaSubscriptionKind.DIRECT:
            subscription.queue = subscription.destination
        else:
            raise NotImplementedError(f"Unknown subscription kind: {subscription.kind}")
        channel.basic_consume(
            subscription.queue,
            subscription.on_message_callback,
            auto_ack=subscription.auto_ack,
            consumer_tag=str(subscription_id),
        )
        # Only now we have subscribed successfully, add to the list
        self._pika_channels[subscription_id] = channel
        self._subscriptions[subscription_id] = subscription
        logger.debug("Consuming (%d) on %s", subscription_id, subscription.queue)

    def _run(self):
        if self._please_stop.is_set():
            # stop() was called before start()... so quit
            self._state == _PikaThreadStatus.STOPPED
            return
        assert self._state == _PikaThreadStatus.NEW
        assert self._reconnection_allowed, "Should be true until first synchronize"
        logger.debug("pika thread starting")
        connection_counter = 0
        connection_attempts_since_last_connection = 0

        # Loop until we either can't, or are asked not to
        while (
            connection_counter == 0 or self._reconnection_allowed
        ) and not self._please_stop.is_set():
            try:
                # If we've reconnecting to the limit, give up
                if (
                    self._reconnection_attempt_limit
                    and connection_attempts_since_last_connection
                    > self._reconnection_attempt_limit
                ):
                    logger.error(
                        "Failed to establish connection after %d attempts; giving up"
                    )
                    break
                # If we are hitting repeat reconnections without success, sleep for a bit and hope
                # that the server comes back
                if connection_attempts_since_last_connection > 0:
                    sleep_time = 2 ** (connection_attempts_since_last_connection - 1)
                    logger.info(
                        "Failed to connect on attempt %d - sleeping %ds",
                        connection_attempts_since_last_connection,
                        sleep_time,
                    )
                    time.sleep(sleep_time)

                connection_attempts_since_last_connection += 1
                if connection_counter == 0:
                    logger.debug("Connecting to AMPQ")
                else:
                    logger.debug(f"Reconnecting to AMPQ #{connection_counter}")
                # Clear any exceptions from past connections
                self._exc_info = None
                self._state = _PikaThreadStatus.CONNECTING

                self._connection = pika.BlockingConnection(self._connection_parameters)
                logger.debug(f"Connection #{connection_counter} connected")
                connection_counter += 1

                # Clear the channels because this might be a reconnect
                self._pika_channels = {}
                self._pika_shared_channel = None
                self._transaction_on_channel = bidict()
                self._channel_has_active_tx = {}
                self._channel_is_transactional = {}

                # [Re]create subscriptions
                self._recreate_subscriptions()

                # Resubmit any outstanding future-connected callbacks
                for f, callback in self._future_tracker.items():
                    assert not f.done(), f"Future {f} is already completed"
                    self._connection.add_callback_threadsafe(callback)

                # We set up here because we don't want to class as CONNECTED
                # until all requested subscriptions have been actioned.
                self._state = _PikaThreadStatus.CONNECTED
                self._connected.set()
                # Since we are properly reconnected, reset the fail count
                connection_attempts_since_last_connection = 0
                logger.debug("Waiting for events")

                # Run until we are asked to stop, or fail
                while not self._please_stop.is_set():
                    self._connection.process_data_events(None)
            except pika.exceptions.ConnectionClosed:
                self._exc_info = sys.exc_info()
                if self._please_stop.is_set():
                    logger.info("Connection closed on request")
                else:
                    logger.error("Connection closed unexpectedly")
            except pika.exceptions.ChannelClosed as e:
                logger.error("Channel closed: %r", e)
                self._exc_info = sys.exc_info()
            except pika.exceptions.ConnectionWrongStateError:
                logger.debug(
                    "Got ConnectionWrongStateError - connection closed by other means?"
                )
                self._exc_info = sys.exc_info()
            except pika.exceptions.AMQPConnectionError as e:
                self._exc_info = sys.exc_info()
                # Connection failed. Are we the first?
                if connection_counter == 0:
                    logger.error(f"Initial connection failed: {e!r}")
                    break
                logger.error(f"Connection {connection_counter} failed: {e!r}")
            except BaseException as e:
                logger.error(f"Connection failed for unknown reason: {e!r}")
                self._exc_info = sys.exc_info()
                break
            # Make sure our connection is closed before reconnecting
            if not self._connection.is_closed:
                logger.info("Connection not closed. Closing.")
                self._connection.close()

        logger.debug(f"Shutting down thread. Requested? {self._please_stop.is_set()}")
        # We're shutting down. Do this cleanly.
        self._state = _PikaThreadStatus.STOPPING

        # Make sure the connection is closed
        if self._connection and not self._connection.is_closed:
            self._connection.close()

        for f in list(self._future_tracker):
            f.set_exception(workflows.Disconnected())

        self._state = _PikaThreadStatus.STOPPED
        self._please_stop.set()

        # We might not have successfully connected - but we might be waiting
        # on a connection somewhere. So now we've actually marked ourselves
        # as disconnected, mark all other outstanding events as "complete"
        # to resolve deadlocks.
        self._connected.set()

        logger.debug("Leaving PikaThread runloop")

    def _register_future_and_callback(
        self, future: Future[Any], callback: Callable[[], None]
    ) -> None:
        """
        Queue a callback with the connection, and record its future.

        Recording the future allows us to keep track of them even
        following an unexpected connection loss, and either send them
        upon reconnection or mark them as failed.
        """
        assert self._connection is not None
        assert future not in self._future_tracker, "Futures must be unique"
        self._future_tracker[future] = callback
        future.add_done_callback(self._future_tracker.pop)
        self._connection.add_callback_threadsafe(callback)

    def _add_subscription_in_thread(
        self,
        subscription_id: int,
        subscription: _PikaSubscription,
        result: Future,
    ):
        """
        Add a subscription to the pika connection.

        Will be called on the connection thread.
        """
        try:
            if result.set_running_or_notify_cancel():
                assert (
                    subscription_id not in self._subscriptions
                ), f"Subscription request {subscription_id} rejected due to existing subscription {self._subscriptions[subscription_id]}"
                self._add_subscription(subscription_id, subscription)
                result.set_result(self._subscriptions[subscription_id].queue)
        except BaseException as e:
            result.set_exception(e)
            raise
