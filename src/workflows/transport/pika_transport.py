import collections
import configparser
import dataclasses
import functools
import itertools
import json
import logging
import random
import threading
import time
from enum import Enum, auto
from typing import Any, Callable, Dict, Iterable, List, Union, Optional, Tuple
import concurrent.futures
import sys

import pika
from pika.adapters.utils import connection_workflow

import workflows
from workflows.transport.common_transport import CommonTransport, json_serializer

from collections.abc import Hashable, Mapping, Container

logger = logging.getLogger("workflows.transport.pika_transport")

# The form of callback used by this library
MessageCallback = Callable[[Mapping[str, Any], Any], None]
# The form pika expects callbacks in
PikaCallback = Callable[
    [pika.channel.Channel, pika.spec.Basic.Return, pika.spec.BasicProperties, bytes],
    None,
]


def _rewrite_callback_to_pika(callback: MessageCallback) -> PikaCallback:
    """
    Transform a callback function into the right form for pika.

    This involves unwrapping the pika headers and putting them into a
    general headers dictionaly for the workflows.transport callback.
    """
    return lambda _channel, method, properties, body: callback(
        {
            "consumer_tag": method.consumer_tag,
            "delivery_mode": properties.delivery_mode,
            "exchange": method.exchange,
            "headers": properties.headers,
            "message-id": method.delivery_tag,
            "redelivered": method.redelivered,
            "routing_key": method.routing_key,
        },
        body,
    )


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

    def __init__(self):
        self._channel = None
        self._conn = None
        self._connected = False
        self._lock = threading.RLock()
        self._pika_thread = None
        self._vhost = "/"

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
            if not self._pika_thread:
                self._pika_thread = _PikaThread(self._generate_connection_parameters())
                self._pika_thread.start()
            self._pika_thread.wait_for_connection()
        return self._pika_thread.connected

    def is_connected(self) -> bool:
        """Return connection status."""
        return self._pika_thread and self._pika_thread.connected

    def _reconnect(self):
        return
        """An internal function that ensures a connection is up and running,
        but it will not force a reconnection when the connection state could not
        be restored. This will be the case if any subscription has been set up."""
        if not self._reconnection_allowed:
            raise workflows.Disconnected("Reconnection is disallowed")
        with self._lock:
            if self._connected:
                return True
            self._generate_connection_parameters()
            failed_attempts = []
            for connection_attempt in range(self._reconnection_attempts):
                parameters = next(self._connection_parameters)
                logger.debug(
                    "Attempting connection %s (%d failed previous attempts)",
                    parameters,
                    connection_attempt,
                )
                try:
                    self._conn = pika.BlockingConnection(parameters)
                except pika.exceptions.AMQPConnectionError:
                    logger.debug(
                        "Could not initiate connection to RabbitMQ server at %s",
                        parameters,
                    )
                    failed_attempts.append(("Connection Error", parameters))
                    continue
                try:
                    self._channel = self._conn.channel()
                    self._channel.confirm_delivery()
                except pika.exceptions.AMQPChannelError:
                    self.disconnect()
                    logger.debug(
                        "Channel error while connecting to RabbitMQ server at %s",
                        parameters,
                    )
                    failed_attempts.append(("Channel Error", parameters))
                    continue
                self._connected = True
                break
            else:
                raise workflows.Disconnected(
                    "Could not initiate connection to RabbitMQ server after %d failed attempts.\n%s",
                    len(failed_attempts),
                    "\n".join(f"{e[1]}: {e[0]}" for e in failed_attempts),
                )
        return True

    def disconnect(self):
        """Gracefully close connection to pika server"""
        with self._lock:
            if not self._pika_thread:
                return
            self._pika_thread.stop()
            self._pika_thread.wait_for_disconnection()
            self._pika_thread.join()

    def broadcast_status(self, status):
        """Broadcast transient status information to all listeners"""
        return  # XXX
        self._broadcast(
            "transient.status",
            json.dumps(status),
            headers={"x-message-ttl": str(int(15 + time.time()) * 1000)},
        )

    def _subscribe(self, sub_id: int, channel, callback, **kwargs):
        """Listen to a queue, notify via callback function.
        :param sub_id: ID for this subscription in the transport layer
        :param channel: Queue name to subscribe to
        :param callback: Function to be called when messages are received
        :param **kwargs: Further parameters for the transport layer. For example
          acknowledgement:  If true receipt of each message needs to be
                            acknowledged.
          exclusive:        Allow only one concurrent consumer on the queue.
          reconnectable:    Allow automatic re-establishing of the subscription
                            over a new connection in case of connection failure.
          prefetch_count:   Override the limit of prefetched messages on the
                            subscription. This makes little difference unless
                            acknowledgement is also set. (default: 1)
        """
        auto_ack = not kwargs.get("acknowledgement")
        exclusive = bool(kwargs.get("exclusive"))
        prefetch_count = int(kwargs.get("prefetch_count", 1))
        reconnectable = bool(kwargs.get("reconnectable"))
        assert not (
            reconnectable and not auto_ack
        ), "acknowledgements can't be reliably used on reconnectable connections"

        # Callback is stored on an internal property before calling this
        # Validate that it's identical to avoid mismatch errors
        assert (
            callback is None or callback == self.__subscriptions[sub_id]["callback"]
        ), "Pased callback does not match stored"

        with self._lock:
            if not self._pika_thread.alive:
                self.disconnect()
                raise workflows.Disconnected("No connection to RabbitMQ server.")
            self._pika_thread.subscribe_queue(
                queue=channel,
                callback=_rewrite_callback_to_pika(callback),
                auto_ack=auto_ack,
                exclusive=exclusive,
                consumer_tag=sub_id,
                reconnectable=bool(kwargs.get("reconnectable")),
                prefetch_count=prefetch_count,
            )

    def _subscribe_broadcast(
        self,
        consumer_tag: Hashable,
        queue: str,
        callback: MessageCallback,
        *,
        reconnectable: bool = True,
        **kwargs,
    ):
        """Listen to a queue, notify via callback function.

        Args:
            consumer_tag: Internal ID for this subscription
            queue: Name of the exchange to bind to
            callback:
                Function to be called when message are received. This Is
                ignored by some transports and so MUST be identical to
                the internal self.__subcription[sub_id]["callback"]
                object
            reconnectable: (maybe better written as durably reconnectable?)
        """
        # Callback is stored on an internal property before calling this
        # Validate that it's identical to avoid mismatch errors
        assert (
            callback is None
            or callback == self.__subscriptions[consumer_tag]["callback"]
        ), "Pased callback does not match stored"

        headers = {}

        def _redirect_callback(ch, method, properties, body):
            callback(
                {
                    "message-id": method.delivery_tag,
                    "pika-method": method,
                    "pika-properties": properties,
                },
                body,
            )

        self._reconnection_allowed = False
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            queue=queue,
            on_message_callback=_redirect_callback,
            auto_ack=True,
            consumer_tag=str(consumer_tag),
            arguments=headers,
        )
        try:
            self._channel.start_consuming()
        except pika.exceptions.AMQPChannelError:
            self.disconnect()
            raise workflows.Disconnected("Caught a channel error")
        except pika.exceptions.AMQPConnectionError:
            self.disconnect()
            raise workflows.Disconnected("Connection to RabbitMQ server was closed.")

    def _unsubscribe(self, consumer_tag):
        """Stop listening to a queue
        :param consumer_tag: Consumer Tag to cancel
        """
        self._channel.basic_cancel(consumer_tag=consumer_tag, callback=None)
        # Callback reference is kept as further messages may already have been received

    def _send(
        self, destination, message, headers=None, delay=None, expiration=None, **kwargs
    ):
        """Send a message to a queue.
        :param destination: Queue name to send to
        :param message: A string to be sent
        :param **kwargs: Further parameters for the transport layer. For example
          delay:            Delay transport of message by this many seconds
          expiration:       Optional expiration time, relative to sending time
          headers:          Optional dictionary of header entries
          transaction:      TxID if sending the message as part of a transaction
        """
        if not headers:
            headers = {}
        if delay:
            headers["x-delay"] = 1000 * delay
        if expiration:
            headers["x-message-ttl"] = int((time.time() + expiration) * 1000)

        properties = pika.BasicProperties(headers=headers, delivery_mode=2)
        self._pika_thread.send(
            exchange="",
            routing_key=destination,
            body=message,
            properties=properties,
            mandatory=True,  # message must be routable by the server
            # this is meaningless on the default exchange though
        )

    def _broadcast(
        self, destination, message, headers=None, delay=None, expiration=None, **kwargs
    ):
        """Broadcast a message.
        :param destination: Topic name to send to
        :param message: A string to be broadcast
        :param **kwargs: Further parameters for the transport layer. For example
          delay:            Delay transport of message by this many seconds
          expiration:       Optional expiration time, relative to sending time
          headers:          Optional dictionary of header entries
          ignore_namespace: Do not apply namespace to the destination name
          transaction:      Transaction ID if message should be part of a
                            transaction
        """
        logger.debug("broadcast disabled")
        return
        if not headers:
            headers = {}
        if delay:
            headers["x-delay"] = 1000 * delay
        if expiration:
            headers["x-message-ttl"] = int((time.time() + expiration) * 1000)
        properties = pika.BasicProperties(headers=headers, delivery_mode=2)
        self._reconnect()  # Ensure we are connected
        try:
            self._channel.basic_publish(
                exchange=destination,
                routing_key="",
                body=message,
                properties=properties,
                mandatory=True,
            )
        except (pika.exceptions.AMQPChannelError, pika.exceptions.AMQPConnectionError):
            self.disconnect()
            self._reconnect()
            try:
                self._channel.basic_publish(
                    exchange=destination,
                    routing_key="",
                    body=message,
                    properties=properties,
                    mandatory=True,
                )
            except (
                pika.exceptions.AMQPChannelError,
                pika.exceptions.AMQPConnectionError,
            ):
                self.disconnect()
                raise workflows.Disconnected("Connection to RabbitMQ server lost")

    def _transaction_begin(self, **kwargs):
        """Enter transaction mode.
        :param **kwargs: Further parameters for the transport layer.
        """
        self._channel.tx_select()

    def _transaction_abort(self, **kwargs):
        """Abort a transaction and roll back all operations.
        :param **kwargs: Further parameters for the transport layer.
        """
        self._channel.tx_rollback()

    def _transaction_commit(self, **kwargs):
        """Commit a transaction.
        :param **kwargs: Further parameters for the transport layer.
        """
        self._channel.tx_commit()

    def _ack(self, message_id, **kwargs):
        """Acknowledge receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        :param message_id: ID of the message to be acknowledged
        :param **kwargs: Further parameters for the transport layer.
        """
        self._channel.basic_ack(delivery_tag=message_id)

    def _nack(self, message_id, **kwargs):
        """Reject receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        :param message_id: ID of the message to be rejected
        :param **kwargs: Further parameters for the transport layer.
        """
        self._channel.basic_nack(delivery_tag=message_id)

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

    # class ExpectedStateError(Exception):
    #     pass


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
        exclusive: Should we be the only consumer?
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
    exclusive: bool
    kind: _PikaSubscriptionKind
    on_message_callback: PikaCallback = dataclasses.field(repr=False)
    prefetch_count: int
    queue: str = dataclasses.field(init=False, default=None)
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
    """

    def __init__(self, connection_parameters: Iterable[pika.ConnectionParameters]):
        super().__init__(
            name="workflows pika_transport", daemon=False, target=self._run
        )
        self._state: _PikaThreadStatus = _PikaThreadStatus.NEW
        # Internal store of subscriptions, to resubscribe if necessary
        self._subscriptions: Dict[int, _PikaSubscription] = {}
        # The pika connection object
        self._connection: pika.BlockingConnection
        # Per-subscription channels. May be pointing to the shared channel
        self._pika_channels: Dict[int, pika.channel.Channel] = {}
        # A common, shared channel, used for non-QoS subscriptions
        self._pika_shared_channel: Optional[pika.channel.Channel]
        # Are we allowed to reconnect. Can only be turned off, never on
        self._reconnection_allowed: bool = True
        # Our list of connection parameters, so we know where to connect to
        self._connection_parameters = list(connection_parameters)
        # If we failed with an unexpected exception
        self._exc_info: Optional[Tuple[Any, Any, Any]] = None

        # General bookkeeping events

        # When set, requests the main loop to stop when convenient
        self._please_stop = threading.Event()
        # Have we requested start - _started exists on Thread base class but private
        self.__started = threading.Event()
        # Have we had an initial connection - after this, we assume reconnection or failure
        # This is also set on complete failure, to prevent clients blocking forever
        self._connected = threading.Event()

        # General bookkeeping locks

        # Make sure we cannot race condition on starting
        self._start_lock = threading.Lock()

    @property
    def state(self) -> _PikaThreadStatus:
        """Read the current connection state"""
        return self._state

    def start(self, *, wait_for_connection=True):
        """Spawn the thread. Can Only call once per instance. Thread-safe."""

        # Ensure we can never accidentally call this twice
        with self._start_lock:
            logger.info("Starting thread")
            if self.__started.is_set():
                raise RuntimeError("pika.thread objects are not reusable")
            self.__started.set()

        # Someone called stop() before start().... just accept
        if self._please_stop.is_set():
            self._state == _PikaThreadStatus.STOPPED
            return

        super().start()

        if wait_for_connection:
            self.wait_for_connection()

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
        if re_raise:
            self.raise_if_exception()

    def wait_for_connection(self, timeout=None):
        """
        Safely wait until the thread has connected and is communicating with the server.

        When the timeout argument is present and not None, it should be
        a floating point number specifying a timeout for the operation
        in seconds (or fractions thereof).
        """
        self._connected.wait(timeout)

    def raise_if_exception(self):
        """If the thread has failed with an exception, raise it in the callers thread."""
        exception = self._exc_info
        if exception:
            raise exception[0].with_traceback(exception[1], exception[2])

    def subscribe_queue(
        self,
        queue: str,
        callback: PikaCallback,
        *,
        auto_ack: bool = True,
        consumer_tag: Optional[int] = None,
        exclusive: bool = False,
        prefetch_count: int = 1,
        reconnectable: bool = False,
    ) -> concurrent.futures.Future:
        """
        Subscribe to a queue. Thread-safe.

        Args:
            consumer_tag: Internal ID representing this subscription
            queue: The queue to listen for messages on
            callback: The function to call when receiving messages on this queue
            auto_ack: Should this subscription auto-acknowledge messages?
            exclusive: Should we be the only consumer?
            prefetch_count: How many messages are we allowed to prefetch
            reconnectable: Are we allowed to reconnect to this subscription

        Returns:
            A Future representing the subscription state. It will be set
            to an empty value upon subscription success.
        """

        new_sub = _PikaSubscription(
            arguments={},
            auto_ack=auto_ack,
            destination=queue,
            exclusive=exclusive,
            kind=_PikaSubscriptionKind.DIRECT,
            on_message_callback=callback,
            prefetch_count=prefetch_count,
            reconnectable=reconnectable,
        )
        result = concurrent.futures.Future()
        self._connection.add_callback_threadsafe(
            functools.partial(
                self._add_subscription_in_thread, consumer_tag, new_sub, result
            )
        )
        return result

    def subscribe_broadcast(
        self,
        exchange: str,
        callback: PikaCallback,
        *,
        auto_ack: bool = True,
        consumer_tag: Optional[int] = None,
        reconnectable: bool = False,
        prefetch_count: int = 0,
    ) -> concurrent.futures.Future:
        """
        Subscribe to a broadcast exchange. Thread-safe.

        Args:
            exchange: The queue to listen for messages on
            callback: The function to call when receiving messages on this queue
            auto_ack: Should this subscription auto-acknowledge messages?
            consumer_tag: Internal ID representing this subscription. Generated if unspecified.
            prefetch_count: How many messages are we allowed to prefetch
            reconnectable:
                Are we allowed to reconnect to this subscription?
                **Warning**: Reconnecting to broadcast exchanges has the
                    potential for dropping messages sent while disconnected.

        Returns:
            A Future representing the subscription state. It will be set
            to an empty value upon subscription success.
        """
        new_sub = _PikaSubscription(
            arguments={},
            auto_ack=auto_ack,
            destination=exchange,
            exclusive=True,
            kind=_PikaSubscriptionKind.FANOUT,
            on_message_callback=callback,
            prefetch_count=prefetch_count,
            reconnectable=reconnectable,
        )
        result = concurrent.futures.Future()
        self._connection.add_callback_threadsafe(
            functools.partial(
                self._add_subscription_in_thread, consumer_tag, new_sub, result
            )
        )
        return result

    def send(
        self,
        exchange: str,
        routing_key: str,
        body: Union[str, bytes],
        properties: pika.spec.BasicProperties = None,
        mandatory: bool = True,
    ) -> concurrent.futures.Future:
        """Send a message. Thread-safe."""
        future = concurrent.futures.Future()

        def _send():
            if future.set_running_or_notify_cancel():
                try:
                    self._get_shared_channel().basic_publish(
                        exchange=exchange,
                        routing_key=routing_key,
                        body=body,
                        properties=properties,
                        mandatory=mandatory,
                    )
                    future.set_result(None)
                except BaseException as e:
                    future.set_exception(e)

        self._connection.add_callback_threadsafe(_send)
        return future

    ####################################################################
    # PikaThread Internal methods

    def _get_shared_channel(self) -> pika.spec.Channel:
        """Get the shared (no prefetch) channel. Create if necessary."""
        if not self._pika_shared_channel:
            self._pika_shared_channel = self._connection.channel()
            self._pika_shared_channel.confirm_delivery()
        return self._pika_shared_channel

    def _synchronize_subscriptions(self):
        """Synchronize subscriptions list with the current connection."""
        # assert self._state == _PikaThreadStatus.CONNECTED

        for consumer_tag, subscription in self._subscriptions.items():
            # If we have a channel, then we've already handled
            if consumer_tag in self._pika_channels:
                continue

            # We flip reconnection to False if any subscription is not reconnectable
            if self._reconnection_allowed and not subscription.reconnectable:
                self._reconnection_allowed = False
                logger.debug(
                    f"Subscription {consumer_tag} to '{subscription.destination}' is not reconnectable. Turning reconnection off."
                )

            # Either open a channel (if prefetch) or use the shared one
            if subscription.prefetch_count == 0:
                channel = self._get_shared_channel()
            else:
                channel = self._connection.channel()
                channel.confirm_delivery()

            if subscription.kind == _PikaSubscriptionKind.FANOUT:
                # If a FANOUT subscription, then we need to create and bind
                # a temporary queue to receive messages from the exchange
                queue = channel.queue_declare("", exclusive=True).method.queue
                channel.queue_bind(queue, subscription.destination)
                subscription.queue = queue
            elif subscription.kind == _PikaSubscriptionKind.DIRECT:
                subscription.queue = subscription.destination
            else:
                raise NotImplementedError(
                    f"Unknown subscription kind: {subscription.kind}"
                )
            channel.basic_consume(
                subscription.queue,
                subscription.on_message_callback,
                auto_ack=subscription.auto_ack,
                exclusive=subscription.exclusive,
                consumer_tag=str(consumer_tag),
            )
            logger.debug("Consuming (%d) on %s", consumer_tag, subscription.queue)

            # Record the consumer channel
            self._pika_channels[consumer_tag] = channel
        logger.debug(
            f"Subscriptions synchronized. Reconnections allowed? - {'Yes' if self._reconnection_allowed else 'No.'}"
        )

    def _run(self):
        assert self._state == _PikaThreadStatus.NEW
        assert self._reconnection_allowed, "Should be true until first synchronize"
        logger.debug("pika thread starting")
        connection_counter = 0

        # Loop until we either can't, or are asked not to
        while (
            connection_counter == 0 or self._reconnection_allowed
        ) and not self._please_stop.is_set():
            try:
                if connection_counter == 0:
                    logger.debug("Connecting to AMPQ")
                else:
                    logger.debug(f"Reconnecting to AMPQ #{connection_counter}")
                self._state = _PikaThreadStatus.CONNECTING

                # Make sure we don't always connect to the same server first
                random.shuffle(self._connection_parameters)
                self._connection = pika.BlockingConnection(self._connection_parameters)
                logger.debug(f"Connection #{connection_counter} connected")
                connection_counter += 1

                # Clear the channels because this might be a reconnect
                self._pika_channels = {}
                self._pika_shared_channel = None

                # [Re]create subscriptions
                logger.debug("Setting up subscriptions")
                self._synchronize_subscriptions()
                # We set up here because we don't want to class as CONNECTED
                # until all requested subscriptions have been actioned.
                self._state = _PikaThreadStatus.CONNECTED
                self._connected.set()

                logger.debug("Waiting for events")

                # Run until we are asked to stop, or fail
                while not self._please_stop.is_set():
                    self._connection.process_data_events(None)
            except connection_workflow.AMQPConnectionWorkflowFailed:
                # We failed to even connect the first time - in this case fail visibly
                logger.error("Failed to connect to pika server")
                break
            except pika.exceptions.ConnectionClosed:
                self._exc_info = sys.exc_info()
                if self._please_stop.is_set():
                    logger.info("Connection closed on request")
                else:
                    logger.error("Connection closed unexpectedly")
            except pika.exceptions.ChannelClosed as e:
                logger.error("Channel closed: {e}")
                self._exc_info = sys.exc_info()
            except pika.exceptions.ConnectionWrongStateError as e:
                logger.debug(
                    "Got ConnectionWrongStateError - connection closed by other means?"
                )
                self._exc_info = sys.exc_info()
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
        if not self._connection.is_closed:
            self._connection.close()

        self._state = _PikaThreadStatus.STOPPED
        self._please_stop.set()

        # We might not have successfully connected - but we might be waiting
        # on a connection somewhere. So now we've actually marked ourselves
        # as disconnected, mark this.
        #
        # Note: Probably the wrong way to do this. Make more elaborate later.
        self._connected.set()

    def _add_subscription_in_thread(
        self,
        consumer_tag: Optional[int],
        subscription: _PikaSubscription,
        result: concurrent.futures.Future,
    ):
        """Add a subscription to the pika connection, on the connection thread."""
        try:
            if result.set_running_or_notify_cancel():
                # If not specified, generate a consumer_tag automatically
                if consumer_tag is None:
                    consumer_tag = min([-5000] + list(self._subscriptions.keys())) - 1
                assert (
                    consumer_tag not in self._subscriptions
                ), f"Subscription request {consumer_tag} rejected due to existing subscription {self._subscriptions[consumer_tag]}"
                self._subscriptions[consumer_tag] = subscription
                self._synchronize_subscriptions()
                result.set_result(None)
        except BaseException as e:
            result.set_exception(e)
