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
            "pika-method": method,
            "pika-properties": properties,
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
        try:
            self._pika_thread.start(wait_for_connection=True)
        except pika.exceptions.AMQPConnectionError as e:
            raise workflows.Disconnected(e)

        # This either succeeded or the entire connection failed irreversably
        return self._pika_thread.alive

    def is_connected(self) -> bool:
        """Return connection status."""
        # TODO: Does this question even make sense with reconnection?
        #       Surely .alive is (slightly) better?
        return self._pika_thread and self._pika_thread.alive

    def disconnect(self):
        """Gracefully close connection to pika server"""
        self._pika_thread.join(stop=True)

    def broadcast_status(self, status):
        """Broadcast transient status information to all listeners"""

        self._broadcast("transient.status", json.dumps(status), expiration=15)

    def _subscribe(
        self,
        sub_id: int,
        channel: str,
        callback: MessageCallback,
        *,
        acknowledgement: bool = False,
        exclusive: bool = False,
        prefetch_count: int = 1,
        reconnectable: bool = False,
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
            exclusive: Allow only one concurrent consumer on the queue.
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

        # Callback is stored on an internal property in common before
        # calling this. Validate that it's identical to avoid mismatch errors.
        assert (
            sub_id is None
            or callback == self._CommonTransport__subscriptions[sub_id]["callback"]
        ), "Pased callback does not match stored"

        return self._pika_thread.subscribe_queue(
            queue=channel,
            callback=_rewrite_callback_to_pika(callback),
            auto_ack=not acknowledgement,
            exclusive=exclusive,
            consumer_tag=sub_id,
            reconnectable=reconnectable,
            prefetch_count=prefetch_count,
        ).result()

    def _subscribe_broadcast(
        self,
        consumer_tag: Hashable,
        queue: str,
        callback: MessageCallback,
        *,
        reconnectable: bool = False,
    ):
        """
        Listen to a FANOUT exchange, notify via callback function.

        Waits until subscription is complete.

        Args:
            consumer_tag: Internal ID for this subscription
            queue: Name of the exchange to bind to
            callback:
                Function to be called when message are received. This Is
                ignored by some transports and so MUST be identical to
                the internal self.__subcription[sub_id]["callback"]
                object
            reconnectable:
                Can we reconnect to this exchange if the connection is
                lost. Currently, this means that messages can be missed
                if they are sent while we are reconnecting.
        """
        # Callback is stored on an internal property before calling this
        # Validate that it's identical to avoid mismatch errors
        assert (
            consumer_tag is None
            or callback
            == self._CommonTransport__subscriptions[consumer_tag]["callback"]
        ), "Pased callback does not match stored"

        self._pika_thread.subscribe_broadcast(
            exchange=queue,
            callback=_rewrite_callback_to_pika(callback),
            consumer_tag=consumer_tag,
            reconnectable=reconnectable,
        ).result()

    def _unsubscribe(self, consumer_tag):
        """Stop listening to a queue
        :param consumer_tag: Consumer Tag to cancel
        """
        raise NotImplementedError()
        self._channel.basic_cancel(consumer_tag=consumer_tag, callback=None)
        # Callback reference is kept as further messages may already have been received

    def _send(
        self, destination, message, headers=None, delay=None, expiration=None, **kwargs
    ):
        """
        Send a message to a queue.

        Args:
            destination: Queue name to send to
            message: A string or bytes to be sent
            headers: Further arbitrary headers to pass to pika
            delay: Delay transport of message by this many seconds
            expiration: Optional TTL expiration time, relative to sending time
        """
        if not headers:
            headers = {}
        assert delay is None, "Not implemented"

        # if delay:
        #     headers["x-delay"] = 1000 * delay

        properties = pika.BasicProperties(headers=headers, delivery_mode=2)
        if expiration:
            properties.expiration = str(expiration * 1000)

        self._pika_thread.send(
            exchange="",
            routing_key=str(destination),
            body=message,
            properties=properties,
            mandatory=True,
        ).result()

    def _broadcast(
        self,
        destination,
        message,
        headers=None,
        delay=None,
        expiration: Optional[int] = None,
        **kwargs,
    ):
        """Send a message to a fanout exchange.

        Args:
            destination: Exchange to post to
            message: A string to be broadcast
            headers: Further arbitrary headers to pass to pika
            delay: Delay transport of message by this many seconds
            expiration: Optional TTL expiration time, in seconds, relative to sending time
            kwargs: Arbitrary arguments for other transports. Ignored.
        """
        assert delay is None, "Delay Not implemented"

        if not headers:
            headers = {}
        # if delay:
        #     headers["x-delay"] = 1000 * delay
        properties = pika.BasicProperties(headers=headers, delivery_mode=2)
        if expiration is not None:
            properties.expiration = str(expiration * 1000)

        self._pika_thread.send(
            exchange=destination,
            routing_key="",
            body=message,
            properties=properties,
            mandatory=True,
        ).result()

    def _transaction_begin(self, **kwargs):
        """Enter transaction mode.
        :param **kwargs: Further parameters for the transport layer.
        """
        raise NotImplementedError()

        self._channel.tx_select()

    def _transaction_abort(self, **kwargs):
        """Abort a transaction and roll back all operations.
        :param **kwargs: Further parameters for the transport layer.
        """
        raise NotImplementedError()
        self._channel.tx_rollback()

    def _transaction_commit(self, **kwargs):
        """Commit a transaction.
        :param **kwargs: Further parameters for the transport layer.
        """
        raise NotImplementedError()
        self._channel.tx_commit()

    def _ack(self, message_id, **kwargs):
        """Acknowledge receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        :param message_id: ID of the message to be acknowledged
        :param **kwargs: Further parameters for the transport layer.
        """
        raise NotImplementedError()
        self._channel.basic_ack(delivery_tag=message_id)

    def _nack(self, message_id, **kwargs):
        """Reject receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        :param message_id: ID of the message to be rejected
        :param **kwargs: Further parameters for the transport layer.
        """
        raise NotImplementedError()
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
        super().__init__(
            name="workflows pika_transport", daemon=False, target=self._run
        )
        self._state: _PikaThreadStatus = _PikaThreadStatus.NEW
        # Internal store of subscriptions, to resubscribe if necessary
        self._subscriptions: Dict[int, _PikaSubscription] = {}
        # The pika connection object
        self._connection: Optional[pika.BlockingConnection] = None
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
        self._reconnection_attempt_limit = reconnection_attempts
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
        if not self.alive:
            raise RuntimeError("Connection stopped or failed")
        self._connected.wait(timeout)
        self.raise_if_exception()

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

    @property
    def alive(self) -> bool:
        """
        Is the connection object connected, or in the process of reconnecting?

        This will return True even if the connection is not physically
        connected, or in the process of reconnecting.
        """
        return self.__started.is_set() and self._state not in (
            _PikaThreadStatus.STOPPED,
            _PikaThreadStatus.STOPPING,
        )

    # NOTE: With reconnection lifecycle this probably doesn't make sense
    #       on it's own. It might make sense to add this returning a
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
    #     return self.alive and self._connected.is_set()

    ####################################################################
    # PikaThread Internal methods

    def _debug_close_connection(self):
        self._connection.add_callback_threadsafe(lambda: self._connection.close())

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
                logger.error("Channel closed: {e}")
                self._exc_info = sys.exc_info()
            except pika.exceptions.ConnectionWrongStateError as e:
                logger.debug(
                    "Got ConnectionWrongStateError - connection closed by other means?"
                )
                self._exc_info = sys.exc_info()
            except pika.exceptions.AMQPConnectionError as e:
                self._exc_info = sys.exc_info()
                # Connection failed. Are we the first?
                if connection_counter == 0:
                    logger.error("Initial connection failed: %s", repr(e))
                    break
                logger.error("Connection %d failed: %s", connection_counter, repr(e))
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

        self._state = _PikaThreadStatus.STOPPED
        self._please_stop.set()

        # We might not have successfully connected - but we might be waiting
        # on a connection somewhere. So now we've actually marked ourselves
        # as disconnected, mark all other outstanding events as "complete"
        # to resolve deadlocks.
        self._connected.set()

        logger.debug("Leaving PikaThread runloop")

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
                result.set_result(self._subscriptions[consumer_tag].queue)
        except BaseException as e:
            result.set_exception(e)
