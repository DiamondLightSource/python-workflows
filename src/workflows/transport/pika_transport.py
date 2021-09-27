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
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Union

import pika

import workflows
from workflows.transport.common_transport import CommonTransport, json_serializer

logger = logging.getLogger("workflows.transport.pika_transport")


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
        self._vhost = "/"
        self._reconnection_allowed = True

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

    def _generate_connection_parameters(self):
        if hasattr(self, "_connection_parameters"):
            return
        username = self.config.get("--rabbit-user", self.defaults.get("--rabbit-user"))
        password = self.config.get("--rabbit-pass", self.defaults.get("--rabbit-pass"))
        credentials = pika.PlainCredentials(username, password)
        host = self.config.get("--rabbit-host", self.defaults.get("--rabbit-host"))
        port = str(self.config.get("--rabbit-port", self.defaults.get("--rabbit-port")))
        vhost = self.config.get("--rabbit-vhost", self.defaults.get("--rabbit-vhost"))
        if "," in host:
            host = host.split(",")
        else:
            host = [host]
        if "," in port:
            port = [int(p) for p in port.split(",")]
        else:
            port = [int(port)]
        if len(port) > len(host):
            raise workflows.Disconnected(
                "Invalid hostname/port specifications: cannot specify more ports than hosts"
            )
        if len(host) != len(port) and len(port) != 1:
            raise workflows.Disconnected(
                "Invalid hostname/port specifications: must either specify a single port, or one port per host"
            )
        if len(host) > len(port):
            port = port * len(host)
        connection_parameters = [
            pika.ConnectionParameters(
                host=h,
                port=p,
                virtual_host=vhost,
                credentials=credentials,
                retry_delay=5,
                connection_attempts=3,
            )
            for h, p in zip(host, port)
        ]

        # Randomize connection order once to spread connection attempts equally across all servers
        random.shuffle(connection_parameters)
        self._reconnection_attempts = max(3, len(connection_parameters))
        self._connection_parameters = itertools.cycle(connection_parameters)

    def connect(self) -> bool:
        """Ensure both connection and channel to the RabbitMQ server are open."""
        self._reconnection_allowed = True
        return self._reconnect()

    def _reconnect(self):
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
                connection = next(self._connection_parameters)
                logger.debug(
                    "Attempting connection %s (%d failed previous attempts)",
                    connection,
                    connection_attempt,
                )
                try:
                    self._conn = pika.BlockingConnection(connection)
                except pika.exceptions.AMQPConnectionError:
                    logger.debug(
                        "Could not initiate connection to RabbitMQ server at %s",
                        connection,
                    )
                    failed_attempts.append(("Connection Error", connection))
                    continue
                try:
                    self._channel = self._conn.channel()
                    self._channel.confirm_delivery()
                except pika.exceptions.AMQPChannelError:
                    self.disconnect()
                    logger.debug(
                        "Channel error while connecting to RabbitMQ server at %s",
                        connection,
                    )
                    failed_attempts.append(("Channel Error", connection))
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

    def is_connected(self) -> bool:
        """Return connection status."""
        if (
            self._connected
            and self._conn
            and self._conn.is_open
            and self._channel
            and self._channel.is_open
        ):
            return True
        self.disconnect()
        return False

    def disconnect(self):
        """Gracefully close connection to pika server"""
        if self._connected:
            self._connected = False
        if self._channel and self._channel.is_open:
            self._channel.close()
        if self._conn and self._conn.is_open:
            self._conn.close()

    def broadcast_status(self, status):
        """Broadcast transient status information to all listeners"""
        self._broadcast(
            "transient.status",
            json.dumps(status),
            headers={"x-message-ttl": str(int(15 + time.time()) * 1000)},
        )

    def _subscribe(self, consumer_tag, queue, callback, **kwargs):
        """Listen to a queue, notify via callback function.
        :param consumer_tag: ID for this subscription in the transport layer
        :param queue: Queue name to subscribe to
        :param callback: Function to be called when messages are received
        :param **kwargs: Further parameters for the transport layer. For example
          acknowledgement:  If true receipt of each message needs to be
                            acknowledged.
          selector:         Only receive messages filtered by a selector. See
                            https://activemq.apache.org/activemq-message-properties.html
                            for potential filter criteria. Uses SQL 92 syntax.
        """
        headers = {}
        auto_ack = not kwargs.get("acknowledgement")

        def _redirect_callback(ch, method, properties, body):
            callback(
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
            if not auto_ack:
                self._ack(method.delivery_tag)

        self._reconnection_allowed = False
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(
            queue=queue,
            on_message_callback=_redirect_callback,
            auto_ack=auto_ack,
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

    def _subscribe_broadcast(self, consumer_tag, queue, callback, **kwargs):
        """Listen to a queue, notify via callback function.
        :param consumer_tag: ID for this subscription in the transport layer
        :param queue: Queue name to subscribe to. Queue is bind to exchange
        :param callback: Function to be called when messages are received
        :param **kwargs: Further parameters for the transport layer. For example
          ignore_namespace: Do not apply namespace to the destination name
          retroactive:      Ask broker to send old messages if possible
        """

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
        self._reconnect()  # Ensure we are connected
        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=destination,
                body=message,
                properties=properties,
                mandatory=True,
            )
        except (pika.exceptions.AMQPChannelError, pika.exceptions.AMQPConnectionError):
            self.disconnect()
            self._reconnect()
            try:
                self._channel.basic_publish(
                    exchange="",
                    routing_key=destination,
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
    NEW = 0
    CONNECTING = 1
    CONNECTED = 2
    STOPPING = 3
    STOPPED = 4

    @property
    def is_new(self):
        return self is self.NEW

    @property
    def is_end_of_life(self):
        return self in {self.STOPPING, self.STOPPED}


@dataclasses.dataclass
class _PikaSubscription:
    arguments: Dict[str, Any]
    auto_ack: bool
    exclusive: bool
    on_message_callback: Callable = dataclasses.field(repr=False)
    prefetch_count: int
    queue: str
    reconnectable: bool
    state_channel_requested: bool = False
    state_subscription_requested: bool = False
    state_closing: bool = False


class _PikaThread(threading.Thread):
    """Internal helper thread that communicates with the pika package."""

    def __init__(self, connection_parameters: Iterable[pika.ConnectionParameters]):
        super().__init__(
            name="workflows pika_transport", daemon=False, target=self._run
        )
        self._state: _PikaThreadStatus = _PikaThreadStatus.NEW
        self._events: Dict[str, threading.Event] = {
            "connected": threading.Event(),
            "disconnected": threading.Event(),
        }
        self._events["disconnected"].set()
        self._parameters = collections.deque(connection_parameters)
        self._connection_attempt = 0
        self._pika_channel: Dict[int, pika.channel.Channel] = {}
        self._pika_connection: pika.SelectConnection
        self._reconnection_allowed: bool = True
        self._subscriptions: Dict[int, _PikaSubscription] = {}

    def stop(self):
        """Close all connections and wait for object cleanup."""
        self._stop()
        self.wait_for_disconnection(timeout=3)
        self._finalize()

    def _stop(self):
        """Close all connections without waiting."""
        if self._state.is_new:
            self._state = _PikaThreadStatus.STOPPED
            self._finalize()
        if self._state.is_end_of_life:
            return
        self._state = _PikaThreadStatus.STOPPING
        if self._pika_connection:
            for channel in self._pika_channel.values():
                self._pika_connection.ioloop.add_callback_threadsafe(channel.close)
            self._pika_connection.ioloop.add_callback_threadsafe(
                self._pika_connection.close
            )

    @property
    def connected(self) -> bool:
        return self._state is _PikaThreadStatus.CONNECTED

    @property
    def alive(self) -> bool:
        """Ensure the connection object is reasonably likely to connect at some point."""
        return not self._state.is_new and not self._state.is_end_of_life

    def wait_for_connection(self):
        self._events["connected"].wait()

    def wait_for_disconnection(self, **kwargs):
        self._events["disconnected"].wait(**kwargs)

    def _event_connected(self):
        if self._state.is_end_of_life:
            self._state = _PikaThreadStatus.CONNECTED
            self._stop()
            return
        self._state = _PikaThreadStatus.CONNECTED
        self._events["connected"].set()

    def _event_disconnected(self):
        self._pika_connection.ioloop.stop()
        if self._state.is_end_of_life or not self._reconnection_allowed:
            self._state = _PikaThreadStatus.STOPPED
        else:
            self._state = _PikaThreadStatus.CONNECTING
        self._events["disconnected"].set()

    def _finalize(self):
        """Fire all events to release all waiters as the object is now dead."""
        self._state = _PikaThreadStatus.STOPPED
        if hasattr(self, "_pika_connection"):
            self._pika_connection.ioloop.stop()
        if hasattr(self, "_pika_connection"):
            del self._pika_connection
        self._pika_channel.clear()
        for event in self._events.values():
            event.set()

    def start(self):
        """Spawn the pika communication thread
        after running a quick sanity check on the object."""
        if not self._state.is_new:
            raise RuntimeError("pika.thread objects are not reusable")
        super().start()

    def _run(self):
        """This function will be called from the python threading library and
        runs in a separate, named thread."""
        if not self._state.is_new:
            raise RuntimeError("pika.thread object already started")
        logger.debug("pika.thread starting")
        self._state = _PikaThreadStatus.CONNECTING
        while not self._state.is_end_of_life:
            self._parameters.rotate()
            if self._connection_attempt > max(3, len(self._parameters)):
                logger.error("Reached maximum connection attempts. Giving up.")
                break
            if self._connection_attempt > 0:
                wait_time = 0.5 * self._connection_attempt * self._connection_attempt
                logger.info(
                    f"Waiting {wait_time} seconds before next connection attempt..."
                )
                time.sleep(wait_time)
            self._connection_attempt += 1
            logger.info(
                f"Connecting to {self._parameters[0].host}:{self._parameters[0].port} (attempt {self._connection_attempt})"
            )
            self._connect(self._parameters[0])
        logger.debug("pika.thread cleaning up")
        self._stop()
        if (
            getattr(self, "_pika_connection", None)
            and not self._pika_connection.is_closed
        ):
            self._pika_connection.ioloop.start()
        self._finalize()
        logger.debug("pika.thread terminating")

    def _connect(self, parameters: pika.ConnectionParameters):
        self._state = _PikaThreadStatus.CONNECTING
        self._events["connected"].clear()
        self._events["disconnected"].clear()
        self._pika_connection = pika.SelectConnection(
            parameters,
            on_open_callback=self.on_open_connection_callback,
            on_open_error_callback=self.on_open_error_callback,
            on_close_callback=self.on_close_callback,
        )
        logger.info("pika.thread entering IO loop")
        self._pika_connection.ioloop.start()  # pika thread blocks here for the duration of the connection
        logger.info("pika.thread leaving IO loop")

    def on_open_connection_callback(self, connection):
        logger.info(f"Connection established {connection}")
        # open channel 0 immediately to allow sending messages
        self._pika_channel[0] = self._pika_connection.channel(
            on_open_callback=functools.partial(
                self.on_open_channel_callback, channel_id=0
            )
        )
        self._pika_connection.ioloop.add_callback_threadsafe(self.update_subscriptions)

    def on_open_channel_callback(self, channel, *, channel_id: int):
        logger.info(f"Channel opened, {channel}")
        channel.add_on_close_callback(self.on_closed_channel_callback)
        if channel_id == 0:
            # now we're open for business
            self._event_connected()
        else:
            # set up channel according to subscription requirements
            if self._subscriptions[channel_id].prefetch_count:
                channel.basic_qos(
                    prefetch_count=self._subscriptions[channel_id].prefetch_count
                )
        self._pika_connection.ioloop.add_callback_threadsafe(self.update_subscriptions)

    def on_closed_channel_callback(self, channel, reason):
        if reason.reply_code in (200, 0):
            logger.debug(
                f"Closed channel {channel} with {reason.reply_code}: {reason.reply_text}"
            )
        else:
            logger.error(
                f"Channel {channel} unexpectedly closed with {reason.reply_code}: {reason.reply_text}"
            )
            self._stop()

    def on_open_error_callback(self, *args, **kwargs):
        logger.info(f"open error callback {args}  {kwargs}")
        self._event_disconnected()

    def on_close_callback(self, connection, reason):
        logger.info(f"Closed connection {connection} with {reason}")
        self._event_disconnected()

    def send(
        self,
        exchange: str,
        routing_key: str,
        body: Union[str, bytes],
        properties: Any,
        mandatory: bool = True,
    ):
        if (
            not self.connected
            or 0 not in self._pika_channel
            or not self._pika_channel[0].is_open
        ):
            raise workflows.Disconnected("Connection not ready for sending")
        # Opportunity for race condition below. Does this matter?
        # How should we handle connection loss after send?
        send_call = functools.partial(
            self._pika_channel[0].basic_publish,
            exchange=exchange,
            routing_key=routing_key,
            body=body,
            properties=properties,
            mandatory=mandatory,
        )
        self._pika_connection.ioloop.add_callback_threadsafe(send_call)

    def subscribe_queue(
        self,
        *,
        consumer_tag: int,
        queue: str,
        callback,
        auto_ack: bool,
        reconnectable: bool,
        exclusive: bool,
        prefetch_count: int,
    ):
        assert (
            consumer_tag not in self._subscriptions
        ), f"Subscription request {consumer_tag} rejected due to existing subscription {self._subscriptions[consumer_tag]}"
        self._subscriptions[consumer_tag] = _PikaSubscription(
            arguments={},
            auto_ack=auto_ack,
            exclusive=exclusive,
            prefetch_count=prefetch_count,
            on_message_callback=callback,
            queue=queue,
            reconnectable=reconnectable,
        )
        self._pika_connection.ioloop.add_callback_threadsafe(self.update_subscriptions)

    def update_subscriptions(self):
        if not self.connected:
            pass  # TODO: handle case
        for consumer_tag, subscription in self._subscriptions.items():
            if subscription.prefetch_count == 0:
                channel_id = 0  # use shared channel
            else:
                channel_id = consumer_tag
            if subscription.state_closing:
                continue  # TODO implement
            if not subscription.state_channel_requested:
                if channel_id not in self._pika_channel:
                    self._pika_channel[channel_id] = self._pika_connection.channel(
                        on_open_callback=functools.partial(
                            self.on_open_channel_callback, channel_id=channel_id
                        )
                    )
                subscription.state_channel_requested = True
            if not subscription.state_subscription_requested:
                if self._pika_channel[channel_id].is_open:
                    self._pika_channel[channel_id].basic_consume(
                        queue=subscription.queue,
                        on_message_callback=subscription.on_message_callback,
                        auto_ack=subscription.auto_ack,
                        consumer_tag=str(consumer_tag),
                        arguments=subscription.arguments,
                        exclusive=subscription.exclusive,
                    )
                    subscription.state_subscription_requested = True

    def _on_message(self, frame):
        headers = frame.headers
        body = frame.body
        subscription_id = int(headers.get("subscription"))
        target_function = self.subscription_callback(subscription_id)
        if target_function:
            target_function(headers, body)
        else:
            raise workflows.Error(
                "Unhandled message {} {}".format(repr(headers), repr(body))
            )
