import collections
import configparser
import json
import logging
import random
import threading
import time
from enum import Enum
from typing import Any, Dict, Iterable, List

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
        self._lock = threading.Lock()
        self._pika_thread = None
        self._reconnection_allowed = True
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
        self._reconnection_allowed = True  # deprecated
        return self._pika_thread.connected

    def is_connected(self) -> bool:
        """Return connection status."""
        return self._pika_thread and self._pika_thread.connected

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

    def broadcast_status(self, status):
        """Broadcast transient status information to all listeners"""
        return  # XXX
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
          transformation:   Transform messages into different format. If set
                            to True, will use 'jms-object-json' formatting.
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
          ignore_namespace: Do not apply namespace to the destination name
          persistent:       Whether to mark messages as persistent, to be kept
                            between broker restarts. Default is 'true'
          transaction:      Transaction ID if message should be part of a transaction
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
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2
    STOPPING = 3
    STOPPED = 4


class _PikaThread(threading.Thread):
    """Internal helper thread that communicates with the pika package."""

    def __init__(self, connection_parameters: Iterable[pika.ConnectionParameters]):
        super().__init__(
            name="workflows pika_transport", daemon=False, target=self._run
        )

        self._events: Dict[str, threading.Event] = {"connected": threading.Event()}
        self._parameters = collections.deque(connection_parameters)
        self._state: _PikaThreadStatus = _PikaThreadStatus.DISCONNECTED

        self._connection_attempt = 0
        self._pika_channel: Any
        self._pika_connection: pika.SelectConnection

    def stop(self):
        self._state = _PikaThreadStatus.STOPPING
        time.sleep(1)  # shutting down
        self._event_finalized()

    @property
    def connected(self) -> bool:
        return self._state == _PikaThreadStatus.CONNECTED

    def wait_for_connection(self):
        self._events["connected"].wait()
        if not self.connected:
            raise workflows.Disconnected("No connection to RabbitMQ server")

    def _event_connected(self):
        self._state = _PikaThreadStatus.CONNECTED
        self._events["connected"].set()

    def _event_finalized(self):
        """Fires all events to release all waiters as the object is now dead."""
        self._state = _PikaThreadStatus.STOPPED
        for event in self._events.values():
            event.set()

    def _run(self):
        """This function will be called from the python threading library and
        runs in a separate, named thread."""

        logger.info("Hi from thread!")
        self._state = _PikaThreadStatus.CONNECTING
        while self._state not in (
            _PikaThreadStatus.STOPPING,
            _PikaThreadStatus.STOPPED,
        ):
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
        #           self._connect()
        #        time.sleep(2)
        #        self._event_connected()
        #        time.sleep(2)
        self.stop()

    def _connect(self):
        self._state = _PikaThreadStatus.CONNECTING
        self._parameters.rotate()
        print(self._parameters[0])
        self._pika_connection = pika.SelectConnection(
            self._parameters[0],
            #            on_open_callback=self.on_connection_open,
            #            on_open_error_callback=self.on_connection_open_error,
            #            on_close_callback=self.on_connection_closed)
        )
