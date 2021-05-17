import configparser
import itertools
import json
import logging
import random
import threading
import time

import pika

import workflows
from workflows.transport.common_transport import CommonTransport, json_serializer

logger = logging.getLogger("workflows.transport.pika_transport")


class PikaTransport(CommonTransport):
    """Abstraction layer for messaging infrastructure.
    Here we are using RabbitMQ via pika."""

    # Set some sensible defaults
    defaults = {
        "--rabbit-host": "localhost",
        "--rabbit-port": 5672,
        "--rabbit-user": "guest",
        "--rabbit-pass": "guest",
        "--rabbit-vhost": "/",
    }

    # Effective configuration
    config = {}

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

        # Randomize connection order once to spread out equally across all servers
        random.shuffle(connection_parameters)
        self._reconnection_attempts = max(3, len(connection_parameters))
        self._connection_parameters = itertools.cycle(connection_parameters)

    def connect(self):
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
                    "message-id": method.delivery_tag,
                    "pika-method": method,
                    "pika-properties": properties,
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
        self._channel.confirm_delivery()
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
            self._channel.confirm_delivery()
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
