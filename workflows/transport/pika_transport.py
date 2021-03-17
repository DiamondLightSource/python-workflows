import configparser
import json
import threading
import time
import random

import pika
import workflows
from workflows.transport.common_transport import CommonTransport


class PikaTransport(CommonTransport):
    """Abstraction layer for messaging infrastructure. Here we are using Pika"""

    # Set some sensible defaults
    defaults = {
        "--rabbit-host": "cs04r-sc-vserv-253",
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
        self._reconnection_attempts = 0

    def get_namespace(self):
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
            help="Rabbit broker address, default '%(default)s'",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-port",
            metavar="PORT",
            default=cls.defaults.get("--rabbit-port"),
            help="Rabbit broker port, default '%(default)s",
            type=int,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-user",
            metavar="USER",
            default=cls.defaults.get("--rabbit-user"),
            help="Rabbit user, default '%(default)s'",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-pass",
            metavar="PASS",
            default=cls.defaults.get("--rabbit-pass"),
            help="Rabbit password",
            type=str,
            action=SetParameter,
        )
        argparser.add_argument(
            "--rabbit-vhost",
            metavar="VHST",
            default=cls.defaults.get("--rabbit-vhost"),
            help="Rabbit virtual host, default '%(default)s'",
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
            help="Rabbit broker address, default '%default'",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-port",
            metavar="PORT",
            default=cls.defaults.get("--rabbit-port"),
            help="Rabbit broker port, default '%default'",
            type="int",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-user",
            metavar="USER",
            default=cls.defaults.get("--rabbit-user"),
            help="Rabbit user, default '%default'",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-pass",
            metavar="PASS",
            default=cls.defaults.get("--rabbit-pass"),
            help="Rabbit password",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-vhost",
            metavar="VHST",
            default=cls.defaults.get("--rabbit-vhost"),
            help="Rabbit virtual host, default '%default'",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )
        optparser.add_option(
            "--rabbit-conf",
            metavar="CNF",
            default=cls.defaults.get("--rabbit-conf"),
            help="Rabbit configuration file containing connection information, disables default values",
            type="string",
            nargs=1,
            action="callback",
            callback=set_parameter,
        )

    def connect(self):
        """ It opens a connection and channel"""
        with self._lock:
            if self._connected:
                return True
            username = self.config.get(
                "--rabbit-user", self.defaults.get("--rabbit-user")
            )
            password = self.config.get(
                "--rabbit-pass", self.defaults.get("--rabbit-pass")
            )
            credentials = pika.PlainCredentials(username, password)
            host = self.config.get("--rabbit-host", self.defaults.get("--rabbit-host"))
            port = int(
                self.config.get("--rabbit-port", self.defaults.get("--rabbit-port"))
            )
            vhost = self.config.get(
                "--rabbit-vhost", self.defaults.get("--rabbit-vhost")
            )
            try:
                self._conn = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=host,
                        port=port,
                        virtual_host=vhost,
                        credentials=credentials,
                    )
                )
            except pika.exceptions.AMQPConnectionError:
                raise workflows.Disconnected(
                    "Could not initiate connection to RabbitMQ server"
                )
            try:
                self._channel = self._conn.channel()
            except pika.exceptions.AMQPChannelError:
                self.disconnect()
                raise workflows.Disconnected("Caught a channel error")
            self._connected = True
        return True

    def is_connected(self):
        """Return connection status."""
        self._connected = (
            self._connected
            and self._conn
            and self._conn.is_open
            and self._channel
            and self._channel.is_open
        )
        return self._connected

    def disconnect(self):
        """Gracefully close connection to pika server"""
        if self._connected:
            self._connected = False
        if self._channel and self._channel.is_open:
            self._channel.close()
        if self._conn and self._conn.is_open:
            self._conn.close()

    def reconnect(self):
        """ It opens a connection and channel"""
        with self._lock:
            if self._connected:
                return True
            max_reconnection_attempts = 3
            if self._reconnection_attempts > max_reconnection_attempts:
                return False
            username = self.config.get(
                "--rabbit-user", self.defaults.get("--rabbit-user")
            )
            password = self.config.get(
                "--rabbit-pass", self.defaults.get("--rabbit-pass")
            )
            credentials = pika.PlainCredentials(username, password)

            port = int(
                self.config.get("--rabbit-port", self.defaults.get("--rabbit-port"))
            )
            vhost = self.config.get(
                "--rabbit-vhost", self.defaults.get("--rabbit-vhost")
            )

            host1 = "cs04r-sc-vserv-252"
            host2 = "cs04r-sc-vserv-253"
            host3 = "cs04r-sc-vserv-254"

            retry_delay = 5
            connection_attempts = 3

            node1 = pika.ConnectionParameters(
                host=host1,
                port=port,
                virtual_host=vhost,
                credentials=credentials,
                retry_delay=retry_delay,
                connection_attempts=connection_attempts,
            )

            node2 = pika.ConnectionParameters(
                host=host2,
                port=port,
                virtual_host=vhost,
                credentials=credentials,
                retry_delay=retry_delay,
                connection_attempts=connection_attempts,
            )

            node3 = pika.ConnectionParameters(
                host=host3,
                port=port,
                virtual_host=vhost,
                credentials=credentials,
                retry_delay=retry_delay,
                connection_attempts=connection_attempts,
            )

            endpoints = [node1, node2, node3]

            if self._reconnection_attempts == 0:
                random.shuffle(endpoints)
            else:
                endpoints.append(endpoints.pop(0))

            try:
                self._reconnection_attempts += 1
                self._conn = pika.BlockingConnection(endpoints)
            except pika.exceptions.AMQPConnectionError:
                raise workflows.Disconnected(
                    "Could not initiate connection to RabbitMQ server"
                )
            try:
                self._channel = self._conn.channel()
            except pika.exceptions.AMQPChannelError:
                self.disconnect()
                raise workflows.Disconnected("Caught a channel error")
            self._connected = True
        return True

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
        self._channel.confirm_delivery()
        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=destination,
                body=message,
                properties=properties,
                mandatory=True,
            )
        except pika.exceptions.AMQPChannelError:
            self.disconnect()
            raise workflows.Disconnected("Caught a channel error")
        except pika.exceptions.AMQPConnectionError:
            self.disconnect()
            time.sleep(5)
            if not self.reconnect():
                raise workflows.Disconnected(
                    "Connection to RabbitMQ server was closed."
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

        if not headers:
            headers = {}
        if delay:
            headers["x-delay"] = 1000 * delay
        if expiration:
            headers["x-message-ttl"] = int((time.time() + expiration) * 1000)

        properties = pika.BasicProperties(headers=headers, delivery_mode=2)
        try:
            self._channel.basic_publish(
                exchange=destination,
                routing_key="",
                body=message,
                properties=properties,
                mandatory=True,
            )
        except pika.exceptions.AMQPChannelError:
            self.disconnect()
            raise workflows.Disconnected("Caught a channel error")
        except pika.exceptions.AMQPConnectionError:
            self.disconnect()
            time.sleep(5)
            if not self.reconnect():
                raise workflows.Disconnected(
                    "Connection to RabbitMQ server was closed."
                )

    def _transaction_begin(self, transaction_id, **kwargs):
        """Start a new transaction.
        Pika does not support transaction_id as argument.
        :param transaction_id: ID for this transaction in the transport layer.
        :param **kwargs: Further parameters for the transport layer.
        """
        self._channel.tx_select()

    def _transaction_abort(self, transaction_id, **kwargs):
        """Abort a transaction and roll back all operations.
        Pika does not support transaction_id as argument.
        :param transaction_id: ID of transaction to be aborted.
        :param **kwargs: Further parameters for the transport layer.
        """
        self._channel.tx_rollback()

    def _transaction_commit(self, transaction_id, **kwargs):
        """Commit a transaction.
        Pika does not support transaction_id as argument.
        :param transaction_id: ID of transaction to be committed.
        :param **kwargs: Further parameters for the transport layer.
        """
        self._channel.tx_commit()

    def _ack(self, message_id, **kwargs):
        """Acknowledge receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        Pika does not support acknowledgment with subscription ID.
        :param message_id: ID of the message to be acknowledged
        :param subscription: ID of the relevant subscriptiong
        :param **kwargs: Further parameters for the transport layer. For example
               transaction: Transaction ID if acknowledgement should be part of
                            a transaction
        """
        self._channel.basic_ack(delivery_tag=message_id)

    def _nack(self, message_id, **kwargs):
        """Reject receipt of a message. This only makes sense when the
        'acknowledgement' flag was set for the relevant subscription.
        Pika does not support acknowledgment with subscription ID.
        :param message_id: ID of the message to be rejected
        :param subscription: ID of the relevant subscriptiong
        :param **kwargs: Further parameters for the transport layer. For example
               transaction: Transaction ID if rejection should be part of a
                            transaction
        """
        self._channel.basic_nack(delivery_tag=message_id)

    @staticmethod
    def _mangle_for_sending(message):
        """Function that any message will pass through before it being forwarded to
        the actual _send* functions.
        Pika only deals with serialized strings, so serialize message as json.
        """
        return json.dumps(message)

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
