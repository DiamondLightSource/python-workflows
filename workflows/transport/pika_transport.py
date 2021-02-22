import configparser
import json
import threading
import time

import pika
import workflows
from workflows.transport.common_transport import CommonTransport

class PikaTransport(CommonTransport):
    """Abstraction layer for messaging infrastructure. Here we are using Pika"""

    # Set some sensible defaults
    defaults = {
        "--rabbit-host": "cs04r-sc-vserv-253",
        "--rabbit-port": 5672,
        "--rabbit-user": "myuser",
        "--rabbit-pass": "pass",
        "--rabbit-prfx": "",
    }

    # Effective configuration
    config = {}

    def __init__(self):
        self._connected = False
        # self._namespace = ""
        self._idcounter = 0
        self._lock = threading.RLock()
        # self._stomp_listener = stomp.listener.ConnectionListener()
        # self._stomp_listener = stomp.PrintingListener()
        # self._stomp_listener.on_message = self._on_message
        # self._stomp_listener.on_before_message = lambda x, y: (x, y)

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
            ("prefix", "--rabbit-prfx"),
        ]:
            try:
                cls.defaults[target] = cfgparser.get("rabbit", cfgoption)
            except configparser.NoOptionError:
                pass
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
                metavar="PRE",
                default=cls.defaults.get("--rabbit-pass"),
                help="Rabbit password",
                type=str,
                action=SetParameter,
            )
            # prefix
            argparser.add_argument(
                "--rabbit-conf",
                metavar="CNF",
                default=cls.defaults.get("--rabbit-conf"),
                help="Rabbit configuration file containing connection information, disables default values",
                type=str,
                action=SetParamter,
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
            username = self.config.get("--rabbit-user", self.defaults.get("--rabbit-user"))
            password = self.config.get("--rabbit-pass", self.defaults.get("--rabbit-pass"))
            host = self.config.get("--rabbit-host", self.defaults.get("--rabbit-host"))
            port = int(self.config.get("--rabbit-port", self.defaults.get("--rabbit-port")))
            credentials = pika.PlainCredentials(username, password)
            self._conn = pika.BlockingConnection(pika.ConnectionParameters(host=host,
             port=port, credentials=credentials))
            self._channel = self._conn.channel()
            self._connected = True
        return True
    
    def is_connected(self):
        self._connected = self._connected and self._conn.is_open() and self._channel.is_open()
        return self._connected

    def disconnect(self):
        if self._connected:
            self._connected = False
            self._channel.close()
            self._conn.close()
    
    def broadcast_status(self, status):
        self._broadcast(
            "transient.status",
            json.dumps(status),
            headers= {"expires": str(int(15 + time.time()) * 1000)}
        )

    def _subscribe(self, consumer_tag, queue, callback, **kwargs):
        headers = {}
        if kwargs.get("exclusive"):
            headers["rabbitmq.exclusive"] = "true"
        if kwargs.get("acknowledgement"):
            ack = "client-individual"
        else:
            ack = "auto"

        exclusive = kwargs.get("exclusive")
        auto_ack = not kwargs.get("acknowledgement")

        def _redirect_callback(ch, method, properties, body):
            # callback({"message-id": method.delivery_tag}, body.decode())
            if not auto_ack:
                self._ack()
        
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue=queue,
                                    on_message_callback=_redirect_callback,
                                    auto_ack=auto_ack,
                                    exclusive=exclusive,
                                    consumer_tag=consumer_tag,
                                    arguments=None)

    
    def _unsubscribe(self, consumer_tag):

        self._channel.basic_cancel(consumer_tag=consumer_tag, callback=None)
        # Callback reference is kept as further messages may already have been received


    def _send(self, destination, message, headers=None, delay=None, expiration=None, **kwargs):

        properties = pika.BasicProperties(headers=None,
                                          delivery_mode=2)
                                          #user_id=self.username)
        try:
            self._channel.basic_publish(exchange='',
                                        routing_key=destination,
                                        body=message,
                                        properties=properties)

        except pika.exception.ChannelClosed:
            self._connected = False
            raise workflows.Disconnected("No connection to channel")
        except pika.exception.ConnectionClosed:
            self._connected = False
            raise workflows.Disconnected("No connection to Rabbit host")

    def _broadcast(self, destination, message, headers=None, delay=None, expiration=None, **kwargs):

        properties = pika.BasicProperties(headers=None,
                                          delivery_mode=2,
                                          #user_id=self.username)

        if not headers:
            headers = {}
        if expiration:
            headers["expires"] = int((time.time() + expiration) * 1000)

        try:
            self._channel.basic_publish(exchange=destination,
                                    routing_key="",
                                    body=message,
                                    properties=properties)

        except pika.exception.ChannelClosed:
            self._connected = False
            raise workflows.Disconnected("No connection to channel")
        except pika.exception.ConnectionClosed:
            self._connected = False
            raise workflows.Disconnected("No connection to Rabbit host")

    def _transaction_begin(self, transaction_id, **kwargs):
        #Pika doesn't support transaction_id in transactions methods
        self._channel.tx_select()

    def transaction_abort(self, transaction_id, **kwargs):
        #abort a transaction and roll back all operations
        self._channel.tx_rollback()

    def transaction_commit(self, transaction_id, **kwargs):
        self._channel.tx_commit()

    def _ack(self, message_id, **kwargs):
        # argument in stomp_transport: subscription_id
        # delivery_tag is the message id, no argument for subscription_id
        self._channel.basic_ack(delivery_tag=message_id)
    
    def _nack(self, message_id, **kwargs):
        # argument in stomp_transport: subscription_id
        # delivery_tag is the message id, no argument for subscription_id
        self._channel.basic_nack(delivery_tag=message_id)

    @staticmethod
    def _mangle_for_sending(message):
        """Function that any message will pass through before it being forwarded to
        the actual _send* functions.
        Stomp only deals with serialized strings, so serialize message as json.
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





        


    