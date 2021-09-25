import argparse
import inspect
import json
import optparse
from unittest import mock

import pika
import pytest

import workflows
import workflows.transport
from workflows.transport.pika_transport import PikaTransport, _PikaThread


def test_lookup_and_initialize_pika_transport_layer():
    """Find the pika transport layer via the lookup mechanism and
    run its constructor with default settings
    """
    transport = workflows.transport.lookup("PikaTransport")
    assert transport == PikaTransport
    assert transport()


def test_add_command_line_help_optparse():
    """Check that command line parameters are registered in the parser."""
    parser = mock.MagicMock()

    PikaTransport().add_command_line_options(parser)

    parser.add_argument.assert_not_called()
    parser.add_option.assert_called()
    assert parser.add_option.call_count > 4
    for call in parser.add_option.call_args_list:
        assert call[1]["action"] == "callback"


def test_add_command_line_help_argparse():
    """Check that command line parameters are registered in the parser."""
    parser = mock.MagicMock()
    parser.add_argument = mock.Mock()

    PikaTransport().add_command_line_options(parser)

    parser.add_argument.assert_called()
    parser.add_option.assert_not_called()
    assert parser.add_argument.call_count > 4
    for call in parser.add_argument.call_args_list:
        assert inspect.isclass(call[1]["action"])


def test_adding_arguments_to_argparser():
    """Check that command line parameters can be added to the parser."""
    parser = argparse.ArgumentParser()

    PikaTransport().add_command_line_options(parser)

    result = parser.parse_args([])
    assert result.rabbit_host
    assert result.rabbit_port
    assert result.rabbit_user
    assert result.rabbit_pass


@mock.patch("workflows.transport.pika_transport.pika")
def test_check_config_file_behaviour(mockpika, tmp_path):
    """Check that a specified configuration file is read, that command line
    parameters have precedence and are passed on to the pika layer."""
    mockconn = mock.Mock()
    mockpika.BlockingConnection.return_value = mockconn

    parser = optparse.OptionParser()
    transport = PikaTransport()
    transport.add_command_line_options(parser)

    try:
        stored_defaults = transport.defaults.copy()
        stored_config = transport.config.copy()

        cfgfile = tmp_path / "config"
        cfgfile.write_text(
            """
    # An example pika configuration file
    # Only lines in the [pika] block will be interpreted

    [rabbit]
    host = localhost
    port = 5672
    username = someuser
    password = somesecret
    vhost = namespace
    """
        )

        parser.parse_args(
            ["--rabbit-conf", str(cfgfile), "--rabbit-user", mock.sentinel.user]
        )

        transport = PikaTransport()
        transport.connect()

        mockpika.BlockingConnection.assert_called_once()
        mockpika.PlainCredentials.assert_called_once_with(
            mock.sentinel.user, "somesecret"
        )
        args, kwargs = mockpika.ConnectionParameters.call_args
        assert not args
        assert kwargs == {
            "host": "localhost",
            "port": 5672,
            "virtual_host": "namespace",
            "credentials": mockpika.PlainCredentials.return_value,
            "connection_attempts": mock.ANY,
            "retry_delay": mock.ANY,
        }

        with pytest.raises(workflows.Error):
            parser.parse_args(["--rabbit-conf", ""])

    finally:
        transport.defaults = stored_defaults
        transport.config = stored_config


@mock.patch("workflows.transport.pika_transport.pika")
def test_anonymous_connection(mockpika):
    """check that a specified configuration file is read, that command line
    parameters have precedence and are passed on the pika layer"""

    mockconn = mock.Mock()
    mockpika.BlockingConnection.return_value = mockconn
    parser = optparse.OptionParser()
    transport = PikaTransport()
    transport.add_command_line_options(parser)

    parser.parse_args(["--rabbit-user=", "--rabbit-pass="])

    transport = PikaTransport()
    transport.connect()

    mockpika.BlockingConnection.assert_called_once()
    mockpika.PlainCredentials.assert_called_once_with("", "")


@mock.patch("workflows.transport.pika_transport.pika")
def test_instantiate_link_and_connect_to_broker(mockpika):
    """Test the Pika connection routine."""
    transport = PikaTransport()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    assert not transport.is_connected()

    transport.connect()

    mockconn.assert_called_once()
    mockconn.return_value.channel.assert_called_once()
    assert transport.is_connected()

    transport.connect()

    mockconn.assert_called_once()
    mockconn.return_value.channel.assert_called_once()
    assert transport.is_connected()

    transport.disconnect()

    mockconn.assert_called_once()
    mockconn.return_value.close.assert_called_once()
    mockconn.return_value.is_open = False
    mockchannel.close.assert_called_once()
    mockchannel.is_open = False
    assert not transport.is_connected()

    transport.disconnect()

    mockconn.assert_called_once()
    mockconn.return_value.close.assert_called_once()
    mockchannel.close.assert_called_once()
    assert not transport.is_connected()


@mock.patch("workflows.transport.pika_transport.pika")
def test_error_handling_when_connecting_to_broker(mockpika):
    """Test the Pika connection routine."""
    transport = PikaTransport()
    mockpika.BlockingConnection.side_effect = pika.exceptions.AMQPConnectionError()
    mockpika.exceptions.AMQPConnectionError = pika.exceptions.AMQPConnectionError

    with pytest.raises(workflows.Disconnected):
        transport.connect()

    assert not transport.is_connected()

    mockconn = mockpika.BlockingConnection
    mockconn.return_value.channel.side_effect = pika.exceptions.AMQPChannelError()
    mockpika.exceptions.AMQPChannelError = pika.exceptions.AMQPChannelError

    with pytest.raises(workflows.Disconnected):
        transport.connect()

    assert not transport.is_connected()


@mock.patch("workflows.transport.pika_transport.time")
@mock.patch("workflows.transport.pika_transport.pika")
def test_broadcast_status(mockpika, mocktime):
    """Test the status broadcast function."""
    mocktime.time.return_value = 20000
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties

    transport.broadcast_status({"status": str(mock.sentinel.status)})

    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args

    assert not args
    assert int(mockproperties.call_args[1].get("delivery_mode")) == 2
    assert int(
        mockproperties.call_args[1].get("headers").get("x-message-ttl")
    ) == 1000 * (20000 + 15)
    assert kwargs == {
        "exchange": "transient.status",
        "routing_key": "",
        "body": mock.ANY,
        "properties": mock.ANY,
        "mandatory": True,
    }
    statusdict = json.loads(kwargs.get("body"))
    assert statusdict["status"] == str(mock.sentinel.status)


@mock.patch("workflows.transport.pika_transport.pika")
def test_send_message(mockpika):
    """Test the message sending function"""
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties

    transport._send(str(mock.sentinel.queue), mock.sentinel.message)

    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args

    assert not args
    assert kwargs == {
        "exchange": "",
        "routing_key": str(mock.sentinel.queue),
        "body": mock.sentinel.message,
        "mandatory": True,
        "properties": mock.ANY,
    }
    assert mockproperties.call_args[1].get("headers") == {}
    assert int(mockproperties.call_args[1].get("delivery_mode")) == 2

    transport._send(
        str(mock.sentinel.queue),
        mock.sentinel.message,
        headers={"hdr": mock.sentinel.header},
        delay=123,
    )

    assert mockchannel.basic_publish.call_count == 2
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    assert mockproperties.call_args[1] == {
        "headers": {"hdr": mock.sentinel.header, "x-delay": 123000},
        "delivery_mode": 2,
    }
    assert kwargs == {
        "exchange": "",
        "routing_key": str(mock.sentinel.queue),
        "body": mock.sentinel.message,
        "mandatory": True,
        "properties": mock.ANY,
    }


@mock.patch("workflows.transport.pika_transport.pika")
@mock.patch("workflows.transport.pika_transport.time")
def test_sending_message_with_expiration(time, mockpika):
    """Test sending a message that expires some time in the future."""
    system_time = 1234567.1234567
    message_lifetime = 120
    expiration_time = int((system_time + message_lifetime) * 1000)
    time.time.return_value = system_time

    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties

    transport._send(str(mock.sentinel.queue), mock.sentinel.message, expiration=120)

    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    assert kwargs == {
        "exchange": "",
        "routing_key": str(mock.sentinel.queue),
        "body": mock.sentinel.message,
        "mandatory": True,
        "properties": mock.ANY,
    }
    properties = mockproperties.call_args[1]
    assert properties.get("headers") == {"x-message-ttl": expiration_time}


@mock.patch("workflows.transport.pika_transport.pika")
def test_error_handling_on_send(mockpika):
    """Unrecoverable errors during sending should lead to one reconnection attempt.
    Further errors should raise an Exception, further send attempts to try to reconnect."""
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockchannel.basic_publish.side_effect = pika.exceptions.AMQPChannelError()
    mockpika.exceptions = pika.exceptions

    assert mockconn.call_count == 1
    with pytest.raises(workflows.Disconnected):
        transport._send(str(mock.sentinel.queue), mock.sentinel.message)
    assert not transport.is_connected()
    assert mockconn.call_count == 2

    mockchannel.basic_publish.side_effect = None
    transport._send(str(mock.sentinel.queue), mock.sentinel.message)
    assert transport.is_connected()
    assert mockconn.call_count == 3


@mock.patch("workflows.transport.pika_transport.pika")
def test_send_broadcast(mockpika):
    """Test the broadcast sending function"""
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties

    transport._broadcast(str(mock.sentinel.exchange), mock.sentinel.message)

    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    properties = mockproperties.call_args[1]
    assert properties.get("headers") in (None, {})
    assert kwargs == {
        "exchange": str(mock.sentinel.exchange),
        "routing_key": "",
        "body": mock.sentinel.message,
        "properties": mock.ANY,
        "mandatory": True,
    }

    transport._broadcast(
        str(mock.sentinel.exchange),
        mock.sentinel.message,
        headers=mock.sentinel.headers,
    )

    assert mockchannel.basic_publish.call_count == 2
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    properties = mockproperties.call_args[1]
    assert properties == {"headers": mock.sentinel.headers, "delivery_mode": 2}
    assert kwargs == {
        "exchange": str(mock.sentinel.exchange),
        "routing_key": "",
        "body": mock.sentinel.message,
        "properties": mock.ANY,
        "mandatory": True,
    }

    transport._broadcast(str(mock.sentinel.exchange), mock.sentinel.message, delay=123)
    assert mockchannel.basic_publish.call_count == 3
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    properties = mockproperties.call_args[1]
    assert properties.get("headers").get("x-delay") == 123000
    assert kwargs == {
        "exchange": str(mock.sentinel.exchange),
        "routing_key": "",
        "body": mock.sentinel.message,
        "properties": mock.ANY,
        "mandatory": True,
    }


@mock.patch("workflows.transport.pika_transport.pika")
@mock.patch("workflows.transport.pika_transport.time")
def test_broadcasting_message_with_expiration(time, mockpika):
    """Test sending a message that expires some time in the future"""
    system_time = 1234567.1234567
    message_lifetime = 120
    expiration_time = int((system_time + message_lifetime) * 1000)
    time.time.return_value = system_time

    transport = PikaTransport()
    transport.connect()

    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties

    transport._broadcast(
        str(mock.sentinel.exchange), mock.sentinel.message, expiration=120
    )

    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    properties = mockproperties.call_args[1]
    assert properties.get("headers").get("x-message-ttl") == expiration_time
    assert properties.get("headers") == {"x-message-ttl": expiration_time}
    assert kwargs == {
        "exchange": str(mock.sentinel.exchange),
        "routing_key": "",
        "body": mock.sentinel.message,
        "properties": mock.ANY,
        "mandatory": True,
    }


@mock.patch("workflows.transport.pika_transport.pika")
def test_error_handling_on_broadcast(mockpika):
    """Unrecoverable errors during broadcasting should lead to one reconnection attempt.
    Further errors should raise an Exception, further send attempts to try to reconnect."""
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockchannel.basic_publish.side_effect = pika.exceptions.AMQPChannelError()
    mockpika.exceptions = pika.exceptions

    assert mockconn.call_count == 1
    with pytest.raises(workflows.Disconnected):
        transport._broadcast(str(mock.sentinel.channel), mock.sentinel.message)
    assert not transport.is_connected()
    assert mockconn.call_count == 2

    mockchannel.basic_publish.side_effect = None
    transport._broadcast(str(mock.sentinel.channel), mock.sentinel.message)
    assert transport.is_connected()
    assert mockconn.call_count == 3


@mock.patch("workflows.transport.pika_transport.pika")
def test_messages_are_serialized_for_transport(mockpika):
    banana = {"entry": [0, "banana"]}
    banana_str = '{"entry": [0, "banana"]}'
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    transport.send(str(mock.sentinel.queue1), banana)
    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    assert kwargs == {
        "exchange": "",
        "routing_key": str(mock.sentinel.queue1),
        "body": banana_str,
        "properties": mock.ANY,
        "mandatory": True,
    }

    transport.broadcast(str(mock.sentinel.queue2), banana)
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    assert kwargs == {
        "exchange": str(mock.sentinel.queue2),
        "routing_key": "",
        "body": banana_str,
        "properties": mock.ANY,
        "mandatory": True,
    }

    with pytest.raises(Exception):
        transport.send(str(mock.sentinel.queue), mock.sentinel.unserializable)


@mock.patch("workflows.transport.pika_transport.pika")
def test_messages_are_not_serialized_for_raw_transport(mockpika):
    """Test the raw sending methods"""
    banana = '{"entry": [0, "banana"]}'
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    transport.raw_send(str(mock.sentinel.queue1), banana)
    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    assert kwargs == {
        "exchange": "",
        "routing_key": str(mock.sentinel.queue1),
        "body": banana,
        "mandatory": True,
        "properties": mock.ANY,
    }

    mockchannel.basic_publish.reset_mock()
    transport.raw_broadcast(str(mock.sentinel.queue2), banana)
    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    assert kwargs == {
        "exchange": str(mock.sentinel.queue2),
        "routing_key": "",
        "body": banana,
        "properties": mock.ANY,
        "mandatory": True,
    }

    mockchannel.basic_publish.reset_mock()
    transport.raw_send(str(mock.sentinel.queue), mock.sentinel.unserializable)
    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    assert kwargs == {
        "exchange": "",
        "routing_key": str(mock.sentinel.queue),
        "body": mock.sentinel.unserializable,
        "mandatory": True,
        "properties": mock.ANY,
    }


@mock.patch("workflows.transport.pika_transport.pika")
def test_messages_are_deserialized_after_transport(mockpika):
    """Test the message serialization."""
    banana = {"entry": [0, "banana"]}
    banana_str = '{"entry": [0, "banana"]}'
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties
    mockdeliver = mockpika.BasicDeliver

    # Test subscriptions
    callback = mock.Mock()
    transport.subscribe("queue", callback)
    message_handler = mockchannel.basic_consume.call_args[1].get("on_message_callback")

    message_handler(mockchannel, mockdeliver, mockproperties, banana_str)
    callback.assert_called_once()
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == banana

    message_handler(
        mockchannel, mockdeliver, mockproperties, mock.sentinel.undeserializable
    )
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == mock.sentinel.undeserializable

    # Test broadcast subscriptions
    callback = mock.Mock()
    transport.subscribe_broadcast("queue", callback)
    message_handler = mockchannel.basic_consume.call_args[1].get("on_message_callback")

    message_handler(mockchannel, mockdeliver, mockproperties, banana_str)
    callback.assert_called_once()
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == banana
    message_handler(
        mockchannel, mockdeliver, mockproperties, mock.sentinel.undeserializable
    )
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == mock.sentinel.undeserializable

    # Test subscriptions with mangling disabled
    callback = mock.Mock()
    transport.subscribe("queue", callback, disable_mangling=True)
    message_handler = mockchannel.basic_consume.call_args[1].get("on_message_callback")
    message_handler(mockchannel, mockdeliver, mockproperties, banana_str)
    callback.assert_called_once()
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == banana_str

    # Test broadcast subscriptions with mangling disabled
    callback = mock.Mock()
    transport.subscribe_broadcast("queue", callback, disable_mangling=True)
    message_handler = mockchannel.basic_consume.call_args[1].get("on_message_callback")
    message_handler(mockchannel, mockdeliver, mockproperties, banana_str)
    callback.assert_called_once()
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == banana_str


@mock.patch("workflows.transport.pika_transport.pika")
def test_subscribe_to_queue(mockpika):
    """Test subscribing to a queue (producer-consumer), callback functions and unsubscribe"""
    mock_cb = mock.Mock()
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    transport._subscribe(1, str(mock.sentinel.queue1), mock_cb)

    mockchannel.basic_consume.assert_called_once()
    args, kwargs = mockchannel.basic_consume.call_args
    assert not args
    assert kwargs == {
        "queue": str(mock.sentinel.queue1),
        "on_message_callback": mock.ANY,
        "auto_ack": True,
        "consumer_tag": "1",
        "arguments": {},
    }

    transport._subscribe(2, str(mock.sentinel.queue2), mock_cb)

    assert mockchannel.basic_consume.call_count == 2
    args, kwargs = mockchannel.basic_consume.call_args
    assert not args
    assert kwargs == {
        "queue": str(mock.sentinel.queue2),
        "on_message_callback": mock.ANY,
        "auto_ack": True,
        "consumer_tag": "2",
        "arguments": {},
    }

    transport._subscribe(3, str(mock.sentinel.queue3), mock_cb, acknowledgement=True)
    assert mockchannel.basic_consume.call_count == 3
    args, kwargs = mockchannel.basic_consume.call_args
    assert not args
    assert kwargs == {
        "queue": str(mock.sentinel.queue3),
        "on_message_callback": mock.ANY,
        "auto_ack": False,
        "consumer_tag": "3",
        "arguments": {},
    }

    transport._unsubscribe(1)
    mockchannel.basic_cancel.assert_called_once_with(callback=None, consumer_tag=1)
    transport._unsubscribe(2)
    mockchannel.basic_cancel.assert_called_with(callback=None, consumer_tag=2)


@mock.patch("workflows.transport.pika_transport.pika")
def test_subscribe_to_broadcast(mockpika):
    """Test subscribing to a queue (producer-consumer), callback functions and unsubscribe"""
    mock_cb = mock.Mock()
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    transport._subscribe_broadcast(1, str(mock.sentinel.queue1), mock_cb)

    mockchannel.basic_consume.assert_called_once()
    args, kwargs = mockchannel.basic_consume.call_args
    assert not args
    assert kwargs == {
        "queue": str(mock.sentinel.queue1),
        "on_message_callback": mock.ANY,
        "auto_ack": True,
        "consumer_tag": "1",
        "arguments": {},
    }

    transport._subscribe(
        2,
        str(mock.sentinel.queue2),
        mock_cb,
    )

    assert mockchannel.basic_consume.call_count == 2
    args, kwargs = mockchannel.basic_consume.call_args
    assert not args
    assert kwargs == {
        "queue": str(mock.sentinel.queue2),
        "on_message_callback": mock.ANY,
        "auto_ack": True,
        "consumer_tag": "2",
        "arguments": {},
    }

    transport._unsubscribe(1)
    mockchannel.basic_cancel.assert_called_once_with(callback=None, consumer_tag=1)
    transport._unsubscribe(2)
    mockchannel.basic_cancel.assert_called_with(callback=None, consumer_tag=2)


@mock.patch("workflows.transport.pika_transport.pika")
def test_error_handling_on_subscribing(mockpika):
    """Unrecoverable errors during subscribing should mark the connection as disconnected."""
    mock_cb = mock.Mock()
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockchannel.start_consuming.side_effect = pika.exceptions.AMQPChannelError()
    mockpika.exceptions = pika.exceptions

    with pytest.raises(workflows.Disconnected):
        transport._subscribe(1, str(mock.sentinel.queue1), mock_cb)
    assert not transport.is_connected()

    mockchannel.start_consuming.side_effect = pika.exceptions.AMQPConnectionError()
    mockpika.exceptions = pika.exceptions

    with pytest.raises(workflows.Disconnected):
        transport._subscribe(1, str(mock.sentinel.queue1), mock_cb)
    assert not transport.is_connected()


@mock.patch("workflows.transport.pika_transport.pika")
def test_transaction_calls(mockpika):
    """Test that calls to create, commit, abort transactions are passed to Pika properly."""
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties

    transport._transaction_begin()
    mockchannel.tx_select.assert_called_once()

    transport._send(
        "destination", mock.sentinel.message, transaction=mock.sentinel.txid
    )
    args, kwargs = mockchannel.basic_publish.call_args
    assert not args
    assert kwargs == {
        "exchange": "",
        "routing_key": "destination",
        "body": mock.sentinel.message,
        "mandatory": True,
        "properties": mock.ANY,
    }
    assert mockproperties.call_args[1] == {"headers": {}, "delivery_mode": 2}

    transport._transaction_abort()
    mockchannel.tx_rollback.assert_called_once()

    transport._transaction_commit()
    mockchannel.tx_commit.assert_called_once()


@mock.patch("workflows.transport.pika_transport.pika")
def test_ack_message(mockpika):
    """Test that the _ack function call is properly forwarded to pika"""
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    transport._ack(mock.sentinel.messageid)
    mockchannel.basic_ack.assert_called_once_with(delivery_tag=mock.sentinel.messageid)


@mock.patch("workflows.transport.pika_transport.pika")
def test_nack_message(mockpika):
    """Test that the _nack function call is properly forwarded to pika"""
    transport = PikaTransport()
    transport.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    transport._nack(mock.sentinel.messageid)
    mockchannel.basic_nack.assert_called_once_with(delivery_tag=mock.sentinel.messageid)

    # defaults = {
    #     "--rabbit-host": "localhost",
    #     "--rabbit-port": 5672,
    #     "--rabbit-user": "guest",
    #     "--rabbit-pass": "guest",
    #     "--rabbit-vhost": "/",
    # }


@pytest.fixture
def connection_params():
    params = [
        pika.ConnectionParameters(
            "localhost",
            5672,
            "/",
            pika.PlainCredentials("guest", "guest"),
            connection_attempts=1,
            retry_delay=1,
            socket_timeout=1,
            stack_timeout=2,
        )
    ]
    # Try a connection here to make sure this is valid
    try:
        pika.BlockingConnection(params)
    except BaseException:
        pytest.skip("Failed to create test RabbitMQ connection")
    return params


def test_multiple_subscribe_to_broadcast():
    """Test multiple subscriptions to a broadcast channel"""
    # Make an entirely separate connection to RabbitMQ
    import pika

    conn = pika.BlockingConnection()
    side_channel = conn.channel()

    # pika = PikaTransport()
    # pika.connect()

    # We create our own channel to check assumptions about multiple delivery
    # ?    side_channel = pika._conn.channel()
    # Make sure that the queue and exchange exists
    side_channel.exchange_declare("transient.status", passive=True)
    side_channel.queue_declare("transient.status", exclusive=True, auto_delete=True)
    side_channel.queue_bind("transient.status", "transient.status")
    breakpoint()

    # def _subscribe_broadcast(self, consumer_tag, queue, callback, **kwargs):

    messages = []

    def cb(*args, **kwargs):
        messages.append((args, kwargs))

    pika.subscribe_broadcast("transient.status", cb)


def test_pikathread(connection_params):
    thread = _PikaThread(connection_params)
    thread.start()
    print("Waiting for pika connection")
    thread.wait_for_connection()
    print("stopping connection")
    thread.stop()
    thread.join(re_raise=True)
