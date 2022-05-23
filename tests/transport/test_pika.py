from __future__ import annotations

import argparse
import copy
import inspect
import json
import optparse
import pprint
import threading
import uuid
from queue import Empty, Queue
from unittest import mock

import pika
import pytest

import workflows.transport.pika_transport
from workflows.transport.common_transport import TemporarySubscription
from workflows.transport.pika_transport import PikaTransport, _PikaThread


@pytest.fixture
def mock_pikathread(monkeypatch):
    instance = mock.Mock(spec=_PikaThread)
    instance.return_value = instance

    monkeypatch.setattr(workflows.transport.pika_transport, "_PikaThread", instance)

    return instance


@pytest.fixture
def revert_classvariables():
    """
    PikaTransport.config and .defaults are class variables, and so are
    changed by various tests. This fixture ensures the class is restored
    to its original state at the end of the test.
    """
    config = PikaTransport.config
    defaults = PikaTransport.defaults

    PikaTransport.config = config.copy()
    PikaTransport.defaults = defaults.copy()

    yield

    PikaTransport.config = config
    PikaTransport.defaults = defaults


@pytest.fixture
def pikatransport(revert_classvariables, connection_params):
    # connection_params is unused here, but implements the fixture skipping
    # logic following a single test, instead of attempting a connection for
    # every individual test.
    pt = PikaTransport()
    pt.connect()
    yield pt
    pt.disconnect()


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
def test_check_config_file_behaviour(
    mockpika, mock_pikathread, tmp_path, revert_classvariables
):
    """Check that a specified configuration file is read, that command line
    parameters have precedence and are passed on to the pika layer."""

    parser = optparse.OptionParser()
    transport = PikaTransport()
    transport.add_command_line_options(parser)

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

    mock_pikathread.start.assert_called_once()
    mockpika.PlainCredentials.assert_called_once_with(mock.sentinel.user, "somesecret")
    args, kwargs = mockpika.ConnectionParameters.call_args
    assert not args
    assert kwargs == {
        "host": "localhost",
        "port": 5672,
        "virtual_host": "namespace",
        "credentials": mockpika.PlainCredentials.return_value,
    }

    with pytest.raises(workflows.Error):
        parser.parse_args(["--rabbit-conf", ""])


@mock.patch("workflows.transport.pika_transport.pika")
def test_anonymous_connection(mockpika, mock_pikathread, revert_classvariables):
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

    mock_pikathread.start.assert_called_once()
    mockpika.PlainCredentials.assert_called_once_with("", "")


def test_instantiate_link_and_connect_to_broker(mock_pikathread):
    """Test the Pika connection routine."""
    transport = PikaTransport()
    assert not transport.is_connected()

    transport.connect()
    mock_pikathread.start.assert_called_once()
    mock_pikathread.start.side_effect = RuntimeError

    assert transport.is_connected()

    with pytest.raises(RuntimeError):
        transport.connect()

    mock_pikathread.join.assert_not_called()
    assert transport.is_connected()

    transport.disconnect()

    mock_pikathread.join.assert_called_once()
    mock_pikathread.connection_alive = False
    assert not transport.is_connected()

    transport.disconnect()

    assert mock_pikathread.join.call_count == 2
    assert not transport.is_connected()


@mock.patch("workflows.transport.pika_transport.pika")
def test_error_handling_when_connecting_to_broker(mockpika, mock_pikathread):
    """Test the Pika connection routine."""
    transport = PikaTransport()
    mock_pikathread.start.side_effect = pika.exceptions.AMQPConnectionError
    mockpika.exceptions = pika.exceptions

    with pytest.raises(workflows.Disconnected):
        transport.connect()

    assert transport.is_connected() is mock_pikathread.connection_alive


@mock.patch("workflows.transport.pika_transport.pika")
def test_broadcast_status(mockpika, mock_pikathread):
    """Test the status broadcast function."""
    transport = PikaTransport()
    transport.connect()

    mockproperties = mockpika.BasicProperties

    transport.broadcast_status(
        {"status": str(mock.sentinel.status), "host": "localhost", "workflows": True}
    )

    mock_pikathread.send.assert_called_once()
    args, kwargs = mock_pikathread.send.call_args
    assert not args
    assert int(mockproperties.call_args[1].get("delivery_mode")) == 2
    assert int(mockproperties.return_value.expiration) == 1000 * 15
    assert kwargs == {
        "exchange": "transient.status",
        "routing_key": "",
        "body": mock.ANY,
        "properties": mock.ANY,
        "mandatory": False,
        "transaction_id": None,
    }
    statusdict = json.loads(kwargs.get("body"))
    assert statusdict["status"] == str(mock.sentinel.status)


@mock.patch("workflows.transport.pika_transport.pika")
def test_send_message(mockpika, mock_pikathread):
    """Test the message sending function"""
    transport = PikaTransport()
    transport.connect()

    mockproperties = mockpika.BasicProperties

    transport._send(mock.sentinel.queue, mock.sentinel.message)
    mock_pikathread.send.assert_called_once()
    args, kwargs = mock_pikathread.send.call_args

    assert not args
    assert kwargs == {
        "exchange": "",
        "routing_key": str(mock.sentinel.queue),
        "body": mock.sentinel.message,
        "mandatory": True,
        "properties": mock.ANY,
        "transaction_id": None,
    }
    assert mockproperties.call_args[1].get("headers") == {}
    assert int(mockproperties.call_args[1].get("delivery_mode")) == 2

    transport._send(
        str(mock.sentinel.queue),
        mock.sentinel.message,
        headers={"hdr": mock.sentinel.header},
        delay=123.456,
    )
    headers = mockproperties.call_args[1].get("headers")
    assert headers == {
        "hdr": mock.sentinel.header,
        "x-delay": 123456,
    }
    assert isinstance(headers["x-delay"], int)
    assert int(mockproperties.call_args[1].get("delivery_mode")) == 2

    # assert mockchannel.basic_publish.call_count == 2
    # args, kwargs = mockchannel.basic_publish.call_args
    # assert not args
    # assert mockproperties.call_args[1] == {
    #     "headers": {"hdr": mock.sentinel.header, "x-delay": 123000},
    #     "delivery_mode": 2,
    # }
    # assert kwargs == {
    #     "exchange": "",
    #     "routing_key": str(mock.sentinel.queue),
    #     "body": mock.sentinel.message,
    #     "mandatory": True,
    #     "properties": mock.ANY,
    # }


@mock.patch("workflows.transport.pika_transport.pika")
def test_send_message_to_named_exchange(mockpika, mock_pikathread):
    """Test sending a message to an explicitly named exchange"""
    transport = PikaTransport()
    transport.connect()

    mockproperties = mockpika.BasicProperties

    transport._send(mock.sentinel.queue, mock.sentinel.message, exchange="foo")
    mock_pikathread.send.assert_called_once()
    args, kwargs = mock_pikathread.send.call_args

    assert not args
    assert kwargs == {
        "exchange": "foo",
        "routing_key": str(mock.sentinel.queue),
        "body": mock.sentinel.message,
        "mandatory": True,
        "properties": mock.ANY,
        "transaction_id": None,
    }
    assert mockproperties.call_args[1].get("headers") == {}
    assert int(mockproperties.call_args[1].get("delivery_mode")) == 2


@mock.patch("workflows.transport.pika_transport.pika")
def test_sending_message_with_expiration(mockpika, mock_pikathread):
    """Test sending a message that expires some time in the future."""
    transport = PikaTransport()
    transport.connect()
    mockproperties = mockpika.BasicProperties

    transport._send(str(mock.sentinel.queue), mock.sentinel.message, expiration=120)

    mock_pikathread.send.assert_called_once()

    args, kwargs = mock_pikathread.send.call_args
    assert not args
    assert kwargs == {
        "exchange": "",
        "routing_key": str(mock.sentinel.queue),
        "body": mock.sentinel.message,
        "mandatory": True,
        "properties": mock.ANY,
        "transaction_id": None,
    }
    assert int(mockproperties.return_value.expiration) == 120 * 1000


@mock.patch("workflows.transport.pika_transport.pika")
def test_error_handling_on_send(mockpika, mock_pikathread):
    """Unrecoverable errors during sending should lead to one reconnection attempt.
    Further errors should raise an Exception, further send attempts to try to reconnect."""

    pytest.xfail("Don't understand send failure modes yet")

    # transport = PikaTransport()
    # transport.connect()
    # mockpika.exceptions = pika.exceptions
    # mock_pikathread.send.return_value.result.side_effect = (
    #     pika.exceptions.AMQPChannelError
    # )

    # with pytest.raises(workflows.Disconnected):
    #     transport._send(str(mock.sentinel.queue), mock.sentinel.message)

    # assert not transport.is_connected()
    # assert mockconn.call_count == 2

    # mockchannel.basic_publish.side_effect = None
    # transport._send(str(mock.sentinel.queue), mock.sentinel.message)
    # assert transport.is_connected()
    # assert mockconn.call_count == 3


@mock.patch("workflows.transport.pika_transport.pika")
def test_send_broadcast(mockpika, mock_pikathread):
    """Test the broadcast sending function"""
    transport = PikaTransport()
    transport.connect()
    mockproperties = mockpika.BasicProperties

    transport._broadcast(str(mock.sentinel.exchange), mock.sentinel.message)

    mock_pikathread.send.assert_called_once()
    args, kwargs = mock_pikathread.send.call_args
    assert not args
    properties = mockproperties.call_args[1]
    assert properties.get("headers") in (None, {})
    assert kwargs == {
        "exchange": str(mock.sentinel.exchange),
        "routing_key": "",
        "body": mock.sentinel.message,
        "properties": mock.ANY,
        "mandatory": False,
        "transaction_id": None,
    }

    transport._broadcast(
        str(mock.sentinel.exchange),
        mock.sentinel.message,
        headers=mock.sentinel.headers,
    )

    mock_pikathread.send.call_count == 2
    args, kwargs = mock_pikathread.send.call_args
    assert not args
    properties = mockproperties.call_args[1]
    assert properties == {"headers": mock.sentinel.headers, "delivery_mode": 2}
    assert kwargs == {
        "exchange": str(mock.sentinel.exchange),
        "routing_key": "",
        "body": mock.sentinel.message,
        "properties": mock.ANY,
        "mandatory": False,
        "transaction_id": None,
    }

    # Delay not implemented yet
    with pytest.raises(AssertionError):
        transport._broadcast(
            str(mock.sentinel.exchange), mock.sentinel.message, delay=123
        )
    # assert mockchannel.basic_publish.call_count == 3
    # args, kwargs = mockchannel.basic_publish.call_args
    # assert not args
    # properties = mockproperties.call_args[1]
    # assert properties.get("headers").get("x-delay") == 123000
    # assert kwargs == {
    #     "exchange": str(mock.sentinel.exchange),
    #     "routing_key": "",
    #     "body": mock.sentinel.message,
    #     "properties": mock.ANY,
    #     "mandatory": False,
    # }


@mock.patch("workflows.transport.pika_transport.pika")
def test_broadcasting_message_with_expiration(mockpika, mock_pikathread):
    """Test sending a message that expires some time in the future"""
    message_lifetime = 120
    transport = PikaTransport()
    transport.connect()

    mockproperties = mockpika.BasicProperties

    transport._broadcast(
        str(mock.sentinel.exchange), mock.sentinel.message, expiration=message_lifetime
    )

    mock_pikathread.send.assert_called_once()
    args, kwargs = mock_pikathread.send.call_args
    assert not args
    assert mockproperties.return_value.expiration == str(message_lifetime * 1000)
    assert kwargs == {
        "exchange": str(mock.sentinel.exchange),
        "routing_key": "",
        "body": mock.sentinel.message,
        "properties": mock.ANY,
        "mandatory": False,
        "transaction_id": None,
    }


@mock.patch("workflows.transport.pika_transport.pika")
def test_error_handling_on_broadcast(mockpika):
    """Unrecoverable errors during broadcasting should lead to one reconnection attempt.
    Further errors should raise an Exception, further send attempts to try to reconnect."""
    pytest.xfail("Don't understand send lifecycle errors yet")
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


def test_messages_are_serialized_for_transport(mock_pikathread):
    banana = {"entry": [0, "banana"]}
    banana_str = '{"entry": [0, "banana"]}'
    transport = PikaTransport()
    transport.connect()

    transport.send(str(mock.sentinel.queue1), banana)
    mock_pikathread.send.assert_called_once()
    args, kwargs = mock_pikathread.send.call_args
    assert not args
    assert kwargs == {
        "exchange": "",
        "routing_key": str(mock.sentinel.queue1),
        "body": banana_str,
        "properties": pika.BasicProperties(delivery_mode=2, headers={}),
        "mandatory": True,
        "transaction_id": None,
    }

    transport.broadcast(str(mock.sentinel.queue2), banana)
    args, kwargs = mock_pikathread.send.call_args
    assert not args
    assert kwargs == {
        "exchange": str(mock.sentinel.queue2),
        "routing_key": "",
        "body": banana_str,
        "properties": pika.BasicProperties(delivery_mode=2, headers={}),
        "mandatory": False,
        "transaction_id": None,
    }

    with pytest.raises(TypeError):
        transport.send(str(mock.sentinel.queue), mock.sentinel.unserializable)


@mock.patch("workflows.transport.pika_transport.pika")
def test_messages_are_not_serialized_for_raw_transport(_mockpika, mock_pikathread):
    """Test the raw sending methods"""
    banana = '{"entry": [0, "banana"]}'
    transport = PikaTransport()
    transport.connect()

    transport.raw_send(str(mock.sentinel.queue1), banana)
    mock_pikathread.send.assert_called_once()
    args, kwargs = mock_pikathread.send.call_args
    assert not args
    assert kwargs == {
        "exchange": "",
        "routing_key": str(mock.sentinel.queue1),
        "body": banana,
        "mandatory": True,
        "properties": mock.ANY,
        "transaction_id": None,
    }

    mock_pikathread.send.reset_mock()
    transport.raw_broadcast(str(mock.sentinel.queue2), banana)
    mock_pikathread.send.assert_called_once()
    args, kwargs = mock_pikathread.send.call_args
    assert not args
    assert kwargs == {
        "exchange": str(mock.sentinel.queue2),
        "routing_key": "",
        "body": banana,
        "properties": mock.ANY,
        "mandatory": False,
        "transaction_id": None,
    }

    mock_pikathread.send.reset_mock()
    transport.raw_send(str(mock.sentinel.queue), mock.sentinel.unserializable)
    mock_pikathread.send.assert_called_once()
    args, kwargs = mock_pikathread.send.call_args
    assert not args
    assert kwargs == {
        "exchange": "",
        "routing_key": str(mock.sentinel.queue),
        "body": mock.sentinel.unserializable,
        "mandatory": True,
        "properties": mock.ANY,
        "transaction_id": None,
    }


def test_messages_are_deserialized_after_transport(mock_pikathread):
    """Test the message serialization."""
    banana = {"entry": [0, "banana"]}
    banana_str = '{"entry": [0, "banana"]}'
    transport = PikaTransport()
    transport.connect()

    # Test subscriptions
    callback = mock.Mock()
    transport.subscribe("queue", callback)

    mock_properties = mock.Mock()
    mock_properties.headers = {"mock_header": 1}

    # Extract the function passed to pikathread, and call it
    args, kwargs = mock_pikathread.subscribe_queue.call_args
    message_handler = kwargs["callback"]
    message_handler(
        mock.Mock(),
        mock.Mock(),
        mock_properties,
        banana_str,
    )
    callback.assert_called_once()
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == banana

    message_handler(
        mock.Mock(), mock.Mock(), mock_properties, mock.sentinel.undeserializable
    )
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == mock.sentinel.undeserializable

    # Test broadcast subscriptions
    callback = mock.Mock()
    transport.subscribe_broadcast("queue", callback)
    message_handler = mock_pikathread.subscribe_broadcast.call_args[1]["callback"]

    message_handler(mock.Mock(), mock.Mock(), mock_properties, banana_str)
    callback.assert_called_once()
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == banana

    message_handler(
        mock.Mock(), mock.Mock(), mock_properties, mock.sentinel.undeserializable
    )
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == mock.sentinel.undeserializable

    # Test subscriptions with mangling disabled
    callback = mock.Mock()
    transport.subscribe("queue", callback, disable_mangling=True)
    message_handler = mock_pikathread.subscribe_queue.call_args[1]["callback"]
    message_handler(mock.Mock(), mock.Mock(), mock_properties, banana_str)
    callback.assert_called_once()
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == banana_str

    # Test broadcast subscriptions with mangling disabled
    callback = mock.Mock()
    transport.subscribe_broadcast("queue", callback, disable_mangling=True)
    message_handler = mock_pikathread.subscribe_broadcast.call_args[1]["callback"]
    message_handler(mock.Mock(), mock.Mock(), mock_properties, banana_str)
    callback.assert_called_once()
    args, kwargs = callback.call_args
    assert not kwargs
    assert args[1] == banana_str


def test_subscribe_to_queue(mock_pikathread):
    """Test subscribing to a queue (producer-consumer), callback functions and unsubscribe"""
    transport = PikaTransport()
    transport.connect()

    mock_cb = mock.Mock()
    transport._subscribe(1, str(mock.sentinel.queue1), mock_cb)

    mock_pikathread.subscribe_queue.assert_called_once()

    args, kwargs = mock_pikathread.subscribe_queue.call_args
    assert not args
    assert kwargs == {
        "auto_ack": True,
        "callback": mock.ANY,
        "subscription_id": 1,
        "prefetch_count": 1,
        "queue": str(mock.sentinel.queue1),
        "reconnectable": False,
    }

    transport._subscribe(2, str(mock.sentinel.queue2), mock_cb)

    assert mock_pikathread.subscribe_queue.call_count == 2
    args, kwargs = mock_pikathread.subscribe_queue.call_args
    assert not args
    assert kwargs == {
        "auto_ack": True,
        "callback": mock.ANY,
        "subscription_id": 2,
        "prefetch_count": 1,
        "queue": str(mock.sentinel.queue2),
        "reconnectable": False,
    }

    transport._subscribe(3, str(mock.sentinel.queue3), mock_cb, acknowledgement=True)
    assert mock_pikathread.subscribe_queue.call_count == 3
    args, kwargs = mock_pikathread.subscribe_queue.call_args
    assert not args
    assert kwargs == {
        "auto_ack": False,
        "callback": mock.ANY,
        "subscription_id": 3,
        "prefetch_count": 1,
        "queue": str(mock.sentinel.queue3),
        "reconnectable": False,
    }

    transport._unsubscribe(1)
    mock_pikathread.unsubscribe.assert_called_once_with(1)
    transport._unsubscribe(2)
    mock_pikathread.unsubscribe.assert_called_with(2)


def test_subscribe_to_broadcast(mock_pikathread):
    """Test subscribing to a queue (producer-consumer), callback functions and unsubscribe"""
    mock_cb = mock.Mock()
    transport = PikaTransport()
    transport.connect()

    transport._subscribe_broadcast(1, str(mock.sentinel.queue1), mock_cb)

    mock_pikathread.subscribe_broadcast.assert_called_once()
    args, kwargs = mock_pikathread.subscribe_broadcast.call_args
    assert not args
    assert kwargs == {
        "callback": mock.ANY,
        "exchange": str(mock.sentinel.queue1),
        "subscription_id": 1,
        "reconnectable": False,
    }

    transport._subscribe_broadcast(
        2,
        str(mock.sentinel.queue2),
        mock_cb,
    )

    assert mock_pikathread.subscribe_broadcast.call_count == 2
    args, kwargs = mock_pikathread.subscribe_broadcast.call_args
    assert not args
    assert kwargs == {
        "callback": mock.ANY,
        "exchange": str(mock.sentinel.queue2),
        "subscription_id": 2,
        "reconnectable": False,
    }

    transport._unsubscribe(1)
    mock_pikathread.unsubscribe.assert_called_once_with(1)
    transport._unsubscribe(2)
    mock_pikathread.unsubscribe.assert_called_with(2)


@mock.patch("workflows.transport.pika_transport.pika")
def test_error_handling_on_subscribing(mockpika, mock_pikathread):
    """Unrecoverable errors during subscribing should mark the connection as disconnected."""
    mock_cb = mock.Mock()

    transport = PikaTransport()
    transport.connect()
    mockpika.exceptions = pika.exceptions
    mock_pikathread.connection_alive = False
    mock_pikathread.subscribe_queue.return_value.result.side_effect = (
        pika.exceptions.AMQPChannelError
    )

    with pytest.raises(workflows.Disconnected):
        transport._subscribe(1, str(mock.sentinel.queue1), mock_cb)
    assert not transport.is_connected()

    # Now try connectionError instead of ChannelError
    mock_pikathread.subscribe_queue.return_value.result.side_effect = (
        pika.exceptions.AMQPConnectionError
    )

    with pytest.raises(workflows.Disconnected):
        transport._subscribe(1, str(mock.sentinel.queue1), mock_cb)
    assert not transport.is_connected()


@mock.patch("workflows.transport.pika_transport.pika")
def test_transaction_calls(mockpika):
    """Test that calls to create, commit, abort transactions are passed to Pika properly."""
    pytest.xfail("Transactions not implemented in pika transport yet")
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


def test_ack_message(mock_pikathread):
    transport = PikaTransport()
    transport.connect()

    transport._ack(mock.sentinel.messageid, mock.sentinel.sub_id)
    mock_pikathread.ack.assert_called_once_with(
        mock.sentinel.messageid,
        mock.sentinel.sub_id,
        multiple=False,
        transaction_id=None,
    )


def test_nack_message(mock_pikathread):
    """Test that the _nack function call is properly forwarded to pika"""
    transport = PikaTransport()
    transport.connect()

    transport._nack(mock.sentinel.messageid, mock.sentinel.sub_id)

    mock_pikathread.nack.assert_called_once_with(
        mock.sentinel.messageid,
        mock.sentinel.sub_id,
        multiple=False,
        requeue=True,
        transaction_id=None,
    )


@pytest.fixture(scope="session")
def connection_params():
    """Connection Parameters for connecting to a physical RabbitMQ server"""
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
        bc = pika.BlockingConnection(params)
    except BaseException:
        pytest.skip("Failed to create test RabbitMQ connection")
    bc.close()
    return params


@pytest.fixture
def test_channel(connection_params) -> pika.channel.Channel:
    """Convenience fixture to give a channel-like object with helpful methods for testing."""
    conn = pika.BlockingConnection(connection_params)
    try:

        class _test_channel:
            def __init__(self, channel):
                self.channel = channel
                self._on_close = []

            def __getattr__(self, name):
                return getattr(self.channel, name)

            def temporary_exchange_declare(self, auto_delete=False, **kwargs):
                """
                Declare an auto-named exchange that is automatically deleted on test end.

                This differs from auto_delete in that even if auto_delete=False,
                an attempt to auto-delete the exchange will be made on test-teardown.
                """
                exchange_name = "_test_pika_" + str(uuid.uuid4())
                self.channel.exchange_declare(
                    exchange_name, auto_delete=auto_delete, **kwargs
                )
                # If the server won't clean this up, we need to
                if not auto_delete:
                    self.register_cleanup(
                        lambda: self.channel.exchange_delete(exchange_name)
                    )
                return exchange_name

            def temporary_queue_declare(
                self, auto_delete=False, exclusive=False, exchange="", **kwargs
            ):
                """
                Declare an auto-named queue that is automatically deleted on test end.
                """
                queue = self.channel.queue_declare(
                    "", auto_delete=auto_delete, exclusive=exclusive, **kwargs
                ).method.queue
                if not (auto_delete or exclusive):
                    self.register_cleanup(lambda: self.channel.queue_delete(queue))
                if exchange:
                    self.channel.queue_bind(queue, exchange)
                print(f"Got temporary queue for testing: {queue}")
                return queue

            def register_cleanup(self, task):
                caller = inspect.getframeinfo(inspect.stack()[1][0])
                self._on_close.append((caller.filename, caller.lineno, task))

        channel = _test_channel(conn.channel())
        channel.confirm_delivery()
        try:
            yield channel
        finally:
            # Make an attempt to run all of the shutdown tasks
            for (filename, lineno, task) in reversed(channel._on_close):
                try:
                    print(f"Cleaning up from {filename}:{lineno}")
                    task()
                except BaseException as e:
                    print(
                        f"Encountered error cleaning up test channel, cleanup from {filename}:{lineno} may not be complete: {e}",
                    )
    finally:
        conn.close()


def test_pikathread(connection_params):
    thread = _PikaThread(connection_params)
    thread.start()
    print("Waiting for pika connection")
    thread.wait_for_connection()
    print("stopping connection")
    thread.join(re_raise=True, stop=True)


def test_pikathread_broadcast_subscribe(connection_params, test_channel):
    thread = _PikaThread(connection_params)
    thread.start()
    thread.wait_for_connection()

    exchange = test_channel.temporary_exchange_declare(
        exchange_type="fanout", auto_delete=True
    )

    # Basic check that a message has come through
    got_message = threading.Event()

    def _callback(channel, basic_deliver, properties, body):
        print(
            f"Got message:\n  Channel: {channel.channel_number}\n  basic_deliver: {basic_deliver}\n  properties: {properties}\n\n  {body.decode()}"
        )
        got_message.set()

    # Make a subscription and wait for it to be valid
    thread.subscribe_broadcast(
        exchange, _callback, subscription_id=1, reconnectable=True
    ).result()

    test_channel.basic_publish(exchange, routing_key="", body="A Message")

    got_message.wait(5)
    assert got_message.is_set()

    thread.join(re_raise=True, stop=True)


def test_pikathread_broadcast_reconnection(
    connection_params, test_channel: pika.channel.Channel
):
    thread = _PikaThread(connection_params)
    try:
        thread.start()
        thread.wait_for_connection()

        got_message = threading.Event()

        def _got_message(*args):
            got_message.set()

        exchange = test_channel.temporary_exchange_declare(exchange_type="fanout")
        thread.subscribe_broadcast(
            exchange, _got_message, reconnectable=True, subscription_id=1
        ).result()

        # Force reconnection - normally we want this to be transparent, but
        # let's twiddle the internals so we can wait for reconnection as we
        # don't want to pick up the message before it resets
        print("Terminating connection")
        thread._connected.clear()
        thread._debug_close_connection()
        thread.wait_for_connection()
        print("Reconnected")
        # Now, make sure we still get this message
        test_channel.basic_publish(exchange, routing_key="", body="A Message")
        got_message.wait(2)
        assert got_message.is_set()

        # Add a non-resubscribable connection
        got_message_2 = threading.Event()

        def _got_message_2(*args):
            got_message_2.set()

        thread.subscribe_broadcast(
            exchange,
            _got_message_2,
            reconnectable=False,
            subscription_id=2,
        ).result()

        # Make sure that the thread ends instead of reconnect if we force a disconnection
        thread._debug_close_connection()
        thread.join()

    finally:
        thread.join(stop=True)


def test_pikathread_subscribe_queue(connection_params, test_channel):
    queue = test_channel.temporary_queue_declare()
    thread = _PikaThread(connection_params)
    try:
        thread.start()
        thread.wait_for_connection()

        messages = Queue()

        def _get_message(*args):
            print(f"Got message: {pprint.pformat(args)}")
            messages.put(args[3])

        thread.subscribe_queue(
            queue, _get_message, reconnectable=True, subscription_id=1
        )
        test_channel.basic_publish("", queue, "This is a message")
        assert messages.get(timeout=2) == b"This is a message"

        print("Terminating connection")
        thread._connected.clear()
        thread._debug_close_connection()
        thread.wait_for_connection()
        print("Reconnected")
        test_channel.basic_publish("", queue, "This is another message")
        assert messages.get(timeout=2) == b"This is another message"

    finally:
        thread.join(stop=True)


def test_pikathread_send(connection_params, test_channel):
    queue = test_channel.temporary_queue_declare(exclusive=True)
    thread = _PikaThread(connection_params)
    try:
        thread.start()
        thread.wait_for_connection()
        thread.send("", queue, "Test Message").result()

        assert test_channel.basic_get(queue, auto_ack=True)[2] == b"Test Message"

        # Make sure we don't have the missing queue declared
        with pytest.raises(pika.exceptions.ChannelClosedByBroker):
            check_channel = test_channel.connection.channel()
            check_channel.queue_declare("unroutable-missing-queue", passive=True)

        # Should not fail with mandatory=False
        thread.send(
            "", "unroutable-missing-queue", "Another Message", mandatory=False
        ).result()

        # But should fail with it declared
        pytest.xfail("UnroutableError is not raised without publisher confirms, #96")
        with pytest.raises(pika.exceptions.UnroutableError):
            thread.send(
                "", "unroutable-missing-queue", "Another Message", mandatory=True
            ).result()

    finally:
        thread.join(stop=True)


def test_pikathread_send_named_exchange(connection_params, test_channel):
    exchange = test_channel.temporary_exchange_declare(
        exchange_type="topic", auto_delete=True
    )
    queue = test_channel.temporary_queue_declare(exchange=exchange)
    thread = _PikaThread(connection_params)
    try:
        thread.start()
        thread.wait_for_connection()
        thread.send(exchange, queue, "Test Message").result()
        assert test_channel.basic_get(queue, auto_ack=True)[2] == b"Test Message"
    finally:
        thread.join(stop=True)


def test_pikathread_bad_conn_params(connection_params):
    # Ensure that a bad connection parameter fails immediately
    bad_params = [copy.copy(connection_params[0])]
    bad_params[0].port = 1
    thread = _PikaThread(bad_params)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()

    with pytest.raises(pika.exceptions.AMQPConnectionError):
        thread.raise_if_exception()

    # Start a working connection
    params = [copy.copy(connection_params[0])]
    thread = _PikaThread(params)
    thread.start()
    thread.wait_for_connection()
    # Forcibly cause a failure of reconnection by editing the inner connection dict
    # - otherwise, we can't guarantee that it will do them in the right order
    thread._connection_parameters[0].port = 1
    # Make sure it will fail on the next attempt
    thread._reconnection_attempt_limit = 1
    # Force a reconnection
    thread._debug_close_connection()
    # Wait for it to do this reconnection wait of 1s
    thread.join(timeout=5, stop=False)
    assert not thread.is_alive()


def test_pikathread_unsubscribe(test_channel, connection_params):
    queue = test_channel.temporary_queue_declare()
    thread = _PikaThread(connection_params)
    try:
        thread.start()
        thread.wait_for_connection()

        messages = Queue()

        def _get_message(*args):
            print(f"Got message: {pprint.pformat(args)}")
            messages.put(args[3])

        thread.subscribe_queue(
            queue, _get_message, reconnectable=True, subscription_id=1
        )
        test_channel.basic_publish("", queue, "This is a message")
        assert messages.get(timeout=1) == b"This is a message"

        # Issue an unsubscribe then wait for confirmation
        thread.unsubscribe(1).result()

        # Send a message again
        test_channel.basic_publish("", queue, "This is a message again")
        # Wait a short time to make sure it does not get delivered
        with pytest.raises(Empty):
            messages.get(timeout=1)

    finally:
        thread.join(stop=True)


def test_pikathread_ack_transaction(test_channel, connection_params):

    queue = test_channel.temporary_queue_declare()
    thread = _PikaThread(connection_params)
    try:
        thread.start()
        thread.wait_for_connection()

        messages = Queue()

        def _get_message(channel, method_frame, header_frame, body):
            print(
                f"Received message delivery_tag={method_frame.delivery_tag} body={body}"
            )
            messages.put((method_frame.delivery_tag, body))

        thread.subscribe_queue(
            queue,
            _get_message,
            reconnectable=False,
            subscription_id=1,
            auto_ack=False,
        ).result()

        # Send a message and then ack it within a transaction
        transaction_id = 0
        for i in range(10):
            transaction_id += 1
            test_channel.basic_publish("", queue, "ack")
            delivery_tag, body = messages.get(timeout=1)
            assert body == b"ack"
            thread.tx_select(transaction_id=transaction_id, subscription_id=1).result()
            thread.ack(
                delivery_tag=delivery_tag,
                subscription_id=1,
                transaction_id=transaction_id,
            )
            thread.tx_commit(transaction_id=transaction_id)

        # Wait a short time to make sure no messages have been delivered
        with pytest.raises(Empty):
            messages.get(timeout=1)

    finally:
        thread.join(stop=True)


def test_pikathread_nack_transaction(test_channel, connection_params):

    queue = test_channel.temporary_queue_declare()
    thread = _PikaThread(connection_params)
    try:
        thread.start()
        thread.wait_for_connection()

        messages = Queue()

        def _get_message(channel, method_frame, header_frame, body):
            print(
                f"Received message delivery_tag={method_frame.delivery_tag} body={body}"
            )
            messages.put((method_frame.delivery_tag, body))

        thread.subscribe_queue(
            queue,
            _get_message,
            reconnectable=False,
            subscription_id=1,
            auto_ack=False,
            prefetch_count=10,
        ).result()

        # Send 10 message, start a transaction, nack them all, commit transaction
        for i in range(10):
            test_channel.basic_publish("", queue, "nack")
        transaction_id = 0
        thread.tx_select(transaction_id=transaction_id, subscription_id=1).result()
        for i in range(10):
            delivery_tag, body = messages.get(timeout=1)
            assert body == b"nack"
            thread.nack(
                delivery_tag=delivery_tag,
                subscription_id=1,
                transaction_id=transaction_id,
                requeue=False,
            )
        thread.tx_commit(transaction_id=transaction_id)

        # Wait a short time to make sure no messages have been delivered
        with pytest.raises(Empty):
            messages.get(timeout=1)

    finally:
        thread.join(stop=True)


def test_pikathread_tx_rollback_nack(test_channel, connection_params):

    queue = test_channel.temporary_queue_declare()
    thread = _PikaThread(connection_params)
    try:
        thread.start()
        thread.wait_for_connection()

        messages = Queue()

        def _get_message(channel, method_frame, header_frame, body):
            print(
                f"Received message delivery_tag={method_frame.delivery_tag} body={body}"
            )
            messages.put((method_frame.delivery_tag, body))

        thread.subscribe_queue(
            queue,
            _get_message,
            reconnectable=False,
            subscription_id=1,
            auto_ack=False,
        ).result()

        # Send a message, start a transaction, ack it and then rollback, followed by a nack
        transaction_id = 0
        for i in range(10):
            transaction_id += 1
            test_channel.basic_publish("", queue, "nack")
            delivery_tag, body = messages.get(timeout=1)
            assert body == b"nack"
            thread.tx_select(transaction_id=transaction_id, subscription_id=1).result()
            thread.ack(
                delivery_tag=delivery_tag,
                subscription_id=1,
                transaction_id=transaction_id,
            )
            thread.tx_rollback(transaction_id=transaction_id).result()
            thread.nack(
                delivery_tag=delivery_tag,
                subscription_id=1,
                transaction_id=None,
                requeue=False,
            )

        # Wait a short time to make sure no messages have been delivered
        with pytest.raises(Empty):
            messages.get(timeout=1)

    finally:
        thread.join(stop=True)


def test_full_stack_temporary_queue_roundtrip(pikatransport):
    known_subscriptions = set()
    known_queues = set()

    def assert_not_seen_before(ts: TemporarySubscription):
        assert ts.subscription_id, "Temporary subscription is missing an ID"
        assert (
            ts.subscription_id not in known_subscriptions
        ), "Duplicate subscription ID"
        assert ts.queue_name, "Temporary queue does not have a name"
        assert ts.queue_name not in known_queues, "Duplicate temporary queue name"
        known_subscriptions.add(ts.subscription_id)
        known_queues.add(ts.queue_name)
        print(f"Temporary subscription: {ts}")

    replies = Queue()

    def callback(subscription):
        def _callback(header, message):
            print(f"Received message for {subscription}: {message}")
            replies.put((subscription, header, message))

        return _callback

    ts = {}
    for n, queue_hint in enumerate(
        ("", "", "hint", "hint", "transient.hint", "transient.hint")
    ):
        ts[n] = pikatransport.subscribe_temporary(queue_hint, callback(n))
        assert_not_seen_before(ts[n])
        assert queue_hint in ts[n].queue_name
        assert "transient.transient." not in ts[n].queue_name
        if not queue_hint:
            assert ts[n].queue_name.startswith("amq.gen-")

    assert replies.empty()

    outstanding_messages = set()
    for n in range(6):
        outstanding_messages.add((n, f"message {n}"))
        pikatransport.send(ts[n].queue_name, f"message {n}")

    try:
        while outstanding_messages:
            s, _, m = replies.get(timeout=1.5)
            if (s, m) not in outstanding_messages:
                raise RuntimeError(
                    f"Received unexpected message {m} on subscription {s}"
                )
            outstanding_messages.remove((s, m))
    except Empty:
        raise RuntimeError(f"Missing replies for {len(outstanding_messages)} messages")
