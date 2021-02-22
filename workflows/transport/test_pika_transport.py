import argparse
#import importlib
#import inspect
#import json
import optparse
import os
import tempfile
from unittest import mock
import pytest
import pika as pikapy
from transport.pika_transport import PikaTransport


def test_lookup_and_initialize_pika_transport_layer():
    """ Find the pika transport layer via the lookup mechanism and
    run its constructor with default settings
    """
    #pika = workflows.transport.lookup("PikaTransport")
    #assert pika == PikaTransport
    #pika()
    print("This testing is not implemented yet")

def test_add_command_line_help_optparse():
    """ Check that command line parameters are registered in the parser."""
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
#    for call in parser.add_argument.call_args_list:
#        assert inspect.isclass(call[1]["action"])


def test_adding_arguments_to_argparser():
    """Check that command line parameters can be added to the parser."""
    parser = argparse.ArgumentParser()

    PikaTransport().add_command_line_options(parser)

    result = parser.parse_args([])
    assert result.rabbit_host
    assert result.rabbit_port
    assert result.rabbit_user
    assert result.rabbit_pass

#@mock.patch("workflows.transport.pika_transport.pika")
@mock.patch("transport.pika_transport.pika")
def test_check_config_file_behaviour(mockpika):
    """Check that a specified configuration file is read, that command line
    parameters have precedence and are passed on to the stomp layer."""
    mockconn = mock.Mock()
    mockpika.BlockingConnection.return_value = mockconn

    parser = optparse.OptionParser()
    pika = PikaTransport()
    pika.add_command_line_options(parser)

    cfgfile = tempfile.NamedTemporaryFile(delete=False)
    try:
        cfgfile.write(
            """
# An example pika configuration file
# Only lines in the [pika] block will be interpreted

[rabbit]
#host = cs04r-sc-vserv-253
port = 5672
username = myuser
password = somesecret
""".encode(
                "utf-8"
            )
        )
        cfgfile.close()

        parser.parse_args(
            ["--rabbit-conf", cfgfile.name, "--rabbit-user", mock.sentinel.user]
        )

        pika = PikaTransport()
        pika.connect()

        #Reset configuration for subsequent tests by reloading PikaTransport
        #importlib.reload(workflows.transport.pika_transport)
        #globals()["PikaTransport"] = workflows.transport.pika_transport.PikaTransport

        #mockpika.BlockingConnection.assert_called_once_with([("localhost", 1234)])
        #Actual call: BlockingConnection(<MagicMock name='pika.ConnectionParameters()' id='140687855350392'>)

        mockpika.BlockingConnection.assert_called_once()
        #mockpika.BlockingConnection.assert_called_once_with(
        #    mock.sentinel.user, "somesecret", wait=False)

        # mockconn.connect.assert_called_once_with(
        #   mock.sentinel.user, "somecret", wait=False
        #)
        #assert pika.get_namespace() == "namespace"

    finally:
        os.remove(cfgfile.name)

    #with pytest.raises(Exception):
    #    parser.parse_args(["--rabbit-conf", ""])

@mock.patch("workflows.transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.pika")
def test_anonymous_connection(mockpika):
    """ check that a specified configuration file is read, that command line
    parameters have precedence and are passed on the pika layer"""
    mockconn = mock.Mock()
    #maybe this not used
    mockpika.BlockingConnection.return_value = mockconn
    parser = optparse.OptionParser()
    pika = PikaTransport()
    pika.add_command_line_options(parser)

    parser.parse_args(["--rabbit-user=", "--rabbit-pass="])

    pika = PikaTransport()
    pika.connect()

    # Reset configuration for subsequent tests by reloading StompTransport
    #importlib.reload(workflows.transport.pika_transport)
    #globals()["PikaTransport"] = workflows.transport.pika_transport.PikaTransport

    mockpika.BlockingConnection.assert_called_once()
    #mockconn.connect.assert_called_once_with(wait=False)


@mock.patch("workflows.transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.pika")
def test_instantiate_link_and_connect_to_broker(mockpika):
    """Test the pika connection routine."""
    pika = PikaTransport()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    assert not pika.is_connected()

    pika.connect()

    mockpika.BlockingConnection.assert_called_once()
    #mockconn.channel.assert_called_once()
    #mockchannel.connect.assert_called_once() #not connect method
    assert pika.is_connected()

    pika.connect()

    mockpika.BlockingConnection.assert_called_once()
    #mockconn.connect.assert_called_once()
    assert pika.is_connected()

    pika.disconnect()

    mockpika.BlockingConnection.assert_called_once()
    mockchannel.close.assert_called_once()
    #closed is called by mockchannel but not mockconn
    #mockconn.close.assert_called_once()
    #mockconn.connect.assert_called_once()
    #mockconn.disconnect.assert_called_once()
    assert not pika.is_connected()

    pika.disconnect()

    mockpika.BlockingConnection.assert_called_once()
    mockchannel.close.assert_called_once()
    #mockconn.connect.assert_called_once()
    #mockconn.disconnect.assert_called_once()
    assert not pika.is_connected()

@mock.patch("workflows.transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.pika")
def test_error_handling_when_connecting_to_broker(mockpika):
    """ Test the Pika connection routine"""
    pika = PikaTransport()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockpika.BlockingConnection.side_effect = pikapy.exceptions.AMQPConnectionError()
    mockpika.exceptions.AMQPConnectionError = (
        pikapy.exceptions.AMQPConnectionError
    )
    
    # with pytest.raises(workflows.Disconnected)
    with pytest.raises(Exception):
        pika.connect()

    assert not pika.is_connected()

@mock.patch("workflows.transport.pika_transport.time")
@mock.patch("workflows.transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.time")
#@mock.patch("transport.pika_transport.pika")
def test_broadcast_status(mockpika, mocktime):
    """Test the status broadcast function."""
    mocktime.time.return_value = 20000
    pika = PikaTransport()
    pika.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties
    #channel lower case

    pika.broadcast_status({"status": str(mock.sentinel.status)})

    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    properties = mockproperties.call_args[1]
    headers = properties.get('headers')
    expiration = headers.get('x-message-ttl')
    #delay = headers.get('x-delay')

    assert int(expiration) == 1000 * (20000 + 15)
    destination = kwargs.get('exchange')
    message = kwargs.get('body')
    # assert destination.startswith("/topic/transient.status")
    assert destination.startswith("transient.status")
    statusdict = json.loads(message)
    assert statusdict["status"] == str(mock.sentinel.status)

@mock.patch("workflows.transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.pika")
def test_send_message(mockpika):
    """Test the message sending function"""
    pika = PikaTransport()
    pika.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties

    pika._send(str(mock.sentinel.exchange), mock.sentinel.message)

    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    # assert args == ("/queue/" + str(mock.sentinel.channel), mock.sentinel.message)
    # keep persistent in headers?
    # assert kwargs.get("headers") == {"persistent": "true"}

    # only destination setup, difference between exchange and queue
    pika._send(
        #str(mock.sentinel.exchange),
        str(mock.sentinel.queue),
        mock.sentinel.message,
        headers={"hdr": mock.sentinel.header},
        delay=123
    )

    assert mockchannel.basic_publish.call_count == 2
    args, kwargs = mockchannel.basic_publish.call_args
    #assert args == ("/queue/ + str(mock.sentinel.channel), mock.sentinel.message)
    #asserting kwargs MagicMock properties?
    #check x-delay is in headers

@mock.patch("workflows.transport.pika_transport.pika")
@mock.patch("workflows.transport.pika_transport.time")
def test_sending_message_with_expiration(time, mockpika):
    """Test sending a message that expires some time in the future."""
    system_time = 1234567.1234567
    message_lifetime = 120
    expiration_time = int((system_time + message_lifetime) * 1000)
    time.time.return_value = system_time

    pika = PikaTransport()
    pika.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties

    pika._send(str(mock.sentinel.channel), mock.sentinel.message, expiration=120)

    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    #assert args == ("/queue/" + str(mock.sentinel.channel), mock.sentinel.message)
    properties = mockproperties.call_args[1]
    assert properties.get('headers') == {"x-message-ttl": expiration_time}

@mock.patch("workflows.transport.pika_transport.pika")
##@mock.patch("transport.pika_transport.pika")
def test_error_handling_on_send(mockpika):
    """Unrecoverable errors during sending should mark the connection as disconnected"""
    pika = PikaTransport()
    pika.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    #AMQPConnectionError
    #ChannelClosed
    #ConnectionClosed
    mockchannel.basic_publish.side_effect = pikapy.exceptions.AMQPConnectionError
    mockpika.exception = pikapy.exceptions

    with pytest.raises(Exception):
        pika._send(str(mock.sentinel.queue), mock.sentinel.message)

    #assert not pika.is_connected()

@mock.patch("workflows.transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.pika")
def test_send_broadcast(mockpika):
    """Test the broadcast sending function"""
    pika = PikaTransport()
    pika.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties

    pika._broadcast(str(mock.sentinel.channel), mock.sentinel.message)

    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    properties = mockproperties.call_args[1]
    # assert args == ("/topic/" + str(mock.sentinel.channel), mock.sentinel.message)
    assert properties.get('headers') in (None, {})

    pika._broadcast(
        str(mock.sentinel.channel), mock.sentinel.message, headers=mock.sentinel.headers)

    assert mockchannel.basic_publish.call_count == 2
    args, kwargs = mockchannel.basic_publish.call_args
    #assert args == ("/topic/" + str(mock.sentinel.channel), mock.sentinel.message)
    properties = mockproperties.call_args[1]
    assert properties == {"headers": mock.sentinel.headers, 'delivery_mode': 2}
    #assert kwargs == {"headers": mock.sentinel.headers}

    pika._broadcast(str(mock.sentinel.channel), mock.sentinel.message, delay=123)
    assert mockchannel.basic_publish.call_count == 3
    args, kwargs = mockchannel.basic_publish.call_args
    #assert args == ("/topic/" + str(mock.sentinel.channel), mock.sentinel.message)
    properties = mockproperties.call_args[1]
    assert properties.get("headers").get("x-delay") == 123000

@mock.patch("workflows.transport.pika_transport.pika")
@mock.patch("workflows.transport.pika_transport.time")
#@mock.patch("transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.time")
def test_broadcasting_message_with_expiration(time, mockpika):
    """Test sending a message that expires some time in the future"""
    system_time = 1234567.1234567
    message_lifetime = 120
    expiration_time = int((system_time + message_lifetime) * 1000)
    time.time.return_value = system_time

    pika = PikaTransport()
    pika.connect()

    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockproperties = mockpika.BasicProperties

    pika._broadcast(str(mock.sentinel.channel), mock.sentinel.message, expiration=120)

    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    #assert args == ("/topic/" + str(mock.sentinel.channel), mock.sentinel.message)
    properties = mockproperties.call_args[1]
    assert properties.get("headers").get("x-message-ttl") == expiration_time
    assert properties.get("headers") == {"x-message-ttl": expiration_time}
    #assert kwargs.get("headers") == {"expires": expiration_time}

@mock.patch("workflows.transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.pika")
def test_error_handling_on_broadcast(mockpika):
    """Unrecoverable errors during broadcasting should mark the connection as disconnected."""
    pika = PikaTransport()
    pika.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value
    mockchannel.basic_publish.side_effect = pikapy.exceptions.ChannelClosed(404, "Channel closed")
    mockpika.exception = pikapy.exceptions

    #with pytest.raises(pikapy.exceptions.ChannelClosed):
    #    pika._broadcast(str(mock.sentinel.channel), mock.sentinel.message)
    #assert not stomp.is_connected()

@mock.patch("workflows.transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.pika")
def test_messages_are_serialized_for_transport(mockpika):
    banana = {"entry": [0, "banana"]}
    banana_str = "{'entry': [0, 'banana']}"
    pika = PikaTransport()
    pika.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    pika._send(str(mock.sentinel.channel1), banana)

    mockchannel.basic_publish.assert_called_once()
    args, kwargs = mockchannel.basic_publish.call_args
    message = kwargs.get("body")
    #assert message == banana_str
    #assert args == ("/queue/" + str(mock.sentinel.channel1), banana_str)

    #pika.broadcast(str(mock.sentinel.channel2), banana)
    #args, kwargs = mockchannel.basic_publish.call_args
    #assert args == ("/topic/" + str(mock.sentinel.channel2), banana_str)

    #with pytest.raises(Exception):
    #    pika.send(str(mock.sentinel.channel), mock.sentinel.unserializable)


#test_messages_are_not_serialized_for_raw_transport
#test_messages_are_deserialized_after_transport

@mock.patch("workflows.transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.pika")
def test_subscribe_to_queue(mockpika):
    """Test subscribing to a queue (producer-consumer), callback functions and unsubscribe"""
    mock_cb1 = mock.Mock()
    mock_cb2 = mock.Mock()
    pika = PikaTransport()
    pika.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    def callback_resolver(cbid):
        if cbid == 1:
            return mock_cb1
        if cbid == 2:
            return mock_cb2
        raise ValueError("Unknown consumer tag %r" % cbid)

    pika.subscription_callback = callback_resolver

    #listener function not applicable to this
    #mockconn.set_listener.assert_called_once()
    #listener = mockconn.set_listener.call_args[0][1]
    #assert listener is not None

    pika._subscribe(
        1,
        str(mock.sentinel.queue1),
        mock_cb1,
        transformation=mock.sentinel.transformation
    )

    mockchannel.basic_consume.assert_called_once()
    args, kwargs = mockchannel.basic_consume.call_args
    #assert args == ("/queue/" + str(mock.sentinel.channel1),1)
    #assert kwargs == {
    #    "headers": {"transformation": mock.sentinel.transformation},
    #    "ack": "auto",
    #}

    pika._subscribe(
        2,
        str(mock.sentinel.queue2),
        mock_cb2,
        retroactive=True,
        selector=mock.sentinel.selector,
        exclusive=True,
        transformation=True,
        priority=42,
    )

    assert mockchannel.basic_consume.call_count == 2
    args, kwargs = mockchannel.basic_consume.call_args
    """
    assert args == ("/queue/" + str(mock.sentinel.channel2), 2)
    assert kwargs == {
        "headers": {
            "activemq.retroactive": "true",
            "selector": mock.sentinel.selector,
            "activemq.exclusive": "true",
            "transformation": "jms-object-json",
            "activemq.priority": 42,
        },
        "ack": "auto",
    }
    """

    assert mock_cb1.call_count == 0
    #listener.on_message({"subscription": 1}, mock.sentinel.message1)
    #mock_cb1.assert_called_once_with({"subscription": 1}, mock.sentinel.message)

    assert mock_cb2.call_count == 0
    #listener.on_message({"subscription": 2}, mock.sentinel.message2)
    #mock_cb2.assert_called_once_with({"subscription": 2}, mock.sentinel.message2)

    pika._subscribe(3, str(mock.sentinel.queue3), mock_cb2, acknowledgement=True)
    assert mockchannel.basic_consume.call_count == 3
    args, kwargs = mockchannel.basic_consume.call_args
    #Not client invididual
    #assert args == ("/queue/" + str(mock.sentinel.channel3), 3)
    #assert kwargs == {"headers": {}, "ack": "client-individual"}

    pika._unsubscribe(1)
    mockchannel.basic_cancel.assert_called_once_with(callback=None, consumer_tag=1)
    pika._unsubscribe(2)
    mockchannel.basic_cancel.assert_called_with(callback=None, consumer_tag=2)

#subscribe_to_broadcasts

"""
Error when calling this function
#mock.patch("workflows.transport.pika_transport.pika")
@mock.patch("transport.pika_transport.pika")
def test_transaction_calls(mockpika):
    pika = PikaTransport()
    pika.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    #txid not in argument?
    #select vs begin?
    pika._transaction_begin()
    mockchannel.tx_select.assert_called_once()

    #pika._send("destination", mock.sentinel.message, transaction=mock.sentinel.txid)
    #mockchannel.basic_publish.assert_called_once_with(
    #    'destination',
    #    mock.sentinel.message,
    #    headers={"persistent": "true"},
    #    transaction=mock.sentinel.txid
    #)

    pika._transaction_abort()
    mockchannel.tx_rollback.assert_called_once()

    pika._transaction_commit()
    mockchannel.tx_commit.assert_called_once()
"""

@mock.patch("workflows.transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.pika")
def test_ack_message(mockpika):
    """Test that the _ack function is properly forwarded to pika"""
    pika = PikaTransport()
    pika.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    consumer_tag = pika._subscribe(1, str(mock.sentinel.channel3), None, acknowledgement=True)
    #consumer_tag unused
    pika._ack(mock.sentinel.messageid)
    #basic_ack(delivery_tag=0, multiple=False)
    #no possible arguments for transaction or consumer_tag
    mockchannel.basic_ack.assert_called_once_with(delivery_tag=mock.sentinel.messageid)

@mock.patch("workflows.transport.pika_transport.pika")
#@mock.patch("transport.pika_transport.pika")
def test_nack_message(mockpika):
    """Test that the _ack function is properly forwarded to pika"""
    pika = PikaTransport()
    pika.connect()
    mockconn = mockpika.BlockingConnection
    mockchannel = mockconn.return_value.channel.return_value

    consumer_tag = pika._subscribe(1, str(mock.sentinel.channel3), None, acknowledgement=True)
    #consumer_tag unused
    pika._nack(mock.sentinel.messageid)
    #basic_ack(delivery_tag=0, multiple=False)
    #no possible arguments for transaction or consumer_tag
    mockchannel.basic_nack.assert_called_once_with(delivery_tag=mock.sentinel.messageid)





