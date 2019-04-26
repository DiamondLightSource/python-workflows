from __future__ import absolute_import, division, print_function

import mock
import pytest
import workflows
import workflows.recipe


def check_message_handling_via_unwrapper(
    callback, recipient, transport, rw_mock, allow_non_recipe
):
    """Test callback function of a recipe wrapper."""

    # This message does not contain an encoded recipe. It should be passed through directly.
    header = {"random-header": mock.sentinel.ID}
    message = mock.Mock()
    recipient.reset_mock()
    transport.reset_mock()
    callback(header, message)
    if allow_non_recipe:
        recipient.assert_called_once_with(None, header, message)
        transport.nack.assert_not_called()
    else:
        transport.nack.assert_called_once_with(header)
        recipient.assert_not_called()

    # This message does not contain an encoded recipe. It should be passed through directly.
    header = {"workflows-recipe": "False"}
    recipient.reset_mock()
    transport.reset_mock()
    callback(header, message)
    if allow_non_recipe:
        recipient.assert_called_once_with(None, header, message)
        transport.nack.assert_not_called()
    else:
        transport.nack.assert_called_once_with(header)
        recipient.assert_not_called()

    # This message contains an encoded recipe. It should be interpreted and the payload passed
    # through with a helper object for simple recipe-conformant replies.
    header = {"workflows-recipe": "True"}
    message = {
        "recipe": mock.sentinel.recipe,
        "recipe-pointer": mock.sentinel.recipe_pointer,
        "recipe-path": [],
        "environment": {
            "ID": mock.sentinel.GUID,
            "source": mock.sentinel.source,
            "timestamp": mock.sentinel.timestamp,
        },
        "payload": mock.sentinel.payload,
    }
    recipient.reset_mock()
    transport.reset_mock()
    callback(header, message)

    recipient.assert_called_once_with(rw_mock.return_value, header, message["payload"])
    rw_mock.assert_called_once_with(message=message, transport=transport)
    transport.nack.assert_not_called()


@mock.patch("workflows.recipe.RecipeWrapper", autospec=True)
def test_wrapping_a_subscription(rw_mock):
    """Test queue subscription with recipe wrapper."""
    transport, recipient = mock.Mock(), mock.Mock()

    sid = workflows.recipe.wrap_subscribe(
        transport,
        mock.sentinel.channel,
        recipient,
        mock.sentinel.irrelevant_extra_arg,
        keyword=mock.sentinel.keyword_arg,
    )

    # Channel and any extra arguments must be passed on to transport layer.
    # Callback function will obviously change.
    transport.subscribe.assert_called_once_with(
        mock.sentinel.channel,
        mock.ANY,
        mock.sentinel.irrelevant_extra_arg,
        keyword=mock.sentinel.keyword_arg,
    )
    callback = transport.subscribe.call_args[0][1]
    assert callback != recipient
    assert sid == transport.subscribe.return_value

    # Part II: Message handling via unwrapper
    check_message_handling_via_unwrapper(callback, recipient, transport, rw_mock, False)


@mock.patch("workflows.recipe.RecipeWrapper", autospec=True)
def test_wrapping_a_subscription_allowing_non_recipe_messages(rw_mock):
    """Test queue subscription with recipe wrapper allowing non-recipe messages to pass through."""
    transport, recipient = mock.Mock(), mock.Mock()

    sid = workflows.recipe.wrap_subscribe(
        transport,
        mock.sentinel.channel,
        recipient,
        mock.sentinel.irrelevant_extra_arg,
        keyword=mock.sentinel.keyword_arg,
        allow_non_recipe_messages=True,
    )

    transport.subscribe.assert_called_once()
    callback = transport.subscribe.call_args[0][1]
    assert sid == transport.subscribe.return_value

    # Part II: Message handling via unwrapper
    check_message_handling_via_unwrapper(callback, recipient, transport, rw_mock, True)


@mock.patch("workflows.recipe.RecipeWrapper", autospec=True)
def test_wrapping_a_broadcast_subscription(rw_mock):
    """Test topic subscription with recipe wrapper."""
    transport, recipient = mock.Mock(), mock.Mock()

    sid = workflows.recipe.wrap_subscribe_broadcast(
        transport,
        mock.sentinel.channel,
        recipient,
        mock.sentinel.irrelevant_extra_arg,
        keyword=mock.sentinel.keyword_arg,
    )

    # Channel and any extra arguments must be passed on to transport layer.
    # Callback function will obviously change.
    transport.subscribe_broadcast.assert_called_once_with(
        mock.sentinel.channel,
        mock.ANY,
        mock.sentinel.irrelevant_extra_arg,
        keyword=mock.sentinel.keyword_arg,
    )
    callback = transport.subscribe_broadcast.call_args[0][1]
    assert callback != recipient
    assert sid == transport.subscribe_broadcast.return_value

    # Part II: Message handling via unwrapper
    check_message_handling_via_unwrapper(callback, recipient, transport, rw_mock, False)


def test_wrapping_a_subscription_with_log_extension():
    """Test queue subscription with recipe wrapper, passing a log_extender function.
    If the recipe contains useful contextual information for log messages,
    such as a unique ID which can be used to connect all messages originating
    from the same recipe, then this information should be passed to the
    log_extender function."""
    transport, lext = mock.Mock(), mock.Mock()

    # Set up context manager mock
    lext.return_value.__enter__ = lext.enter
    lext.return_value.__exit__ = lext.exit

    def recipient(*args, **kwargs):
        """Dummy function accepting everything but must be run in log_extender context."""
        lext.enter.assert_called_once()
        lext.exit.assert_not_called()
        lext.recipient()

    sid = workflows.recipe.wrap_subscribe(
        transport, mock.sentinel.channel, recipient, log_extender=lext
    )

    # Channel and any extra arguments must be passed on to transport layer.
    # Callback function will obviously change.
    transport.subscribe.assert_called_once_with(mock.sentinel.channel, mock.ANY)
    callback = transport.subscribe.call_args[0][1]
    assert callback != recipient
    assert sid == transport.subscribe.return_value

    # Part II: Message handling

    # This message does not contain an encoded recipe. It should be passed through directly.
    header = {"random-header": mock.sentinel.ID}
    callback(header, mock.Mock())
    lext.assert_not_called()

    # This message does not contain an encoded recipe. It should be passed through directly.
    header = {"workflows-recipe": "False"}
    callback(header, mock.Mock())
    lext.assert_not_called()

    # This message contains an encoded recipe. The environment ID should be passed to the
    # log_extender context manager.
    header = {"workflows-recipe": "True"}
    message = {
        "recipe": {1: {}},
        "recipe-pointer": 1,
        "recipe-path": [],
        "environment": {
            "ID": mock.sentinel.GUID,
            "source": mock.sentinel.source,
            "timestamp": mock.sentinel.timestamp,
        },
        "payload": mock.sentinel.payload,
    }
    callback(header, message)
    lext.assert_called_once_with("recipe_ID", mock.sentinel.GUID)
    lext.enter.assert_called_once()
    lext.exit.assert_called_once()
    lext.recipient.assert_called_once()
