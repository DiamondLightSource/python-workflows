import mock
import pytest
import workflows
from workflows.recipe import Recipe
from workflows.recipe.wrapper import RecipeWrapper


def generate_recipe_message():
    """Helper function for tests."""
    message = {
        "recipe": {
            1: {
                "service": "A service",
                "queue": "service.one",
                "output": {"somewhere": [2], "multi-output": [2, 3, 4]},
                "parameter": "some {placeholder} to test replacement",
                "error": 2,
            },
            2: {"service": "service 2", "queue": "queue.two", "output": [3]},
            3: {"service": "service 3", "topic": "topic.three", "transport-delay": 300},
            4: {"service": "service 4", "queue": "queue.four", "transport-delay": 300},
            "start": [(1, {"parameter": mock.sentinel.initial_parameter})],
            "error": [2],
        },
        "recipe-pointer": 1,
        "recipe-path": [],
        "environment": {
            "ID": mock.sentinel.GUID,
            "source": mock.sentinel.source,
            "timestamp": mock.sentinel.timestamp,
        },
        "payload": mock.sentinel.payload,
    }
    return message


def test_recipe_wrapper_instantiated_from_message():
    """A RecipeWrapper built from a message must parse the contained recipe,
    pointers, etc."""
    m = generate_recipe_message()

    rw = RecipeWrapper(message=m)

    assert rw.recipe == Recipe(m["recipe"])
    assert rw.recipe_pointer == m["recipe-pointer"]
    assert rw.recipe_step == m["recipe"][m["recipe-pointer"]]
    assert rw.recipe_path == m["recipe-path"]
    assert rw.environment == m["environment"]
    assert rw.payload == mock.sentinel.payload


def test_recipe_wrapper_instantiated_from_message_with_string_conversion():
    """A RecipeWrapper built from a message must be able to parse numeric strings."""
    m = generate_recipe_message()
    original_pointer = m["recipe-pointer"]
    m["recipe-pointer"] = str(original_pointer)

    rw = RecipeWrapper(message=m)

    assert rw.recipe_pointer == original_pointer
    assert rw.recipe_step == m["recipe"][original_pointer]


def test_recipe_wrapper_instantiated_from_message_with_unicode_conversion():
    """A RecipeWrapper built from a message must be able to parse numeric strings."""
    m = generate_recipe_message()
    original_pointer = m["recipe-pointer"]
    try:
        m["recipe-pointer"] = unicode(original_pointer)
    except NameError:
        pytest.skip("Test skipped on Python 3")

    rw = RecipeWrapper(message=m)

    assert rw.recipe_pointer == original_pointer
    assert rw.recipe_step == m["recipe"][original_pointer]


def test_recipe_wrapper_instantiated_from_recipe():
    """A RecipeWrapper built from a recipe will contain the recipe, but no
    pointers."""
    r = generate_recipe_message()["recipe"]

    rw = RecipeWrapper(recipe=r)

    assert rw.recipe == Recipe(r)
    assert rw.recipe_pointer is None
    assert rw.recipe_step is None
    assert rw.recipe_path == []
    assert rw.environment == {}
    assert rw.payload is None

    # Constructor can also accept Recipe objects

    r = Recipe(r)
    rw = RecipeWrapper(recipe=r)

    assert rw.recipe == r
    assert rw.recipe_pointer is None
    assert rw.recipe_step is None
    assert rw.recipe_path == []
    assert rw.environment == {}
    assert rw.payload is None


def test_recipe_wrapper_empty_constructor_fails():
    """A RecipeWrapper must be built from either a recipe or a message containing
    a recipe. Otherwise there is nothing to wrap."""
    with pytest.raises(ValueError):
        RecipeWrapper()


def test_downstream_message_sending_via_recipewrapper_with_named_outputs():
    """The idea of the RecipeWrapper is that it makes it easy to send messages from
    one service to another, including instructions on how to handle intermediate
    results at each step. This tests the send_to function which embeds a message
    payload inside a data structure containing all the extra processing info.
    """
    m = generate_recipe_message()
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)

    def downstream_message(dest, payload):
        """Helper function to generate expected message contents for downstream
        recipients."""
        ds_message = generate_recipe_message()
        ds_message["recipe-pointer"] = dest
        ds_message["recipe-path"] = [1]
        ds_message["payload"] = payload
        return ds_message

    rw = RecipeWrapper(message=m, transport=t)
    assert rw.transport == t
    assert t.method_calls == []
    t.reset_mock()  # magic call may have been recorded

    # Messages sent to undefined outputs are discarded
    rw.send_to("unknown", mock.sentinel.message_text)
    assert t.mock_calls == []

    # Messages sent to defined outputs are sent
    rw.send_to("somewhere", mock.sentinel.message_text)
    expected = [
        mock.call.send(
            m["recipe"][2]["queue"],
            downstream_message(2, mock.sentinel.message_text),
            headers={"workflows-recipe": True},
        )
    ]
    assert t.mock_calls == expected
    t.reset_mock()

    # Messages sent to outputs with multiple connections are sent to all of them
    rw.send_to(
        "multi-output",
        mock.sentinel.another_message_text,
        transaction=mock.sentinel.txn,
    )
    expected = []
    expected.append(
        mock.call.send(
            m["recipe"][2]["queue"],
            downstream_message(2, mock.sentinel.another_message_text),
            headers={"workflows-recipe": True},
            transaction=mock.sentinel.txn,
        )
    )
    expected.append(
        mock.call.broadcast(
            m["recipe"][3]["topic"],
            downstream_message(3, mock.sentinel.another_message_text),
            headers={"workflows-recipe": True},
            transaction=mock.sentinel.txn,
            delay=300,
        )
    )
    expected.append(
        mock.call.send(
            m["recipe"][4]["queue"],
            downstream_message(4, mock.sentinel.another_message_text),
            headers={"workflows-recipe": True},
            transaction=mock.sentinel.txn,
            delay=300,
        )
    )
    assert t.mock_calls == expected
    t.reset_mock()

    # Messages sent to unnamed output are discarded
    rw.send(mock.sentinel.unnamed_output)
    assert t.mock_calls == []

    # Messages sent to unnamed output are sent when a default output channel is set
    rw.set_default_channel("somewhere")
    rw.send(mock.sentinel.unnamed_output_with_default_name)
    expected = [
        mock.call.send(
            m["recipe"][2]["queue"],
            downstream_message(2, mock.sentinel.unnamed_output_with_default_name),
            headers={"workflows-recipe": True},
        )
    ]
    assert t.mock_calls == expected


def test_downstream_message_sending_via_recipewrapper_with_unnamed_output():
    """Test sending messages via the RecipeWrapper when the current step
    does not have named outputs, or any output at all.
    """

    def downstream_message(dest, payload, path=()):
        """Helper function to generate expected message contents for downstream
        recipients."""
        ds_message = generate_recipe_message()
        ds_message["recipe-pointer"] = dest
        ds_message["recipe-path"] = [1] + list(path)
        ds_message["payload"] = payload
        return ds_message

    m = downstream_message(2, mock.sentinel.payload)
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)
    rw = RecipeWrapper(message=m, transport=t)
    t.reset_mock()

    # When only an unnamed output is specified all named outputs are discarded.
    rw.send_to("output", mock.sentinel.test_unnamed_output)
    assert t.mock_calls == []

    # The send() method can deal with unnamed outputs.
    rw.send(mock.sentinel.test_default_output)
    expected = [
        mock.call.broadcast(
            m["recipe"][3]["topic"],
            downstream_message(3, mock.sentinel.test_default_output, path=(2,)),
            headers={"workflows-recipe": True},
            delay=300,
        )
    ]
    assert t.mock_calls == expected
    t.reset_mock()

    # The send_to() method can also deal with unnamed outputs once a default output has been defined.
    rw.set_default_channel("output")
    rw.send_to("output", mock.sentinel.test_default_named_output)
    expected = [
        mock.call.broadcast(
            m["recipe"][3]["topic"],
            downstream_message(3, mock.sentinel.test_default_named_output, path=(2,)),
            headers={"workflows-recipe": True},
            delay=300,
        )
    ]
    assert t.mock_calls == expected


def test_checkpointing_via_recipewrapper():
    """Test resending modified message to the current destination (checkpointing)."""
    m = generate_recipe_message()
    m["recipe-pointer"] = 3
    # Use a step with transport-delay.
    # Checkpointing should ignore default delay settings.

    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)

    def downstream_message(dest, payload):
        """Helper function to generate expected message contents for downstream
        recipients."""
        ds_message = generate_recipe_message()
        ds_message["recipe-pointer"] = dest
        ds_message["recipe-path"] = []
        ds_message["payload"] = payload
        return ds_message

    # Can't checkpoint on a recipe without a pointer
    rw = RecipeWrapper(recipe=m["recipe"], transport=mock.Mock())
    with pytest.raises(ValueError):
        rw.checkpoint(mock.sentinel.checkpoint, transaction=mock.sentinel.txn)

    rw = RecipeWrapper(message=m, transport=t)
    # Test checkpointing. Delay should be 0.
    rw.checkpoint(mock.sentinel.checkpoint, transaction=mock.sentinel.txn)
    expected = [
        mock.call.broadcast(
            m["recipe"][3]["topic"],
            downstream_message(3, mock.sentinel.checkpoint),
            headers={"workflows-recipe": True},
            delay=0,
            transaction=mock.sentinel.txn,
        )
    ]
    assert t.mock_calls == expected
    t.reset_mock()

    # Test delay override
    rw.checkpoint(mock.sentinel.checkpoint, transaction=mock.sentinel.txn, delay=150)
    expected = [
        mock.call.broadcast(
            m["recipe"][3]["topic"],
            downstream_message(3, mock.sentinel.checkpoint),
            headers={"workflows-recipe": True},
            delay=150,
            transaction=mock.sentinel.txn,
        )
    ]
    assert t.mock_calls == expected


def test_start_command_via_recipewrapper():
    """Test triggering the start of a recipe."""
    m = generate_recipe_message()
    r = m["recipe"]
    t = mock.create_autospec(workflows.transport.common_transport.CommonTransport)

    def downstream_message(dest, payload):
        """Helper function to generate expected message contents for downstream
        recipients."""
        ds_message = generate_recipe_message()
        ds_message["recipe-pointer"] = dest
        ds_message["recipe-path"] = []
        ds_message["payload"] = payload
        ds_message["environment"] = mock.sentinel.environment
        return ds_message

    # Test on already started recipe
    rw = RecipeWrapper(message=m, transport=t)
    with pytest.raises(ValueError):
        rw.start(transaction=mock.sentinel.txn)
    t.reset_mock()

    rw = RecipeWrapper(recipe=r, transport=t)
    rw.environment = mock.sentinel.environment
    assert rw.transport == t
    assert t.method_calls == []
    t.reset_mock()  # magic call may have been recorded

    rw.start(transaction=mock.sentinel.txn)

    expected = [
        mock.call.send(
            r[1]["queue"],
            downstream_message(1, r["start"][0][1]),
            headers={"workflows-recipe": True},
            transaction=mock.sentinel.txn,
        )
    ]
    assert t.mock_calls == expected


def test_parameter_replacement_in_recipe_wrapper():
    """A RecipeWrapper should support the recipe's .apply_parameters call, and
    this should affect the stored recipe as well as internal referenced parts
    of the recipe."""
    m = generate_recipe_message()

    rw = RecipeWrapper(message=m)

    rw.apply_parameters({"placeholder": "replacement"})

    assert rw.recipe[1]["parameter"] == "some replacement to test replacement"
    assert rw.recipe_step["parameter"] == "some replacement to test replacement"

    assert rw.recipe != Recipe(m["recipe"])
    assert rw.recipe_pointer == m["recipe-pointer"]
    assert rw.recipe_step != m["recipe"][m["recipe-pointer"]]
    assert rw.recipe_path == m["recipe-path"]
    assert rw.environment == m["environment"]
    assert rw.payload == mock.sentinel.payload
