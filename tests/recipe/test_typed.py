from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any
from unittest import mock

import pydantic
import pytest

from workflows.recipe import Recipe, RecipeWrapper
from workflows.recipe.typed import NonEmpty, merged_parameters, typed_handler


class Payload(pydantic.BaseModel):
    file: str
    image: int = 1


class Parameters(pydantic.BaseModel):
    timeout: int = 60
    path: str


def make_rw(parameters: dict[str, Any], environment: dict[str, Any] | None = None):
    recipe = Recipe(
        {
            1: {"service": "test", "queue": "test", "parameters": parameters},
            "start": [(1, {})],
        }
    )
    return RecipeWrapper(
        message={
            "recipe": recipe.recipe,
            "recipe-pointer": 1,
            "environment": environment or {},
            "payload": None,
        },
        transport=mock.Mock(),
    )


class Service:
    def __init__(self):
        self.log = mock.Mock()
        self.transport = mock.Mock()
        self.calls: list[tuple] = []

    @typed_handler
    def untyped(self, rw, header, message: Any):
        self.calls.append((rw, header, message))

    @typed_handler
    def typed(self, rw, header, message: Payload, parameters: Parameters):
        self.calls.append((rw, header, message, parameters))

    @typed_handler(substitute=True)
    def substituting(self, rw, header, message: Payload, parameters: Parameters):
        self.calls.append((rw, header, message, parameters))

    @typed_handler(parameters_from=("message.parameters", "step"))
    def overridable(self, rw, header, message: Payload, parameters: Parameters):
        self.calls.append((rw, header, message, parameters))

    @typed_handler(on_invalid="drop")
    def dropping(self, rw, header, message: Payload):
        self.calls.append((rw, header, message))

    @typed_handler
    def late(self, rw, header, message: DefinedLater):
        self.calls.append((rw, header, message))


class DefinedLater(pydantic.BaseModel):
    value: int


def test_merged_parameters_defaults_to_step_only():
    rw = make_rw({"timeout": 10})
    assert merged_parameters(rw, {"timeout": 1, "parameters": {"timeout": 2}}) == {
        "timeout": 10
    }


def test_merged_parameters_from_message_parameters():
    rw = make_rw({"timeout": 10, "path": "/"})
    assert merged_parameters(
        rw,
        {"timeout": 1, "parameters": {"timeout": 2}},
        parameters_from=("message.parameters", "step"),
    ) == {"timeout": 2, "path": "/"}


def test_merged_parameters_from_whole_message():
    rw = make_rw({"timeout": 10, "path": "/"})
    assert merged_parameters(
        rw, {"timeout": 1, "file": "a.h5"}, parameters_from=("message", "step")
    ) == {"timeout": 1, "path": "/", "file": "a.h5"}


def test_merged_parameters_precedence_follows_order():
    rw = make_rw({"a": "step", "b": "step", "ispyb_parameters": {"a": "ispyb"}})
    message = {"a": "message", "b": "message"}
    assert merged_parameters(
        rw, message, parameters_from=("step.ispyb_parameters", "message", "step")
    ) == {"a": "ispyb", "b": "message", "ispyb_parameters": {"a": "ispyb"}}
    assert merged_parameters(
        rw, message, parameters_from=("message", "step.ispyb_parameters", "step")
    ) == {"a": "message", "b": "message", "ispyb_parameters": {"a": "ispyb"}}


@pytest.mark.parametrize("value", [0, "", False, None])
def test_merged_parameters_falsy_values_take_precedence(value):
    rw = make_rw({"a": "step"})
    assert merged_parameters(rw, {"a": value}, parameters_from=("message", "step")) == {
        "a": value
    }


def test_merged_parameters_merge_is_shallow():
    rw = make_rw({"cluster": {"memory": 4, "cpus": 2}})
    assert merged_parameters(
        rw, {"cluster": {"cpus": 8}}, parameters_from=("message", "step")
    ) == {"cluster": {"cpus": 8}}


@pytest.mark.parametrize(
    "message", ["not a dict", [1, 2], None, {"parameters": "not a dict"}, {}]
)
def test_merged_parameters_skips_missing_sources(message):
    rw = make_rw({"a": 1})
    assert merged_parameters(
        rw, message, parameters_from=("message.parameters", "step")
    ) == {"a": 1}


def test_merged_parameters_deeply_nested_source():
    assert merged_parameters(
        None,
        {"parameters": {"extra": {"a": 1}}},
        parameters_from=("message.parameters.extra",),
    ) == {"a": 1}


def test_merged_parameters_without_recipe():
    message = {"parameters": {"a": 1}}
    assert merged_parameters(None, message) == {}
    assert merged_parameters(
        None, message, parameters_from=("message.parameters", "step")
    ) == {"a": 1}


def test_merged_parameters_does_not_substitute_by_default():
    rw = make_rw({"path": "/dls/$visit", "pid": "$$"}, environment={"visit": "cm1234"})
    assert merged_parameters(rw, {}) == {"path": "/dls/$visit", "pid": "$$"}


def test_merged_parameters_substitutes_environment():
    rw = make_rw(
        {"path": "/dls/$visit", "nested": {"a": ["${visit}"]}, "other": "$unknown"},
        environment={"visit": "cm1234"},
    )
    assert merged_parameters(
        rw,
        {"from_message": "$visit"},
        parameters_from=("message", "step"),
        substitute=True,
    ) == {
        "path": "/dls/cm1234",
        "nested": {"a": ["cm1234"]},
        "other": "$unknown",
        "from_message": "cm1234",
    }


@pytest.mark.parametrize(
    "sources,error",
    [
        ("step", TypeError),
        ((), ValueError),
        (("recipe",), ValueError),
        (("message.",), ValueError),
        (("step..x",), ValueError),
    ],
)
def test_merged_parameters_rejects_invalid_sources(sources, error):
    with pytest.raises(error):
        merged_parameters(None, {}, parameters_from=sources)


def test_handler_rejects_invalid_sources_at_decoration():
    with pytest.raises(ValueError):

        @typed_handler(parameters_from=("recipe",))
        def bad(self, rw, header, message, parameters): ...


def test_handler_rejects_sources_without_parameters_argument():
    with pytest.raises(TypeError, match="parameters_from"):

        @typed_handler(parameters_from=("message", "step"))
        def bad(self, rw, header, message): ...


def test_handler_rejects_substitute_without_parameters_argument():
    with pytest.raises(TypeError, match="substitute"):

        @typed_handler(substitute=True)
        def bad(self, rw, header, message): ...


def test_untyped_message_passed_through():
    service = Service()
    rw = make_rw({})
    service.untyped(rw, {}, "anything")
    assert service.calls == [(rw, {}, "anything")]


def test_valid_message_and_parameters_are_converted():
    service = Service()
    rw = make_rw({"path": "/dls/$visit"}, environment={"visit": "cm1234"})
    service.typed(rw, {}, {"file": "a.h5", "parameters": {"timeout": "5"}})
    ((_, _, message, parameters),) = service.calls
    assert message == Payload(file="a.h5")
    assert parameters == Parameters(path="/dls/$visit", timeout=60)


def test_handler_substitutes_parameters():
    service = Service()
    rw = make_rw({"path": "/dls/$visit"}, environment={"visit": "cm1234"})
    service.substituting(rw, {}, {"file": "a.h5"})
    ((_, _, _, parameters),) = service.calls
    assert parameters == Parameters(path="/dls/cm1234")


def test_handler_parameters_from_message():
    service = Service()
    rw = make_rw({"path": "/", "timeout": 10})
    service.overridable(rw, {}, {"file": "a.h5", "parameters": {"timeout": "5"}})
    ((_, _, _, parameters),) = service.calls
    assert parameters == Parameters(path="/", timeout=5)


def test_invalid_message_is_nacked_without_requeue():
    service = Service()
    rw = make_rw({"path": "/"})
    header = {"message-id": 1, "subscription": 1}
    service.typed(rw, header, {"image": "x"})
    assert not service.calls
    rw.transport.nack.assert_called_once_with(header, requeue=False)
    service.log.error.assert_called_once()


def test_invalid_parameters_are_nacked():
    service = Service()
    rw = make_rw({})
    service.typed(rw, {}, {"file": "a.h5"})
    assert not service.calls
    rw.transport.nack.assert_called_once()


def test_invalid_message_can_be_dropped():
    service = Service()
    rw = make_rw({})
    service.dropping(rw, {}, {})
    assert not service.calls
    rw.transport.ack.assert_called_once_with({})
    rw.transport.nack.assert_not_called()


def test_non_recipe_message_rejected_on_service_transport():
    service = Service()
    service.dropping(None, {}, {})
    service.transport.ack.assert_called_once_with({})


def test_annotations_resolved_lazily():
    service = Service()
    service.late(None, {}, {"value": "3"})
    assert service.calls == [(None, {}, DefinedLater(value=3))]


def test_handler_requires_message_argument():
    with pytest.raises(TypeError):

        @typed_handler
        def bad(self, rw, header): ...


@pytest.mark.parametrize("value", ["", None, 3])
def test_nonempty_path_rejects(value):
    with pytest.raises(pydantic.ValidationError):
        pydantic.TypeAdapter(Annotated[Path, NonEmpty]).validate_python(value)


def test_nonempty_path_converts():
    adapter = pydantic.TypeAdapter(Annotated[Path, NonEmpty])
    assert adapter.validate_python("a/b") == Path("a/b")


def test_nonempty_str():
    adapter = pydantic.TypeAdapter(Annotated[str, NonEmpty])
    assert adapter.validate_python("a") == "a"
    with pytest.raises(pydantic.ValidationError):
        adapter.validate_python("")


def test_typed_handler_accepted_by_subscribe(mocker):
    from workflows.services.common_service import CommonService

    wrap_subscribe = mocker.patch("workflows.recipe.wrap_subscribe")

    class TypedService(CommonService):
        @typed_handler
        def receive(self, rw, header, message: Payload, parameters: Parameters): ...

    service = TypedService()
    service.transport = mock.Mock()
    service.subscribe("a", service.receive)
    wrap_subscribe.assert_called_once()
