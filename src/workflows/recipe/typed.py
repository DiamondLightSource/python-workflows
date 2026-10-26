"""Typed message handlers for workflows services.

A service callback decorated with :func:`typed_handler` can annotate its
``message`` argument, and optionally declare a ``parameters`` argument, with
pydantic models (or any type a :class:`pydantic.TypeAdapter` understands).
Incoming messages are validated before the callback is invoked, and messages
failing validation are logged and rejected::

    class Parameters(pydantic.BaseModel):
        output_filename: Annotated[Path, NonEmpty]
        timeout: int = 60

    @typed_handler
    def receive(self, rw, header, message: dict[str, Any], parameters: Parameters):
        ...

Parameter sources
-----------------

By default ``parameters`` is built only from the ``parameters`` dictionary of
the current recipe step. Services that also let incoming messages supply or
override parameters say so with ``parameters_from``, an ordered sequence of
sources with the **highest precedence first**:

``"step"``
    The ``parameters`` dictionary of the current recipe step.
``"message"``
    The top level of the message payload. Every key of the message becomes
    a candidate parameter, including the message's actual data, so this is
    best used with a parameters model that ignores unknown fields (the
    pydantic default).
``"step.<key>"`` / ``"message.<key>"``
    A dictionary nested inside one of the above, eg. ``"message.parameters"``
    or ``"step.ispyb_parameters"``. Further levels may be chained with dots,
    eg. ``"message.parameters.extra"``.

The sources are merged as follows:

* Merging is shallow: a top-level key from a higher precedence source
  replaces the whole value from lower ones. Nested dictionaries are not
  combined.
* A key that is present takes precedence even when its value is falsy, eg.
  ``0``, ``""``, ``False`` or ``None``. This differs from the
  ``message.get(key) or default`` lookups used by some older services.
* A source that does not exist, or is not a dictionary, is skipped. For
  example ``"step"`` contributes nothing to a message that is not part of a
  recipe, and ``"message"`` contributes nothing if the payload is a list.

Variable substitution
---------------------

With ``substitute=True``, after merging, ``$name`` and ``${name}`` in string
values (including inside nested lists and dictionaries) are replaced with
values from the recipe environment. Unknown names are left in place, and
``$$`` becomes ``$``.

Substitution is off by default because parameters often contain text where
``$`` means something else, such as shell commands (``$$``, ``$HOME``) or
email content. A name that happens to match a recipe environment key, eg.
``$ID``, would otherwise be silently replaced.

Some common configurations:

``("step",)`` (the default)
    Recipe step parameters only. The message can not influence them.
``("message", "step")``
    Any top-level message key overrides the recipe step parameter of the
    same name.
``("message.parameters", "step")``
    Only keys in the message's ``parameters`` dictionary override recipe
    step parameters.
``("message.parameters", "step.ispyb_parameters", "step")``
    As above, with the step's ``ispyb_parameters`` also overriding the rest
    of the step parameters.
``("step.ispyb_parameters", "message", "step")``
    The step's ``ispyb_parameters`` take precedence even over the message.

Only let messages override parameters when senders are meant to control
the service's behaviour. In a service whose message is data to be stored or
forwarded, a key in that data could otherwise silently change how it is
handled.
"""

from __future__ import annotations

import functools
import inspect
import logging
import string
import typing
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal

import pydantic

import workflows.recipe

logger = logging.getLogger("workflows.recipe.typed")

# Annotations that mean an argument is passed through without validation:
# explicitly typed as Any, or not annotated at all (inspect.Parameter.empty is
# the sentinel used when get_type_hints has no entry for the argument)
_UNVALIDATED = (Any, inspect.Parameter.empty)


def _require_nonempty(value: Any) -> Any:
    if value == "":
        raise ValueError("must not be empty")
    return value


# Validator rejecting empty strings before conversion, eg. Annotated[Path, NonEmpty].
# Without it Path("") silently becomes Path("."), and constraints such as
# min_length can not be applied to a Path.
NonEmpty = pydantic.BeforeValidator(_require_nonempty)


def _substitute(value: Any, substitutions: Mapping[str, Any]) -> Any:
    """Recursively replace $variables in strings with values from substitutions"""
    if isinstance(value, str) and "$" in value:
        return string.Template(value).safe_substitute(substitutions)
    if isinstance(value, Mapping):
        return {k: _substitute(v, substitutions) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute(v, substitutions) for v in value]
    return value


_DEFAULT_PARAMETERS_FROM = ("step",)


def _parse_sources(sources: Sequence[str]) -> list[tuple[str, list[str]]]:
    """Split parameter source strings into (root, [nested keys]) pairs"""
    if isinstance(sources, str):
        raise TypeError(
            f"parameters_from must be a sequence of sources, not a string: "
            f"use ({sources!r},)"
        )
    if not sources:
        raise ValueError("parameters_from must name at least one source")
    parsed = []
    for source in sources:
        root, *keys = source.split(".")
        if root not in ("step", "message") or "" in keys:
            raise ValueError(
                f"Invalid parameter source {source!r}: sources must be 'step' or "
                "'message', optionally followed by '.key' to select a nested dictionary"
            )
        parsed.append((root, keys))
    return parsed


def merged_parameters(
    rw: workflows.recipe.RecipeWrapper | None,
    message: Any,
    *,
    parameters_from: Sequence[str] = _DEFAULT_PARAMETERS_FROM,
    substitute: bool = False,
) -> dict[str, Any]:
    """Assemble the effective parameters for a recipe step from several sources.

    See the module documentation for a full description of the rules.

    Args:
        rw: The recipe wrapper of the incoming message, or None for messages
            that are not part of a recipe.
        message: The incoming message payload.
        parameters_from: The sources to merge, highest precedence first. Each
            is 'step' (the recipe step parameters) or 'message' (the message
            payload), optionally followed by '.key' to select a nested
            dictionary, eg. 'message.parameters' or 'step.ispyb_parameters'.
        substitute: Whether to replace $variables in string values with
            values from the recipe environment.

    Returns:
        A new dictionary of the merged parameters.
    """
    roots: dict[str, Any] = {
        "step": rw.recipe_step.get("parameters") if rw and rw.recipe_step else None,
        "message": message,
    }
    parameters: dict[str, Any] = {}
    # Apply lowest precedence first, so higher precedence sources overwrite
    for root, keys in reversed(_parse_sources(parameters_from)):
        layer = roots[root]
        for key in keys:
            layer = layer.get(key) if isinstance(layer, Mapping) else None
        if isinstance(layer, Mapping):
            parameters.update(layer)
    if substitute and rw is not None and rw.environment:
        parameters = _substitute(parameters, rw.environment)
    return parameters


def typed_handler(
    func: Callable[..., Any] | None = None,
    *,
    on_invalid: Literal["nack", "drop"] = "nack",
    parameters_from: Sequence[str] | None = None,
    substitute: bool = False,
) -> Any:
    """Decorate a service callback to validate incoming messages.

    The decorated method keeps the ``(self, rw, header, message)`` call
    signature expected by :func:`workflows.recipe.wrap_subscribe`. If the
    message argument (the one after ``header``) is annotated with anything
    other than ``Any``, the message is validated against that type and the
    validated object passed on. If the method declares a ``parameters``
    argument, the parameters are assembled as described by parameters_from,
    validated against its annotation, and passed in as a keyword argument.

    Can be used bare (``@typed_handler``) or with arguments
    (``@typed_handler(parameters_from=("message", "step"))``).

    Args:
        on_invalid: What to do with a message that fails validation. "nack"
            rejects it without requeueing, so it goes to the dead letter queue
            if one is configured. "drop" acknowledges and discards it.
        parameters_from: Where the ``parameters`` argument is assembled from,
            highest precedence first. Defaults to ``("step",)``, the recipe
            step parameters only. See the module documentation for the
            available sources and how they are merged. Only allowed if the
            method declares a ``parameters`` argument.
        substitute: Whether to replace $variables in the parameters with
            values from the recipe environment. See the module documentation.
            Only allowed if the method declares a ``parameters`` argument.

    Raises:
        TypeError: If the method does not accept (self, rw, header, message),
            or parameters_from or substitute is given for a method without a
            ``parameters`` argument.
        ValueError: If parameters_from is empty or names an unknown source.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        signature = inspect.signature(func)
        positional = [
            p
            for p in signature.parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        if len(positional) < 4:
            raise TypeError(
                f"{func.__qualname__} must accept (self, rw, header, message) arguments"
            )
        message_name = positional[3].name
        takes_parameters = "parameters" in signature.parameters
        if not takes_parameters:
            for option, given in (
                ("parameters_from", parameters_from is not None),
                ("substitute", substitute),
            ):
                if given:
                    raise TypeError(
                        f"{func.__qualname__} has no 'parameters' argument, so "
                        f"{option} has no effect"
                    )
        sources = (
            _DEFAULT_PARAMETERS_FROM if parameters_from is None else parameters_from
        )
        # Check the sources now, so that mistakes fail at import time
        _parse_sources(sources)

        # Resolve annotations on first use, so that with postponed evaluation
        # models may be defined after the service class
        adapters: dict[str, pydantic.TypeAdapter[Any] | None] = {}

        def get_adapters() -> dict[str, pydantic.TypeAdapter[Any] | None]:
            if not adapters:
                hints = typing.get_type_hints(func)
                for name in (message_name, "parameters"):
                    hint = hints.get(name, inspect.Parameter.empty)
                    adapters[name] = (
                        None if hint in _UNVALIDATED else pydantic.TypeAdapter(hint)
                    )
            return adapters

        @functools.wraps(func)
        def wrapper(
            self: Any,
            rw: workflows.recipe.RecipeWrapper | None,
            header: dict,
            message: Any,
        ) -> Any:
            adapter = get_adapters()
            log = getattr(self, "log", logger)
            kwargs: dict[str, Any] = {}
            try:
                if takes_parameters:
                    parameters = merged_parameters(
                        rw, message, parameters_from=sources, substitute=substitute
                    )
                    if param_adapter := adapter["parameters"]:
                        parameters = param_adapter.validate_python(parameters)
                    kwargs["parameters"] = parameters
                if message_adapter := adapter[message_name]:
                    message = message_adapter.validate_python(message)
            except pydantic.ValidationError as e:
                log.error(
                    "Rejecting message failing validation in %s: %s",
                    func.__qualname__,
                    e,
                )
                transport = rw.transport if rw is not None else self.transport
                if on_invalid == "drop":
                    transport.ack(header)
                else:
                    transport.nack(header, requeue=False)
                return None
            return func(self, rw, header, message, **kwargs)

        setattr(wrapper, workflows.recipe.TYPED_HANDLER_MARKER, True)
        return wrapper

    if func is not None:
        return decorator(func)
    return decorator
