from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any, overload

import workflows.recipe
from workflows.recipe.recipe import Recipe
from workflows.transport.common_transport import CommonTransport

logger = logging.getLogger("workflows.recipe.wrapper")


class RecipeWrapper:
    """
    Represent a "Live" recipe, including the current state and history.

    Makes it possible to keep track of the current state of the recipe
    and any parameters passed between services. If provided with a
    transport class, then the transport convenience methods can be used
    to manage all the bookkeeping to send onward messages/messages
    between services.

    Services normally do not construct wrappers directly. Instead, the
    :func:`~workflows.recipe.wrap_subscribe` and
    :func:`~workflows.recipe.wrap_subscribe_broadcast` helpers intercept
    incoming messages, build a wrapper positioned at the relevant recipe
    step, and pass it to the service's callback. The callback then uses
    :meth:`send` or :meth:`send_to` to dispatch results onward without
    needing to know the names of the next services in the chain.

    Wrappers can also be constructed from a bare recipe (rather than a
    received message) to kick off a new recipe via :meth:`start`.

    Attributes:
        recipe: The underlying :class:`Recipe` object.
        recipe_pointer: Index of the current step within ``recipe``, or
            ``None`` if the wrapper was built from a recipe that has not
            yet been started.
        recipe_step: The recipe node at ``recipe_pointer``, or ``None``
            before the recipe has started. Inspect this to read output
            channel definitions for the current step.
        recipe_path: History of nodes passed through to reach this node.
        environment: Dictionary of contextual information carried with
            the recipe (e.g. a recipe ``ID`` used for log correlation).
            Propagated unchanged to every downstream message.
        payload: The message payload delivered to this step, or ``None``
            when the wrapper was built from a bare recipe.
        default_channel: Named output channel treated as the target for
            :meth:`send` when the current step has named outputs. Set via
            :meth:`set_default_channel`.
        transport: The transport layer used to dispatch messages.
            Accessing this attribute raises :class:`RuntimeError` if no
            transport was supplied at construction time.
    """

    recipe_pointer: int | None
    default_channel: str | None

    @overload
    def __init__(
        self,
        *,
        transport: CommonTransport | None = None,
        environment: dict[str, Any] | None = None,
        message: dict[str, Any],
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        transport: CommonTransport | None = None,
        environment: dict[str, Any] | None = None,
        recipe: Recipe | dict[str, Any],
    ) -> None: ...

    def __init__(
        self,
        *,
        transport: CommonTransport | None = None,
        environment: dict[str, Any] | None = None,
        message: dict[str, Any] | None = None,
        recipe: Recipe | dict[str, Any] | None = None,
        **kwargs,
    ):
        """
        Create a RecipeWrapper object from a wrapped message.

        transport:
            References to the transport layer, required to make use of
            methods that send directly to downstream processes, such as
            :meth:`send`, :meth:`start`, and :meth:`checkpoint`. If not
            provided, then the RecipeWrapper can only be inspected.
        environment:
            Optional environment dictionary propagated to all downstream
            messages. Used for Recipe-workflow global information, such
            as `ID`.
        recipe:
            A :class:`Recipe` instance, or a raw recipe dictionary that
            will be validated and wrapped in one. Used for construction
            of a wrapper that has not yet been started.
        """
        if message:
            self.recipe = workflows.recipe.Recipe(message["recipe"])
            self.recipe_pointer = int(message["recipe-pointer"])
            self.recipe_step = self.recipe[self.recipe_pointer]
            self.recipe_path = message.get("recipe-path", [])
            if environment is None:
                self.environment = message.get("environment", {})
            else:
                self.environment = environment
            self.payload = message.get("payload")
        elif recipe:
            if isinstance(recipe, workflows.recipe.Recipe):
                self.recipe = recipe
            else:
                self.recipe = workflows.recipe.Recipe(recipe)
            self.recipe_pointer = None
            self.recipe_step = None
            self.recipe_path = []
            self.environment = environment or {}
            self.payload = None
        else:
            raise ValueError(
                "A message or recipe is required to create a RecipeWrapper object."
            )
        self.default_channel = None
        self._transport = transport

    @property
    def transport(self) -> CommonTransport:
        if self._transport is None:
            raise RuntimeError(
                "This RecipeWrapper object does not contain a reference to a transport object."
            )
        return self._transport

    def send(
        self,
        message: Any,
        header: dict[str, Any] | None = None,
        *,
        mangle_for_sending: Callable[[Any], Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Send a message to the current step's downstream services.

        This is another service that is connected to the currently
        running service via the recipe, specified by the "output" field
        of the current step. There are three behaviours:

        - If ``output`` is unset or the step has no ``output`` entry,
          then the message will be discarded.
        - If ``output`` is a single integer or an array of integers
          naming destination nodes in the recipe, then a copy of the
          message will be delivered to each one of them.
        - If ``output`` is a dict of named target nodes; If a default
          channel name has been set with :meth:`set_default_channel`
          then the message will only be sent to any destinations listed
          there.

        For general sending to named output channels, use the
        :meth:`send_to` method instead.

        Args:
            message:
                The payload to deliver. Wrapped in the standard recipe
                envelope (recipe, pointer, path, environment) before
                handing off to the transport.
            header:
                Optional dictionary of transport headers. This is merged
                with any set by the transport (e.g. the ``workflows-recipe``
                header flag is automatically added).
            mangle_for_sending:
                Optional callable applied to the fully formed message
                right before it is handed to the transport. If specified
                here, then the default transport serialization is not
                applied. The default mangling function for transports is
                usually "encode to JSON".
            **kwargs:
                Any additional keyword arguments forwarded to the
                transport's ``send``/``broadcast`` calls, in addition
                to any declared in the recipe.
        """
        if not self.recipe_step:
            raise ValueError(
                "This RecipeWrapper object does not contain "
                "a recipe with a selected step."
            )

        if "output" not in self.recipe_step:
            # The current recipe step does not have output channels.
            return

        # Merge down the annotated function call to bare kwargs
        kwargs["message"] = message
        if header:
            kwargs["header"] = header
        if mangle_for_sending:
            kwargs["mangle_for_sending"] = mangle_for_sending

        if isinstance(self.recipe_step["output"], dict):
            # The current recipe step does have named output channels.
            if self.default_channel:
                # Use named output channel
                self.send_to(self.default_channel, **kwargs)

        else:
            # The current recipe step does have unnamed output channels.
            self._send_to_destinations(self.recipe_step["output"], **kwargs)

    def send_to(
        self,
        channel: str,
        message: Any,
        header: dict[str, Any] | None = None,
        *,
        mangle_for_sending: Callable[[Any], Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Send a message to a service named by this steps' output channel.

        This is another service that is connected to the currently
        running service via the recipe, specified by the "output" field
        of the current step. There are three behaviours:

        - If ``output`` is unset or the step has no ``output`` entry,
          then the message will be discarded.
        - If ``output`` is *NOT* a dictionary, then the message is sent
          to all services specified *ONLY IF* the passed channel name is
          the same as the default channel name.
        - The channel name is looked up in the ``outputs`` dictionary
          and a copy of the message is sent to every destination
          specified there.

        Args:
            channel:
                The name of the output channel, corresponding to an
                entry in the ``outputs`` dictionary.
            message:
                The payload to deliver. Wrapped in the standard recipe
                envelope (recipe, pointer, path, environment) before
                handing off to the transport.
            header:
                Optional dictionary of transport headers. This is merged
                with any set by the transport (e.g. the ``workflows-recipe``
                header flag is automatically added).
            mangle_for_sending:
                Optional callable applied to the fully formed message
                right before it is handed to the transport. If specified
                here, then the default transport serialization is not
                applied. The default mangling function for transports is
                usually "encode to JSON".
            **kwargs:
                Any additional keyword arguments forwarded to the
                transport's ``send``/``broadcast`` calls, in addition
                to any declared in the recipe.
        """

        if not self.recipe_step:
            raise ValueError(
                "This RecipeWrapper object does not contain "
                "a recipe with a selected step."
            )

        if "output" not in self.recipe_step:
            # The current recipe step does not have output channels.
            return

        # Merge down the annotated function call to bare kwargs
        kwargs["message"] = message
        if header:
            kwargs["header"] = header
        if mangle_for_sending:
            kwargs["mangle_for_sending"] = mangle_for_sending

        if not isinstance(self.recipe_step["output"], dict):
            # The current recipe step does not have named output channels.
            if self.default_channel == channel:
                # Use unnamed output channels
                self.send(**kwargs)
            return

        if channel not in self.recipe_step["output"]:
            # The current recipe step does not have an output channel with this name.
            return

        self._send_to_destinations(self.recipe_step["output"][channel], **kwargs)

    def set_default_channel(self, channel: str) -> None:
        """Define one named output channel to be equivalent to unnamed output
        channels. For this channel send() and send_to() will be identical."""
        self.default_channel = channel

    def start(
        self,
        header: dict[str, Any] | None = None,
        *,
        mangle_for_sending: Callable[[Any], Any] | None = None,
        **kwargs,
    ) -> None:
        """
        Trigger the start of a recipe.

        Dispatches the payloads defined in the recipe's ``start`` node
        to their respective recipients via the transport layer. Only
        valid on a wrapper constructed from a bare recipe; calling
        :meth:`start` on a wrapper that already has a selected step
        raises :class:`ValueError`.

        Args:
            header:
                Optional dictionary of transport headers. This is merged
                with any set by the transport (e.g. the ``workflows-recipe``
                header flag is automatically added).
            mangle_for_sending:
                Optional callable applied to the fully formed message
                right before it is handed to the transport. If specified
                here, then the default transport serialization is not
                applied. The default mangling function for transports is
                usually "encode to JSON".
            **kwargs: Keywords passed on to the transport.

        Raises:
            ValueError: If the wrapped recipe has already been started.
        """

        if self.recipe_step:
            raise ValueError("This recipe has already been started.")

        for destination, payload in self.recipe["start"]:
            self._send_to_destination(
                destination,
                header=header,
                payload=payload,
                transport_kwargs=kwargs,
                mangle_for_sending=mangle_for_sending,
            )

    def checkpoint(
        self,
        message: Any,
        header: dict[str, Any] | None = None,
        delay: float = 0.0,
        *,
        mangle_for_sending: Callable[[Any], Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Send a message back to "yourself" e.g. the current recipe destination.

        This can be used to store state in the rabbitmq queue for
        processing of longer-term tasks, without making individual
        services stateful.

        Args:
            message:
                The payload to deliver. Wrapped in the standard recipe
                envelope (recipe, pointer, path, environment) before
                handing off to the transport.
            header:
                Optional dictionary of transport headers. This is merged
                with any set by the transport (e.g. the ``workflows-recipe``
                header flag is automatically added).
            delay: Time, in seconds, to delay delivery of this message.
            mangle_for_sending:
                Optional callable applied to the fully formed message
                right before it is handed to the transport. If specified
                here, then the default transport serialization is not
                applied. The default mangling function for transports is
                usually "encode to JSON".
            **kwargs: Keywords passed on to the transport.
        """
        if not self.recipe_step:
            raise ValueError(
                "This RecipeWrapper object does not contain "
                "a recipe with a selected step."
            )

        kwargs["delay"] = delay

        self._send_to_destination(
            self.recipe_pointer,
            header,
            message,
            kwargs,
            add_path_step=False,
            mangle_for_sending=mangle_for_sending,
        )

    def apply_parameters(self, parameters: dict[str, Any]) -> None:
        """
        Recursively substitute parameter values into the wrapped recipe.

        Delegates to :meth:`Recipe.apply_parameters` and then refreshes
        :attr:`recipe_step` so subsequent :meth:`send` / :meth:`send_to`
        calls observe the substituted outputs and queue names.

        Note:
            This is primarily useful in tests. Mutating the recipe as it
            is passed down the chain of services means each hop sees a
            different recipe, which makes failures very difficult to
            diagnose; prefer baking parameters in before dispatch in
            production.

        Args:
            parameters: Mapping of parameter names to replacement values.
                Keys are referenced from the recipe as ``{name}``, or as
                ``{$REPLACE:name}`` to substitute an entire data
                structure in place of the string. See
                :meth:`Recipe.apply_parameters` for the full grammar.
        """
        self.recipe.apply_parameters(parameters)
        assert self.recipe_pointer is not None
        self.recipe_step = self.recipe[self.recipe_pointer]

    def _generate_full_recipe_message(self, destination, message, add_path_step):
        """Factory function to generate independent message objects for
        downstream recipients with different destinations."""
        if add_path_step and self.recipe_pointer:
            recipe_path = self.recipe_path + [self.recipe_pointer]
        else:
            recipe_path = self.recipe_path

        return {
            "environment": self.environment,
            "payload": message,
            "recipe": self.recipe.recipe,
            "recipe-path": recipe_path,
            "recipe-pointer": destination,
        }

    def _send_to_destinations(
        self,
        destinations,
        message,
        header=None,
        mangle_for_sending: Callable[[Any], Any] | None = None,
        **kwargs,
    ):
        """Send messages to a list of numbered destinations. This is an internal
        helper method used by the public 'send' methods.
        """
        if not isinstance(destinations, list):
            destinations = (destinations,)
        for destination in destinations:
            self._send_to_destination(
                destination,
                header,
                message,
                kwargs,
                mangle_for_sending=mangle_for_sending,
            )

    def _send_to_destination(
        self,
        destination,
        header,
        payload,
        transport_kwargs,
        add_path_step=True,
        mangle_for_sending: Callable[[Any], Any] | None = None,
    ):
        """Helper function to send a message to a specific recipe destination."""
        if header:
            header = header.copy()
            header["workflows-recipe"] = True
        else:
            header = {"workflows-recipe": True}

        dest_kwargs = transport_kwargs.copy()
        if (
            "transport-delay" in self.recipe[destination]
            and "delay" not in transport_kwargs
        ):
            dest_kwargs["delay"] = self.recipe[destination]["transport-delay"]
        if "exchange" in self.recipe[destination]:
            dest_kwargs.setdefault("exchange", self.recipe[destination]["exchange"])

        if self.recipe[destination].get("queue"):
            send = (
                self.transport.raw_send if mangle_for_sending else self.transport.send
            )
            message = self._generate_full_recipe_message(
                destination, payload, add_path_step
            )
            if mangle_for_sending:
                message = mangle_for_sending(message)
            self._retry_transport(
                send,
                self.recipe[destination]["queue"],
                message,
                headers=header,
                **dest_kwargs,
            )
        if self.recipe[destination].get("topic"):
            broadcast = (
                self.transport.raw_broadcast
                if mangle_for_sending
                else self.transport.broadcast
            )
            message = self._generate_full_recipe_message(
                destination, payload, add_path_step
            )
            if mangle_for_sending:
                message = mangle_for_sending(message)
            self._retry_transport(
                broadcast,
                self.recipe[destination]["topic"],
                message,
                headers=header,
                **dest_kwargs,
            )

    def _retry_transport(self, function, *args, **kwargs):
        """Attempt to send a message, and in case the connection has been lost,
        attempt to reconnect. Reconnecting only works on the assumption that
        the previous connection did not include any subscriptions, which should
        be true for wrappers."""
        attempt = 0
        limit = 12
        initial_failure = None
        while True:
            try:
                if initial_failure:
                    logger.info("Reconnection attempt %d of %d", attempt, limit)
                    self.transport.connect()
                return function(*args, **kwargs)
            except workflows.Disconnected as e:
                if not self.transport.is_reconnectable:
                    raise
                attempt = attempt + 1
                if not initial_failure:
                    initial_failure = e
                if attempt > limit:
                    logger.error("Transport connection failure", exc_info=True)
                    raise initial_failure from None
                delay = attempt * attempt * 3
                # wait 3, 12, 27, 48, 75, 108, 147, 192, 243, 300, 363, 432 seconds
                # between attempts for a total maximum of 32.5 minutes
                logger.warning(
                    "Connection failure detected during send attempt."
                    " Retrying in %d seconds",
                    delay,
                    exc_info=True,
                )
                time.sleep(delay)
