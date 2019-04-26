from __future__ import absolute_import, division, print_function

import workflows.recipe


class RecipeWrapper(object):
    """A wrapper object which contains a recipe and a number of functions to make
    life easier for recipe users.
    """

    def __init__(self, message=None, transport=None, recipe=None, **kwargs):
        """Create a RecipeWrapper object from a wrapped message.
        References to the transport layer are required to send directly to
        connected downstream processes.
        """
        if message:
            self.recipe = workflows.recipe.Recipe(message["recipe"])
            self.recipe_pointer = int(message["recipe-pointer"])
            self.recipe_step = self.recipe[self.recipe_pointer]
            self.recipe_path = message.get("recipe-path", [])
            self.environment = message.get("environment", {})
            self.payload = message.get("payload")
        elif recipe:
            if isinstance(recipe, workflows.recipe.Recipe):
                self.recipe = recipe
            else:
                self.recipe = workflows.recipe.Recipe(recipe)
            self.recipe_pointer = None
            self.recipe_step = None
            self.recipe_path = []
            self.environment = {}
            self.payload = None
        else:
            raise ValueError(
                "A message or recipe is required to create " "a RecipeWrapper object."
            )
        self.default_channel = None
        self.transport = transport

    def send(self, *args, **kwargs):
        """Send messages to another service that is connected to the currently
        running service via the recipe. The 'send' method will either use a
        default channel name, set via the set_default_channel method, or an
        unnamed output definition.
        """
        if not self.transport:
            raise ValueError(
                "This RecipeWrapper object does not contain "
                "a reference to a transport object."
            )

        if not self.recipe_step:
            raise ValueError(
                "This RecipeWrapper object does not contain "
                "a recipe with a selected step."
            )

        if "output" not in self.recipe_step:
            # The current recipe step does not have output channels.
            return

        if isinstance(self.recipe_step["output"], dict):
            # The current recipe step does have named output channels.
            if self.default_channel:
                # Use named output channel
                self.send_to(self.default_channel, *args, **kwargs)

        else:
            # The current recipe step does have unnamed output channels.
            self._send_to_destinations(self.recipe_step["output"], *args, **kwargs)

    def send_to(self, channel, *args, **kwargs):
        """Send messages to another service that is connected to the currently
        running service via the recipe. Discard messages if the recipe does
        not have anything connected to the specified output channel.
        """
        if not self.transport:
            raise ValueError(
                "This RecipeWrapper object does not contain "
                "a reference to a transport object."
            )

        if not self.recipe_step:
            raise ValueError(
                "This RecipeWrapper object does not contain "
                "a recipe with a selected step."
            )

        if "output" not in self.recipe_step:
            # The current recipe step does not have output channels.
            return

        if not isinstance(self.recipe_step["output"], dict):
            # The current recipe step does not have named output channels.
            if self.default_channel == channel:
                # Use unnamed output channels
                self.send(*args, **kwargs)
            return

        if channel not in self.recipe_step["output"]:
            # The current recipe step does not have an output channel with this name.
            return

        self._send_to_destinations(self.recipe_step["output"][channel], *args, **kwargs)

    def set_default_channel(self, channel):
        """Define one named output channel to be equivalent to unnamed output
        channels. For this channel send() and send_to() will be identical."""
        self.default_channel = channel

    def start(self, header=None, **kwargs):
        """Trigger the start of a recipe, sending the defined payloads to the
        recipients set in the recipe. Any parameters to this function are
        passed to the transport send/broadcast methods.
        If the wrapped recipe has already been started then a ValueError will
        be raised.
        """
        if not self.transport:
            raise ValueError(
                "This RecipeWrapper object does not contain "
                "a reference to a transport object."
            )

        if self.recipe_step:
            raise ValueError("This recipe has already been started.")

        for destination, payload in self.recipe["start"]:
            self._send_to_destination(destination, header, payload, kwargs)

    def checkpoint(self, message, header=None, delay=0, **kwargs):
        """Send a message to the current recipe destination. This can be used to
        keep a state for longer processing tasks.
        :param delay: Delay transport of message by this many seconds
        """
        if not self.transport:
            raise ValueError(
                "This RecipeWrapper object does not contain "
                "a reference to a transport object."
            )

        if not self.recipe_step:
            raise ValueError(
                "This RecipeWrapper object does not contain "
                "a recipe with a selected step."
            )

        kwargs["delay"] = delay

        self._send_to_destination(
            self.recipe_pointer, header, message, kwargs, add_path_step=False
        )

    def apply_parameters(self, parameters):
        """Recursively apply parameter replacement (see recipe.py) to the wrapped
        recipe, updating internal references afterwards.
        While this operation is useful for testing it should not be used in
        production. Replacing parameters means that the recipe changes as it is
        passed down the chain of services. This makes debugging very difficult.
        """
        self.recipe.apply_parameters(parameters)
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

    def _send_to_destinations(self, destinations, message, header=None, **kwargs):
        """Send messages to a list of numbered destinations. This is an internal
        helper method used by the public 'send' methods.
        """
        if not isinstance(destinations, list):
            destinations = (destinations,)
        for destination in destinations:
            self._send_to_destination(destination, header, message, kwargs)

    def _send_to_destination(
        self, destination, header, payload, transport_kwargs, add_path_step=True
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
        if self.recipe[destination].get("queue"):
            self.transport.send(
                self.recipe[destination]["queue"],
                self._generate_full_recipe_message(destination, payload, add_path_step),
                headers=header,
                **dest_kwargs
            )
        if self.recipe[destination].get("topic"):
            self.transport.broadcast(
                self.recipe[destination]["topic"],
                self._generate_full_recipe_message(destination, payload, add_path_step),
                headers=header,
                **dest_kwargs
            )
