from __future__ import annotations

import time

from workflows.services.common_service import CommonService


class SampleProducer(CommonService):
    """An example service building on top of the workflow.services architecture,
    demonstrating how this architecture can be used.
    This service generates messages into a queue."""

    # Human readable service name
    _service_name = "Message Producer"

    # Logger name
    _logger_name = "workflows.service.sample_producer"

    counter = 0

    def initializing(self):
        """Service initialization. This function is run before any commands are
        received from the frontend. This is the place to request channel
        subscriptions with the messaging layer, and register callbacks.
        This function can be overridden by specific service implementations."""
        self.log.info("Starting message producer")
        self._register_idle(3, self.create_message)

    def create_message(self):
        """Create and send a unique message for this service."""
        self.counter += 1
        self.log.info("Sending message #%d", self.counter)
        self._transport.send(
            "transient.destination",
            "Message #%d\n++++++++Produced @%10.3f ms"
            % (self.counter, (time.time() % 1000) * 1000),
        )
