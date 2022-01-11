from __future__ import annotations

import json
import time

from workflows.services.common_service import CommonService


class SampleConsumer(CommonService):
    """An example service building on top of the workflow.services architecture,
    demonstrating how this architecture can be used.
    This service consumes messages off a queue."""

    # Human readable service name
    _service_name = "Message Consumer"

    # Logger name
    _logger_name = "workflows.service.sample_consumer"

    def initializing(self):
        """Subscribe to a channel."""
        self._transport.subscribe("transient.destination", self.consume_message)

    def consume_message(self, header, message):
        """Consume a message"""
        t = (time.time() % 1000) * 1000

        if header:
            header_str = json.dumps(header, indent=2) + "\n" + "----------------" + "\n"
        else:
            header_str = ""
        if isinstance(message, dict):
            message = json.dumps(message, indent=2) + "\n" + "----------------" + "\n"

        self.log.info(
            f"=== Consume ====\n{header_str}{message}\nReceived message @{t:10.3f} ms"
        )
        time.sleep(0.1)
