from __future__ import absolute_import, division, print_function

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
        logmessage = {
            "time": (time.time() % 1000) * 1000,
            "header": "",
            "message": message,
        }
        if header:
            logmessage["header"] = (
                json.dumps(header, indent=2) + "\n" + "----------------" + "\n"
            )
        if isinstance(message, dict):
            logmessage["message"] = (
                json.dumps(message, indent=2) + "\n" + "----------------" + "\n"
            )

        print("=== Consume ====\n{header}{message}".format(**logmessage))
        self.log.info("Received message @{time}".format(**logmessage))
        self.log.debug(
            "Received message @{time}\n{header}{message}".format(**logmessage)
        )
        time.sleep(0.1)
