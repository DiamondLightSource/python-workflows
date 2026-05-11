from __future__ import annotations

import json
import time
from typing import Any

import workflows.recipe
from workflows.recipe.wrapper import RecipeWrapper
from workflows.services.common_service import CommonService


class SamplePipethrough(CommonService):
    """An example services building on top of the workflow.services architecture,
    demonstrating how this architecture can be used.
    This services consumes messages off a queue, and forwards to another."""

    # Human readable service name
    _service_name = "Message Pipethrough"

    # Logger name
    _logger_name = "workflows.service.sample_pipethrough"

    def initializing(self) -> None:
        """Subscribe to a channel."""
        workflows.recipe.wrap_subscribe(
            self.transport,
            "transient.destination",
            self.process,
        )

    def process(self, rw: RecipeWrapper, header: dict, message: Any) -> None:
        """Consume message and send to output pipe."""
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

        rw.send(0)
