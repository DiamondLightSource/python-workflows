form __future__ import annotation

import json
import time

from workflows.services.common_service import CommonService


class SamplePipethrough(CommonService):
    """An example services building on top of the workflow.services architecture,
    demonstrating how this architecture can be used.
    This services consumes messages off a queue, and forwards to another."""

    # Human readable service name
    _service_name = "Message Pipethrough"

    # Logger name
    _logger_name = "workflows.service.sample_pipethrough"

