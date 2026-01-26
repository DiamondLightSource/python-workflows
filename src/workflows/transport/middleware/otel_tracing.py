from __future__ import annotations

import functools
from collections.abc import Callable

from opentelemetry import trace
from opentelemetry.propagate import extract

from workflows.transport.middleware import BaseTransportMiddleware


class OTELTracingMiddleware(BaseTransportMiddleware):
    def __init__(self, tracer: trace.Tracer, service_name: str):
        self.tracer = tracer
        self.service_name = service_name

    def subscribe(self, call_next: Callable, channel, callback, **kwargs) -> int:
        @functools.wraps(callback)
        def wrapped_callback(header, message):
            # Extract trace context from message headers
            ctx = extract(header) if header else None

            # Start a new span with the extracted context
            with self.tracer.start_as_current_span(
                "transport.subscribe", context=ctx
            ) as span:
                span.set_attribute("service_name", self.service_name)
                span.set_attribute("channel", channel)

                # Call the original callback
                return callback(header, message)

        # Call the next middleware with the wrapped callback
        return call_next(channel, wrapped_callback, **kwargs)
