from opentelemetry import trace
from workflows.transport.middleware import BaseTransportMiddleware
from collections.abc import Callable
import functools
from opentelemetry.propagate import inject, extract

class OTELTracingMiddleware(BaseTransportMiddleware):
    def __init__(self, tracer: trace.Tracer, service_name: str):
        """
        Initialize the OpenTelemetry Tracing Middleware.

        :param tracer: An OpenTelemetry tracer instance used to create spans.
        """
        self.tracer = tracer
        self.service_name = service_name

    def subscribe(self, call_next: Callable, channel, callback, **kwargs) -> int:
        @functools.wraps(callback)
        def wrapped_callback(header, message):
            # Extract trace context from message headers
            ctx = extract(header) if header else None
            
            # Start a new span with the extracted context
            with self.tracer.start_as_current_span(
                "transport.subscribe",
                context=ctx
            ) as span:
                span.set_attribute("service_name", self.service_name)
                span.set_attribute("channel", channel)
        
                
                # Call the original callback
                return callback(header, message)
        
        # Call the next middleware with the wrapped callback
        return call_next(channel, wrapped_callback, **kwargs)