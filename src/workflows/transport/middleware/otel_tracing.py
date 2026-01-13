from opentelemetry import trace
from workflows.transport.middleware import BaseTransportMiddleware
from collections.abc import Callable
import functools
from opentelemetry.propagate import inject

class OTELTracingMiddleware(BaseTransportMiddleware):
    def __init__(self, tracer: trace.Tracer, service_name: str):
        """
        Initialize the OpenTelemetry Tracing Middleware.

        :param tracer: An OpenTelemetry tracer instance used to create spans.
        """
        self.tracer = tracer
        self.service_name = service_name


    def send(self, call_next: Callable, destination, message, **kwargs):
        """
        Middleware for tracing the `send` operation
        
        :param call_next: The next middleware or the original `send` method.
        :param destination: The destination service to which the message is being sent. 
        :param message: The message being sent.
        :param kwargs: Additional arguments for the `send` method. 
        """

        # Start a new span for the `send` operation
        with self.tracer.start_as_current_span("transport.send") as span:
            # Attributes we're interested in
            span.set_attribute("service_name", self.service_name)
            span.set_attribute("destination", destination)
            span.set_attribute("message", str(message))
            
            # Inject trace context into message headers
            headers = kwargs.setdefault("headers", {})
            inject(headers)
            kwargs["headers"] = headers

            # Call the next middleware or the original `send` method
            return call_next(destination, message, **kwargs)

