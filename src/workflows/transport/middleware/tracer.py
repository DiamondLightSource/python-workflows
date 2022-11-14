from __future__ import annotations

import functools
from typing import Callable

from . import BaseTransportMiddleware

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import Status, StatusCode
from opentelemetry.sdk.trace.export import(
        BatchSpanProcessor,
        ConsoleSpanExporter,
        )
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.trace.propagation.tracecontext \
    import TraceContextTextMapPropagator
from opentelemetry.exporter.otlp.proto.http.trace_exporter \
        import OTLPSpanExporter


class TracerMiddleware(BaseTransportMiddleware):
    def __init__(self, service_name: str):
        self.service_name = service_name
        self._initiate_tracers(service_name)

    def _initiate_tracers(self, service_name):
        """Initiates everything needed for tracing."""
        # Label this resource as its service:
        resource = Resource(attributes={
            SERVICE_NAME: service_name 
        })
        # Export to OpenTelemetry Collector:
        processor = BatchSpanProcessor(OTLPSpanExporter( \
                endpoint="http://localhost:4318/v1/traces"))
        # A provider provides tracers:
        provider = TracerProvider(resource=resource)
        provider.add_span_processor(processor)
        # A tracer provides traces:
        trace.set_tracer_provider(provider)
        self.tracer = trace.get_tracer(__name__)

    def _extract_trace_context(self, message):
        """Retrieves Context object from message."""
        carrier = message.get('trace_context')
        if carrier:
            # Deserialise serialised context into a Context object:
            ctx = TraceContextTextMapPropagator().extract(carrier=carrier) 
            return ctx
        # If no context, leave empty:
        return {}

    def _inject_trace_context(self, message):
        """Inserts serialized trace context into message."""
        if type(message) == str:
            print("Warning: string message received")
            return
        carrier = {}
        # If called outside of a span context, just leave carrier empty
        # (very safe!)
        TraceContextTextMapPropagator().inject(carrier)
        message['trace_context'] = carrier

    def subscribe(self, call_next: Callable, channel, callback, **kwargs) -> int:
        """The callback includes 'everything' that happens in a service that
        we care about, so we wrap it in a span context.
        To link the current span context with others from the same request
        we inject/extract the serialized trace context in the recipe message."""
        @functools.wraps(callback)
        def wrapped_callback(header, message):
            ctx = self._extract_trace_context(message)
            with self.tracer.start_as_current_span(self.service_name, context=ctx) as span:
                if ctx == {}:
                    self._inject_trace_context(message)
                return callback(header, message)

        return call_next(channel, wrapped_callback, **kwargs)

    def send(self, call_next: Callable, destination, message, **kwargs):
        """Because send is usually called within a callback, it is inside a span
        context, so we can inject its trace context into the message."""
        self._inject_trace_context(message)
        call_next(destination, message, **kwargs)
