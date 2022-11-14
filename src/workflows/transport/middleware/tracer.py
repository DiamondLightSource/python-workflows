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

resource = Resource(attributes={
    SERVICE_NAME: "Common Service"
})

processor = BatchSpanProcessor(OTLPSpanExporter( \
        endpoint="http://localhost:4318/v1/traces"))
provider = TracerProvider(resource = resource)
# A provider provides tracers:
provider = TracerProvider(resource=resource)
provider.add_span_processor(processor)
# In python trace is global:
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

class TracerMiddleware(BaseTransportMiddleware):
    def __init__(self, service_name: str):
        self.service_name = service_name

    def _extract_trace_context(self, message):
        """Retrieves span context from message"""
        try:
            carrier = message['trace_context']
            ctx = TraceContextTextMapPropagator().extract(carrier=carrier) 
            print(f"Extracting {ctx}")
            return ctx
        except KeyError:
            print(f"Extracted nothing")
            return {}

    def _inject_trace_context(self, message):
        """Inserts trace context into message"""
        carrier = {}
        TraceContextTextMapPropagator().inject(carrier)
        message['trace_context'] = carrier
        print(f"injecting {carrier}")


    def subscribe(self, call_next: Callable, channel, callback, **kwargs) -> int:
        @functools.wraps(callback)
        def wrapped_callback(header, message):
            ctx = self._extract_trace_context(message)
            with tracer.start_as_current_span(self.service_name, context=ctx) as span:
                if ctx == {}:
                    self._inject_trace_context(message)
                return callback(header, message)

        return call_next(channel, wrapped_callback, **kwargs)

    def send(self, call_next: Callable, destination, message, **kwargs):
        self._inject_trace_context(message)
        call_next(destination, message, **kwargs)
