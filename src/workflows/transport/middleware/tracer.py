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

    def _get_trace_context(self, message):
        """If a trace context exists in the recipe wrapper's environment, get it"""
        try:
            #carrier = message['environment']['trace_context']
            carrier = message['trace_context']
            ctx = TraceContextTextMapPropagator().extract(carrier=carrier) 
            return ctx
        except KeyError:
            return {}

    def subscribe(self, call_next: Callable, channel, callback, **kwargs) -> int:
        print(f"call_next: {call_next}, channel: {channel}, callback: {callback}, kwargs: {kwargs}")

        @functools.wraps(callback)
        def wrapped_callback(header, message):
            print(f"wrapped_callback header: {header}, message: {message}")
            
            ctx = self._get_trace_context(message)
            with tracer.start_as_current_span(self.service_name, context=ctx) as span:
                if ctx == {}:
                    print("inserting trace_context into message")
                    message['trace_context'] = "foo"
                return callback(header, message)

        return call_next(channel, wrapped_callback, **kwargs)

    def send(self, call_next: Callable, destination, message, **kwargs):
        carrier = {}
        TraceContextTextMapPropagator().inject(carrier)
        message['trace_context'] = carrier

        call_next(destination, message, **kwargs)
