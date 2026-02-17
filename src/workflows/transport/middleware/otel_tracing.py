from __future__ import annotations

import functools
import json
from collections.abc import Callable

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagate import extract, inject

from workflows.transport.common_transport import MessageCallback, TemporarySubscription


class OTELTracingMiddleware:
    def __init__(self, tracer: trace.Tracer, service_name: str):
        self.tracer = tracer
        self.service_name = service_name

    def send(self, call_next: Callable, destination: str, message, **kwargs):
        # Get current span context (may be None if this is the root span)
        current_span = trace.get_current_span()
        parent_context = (
            trace.set_span_in_context(current_span) if current_span else None
        )

        with self.tracer.start_as_current_span(
            "transport.send",
            context=parent_context,
        ) as span:
            span.set_attribute("service_name", self.service_name)

            span.set_attribute("message", json.dumps(message))
            span.set_attribute("destination", destination)
            print("parent_context is...", parent_context)

            # Inject the current trace context into the message headers
            headers = kwargs.get("headers", {})
            if headers is None:
                headers = {}
            inject(headers)  # This modifies headers in-place
            kwargs["headers"] = headers

            return call_next(destination, message, **kwargs)

    def subscribe(
        self, call_next: Callable, channel: str, callback: Callable, **kwargs
    ) -> int:
        @functools.wraps(callback)
        def wrapped_callback(header, message):
            # Extract trace context from message headers
            ctx = extract(header) if header else Context()

            # Start a new span with the extracted context
            with self.tracer.start_as_current_span(
                "transport.subscribe",
                context=ctx,
            ) as span:
                span.set_attribute("service_name", self.service_name)

                span.set_attribute("message", json.dumps(message))
                span.set_attribute("channel", channel)

                # Call the original callback - this will process the message
                # and potentially call send() which will pick up this context
                return callback(header, message)

        return call_next(channel, wrapped_callback, **kwargs)

    def subscribe_broadcast(
        self, call_next: Callable, channel: str, callback: Callable, **kwargs
    ) -> int:
        @functools.wraps(callback)
        def wrapped_callback(header, message):
            # Extract trace context from message headers
            ctx = extract(header) if header else Context()

            #         # Start a new span with the extracted context
            with self.tracer.start_as_current_span(
                "transport.subscribe_broadcast",
                context=ctx,
            ) as span:
                span.set_attribute("service_name", self.service_name)

                span.set_attribute("message", json.dumps(message))
                span.set_attribute("channel", channel)

                return callback(header, message)

        return call_next(channel, wrapped_callback, **kwargs)

    def subscribe_temporary(
        self,
        call_next: Callable,
        channel_hint: str | None,
        callback: MessageCallback,
        **kwargs,
    ) -> TemporarySubscription:
        @functools.wraps(callback)
        def wrapped_callback(header, message):
            # Extract trace context from message headers
            ctx = extract(header) if header else Context()

            # Start a new span with the extracted context
            with self.tracer.start_as_current_span(
                "transport.subscribe_temporary",
                context=ctx,
            ) as span:
                span.set_attribute("service_name", self.service_name)

                span.set_attribute("message", json.dumps(message))
                if channel_hint:
                    span.set_attribute("channel_hint", channel_hint)

                return callback(header, message)

        return call_next(channel_hint, wrapped_callback, **kwargs)

    def unsubscribe(
        self,
        call_next: Callable,
        subscription: int,
        drop_callback_reference=False,
        **kwargs,
    ):
        # Get current span context
        current_span = trace.get_current_span()
        current_context = (
            trace.set_span_in_context(current_span) if current_span else Context()
        )

        with self.tracer.start_as_current_span(
            "transport.unsubscribe",
            context=current_context,
        ) as span:
            span.set_attribute("service_name", self.service_name)
            span.set_attribute("subscription_id", subscription)

            call_next(
                subscription, drop_callback_reference=drop_callback_reference, **kwargs
            )

    def ack(
        self,
        call_next: Callable,
        message,
        subscription_id: int | None = None,
        **kwargs,
    ):
        # Get current span context
        current_span = trace.get_current_span()
        current_context = (
            trace.set_span_in_context(current_span) if current_span else Context()
        )

        with self.tracer.start_as_current_span(
            "transport.ack",
            context=current_context,
        ) as span:
            span.set_attribute("service_name", self.service_name)
            span.set_attribute("message", json.dumps(message))
            if subscription_id:
                span.set_attribute("subscription_id", subscription_id)

            call_next(message, subscription_id=subscription_id, **kwargs)

    def nack(
        self,
        call_next: Callable,
        message,
        subscription_id: int | None = None,
        **kwargs,
    ):
        # Get current span context
        current_span = trace.get_current_span()
        current_context = (
            trace.set_span_in_context(current_span) if current_span else Context()
        )

        with self.tracer.start_as_current_span(
            "transport.nack",
            context=current_context,
        ) as span:
            span.set_attribute("service_name", self.service_name)

            span.set_attribute("message", json.dumps(message))
            if subscription_id:
                span.set_attribute("subscription_id", subscription_id)

            call_next(message, subscription_id=subscription_id, **kwargs)

    def transaction_begin(
        self, call_next: Callable, subscription_id: int | None = None, **kwargs
    ) -> int:
        """Start a new transaction span"""
        # Get current span context (may be None if this is the root span)
        current_span = trace.get_current_span()
        current_context = (
            trace.set_span_in_context(current_span) if current_span else Context()
        )

        with self.tracer.start_as_current_span(
            "transaction.begin",
            context=current_context,
        ) as span:
            span.set_attribute("service_name", self.service_name)

            if subscription_id:
                span.set_attribute("subscription_id", subscription_id)

            return call_next(subscription_id=subscription_id, **kwargs)

    def transaction_abort(
        self, call_next: Callable, transaction_id: int | None = None, **kwargs
    ):
        """Abort a transaction span"""
        # Get current span context
        current_span = trace.get_current_span()
        current_context = (
            trace.set_span_in_context(current_span) if current_span else Context()
        )

        with self.tracer.start_as_current_span(
            "transaction.abort",
            context=current_context,
        ) as span:
            span.set_attribute("service_name", self.service_name)

            if transaction_id:
                span.set_attribute("transaction_id", transaction_id)

            call_next(transaction_id=transaction_id, **kwargs)

    def transaction_commit(
        self, call_next: Callable, transaction_id: int | None = None, **kwargs
    ):
        """Commit a transaction span"""
        # Get current span context
        current_span = trace.get_current_span()
        current_context = (
            trace.set_span_in_context(current_span) if current_span else Context()
        )

        with self.tracer.start_as_current_span(
            "transaction.commit",
            context=current_context,
        ) as span:
            span.set_attribute("service_name", self.service_name)
            if transaction_id:
                span.set_attribute("transaction_id", transaction_id)

            call_next(transaction_id=transaction_id, **kwargs)
