from __future__ import annotations

import functools
from collections.abc import Callable, Mapping
from typing import Any

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import Span

from workflows.transport.common_transport import MessageCallback, TemporarySubscription
from workflows.transport.middleware import BaseTransportMiddleware


class OTELTracingMiddleware(BaseTransportMiddleware):
    def __init__(self, tracer: trace.Tracer, service_name: str) -> None:
        self.tracer = tracer
        self.service_name = service_name

    def _set_span_attributes(self, span: Span, **attributes: Any) -> None:
        """Helper method to set common span attributes"""
        span.set_attribute("service_name", self.service_name)
        for key, value in attributes.items():
            if value is not None:
                span.set_attribute(key, value)

    def send(
        self,
        call_next: Callable[..., None],
        destination: str,
        message: Any,
        *,
        headers: dict | None = None,
        **kwargs: Any,
    ) -> None:
        # Get current span context (may be None if this is the root span)
        current_span = trace.get_current_span()
        parent_context = (
            trace.set_span_in_context(current_span) if current_span else None
        )

        with self.tracer.start_as_current_span(
            "transport.send",
            context=parent_context,
        ) as span:
            self._set_span_attributes(span, destination=destination)

            # Inject the current trace context into the message headers
            if headers is None:
                headers = {}
            inject(headers)  # This modifies headers in-place

            call_next(destination, message, headers=headers, **kwargs)

    def subscribe(
        self,
        call_next: Callable[..., int],
        channel: str,
        callback: MessageCallback,
        *,
        disable_mangling: bool = False,
        acknowledgement: bool = False,
        **kwargs: Any,
    ) -> int:
        @functools.wraps(callback)
        def wrapped_callback(header: Mapping[str, Any], message: Any) -> None:
            # Extract trace context from message headers
            ctx = extract(header) if header else Context()

            # Start a new span with the extracted context
            with self.tracer.start_as_current_span(
                "transport.subscribe",
                context=ctx,
            ) as span:
                self._set_span_attributes(span, channel=channel)

                # Call the original callback - this will process the message
                # and potentially call send() which will pick up this context
                return callback(header, message)

        return call_next(
            channel,
            wrapped_callback,
            disable_mangling=disable_mangling,
            acknowledgement=acknowledgement,
            **kwargs,
        )

    def subscribe_broadcast(
        self,
        call_next: Callable[..., int],
        channel: str,
        callback: MessageCallback,
        *,
        disable_mangling: bool = False,
        **kwargs: Any,
    ) -> int:
        @functools.wraps(callback)
        def wrapped_callback(header: Mapping[str, Any], message: Any) -> None:
            # Extract trace context from message headers
            ctx = extract(header) if header else Context()

            # Start a new span with the extracted context
            with self.tracer.start_as_current_span(
                "transport.subscribe_broadcast",
                context=ctx,
            ) as span:
                self._set_span_attributes(span, channel=channel)

                return callback(header, message)

        return call_next(
            channel,
            wrapped_callback,
            disable_mangling=disable_mangling,
            **kwargs,
        )

    def subscribe_temporary(
        self,
        call_next: Callable[..., TemporarySubscription],
        channel_hint: str | None,
        callback: MessageCallback,
        *,
        disable_mangling: bool = False,
        acknowledgement: bool = False,
        **kwargs: Any,
    ) -> TemporarySubscription:
        @functools.wraps(callback)
        def wrapped_callback(header: Mapping[str, Any], message: Any) -> None:
            # Extract trace context from message headers
            ctx = extract(header) if header else Context()

            # Start a new span with the extracted context
            with self.tracer.start_as_current_span(
                "transport.subscribe_temporary",
                context=ctx,
            ) as span:
                self._set_span_attributes(span, channel_hint=channel_hint)

                return callback(header, message)

        return call_next(
            channel_hint,
            wrapped_callback,
            disable_mangling=disable_mangling,
            acknowledgement=acknowledgement,
            **kwargs,
        )

    def raw_send(
        self,
        call_next: Callable[..., None],
        destination: str,
        message: Any,
        **kwargs: Any,
    ) -> None:
        # Get current span context (may be None if this is the root span)
        current_span = trace.get_current_span()
        parent_context = (
            trace.set_span_in_context(current_span) if current_span else None
        )

        with self.tracer.start_as_current_span(
            "transport.raw_send",
            context=parent_context,
        ) as span:
            self._set_span_attributes(span, destination=destination)

            # Inject the current trace context into the message headers
            headers = kwargs.get("headers", {})
            if headers is None:
                headers = {}
            inject(headers)  # This modifies headers in-place
            kwargs["headers"] = headers

            call_next(destination, message, **kwargs)

    def broadcast(
        self,
        call_next: Callable[..., None],
        destination: str,
        message: Any,
        **kwargs: Any,
    ) -> None:
        # Get current span context (may be None if this is the root span)
        current_span = trace.get_current_span()
        parent_context = (
            trace.set_span_in_context(current_span) if current_span else None
        )

        with self.tracer.start_as_current_span(
            "transport.broadcast",
            context=parent_context,
        ) as span:
            self._set_span_attributes(span, destination=destination)

            # Inject the current trace context into the message headers
            headers = kwargs.get("headers", {})
            if headers is None:
                headers = {}
            inject(headers)  # This modifies headers in-place
            kwargs["headers"] = headers

            call_next(destination, message, **kwargs)

    def raw_broadcast(
        self,
        call_next: Callable[..., None],
        destination: str,
        message: Any,
        **kwargs: Any,
    ) -> None:
        # Get current span context (may be None if this is the root span)
        current_span = trace.get_current_span()
        parent_context = (
            trace.set_span_in_context(current_span) if current_span else None
        )

        with self.tracer.start_as_current_span(
            "transport.raw_broadcast",
            context=parent_context,
        ) as span:
            self._set_span_attributes(span, destination=destination)

            # Inject the current trace context into the message headers
            headers = kwargs.get("headers", {})
            if headers is None:
                headers = {}
            inject(headers)  # This modifies headers in-place
            kwargs["headers"] = headers

            call_next(destination, message, **kwargs)

    def unsubscribe(
        self,
        call_next: Callable[..., None],
        subscription: int,
        *,
        drop_callback_reference: bool = False,
        **kwargs: Any,
    ) -> None:
        # Get current span context
        current_span = trace.get_current_span()
        current_context = (
            trace.set_span_in_context(current_span) if current_span else Context()
        )

        with self.tracer.start_as_current_span(
            "transport.unsubscribe",
            context=current_context,
        ) as span:
            self._set_span_attributes(span, subscription_id=subscription)

            call_next(
                subscription, drop_callback_reference=drop_callback_reference, **kwargs
            )

    def ack(
        self,
        call_next: Callable[..., None],
        message: Any,
        subscription_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        # Get current span context
        current_span = trace.get_current_span()
        current_context = (
            trace.set_span_in_context(current_span) if current_span else Context()
        )

        with self.tracer.start_as_current_span(
            "transport.ack",
            context=current_context,
        ) as span:
            self._set_span_attributes(span, subscription_id=subscription_id)

            call_next(message, subscription_id=subscription_id, **kwargs)

    def nack(
        self,
        call_next: Callable[..., None],
        message: Any,
        subscription_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        # Get current span context
        current_span = trace.get_current_span()
        current_context = (
            trace.set_span_in_context(current_span) if current_span else Context()
        )

        with self.tracer.start_as_current_span(
            "transport.nack",
            context=current_context,
        ) as span:
            self._set_span_attributes(span, subscription_id=subscription_id)

            call_next(message, subscription_id=subscription_id, **kwargs)

    def transaction_begin(
        self,
        call_next: Callable[..., int],
        subscription_id: int | None = None,
        **kwargs: Any,
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
            self._set_span_attributes(span, subscription_id=subscription_id)

            return call_next(subscription_id=subscription_id, **kwargs)

    def transaction_abort(
        self,
        call_next: Callable[..., None],
        transaction_id: int,
        **kwargs: Any,
    ) -> None:
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
            self._set_span_attributes(span, transaction_id=transaction_id)

            call_next(transaction_id, **kwargs)

    def transaction_commit(
        self,
        call_next: Callable[..., None],
        transaction_id: int,
        **kwargs: Any,
    ) -> None:
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
            self._set_span_attributes(span, transaction_id=transaction_id)

            call_next(transaction_id, **kwargs)
