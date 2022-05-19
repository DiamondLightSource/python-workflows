from __future__ import annotations

import functools
import inspect
import time
from typing import Callable, Optional

from prometheus_client import Counter, Gauge, Histogram

from workflows.transport.common_transport import MessageCallback, TemporarySubscription

from . import BaseTransportMiddleware

SUBSCRIPTIONS = Counter(
    "workflows_transport_subscriptions_total",
    "The total number of transport subscriptions",
    ["source"],
)
BROADCAST_SUBSCRIPTIONS = Counter(
    "workflows_transport_broadcast_subscriptions_total",
    "The total number of transport broadcast subscriptions",
    ["source"],
)
TEMPORARY_SUBSCRIPTIONS = Counter(
    "workflows_transport_temporary_subscriptions_total",
    "The total number of transport temporary subscriptions",
    ["source"],
)
ACTIVE_SUBSCRIPTIONS = Gauge(
    "workflows_transport_active_subscriptions",
    "The total number of transport subscriptions",
    ["source"],
)
CALLBACK_PROCESSING_TIME = Histogram(
    "workflows_callback_processing_time_seconds",
    "Histogram of callback processing time (in seconds)",
    ["source"],
    unit="seconds",
)
ACKS = Counter(
    "workflows_transport_ack_total",
    "Total count of transport acknowledgements.",
    ["source"],
)
NACKS = Counter(
    "workflows_transport_nack_total",
    "Total count of transport negative acknowledgements.",
    ["source"],
)
SENDS = Counter(
    "workflows_transport_send_total",
    "Total number of messages sent",
    ["source"],
)
BROADCASTS = Counter(
    "workflows_transport_broadcast_total",
    "Total number of messages broadcast",
    ["source"],
)
TRANSACTION_BEGIN = Counter(
    "workflows_transport_transaction_begin_total",
    "Total number of transactions begun",
    ["source"],
)
TRANSACTION_ABORT = Counter(
    "workflows_transport_transaction_abort_total",
    "Total number of transactions aborted",
    ["source"],
)
TRANSACTION_COMMIT = Counter(
    "workflows_transport_transaction_commit_total",
    "Total number of transactions committed",
    ["source"],
)
TRANSACTIONS_IN_PROGRESS = Gauge(
    "workflows_transport_transactions_in_progress",
    "Total number of transactions currently in progress",
    ["source"],
)


class PrometheusMiddleware(BaseTransportMiddleware):
    def __init__(self, source: str):
        self.source = source

    @staticmethod
    def get_callback_source(callable: Callable):
        if isinstance(callable, functools.partial):
            # functools.partial objects don't have a __qualname__ attribute
            # account for possibility of nested stack of functools.partials
            return PrometheusMiddleware.get_callback_source(callable.func)
        else:
            # if defined, used the __qualname__ attribute, else fallback on the repr
            qualname = getattr(callable, "__qualname__", repr(callable).split("(")[0])
        module = inspect.getmodule(callable)
        if module:
            return f"{module.__name__}:{qualname}"
        return qualname

    def subscribe(self, call_next: Callable, channel, callback, **kwargs) -> int:
        def wrapped_callback(header, message):
            start_time = time.perf_counter()
            result = callback(header, message)
            end_time = time.perf_counter()
            CALLBACK_PROCESSING_TIME.labels(
                source=self.get_callback_source(callback)
            ).observe(end_time - start_time)
            return result

        SUBSCRIPTIONS.labels(source=self.source).inc()
        ACTIVE_SUBSCRIPTIONS.labels(source=self.source).inc()
        return call_next(channel, wrapped_callback, **kwargs)

    def subscribe_temporary(
        self,
        call_next: Callable,
        channel_hint: Optional[str],
        callback: MessageCallback,
        **kwargs,
    ) -> TemporarySubscription:
        def wrapped_callback(header, message):
            start_time = time.perf_counter()
            result = callback(header, message)
            end_time = time.perf_counter()
            CALLBACK_PROCESSING_TIME.labels(
                source=self.get_callback_source(callback)
            ).observe(end_time - start_time)
            return result

        TEMPORARY_SUBSCRIPTIONS.labels(source=self.source).inc()
        ACTIVE_SUBSCRIPTIONS.labels(source=self.source).inc()
        return call_next(channel_hint, wrapped_callback, **kwargs)

    def subscribe_broadcast(
        self, call_next: Callable, channel, callback, **kwargs
    ) -> int:
        def wrapped_callback(header, message):
            start_time = time.perf_counter()
            result = callback(header, message)
            end_time = time.perf_counter()
            CALLBACK_PROCESSING_TIME.labels(
                source=self.get_callback_source(callback)
            ).observe(end_time - start_time)
            return result

        BROADCAST_SUBSCRIPTIONS.labels(source=self.source).inc()
        ACTIVE_SUBSCRIPTIONS.labels(source=self.source).inc()
        return call_next(channel, wrapped_callback, **kwargs)

    def unsubscribe(
        self,
        call_next: Callable,
        subscription: int,
        drop_callback_reference=False,
        **kwargs,
    ):
        ACTIVE_SUBSCRIPTIONS.labels(source=self.source).dec()
        call_next(
            subscription, drop_callback_reference=drop_callback_reference, **kwargs
        )

    def send(self, call_next: Callable, destination, message, **kwargs):
        SENDS.labels(source=self.source).inc()
        call_next(destination, message, **kwargs)

    def ack(
        self,
        call_next: Callable,
        message,
        subscription_id: Optional[int] = None,
        **kwargs,
    ):
        ACKS.labels(source=self.source).inc()
        call_next(message, subscription_id=subscription_id, **kwargs)

    def nack(
        self,
        call_next: Callable,
        message,
        subscription_id: Optional[int] = None,
        **kwargs,
    ):
        NACKS.labels(source=self.source).inc()
        call_next(message, subscription_id=subscription_id, **kwargs)

    def transaction_begin(self, call_next: Callable, *args, **kwargs) -> int:
        TRANSACTION_BEGIN.labels(source=self.source).inc()
        TRANSACTIONS_IN_PROGRESS.labels(source=self.source).inc()
        return call_next(*args, **kwargs)

    def transaction_abort(self, call_next: Callable, *args, **kwargs):
        TRANSACTION_ABORT.labels(source=self.source).inc()
        TRANSACTIONS_IN_PROGRESS.labels(source=self.source).dec()
        call_next(*args, **kwargs)

    def transaction_commit(self, call_next: Callable, *args, **kwargs):
        TRANSACTION_COMMIT.labels(source=self.source).inc()
        TRANSACTIONS_IN_PROGRESS.labels(source=self.source).dec()
        call_next(*args, **kwargs)
