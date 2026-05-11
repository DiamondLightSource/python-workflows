from __future__ import annotations

import functools
import time
from collections.abc import Callable, Mapping
from typing import Any

from prometheus_client import Counter, Gauge, Histogram

from workflows.transport.common_transport import MessageCallback, TemporarySubscription

from . import BaseTransportMiddleware, get_callback_source

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
    def __init__(self, source: str) -> None:
        self.source = source

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
            start_time = time.perf_counter()
            callback(header, message)
            end_time = time.perf_counter()
            CALLBACK_PROCESSING_TIME.labels(
                source=get_callback_source(callback)
            ).observe(end_time - start_time)

        SUBSCRIPTIONS.labels(source=self.source).inc()
        ACTIVE_SUBSCRIPTIONS.labels(source=self.source).inc()
        return call_next(
            channel,
            wrapped_callback,
            disable_mangling=disable_mangling,
            acknowledgement=acknowledgement,
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
            start_time = time.perf_counter()
            callback(header, message)
            end_time = time.perf_counter()
            CALLBACK_PROCESSING_TIME.labels(
                source=get_callback_source(callback)
            ).observe(end_time - start_time)

        TEMPORARY_SUBSCRIPTIONS.labels(source=self.source).inc()
        ACTIVE_SUBSCRIPTIONS.labels(source=self.source).inc()
        return call_next(
            channel_hint,
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
            start_time = time.perf_counter()
            callback(header, message)
            end_time = time.perf_counter()
            CALLBACK_PROCESSING_TIME.labels(
                source=get_callback_source(callback)
            ).observe(end_time - start_time)

        BROADCAST_SUBSCRIPTIONS.labels(source=self.source).inc()
        ACTIVE_SUBSCRIPTIONS.labels(source=self.source).inc()
        return call_next(
            channel,
            wrapped_callback,
            disable_mangling=disable_mangling,
            **kwargs,
        )

    def unsubscribe(
        self,
        call_next: Callable[..., None],
        subscription: int,
        *,
        drop_callback_reference: bool = False,
        **kwargs: Any,
    ) -> None:
        ACTIVE_SUBSCRIPTIONS.labels(source=self.source).dec()
        call_next(
            subscription, drop_callback_reference=drop_callback_reference, **kwargs
        )

    def send(
        self,
        call_next: Callable[..., None],
        destination: str,
        message: Any,
        *,
        headers: dict | None = None,
        **kwargs: Any,
    ) -> None:
        SENDS.labels(source=self.source).inc()
        call_next(destination, message, headers=headers, **kwargs)

    def ack(
        self,
        call_next: Callable[..., None],
        message: Any,
        subscription_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        ACKS.labels(source=self.source).inc()
        call_next(message, subscription_id=subscription_id, **kwargs)

    def nack(
        self,
        call_next: Callable[..., None],
        message: Any,
        subscription_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        NACKS.labels(source=self.source).inc()
        call_next(message, subscription_id=subscription_id, **kwargs)

    def transaction_begin(
        self,
        call_next: Callable[..., int],
        subscription_id: int | None = None,
        **kwargs: Any,
    ) -> int:
        TRANSACTION_BEGIN.labels(source=self.source).inc()
        TRANSACTIONS_IN_PROGRESS.labels(source=self.source).inc()
        return call_next(subscription_id=subscription_id, **kwargs)

    def transaction_abort(
        self,
        call_next: Callable[..., None],
        transaction_id: int,
        **kwargs: Any,
    ) -> None:
        TRANSACTION_ABORT.labels(source=self.source).inc()
        TRANSACTIONS_IN_PROGRESS.labels(source=self.source).dec()
        call_next(transaction_id, **kwargs)

    def transaction_commit(
        self,
        call_next: Callable[..., None],
        transaction_id: int,
        **kwargs: Any,
    ) -> None:
        TRANSACTION_COMMIT.labels(source=self.source).inc()
        TRANSACTIONS_IN_PROGRESS.labels(source=self.source).dec()
        call_next(transaction_id, **kwargs)
