from __future__ import annotations

import time
from typing import Callable, Optional

from prometheus_client import Counter, Gauge, Histogram

from . import BaseTransportMiddleware

SUBSCRIPTIONS = Counter(
    "workflows_transport_subscriptions_total",
    "The total number of transport subscriptions",
    ["source"],
)
CALLBACK_PROCESSING_TIME = Histogram(
    "workflows_callback_processing_time_seconds",
    "Histogram of callback processing time (in seconds)",
    ["source"],
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

    def subscribe(self, call_next: Callable, channel, callback, **kwargs) -> int:
        def wrapped_callback(header, message):
            start_time = time.perf_counter()
            result = callback(header, message)
            end_time = time.perf_counter()
            CALLBACK_PROCESSING_TIME.labels(source=self.source).observe(
                end_time - start_time
            )
            return result

        SUBSCRIPTIONS.labels(source=self.source).inc()
        return call_next(channel, wrapped_callback, **kwargs)

    def send(self, call_next: Callable, destination, message, **kwargs):
        SENDS.labels(source=self.source).inc()
        call_next(destination, message, **kwargs)

    def ack(
        self,
        call_next: Callable,
        message,
        subscription_id: Optional[int] = None,
        **kwargs
    ):
        ACKS.labels(source=self.source).inc()
        call_next(message, subscription_id=subscription_id, **kwargs)

    def nack(
        self,
        call_next: Callable,
        message,
        subscription_id: Optional[int] = None,
        **kwargs
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
