from __future__ import annotations

import functools
import logging
import time
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from workflows.transport.common_transport import (
        MessageCallback,
        TemporarySubscription,
    )

logger = logging.getLogger(__name__)


class BaseTransportMiddleware:
    def subscribe(self, call_next: Callable, channel, callback, **kwargs) -> int:
        return call_next(channel, callback, **kwargs)

    def subscribe_temporary(
        self,
        call_next: Callable,
        channel_hint: Optional[str],
        callback: MessageCallback,
        **kwargs,
    ) -> TemporarySubscription:
        return call_next(channel_hint, callback, **kwargs)

    def subscribe_broadcast(
        self, call_next: Callable, channel, callback, **kwargs
    ) -> int:
        return call_next(channel, callback, **kwargs)

    def unsubscribe(
        self,
        call_next: Callable,
        subscription: int,
        drop_callback_reference=False,
        **kwargs,
    ):
        call_next(
            subscription, drop_callback_reference=drop_callback_reference, **kwargs
        )

    def send(self, call_next: Callable, destination, message, **kwargs):
        call_next(destination, message, **kwargs)

    def raw_send(self, call_next: Callable, destination, message, **kwargs):
        call_next(destination, message, **kwargs)

    def broadcast(self, call_next: Callable, destination, message, **kwargs):
        call_next(destination, message, **kwargs)

    def raw_broadcast(self, call_next: Callable, destination, message, **kwargs):
        call_next(destination, message, **kwargs)

    def ack(
        self,
        call_next: Callable,
        message,
        subscription_id: Optional[int] = None,
        **kwargs,
    ):
        call_next(message, subscription_id=subscription_id, **kwargs)

    def nack(
        self,
        call_next: Callable,
        message,
        subscription_id: Optional[int] = None,
        **kwargs,
    ):
        call_next(message, subscription_id=subscription_id, **kwargs)

    def transaction_begin(
        self, call_next: Callable, subscription_id: Optional[int] = None, **kwargs
    ) -> int:
        return call_next(subscription_id=subscription_id, **kwargs)

    def transaction_abort(
        self, call_next: Callable, transaction_id: int = None, **kwargs
    ):
        call_next(transaction_id, **kwargs)

    def transaction_commit(
        self, call_next: Callable, transaction_id: int = None, **kwargs
    ):
        call_next(transaction_id, **kwargs)


class CounterMiddleware(BaseTransportMiddleware):
    def __init__(self):
        self.subscribe_count = 0
        self.subscribe_broadcast_count = 0
        self.send_count = 0
        self.broadcast_count = 0
        self.ack_count = 0
        self.nack_count = 0
        self.transaction_begin_count = 0
        self.transaction_abort_count = 0
        self.transaction_commit_count = 0
        super().__init__()

    def subscribe(self, call_next: Callable, channel, callback, **kwargs) -> int:
        self.subscribe_count += 1
        logger.info(f"subscribe() count: {self.subscribe_count}")
        return call_next(channel, callback, **kwargs)

    def send(self, call_next: Callable, destination, message, **kwargs):
        call_next(destination, message, **kwargs)
        self.send_count += 1
        logger.info(f"send() count: {self.send_count}")

    def broadcast(self, call_next: Callable, destination, message, **kwargs):
        call_next(destination, message, **kwargs)
        self.broadcast_count += 1
        logger.info(f"broadcast() count: {self.broadcast_count}")

    def ack(
        self,
        call_next: Callable,
        message,
        subscription_id: Optional[int] = None,
        **kwargs,
    ):
        call_next(message, subscription_id=subscription_id, **kwargs)
        self.ack_count += 1
        logger.info(f"ack() count: {self.ack_count}")

    def nack(
        self,
        call_next: Callable,
        message,
        subscription_id: Optional[int] = None,
        **kwargs,
    ):
        call_next(message, subscription_id=subscription_id, **kwargs)
        self.nack_count += 1
        logger.info(f"nack() count: {self.nack_count}")

    def transaction_begin(self, call_next: Callable, *args, **kwargs) -> int:
        self.transaction_begin_count += 1
        logger.info(f"transaction_begin() count: {self.transaction_begin_count}")
        return call_next(*args, **kwargs)

    def transaction_abort(self, call_next: Callable, *args, **kwargs):
        call_next(*args, **kwargs)
        self.transaction_abort_count += 1
        logger.info(f"transaction_abort() count: {self.transaction_abort_count}")

    def transaction_commit(self, call_next: Callable, *args, **kwargs):
        call_next(*args, **kwargs)
        self.transaction_commit_count += 1
        logger.info(f"transaction_commit() count: {self.transaction_commit_count}")


class TimerMiddleware(BaseTransportMiddleware):
    def subscribe(self, call_next: Callable, channel, callback, **kwargs) -> int:
        def wrapped_callback(header, message):
            start_time = time.perf_counter()
            result = callback(header, message)
            end_time = time.perf_counter()
            logger.info(
                f"Callback for {header['destination']} took: {end_time - start_time:.3f}"
            )
            return result

        return call_next(channel, wrapped_callback, **kwargs)

    def send(self, call_next: Callable, destination, message, **kwargs):
        start_time = time.perf_counter()
        call_next(destination, message, **kwargs)
        end_time = time.perf_counter()
        logger.info(f"send() took: {end_time - start_time:.3f}")


def wrap(f: Callable):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):

        return functools.reduce(
            lambda call_next, m: lambda *args, **kwargs: getattr(m, f.__name__)(
                call_next, *args, **kwargs
            ),
            reversed(self.middleware),
            lambda *args, **kwargs: f(self, *args, **kwargs),
        )(*args, **kwargs)

    return wrapper
