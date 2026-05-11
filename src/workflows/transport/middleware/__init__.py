from __future__ import annotations

import functools
import inspect
import logging
import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Concatenate

if TYPE_CHECKING:
    from workflows.transport.common_transport import (
        MessageCallback,
        TemporarySubscription,
    )

logger = logging.getLogger(__name__)


def get_callback_source(callable: Callable) -> str:
    if isinstance(callable, functools.partial):
        # functools.partial objects don't have a __qualname__ attribute
        # account for possibility of nested stack of functools.partials
        return get_callback_source(callable.func)
    else:
        # if defined, used the __qualname__ attribute, else fallback on the repr
        qualname = getattr(callable, "__qualname__", repr(callable).split("(")[0])
    module = inspect.getmodule(callable)
    if module:
        return f"{module.__name__}:{qualname}"
    return qualname


class BaseTransportMiddleware:
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
        return call_next(
            channel,
            callback,
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
        return call_next(
            channel_hint,
            callback,
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
        return call_next(channel, callback, disable_mangling=disable_mangling, **kwargs)

    def unsubscribe(
        self,
        call_next: Callable[..., None],
        subscription: int,
        *,
        drop_callback_reference: bool = False,
        **kwargs: Any,
    ) -> None:
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
        call_next(destination, message, headers=headers, **kwargs)

    def raw_send(
        self,
        call_next: Callable[..., None],
        destination: str,
        message: Any,
        **kwargs: Any,
    ) -> None:
        call_next(destination, message, **kwargs)

    def broadcast(
        self,
        call_next: Callable[..., None],
        destination: str,
        message: Any,
        **kwargs: Any,
    ) -> None:
        call_next(destination, message, **kwargs)

    def raw_broadcast(
        self,
        call_next: Callable[..., None],
        destination: str,
        message: Any,
        **kwargs: Any,
    ) -> None:
        call_next(destination, message, **kwargs)

    def ack(
        self,
        call_next: Callable[..., None],
        message: Any,
        subscription_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        call_next(message, subscription_id=subscription_id, **kwargs)

    def nack(
        self,
        call_next: Callable[..., None],
        message: Any,
        subscription_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        call_next(message, subscription_id=subscription_id, **kwargs)

    def transaction_begin(
        self,
        call_next: Callable[..., int],
        subscription_id: int | None = None,
        **kwargs: Any,
    ) -> int:
        return call_next(subscription_id=subscription_id, **kwargs)

    def transaction_abort(
        self,
        call_next: Callable[..., None],
        transaction_id: int,
        **kwargs: Any,
    ) -> None:
        call_next(transaction_id, **kwargs)

    def transaction_commit(
        self,
        call_next: Callable[..., None],
        transaction_id: int,
        **kwargs: Any,
    ) -> None:
        call_next(transaction_id, **kwargs)


class CounterMiddleware(BaseTransportMiddleware):
    def __init__(self) -> None:
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
        self.subscribe_count += 1
        logger.info(f"subscribe() count: {self.subscribe_count}")
        return call_next(
            channel,
            callback,
            disable_mangling=disable_mangling,
            acknowledgement=acknowledgement,
            **kwargs,
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
        call_next(destination, message, headers=headers, **kwargs)
        self.send_count += 1
        logger.info(f"send() count: {self.send_count}")

    def broadcast(
        self,
        call_next: Callable[..., None],
        destination: str,
        message: Any,
        **kwargs: Any,
    ) -> None:
        call_next(destination, message, **kwargs)
        self.broadcast_count += 1
        logger.info(f"broadcast() count: {self.broadcast_count}")

    def ack(
        self,
        call_next: Callable[..., None],
        message: Any,
        subscription_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        call_next(message, subscription_id=subscription_id, **kwargs)
        self.ack_count += 1
        logger.info(f"ack() count: {self.ack_count}")

    def nack(
        self,
        call_next: Callable[..., None],
        message: Any,
        subscription_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        call_next(message, subscription_id=subscription_id, **kwargs)
        self.nack_count += 1
        logger.info(f"nack() count: {self.nack_count}")

    def transaction_begin(
        self,
        call_next: Callable[..., int],
        subscription_id: int | None = None,
        **kwargs: Any,
    ) -> int:
        self.transaction_begin_count += 1
        logger.info(f"transaction_begin() count: {self.transaction_begin_count}")
        return call_next(subscription_id=subscription_id, **kwargs)

    def transaction_abort(
        self,
        call_next: Callable[..., None],
        transaction_id: int,
        **kwargs: Any,
    ) -> None:
        call_next(transaction_id, **kwargs)
        self.transaction_abort_count += 1
        logger.info(f"transaction_abort() count: {self.transaction_abort_count}")

    def transaction_commit(
        self,
        call_next: Callable[..., None],
        transaction_id: int,
        **kwargs: Any,
    ) -> None:
        call_next(transaction_id, **kwargs)
        self.transaction_commit_count += 1
        logger.info(f"transaction_commit() count: {self.transaction_commit_count}")


class TimerMiddleware(BaseTransportMiddleware):
    def __init__(
        self, logger: logging.Logger | None = None, level: int = logging.INFO
    ) -> None:
        if logger is None:
            logger = logging.getLogger(__name__)
        self.logger = logger
        self.level = level

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
            source = get_callback_source(callback)
            self.logger.log(
                self.level,
                f"Callback for {source} took {end_time - start_time:.4f} seconds",
            )

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
            source = get_callback_source(callback)
            self.logger.log(
                self.level,
                f"Callback for {source} took {end_time - start_time:.4f} seconds",
            )

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
            source = get_callback_source(callback)
            self.logger.log(
                self.level,
                f"Callback for {source} took {end_time - start_time:.4f} seconds",
            )

        return call_next(
            channel,
            wrapped_callback,
            disable_mangling=disable_mangling,
            **kwargs,
        )


def wrap[S, **P, R](
    f: Callable[Concatenate[S, P], R],
) -> Callable[Concatenate[S, P], R]:
    @functools.wraps(f)
    def wrapper(self: S, *args: P.args, **kwargs: P.kwargs) -> R:
        return functools.reduce(
            lambda call_next, m: (
                lambda *a, **kw: getattr(m, f.__name__)(call_next, *a, **kw)
            ),
            reversed(self.middleware),  # type: ignore[attr-defined]
            lambda *a, **kw: f(self, *a, **kw),
        )(*args, **kwargs)

    return wrapper
