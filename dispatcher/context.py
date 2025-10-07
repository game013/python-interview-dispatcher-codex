from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable, Optional


class RequestContext(AbstractAsyncContextManager):
    """Lightweight analogue to Go's context.Context."""

    def __init__(self, deadline: Optional[datetime] = None, parent: "RequestContext" | None = None):
        self._deadline = deadline
        self._parent = parent
        self._cancel_event = asyncio.Event()
        self._callbacks: list[Callable[[], None]] = []
        if parent:
            parent.add_cancel_callback(self.cancel)

    @property
    def deadline(self) -> Optional[datetime]:
        return self._deadline

    def remaining(self) -> Optional[float]:
        if self._deadline is None:
            return None
        delta = (self._deadline - datetime.now(timezone.utc)).total_seconds()
        if delta < 0:
            return 0.0
        return delta

    def is_cancelled(self) -> bool:
        if self._cancel_event.is_set():
            return True
        if self._parent and self._parent.is_cancelled():
            return True
        remaining = self.remaining()
        return remaining is not None and remaining <= 0

    async def wait_cancelled(self) -> None:
        if self.is_cancelled():
            return
        await self._cancel_event.wait()

    def cancel(self) -> None:
        if not self._cancel_event.is_set():
            self._cancel_event.set()
            for cb in self._callbacks:
                cb()

    def add_cancel_callback(self, cb: Callable[[], None]) -> None:
        self._callbacks.append(cb)

    async def wrap(self, coro: Awaitable):
        remaining = self.remaining()
        if remaining is None:
            return await coro
        try:
            return await asyncio.wait_for(coro, timeout=remaining)
        except asyncio.TimeoutError:  # pragma: no cover - defensive
            self.cancel()
            raise

    def with_timeout(self, timeout: float) -> "RequestContext":
        deadline = datetime.now(timezone.utc) + timedelta(seconds=timeout)
        return RequestContext(deadline=deadline, parent=self)

    def with_deadline(self, deadline: datetime) -> "RequestContext":
        return RequestContext(deadline=deadline, parent=self)

    async def __aenter__(self) -> "RequestContext":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        self.cancel()
        return None


def with_timeout(timeout: float) -> RequestContext:
    base = RequestContext()
    return base.with_timeout(timeout)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
