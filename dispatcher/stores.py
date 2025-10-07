from __future__ import annotations

import asyncio
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Protocol

from .models import Assignment

logger = logging.getLogger(__name__)


class IdempotencyStore(Protocol):
    async def get(self, request_id: str) -> Optional[Assignment]:
        ...

    async def set(self, request_id: str, assignment: Assignment, ttl_seconds: int) -> None:
        ...


class AssignmentStore(Protocol):
    async def get(self, request_id: str) -> Optional[Assignment]:
        ...

    async def save(self, assignment: Assignment) -> None:
        ...


class ReservationStore(Protocol):
    async def try_reserve(self, agent_id: str, start: datetime, end: datetime) -> bool:
        ...


class InMemoryIdempotencyStore:
    def __init__(self):
        self._values: dict[str, tuple[Assignment, datetime]] = {}
        self._lock = asyncio.Lock()

    async def get(self, request_id: str) -> Optional[Assignment]:
        async with self._lock:
            record = self._values.get(request_id)
            if not record:
                return None
            assignment, expires_at = record
            if expires_at < datetime.now(timezone.utc):
                del self._values[request_id]
                return None
            return assignment

    async def set(self, request_id: str, assignment: Assignment, ttl_seconds: int) -> None:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        async with self._lock:
            self._values[request_id] = (assignment, expires_at)


class InMemoryAssignmentStore:
    def __init__(self):
        self._values: dict[str, Assignment] = {}
        self._lock = asyncio.Lock()

    async def get(self, request_id: str) -> Optional[Assignment]:
        async with self._lock:
            return self._values.get(request_id)

    async def save(self, assignment: Assignment) -> None:
        async with self._lock:
            self._values[assignment.request_id] = assignment


class InMemoryReservationStore:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._reservations: dict[str, list[tuple[datetime, datetime]]] = {}

    async def try_reserve(self, agent_id: str, start: datetime, end: datetime) -> bool:
        async with self._lock:
            slots = self._reservations.setdefault(agent_id, [])
            for existing_start, existing_end in slots:
                if overlaps(existing_start, existing_end, start, end):
                    return False
            slots.append((start, end))
            return True


class MetricsRecorder:
    def __init__(self):
        self._counts = Counter()
        self._lock = asyncio.Lock()

    async def increment(self, key: str) -> None:
        async with self._lock:
            self._counts[key] += 1

    async def snapshot(self) -> Counter:
        async with self._lock:
            return Counter(self._counts)


@dataclass
class StoreDecorators:
    logging_enabled: bool = True
    metrics_enabled: bool = True


def overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return max(a_start, b_start) < min(a_end, b_end)


class LoggingIdempotencyStore(IdempotencyStore):
    def __init__(self, inner: IdempotencyStore, logger_name: str = "idempotency"):
        self._inner = inner
        self._logger = logging.getLogger(logger_name)

    async def get(self, request_id: str) -> Optional[Assignment]:
        result = await self._inner.get(request_id)
        self._logger.debug("idempotency.get", extra={"request_id": request_id, "hit": bool(result)})
        return result

    async def set(self, request_id: str, assignment: Assignment, ttl_seconds: int) -> None:
        await self._inner.set(request_id, assignment, ttl_seconds)
        self._logger.debug(
            "idempotency.set",
            extra={"request_id": request_id, "status": assignment.status.value, "ttl": ttl_seconds},
        )


class MetricsIdempotencyStore(IdempotencyStore):
    def __init__(self, inner: IdempotencyStore, recorder: MetricsRecorder):
        self._inner = inner
        self._metrics = recorder

    async def get(self, request_id: str) -> Optional[Assignment]:
        result = await self._inner.get(request_id)
        await self._metrics.increment("idempotency.hit" if result else "idempotency.miss")
        return result

    async def set(self, request_id: str, assignment: Assignment, ttl_seconds: int) -> None:
        await self._metrics.increment("idempotency.set")
        await self._inner.set(request_id, assignment, ttl_seconds)


class LoggingAssignmentStore(AssignmentStore):
    def __init__(self, inner: AssignmentStore, logger_name: str = "assignment"):
        self._inner = inner
        self._logger = logging.getLogger(logger_name)

    async def get(self, request_id: str) -> Optional[Assignment]:
        assignment = await self._inner.get(request_id)
        self._logger.debug(
            "assignment.get",
            extra={"request_id": request_id, "hit": bool(assignment)},
        )
        return assignment

    async def save(self, assignment: Assignment) -> None:
        await self._inner.save(assignment)
        self._logger.debug(
            "assignment.save",
            extra={"request_id": assignment.request_id, "status": assignment.status.value},
        )


class MetricsAssignmentStore(AssignmentStore):
    def __init__(self, inner: AssignmentStore, recorder: MetricsRecorder):
        self._inner = inner
        self._metrics = recorder

    async def get(self, request_id: str) -> Optional[Assignment]:
        assignment = await self._inner.get(request_id)
        await self._metrics.increment("assignment.hit" if assignment else "assignment.miss")
        return assignment

    async def save(self, assignment: Assignment) -> None:
        await self._metrics.increment("assignment.save")
        await self._inner.save(assignment)
