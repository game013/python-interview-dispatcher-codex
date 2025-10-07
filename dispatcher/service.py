from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from .clients import AvailabilityClient, RulesClient
from .context import RequestContext
from .models import (
    Assignment,
    AssignmentStatus,
    DispatchRequest,
    DeadlineExceededError,
    ResourceExhaustedError,
    ValidationError,
)
from .stores import AssignmentStore, IdempotencyStore, InMemoryReservationStore, ReservationStore

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DispatcherConfig:
    num_shards: int = 4
    queue_capacity: int = 100
    enqueue_timeout: float = 0.1
    request_deadline_seconds: float = 0.8
    idempotency_ttl_seconds: int = 24 * 3600


@dataclass(slots=True)
class _DispatchEnvelope:
    request: DispatchRequest
    context: RequestContext
    future: asyncio.Future


class DispatcherService:
    def __init__(
        self,
        *,
        idempotency_store: IdempotencyStore,
        assignment_store: AssignmentStore,
        availability_client: AvailabilityClient,
        rules_client: RulesClient,
        reservation_store: ReservationStore | None = None,
        config: DispatcherConfig | None = None,
    ) -> None:
        self._config = config or DispatcherConfig()
        self._idempotency_store = idempotency_store
        self._assignment_store = assignment_store
        self._availability_client = availability_client
        self._rules_client = rules_client
        self._reservation_store = reservation_store or InMemoryReservationStore()
        self._queues: list[asyncio.Queue] = [
            asyncio.Queue(maxsize=self._config.queue_capacity) for _ in range(self._config.num_shards)
        ]
        self._workers: list[asyncio.Task] = []
        self._shutdown = asyncio.Event()
        self._started = False
        self._start_lock = asyncio.Lock()

    @property
    def config(self) -> DispatcherConfig:
        return self._config

    async def start(self) -> None:
        if self._started:
            return
        async with self._start_lock:
            if self._started:
                return
            for shard_id in range(self._config.num_shards):
                task = asyncio.create_task(
                    self._run_worker(shard_id), name=f"dispatcher-worker-{shard_id}"
                )
                self._workers.append(task)
            self._started = True

    async def stop(self) -> None:
        self._shutdown.set()
        for queue in self._queues:
            await queue.put(None)  # type: ignore[arg-type]
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        self._started = False
        self._shutdown = asyncio.Event()

    async def submit(self, ctx: RequestContext, request: DispatchRequest) -> Assignment:
        await self.start()
        self._validate(request)
        cached = await self._idempotency_store.get(request.request_id)
        if cached:
            logger.info(
                "dispatcher.idempotent",
                extra={"request_id": request.request_id, "status": cached.status.value, "shard": "n/a"},
            )
            return cached

        shard_id = self._select_shard(request.zipcode)
        queue = self._queues[shard_id]
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        envelope = _DispatchEnvelope(request=request, context=ctx, future=future)
        try:
            timeout = self._compute_enqueue_timeout(ctx)
            await asyncio.wait_for(queue.put(envelope), timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise ResourceExhaustedError("Shard queue is full") from exc

        try:
            remaining = ctx.remaining()
            result = await asyncio.wait_for(future, timeout=remaining)
        except asyncio.TimeoutError as exc:
            ctx.cancel()
            raise DeadlineExceededError("Request timed out waiting for assignment") from exc
        return result

    async def _run_worker(self, shard_id: int) -> None:
        queue = self._queues[shard_id]
        logger.info("worker.start", extra={"shard": shard_id})
        while not self._shutdown.is_set():
            envelope: Optional[_DispatchEnvelope] = await queue.get()
            if envelope is None:
                queue.task_done()
                break
            try:
                assignment = await self._process_request(shard_id, envelope)
                if not envelope.future.done():
                    envelope.future.set_result(assignment)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.exception("worker.failure", extra={"shard": shard_id})
                if not envelope.future.done():
                    assignment = Assignment(
                        request_id=envelope.request.request_id,
                        status=AssignmentStatus.ERROR,
                        reason=str(exc),
                    )
                    envelope.future.set_result(assignment)
            finally:
                queue.task_done()
        logger.info("worker.stop", extra={"shard": shard_id})

    async def _process_request(self, shard_id: int, envelope: _DispatchEnvelope) -> Assignment:
        request = envelope.request
        ctx = envelope.context
        start_ts = time.monotonic()
        logger.debug(
            "worker.process",
            extra={"request_id": request.request_id, "shard": shard_id},
        )
        try:
            assignment = await self._attempt_assignment(shard_id, ctx, request)
        except asyncio.TimeoutError:
            ctx.cancel()
            assignment = Assignment(
                request_id=request.request_id,
                status=AssignmentStatus.TIMEOUT,
                reason="deadline exceeded",
            )
        latency_ms = int((time.monotonic() - start_ts) * 1000)
        assignment.latency_ms = latency_ms
        await self._assignment_store.save(assignment)
        await self._idempotency_store.set(
            request_id=request.request_id,
            assignment=assignment,
            ttl_seconds=self._config.idempotency_ttl_seconds,
        )
        logger.info(
            "worker.result",
            extra={
                "request_id": request.request_id,
                "shard": shard_id,
                "status": assignment.status.value,
                "latency_ms": latency_ms,
            },
        )
        return assignment

    async def _attempt_assignment(
        self, shard_id: int, ctx: RequestContext, request: DispatchRequest
    ) -> Assignment:
        remaining = ctx.remaining()
        if remaining is not None and remaining <= 0:
            raise asyncio.TimeoutError
        async for slot in self._availability_client.stream(ctx, request):
            if ctx.is_cancelled():
                raise asyncio.TimeoutError
            if not (await self._rules_client.is_allowed(ctx, request, slot)):
                continue
            reserved = await self._reservation_store.try_reserve(
                slot.agent_id, request.start_time, request.end_time
            )
            if not reserved:
                continue
            assignment = Assignment(
                request_id=request.request_id,
                status=AssignmentStatus.ASSIGNED,
                agent_id=slot.agent_id,
            )
            return assignment
        if ctx.is_cancelled() or (ctx.remaining() is not None and ctx.remaining() <= 0):
            raise asyncio.TimeoutError
        return Assignment(
            request_id=request.request_id,
            status=AssignmentStatus.NO_AGENT,
            reason="no eligible agents",
        )

    def _select_shard(self, zipcode: str) -> int:
        return hash(zipcode) % self._config.num_shards

    def _compute_enqueue_timeout(self, ctx: RequestContext) -> float:
        remaining = ctx.remaining()
        if remaining is None:
            return self._config.enqueue_timeout
        return min(self._config.enqueue_timeout, max(remaining, 0.01))

    def _validate(self, request: DispatchRequest) -> None:
        if request.start_time >= request.end_time:
            raise ValidationError("start_time must be before end_time")
        if not request.request_id:
            raise ValidationError("request_id required")
        if not request.zipcode:
            raise ValidationError("zipcode required")
