import asyncio
from contextlib import suppress
from datetime import datetime, timedelta, timezone

import pytest

from dispatcher.clients import MockAvailabilityClient, MockRulesClient
from dispatcher.context import with_timeout
from dispatcher.models import AgentSlot, AssignmentStatus, DispatchRequest, ResourceExhaustedError
from dispatcher.service import DispatcherConfig, DispatcherService
from dispatcher.stores import (
    InMemoryAssignmentStore,
    InMemoryIdempotencyStore,
    InMemoryReservationStore,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def make_service():
    created = []

    async def _make(
        slots_by_zip: dict[str, list[AgentSlot]],
        *,
        rules: MockRulesClient | None = None,
        config: DispatcherConfig | None = None,
    ) -> DispatcherService:
        service = DispatcherService(
            idempotency_store=InMemoryIdempotencyStore(),
            assignment_store=InMemoryAssignmentStore(),
            availability_client=MockAvailabilityClient(slots_by_zip),
            rules_client=rules or MockRulesClient(),
            reservation_store=InMemoryReservationStore(),
            config=config or DispatcherConfig(num_shards=2, queue_capacity=8),
        )
        await service.start()
        created.append(service)
        return service

    yield _make

    for service in created:
        await service.stop()


def _slot(agent_id: str, zipcode: str, start: datetime, duration_minutes: int = 60) -> AgentSlot:
    return AgentSlot(
        agent_id=agent_id,
        zipcode=zipcode,
        start_time=start,
        end_time=start + timedelta(minutes=duration_minutes),
    )


async def test_dispatcher_assigns_agent(make_service):
    now = datetime.now(timezone.utc)
    service = await make_service({"90001": [_slot("agent-1", "90001", now)]})
    request = DispatchRequest(
        request_id="req-1",
        client_id="client",
        zipcode="90001",
        start_time=now,
        end_time=now + timedelta(minutes=30),
    )
    async with with_timeout(0.5) as ctx:
        assignment = await service.submit(ctx, request)
    assert assignment.status is AssignmentStatus.ASSIGNED
    assert assignment.agent_id == "agent-1"


async def test_rules_reject_all_agents(make_service):
    now = datetime.now(timezone.utc)
    slots = {"90001": [_slot("agent-1", "90001", now)]}
    service = await make_service(slots, rules=MockRulesClient(blacklist={"agent-1"}))
    request = DispatchRequest(
        request_id="req-2",
        client_id="client",
        zipcode="90001",
        start_time=now,
        end_time=now + timedelta(minutes=30),
    )
    async with with_timeout(0.5) as ctx:
        assignment = await service.submit(ctx, request)
    assert assignment.status is AssignmentStatus.NO_AGENT


async def test_idempotent_returns_cached_result(make_service):
    now = datetime.now(timezone.utc)
    slots = {"90001": [_slot("agent-1", "90001", now)]}
    service = await make_service(slots)
    request = DispatchRequest(
        request_id="req-3",
        client_id="client",
        zipcode="90001",
        start_time=now,
        end_time=now + timedelta(minutes=30),
    )
    async with with_timeout(0.5) as ctx:
        first = await service.submit(ctx, request)
    # Wipe availability to force reliance on cache.
    slots["90001"] = []
    async with with_timeout(0.5) as ctx:
        second = await service.submit(ctx, request)
    assert first.status is AssignmentStatus.ASSIGNED
    assert second.status is AssignmentStatus.ASSIGNED
    assert second.agent_id == first.agent_id


async def test_concurrent_requests_multi_shard(make_service):
    now = datetime.now(timezone.utc)
    service = await make_service(
        {
            "90001": [_slot("agent-1", "90001", now)],
            "10001": [_slot("agent-2", "10001", now)],
        },
        config=DispatcherConfig(num_shards=4, queue_capacity=4),
    )
    requests = [
        DispatchRequest(
            request_id=f"req-{i}",
            client_id="client",
            zipcode=zipc,
            start_time=now,
            end_time=now + timedelta(minutes=30),
        )
        for i, zipc in enumerate(["90001", "10001"], start=10)
    ]

    async def _submit(req: DispatchRequest):
        async with with_timeout(0.5) as ctx:
            assignment = await service.submit(ctx, req)
        return assignment

    assignments = await asyncio.gather(*(_submit(r) for r in requests))
    assert {a.agent_id for a in assignments} == {"agent-1", "agent-2"}


async def test_backpressure_resource_exhausted():
    now = datetime.now(timezone.utc)

    class ManualDispatcher(DispatcherService):
        async def start(self) -> None:  # override to avoid workers consuming queue
            self._started = True

    slots = {"90001": [_slot("agent-1", "90001", now)]}
    dispatcher = ManualDispatcher(
        idempotency_store=InMemoryIdempotencyStore(),
        assignment_store=InMemoryAssignmentStore(),
        availability_client=MockAvailabilityClient(slots, per_item_delay=0.5),
        rules_client=MockRulesClient(),
        reservation_store=InMemoryReservationStore(),
        config=DispatcherConfig(num_shards=1, queue_capacity=1, enqueue_timeout=0.05),
    )

    async def _blocked_submit():
        async with with_timeout(1.0) as ctx:
            await dispatcher.submit(ctx, DispatchRequest(
                request_id="req-busy",
                client_id="client",
                zipcode="90001",
                start_time=now,
                end_time=now + timedelta(minutes=30),
            ))

    task = asyncio.create_task(_blocked_submit())
    await asyncio.sleep(0.05)

    with pytest.raises(ResourceExhaustedError):
        async with with_timeout(0.2) as ctx:
            await dispatcher.submit(
                ctx,
                DispatchRequest(
                    request_id="req-over",
                    client_id="client",
                    zipcode="90001",
                    start_time=now,
                    end_time=now + timedelta(minutes=30),
                ),
            )

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    queued = await dispatcher._queues[0].get()
    future = getattr(queued, "future", None)
    if future:
        future.cancel()
    dispatcher._queues[0].task_done()
    await dispatcher.stop()


async def test_timeout_propagates_to_assignment(make_service):
    now = datetime.now(timezone.utc)
    slow_client = MockAvailabilityClient(
        {"90001": [_slot("agent-1", "90001", now)]},
        per_item_delay=0.5,
    )
    service = DispatcherService(
        idempotency_store=InMemoryIdempotencyStore(),
        assignment_store=InMemoryAssignmentStore(),
        availability_client=slow_client,
        rules_client=MockRulesClient(),
        reservation_store=InMemoryReservationStore(),
        config=DispatcherConfig(num_shards=1, queue_capacity=2),
    )
    await service.start()
    request = DispatchRequest(
        request_id="req-timeout",
        client_id="client",
        zipcode="90001",
        start_time=now,
        end_time=now + timedelta(minutes=30),
    )
    async with with_timeout(0.1) as ctx:
        assignment = await service.submit(ctx, request)
    assert assignment.status is AssignmentStatus.TIMEOUT
    await service.stop()
