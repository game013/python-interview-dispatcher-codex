from datetime import datetime, timedelta, timezone

import httpx
import pytest

from dispatcher.api import create_app
from dispatcher.clients import MockAvailabilityClient, MockRulesClient
from dispatcher.models import AgentSlot
from dispatcher.service import DispatcherConfig, DispatcherService
from dispatcher.stores import (
    InMemoryAssignmentStore,
    InMemoryIdempotencyStore,
    InMemoryReservationStore,
)

pytestmark = pytest.mark.asyncio


def _slot(agent_id: str, zipcode: str, start: datetime, duration_minutes: int = 60) -> AgentSlot:
    return AgentSlot(
        agent_id=agent_id,
        zipcode=zipcode,
        start_time=start,
        end_time=start + timedelta(minutes=duration_minutes),
    )


async def test_api_dispatch_returns_assignment():
    now = datetime.now(timezone.utc)
    dispatcher = DispatcherService(
        idempotency_store=InMemoryIdempotencyStore(),
        assignment_store=InMemoryAssignmentStore(),
        availability_client=MockAvailabilityClient({"90001": [_slot("agent-42", "90001", now)]}),
        rules_client=MockRulesClient(),
        reservation_store=InMemoryReservationStore(),
        config=DispatcherConfig(num_shards=2, queue_capacity=4),
    )
    app = create_app(dispatcher)

    transport = httpx.ASGITransport(app=app, lifespan="on")
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {
            "request_id": "api-req",
            "client_id": "client",
            "zipcode": "90001",
            "start_time": now.isoformat(),
            "end_time": (now + timedelta(minutes=30)).isoformat(),
            "party_size": 2,
        }
        response = await client.post("/dispatch", json=payload)
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "ASSIGNED"
        assert body["agent_id"] == "agent-42"

        # Second call should hit idempotency cache via API.
        response2 = await client.post("/dispatch", json=payload)
        assert response2.status_code == 200
        assert response2.json()["agent_id"] == "agent-42"
