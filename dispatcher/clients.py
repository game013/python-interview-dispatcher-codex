from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import AsyncIterator, Dict, Iterable, Protocol

from .context import RequestContext, now_utc
from .models import AgentSlot, DispatchRequest

logger = logging.getLogger(__name__)


class AvailabilityClient(Protocol):
    async def stream(self, ctx: RequestContext, request: DispatchRequest) -> AsyncIterator[AgentSlot]:
        ...


class RulesClient(Protocol):
    async def is_allowed(self, ctx: RequestContext, request: DispatchRequest, slot: AgentSlot) -> bool:
        ...


class MockAvailabilityClient(AvailabilityClient):
    """Simulates server streaming availability results with small delays."""

    def __init__(self, slots_by_zipcode: Dict[str, Iterable[AgentSlot]], per_item_delay: float = 0.02):
        self._slots_by_zipcode = slots_by_zipcode
        self._delay = per_item_delay

    async def stream(self, ctx: RequestContext, request: DispatchRequest) -> AsyncIterator[AgentSlot]:
        slots = list(self._slots_by_zipcode.get(request.zipcode, []))
        for slot in slots:
            remaining = ctx.remaining()
            if remaining is not None and remaining <= 0:
                logger.debug("availability.deadline", extra={"request_id": request.request_id})
                break
            await asyncio.sleep(min(self._delay, remaining) if remaining and remaining > 0 else self._delay)
            if ctx.is_cancelled():
                break
            yield slot


class MockRulesClient(RulesClient):
    """Simple rules engine that does deterministic filtering based on request metadata."""

    def __init__(self, blacklist: set[str] | None = None):
        self._blacklist = blacklist or set()

    async def is_allowed(self, ctx: RequestContext, request: DispatchRequest, slot: AgentSlot) -> bool:
        # Simulate a tiny network hop respecting deadline
        remaining = ctx.remaining()
        sleep_for = 0.005
        if remaining is not None:
            if remaining <= 0:
                return False
            sleep_for = min(sleep_for, remaining)
        await asyncio.sleep(sleep_for)
        if ctx.is_cancelled():
            return False
        if slot.agent_id in self._blacklist:
            return False
        # Example business rule: ensure slot fully covers requested window
        covers_window = slot.start_time <= request.start_time and slot.end_time >= request.end_time
        return covers_window


def default_mock_data() -> Dict[str, list[AgentSlot]]:
    now = now_utc()
    one_hour = timedelta(hours=1)
    slots: Dict[str, list[AgentSlot]] = {}
    for zipcode, agent_ids in {"90001": ["agent-1", "agent-2"], "10001": ["agent-3"]}.items():
        slots[zipcode] = [
            AgentSlot(
                agent_id=agent_id,
                zipcode=zipcode,
                start_time=now,
                end_time=now + one_hour,
            )
            for agent_id in agent_ids
        ]
    return slots
