from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, validator

from .clients import MockAvailabilityClient, MockRulesClient, default_mock_data
from .context import with_timeout
from .models import Assignment, AssignmentStatus, DispatchRequest, ResourceExhaustedError, ValidationError
from .service import DispatcherConfig, DispatcherService
from .stores import (
    InMemoryAssignmentStore,
    InMemoryIdempotencyStore,
    LoggingAssignmentStore,
    LoggingIdempotencyStore,
    MetricsAssignmentStore,
    MetricsIdempotencyStore,
    MetricsRecorder,
)

logger = logging.getLogger(__name__)


class DispatchPayload(BaseModel):
    request_id: str
    client_id: str
    zipcode: str
    start_time: datetime
    end_time: datetime
    party_size: int = 1
    notes: Optional[str] = None

    @validator("end_time")
    def validate_times(cls, end_time: datetime, values):
        start_time = values.get("start_time")
        if start_time and end_time <= start_time:
            raise ValueError("end_time must be after start_time")
        return end_time


class DispatchResponse(BaseModel):
    request_id: str
    status: AssignmentStatus
    agent_id: Optional[str] = None
    reason: Optional[str] = None
    latency_ms: Optional[int] = None


def create_dispatcher_service(config: DispatcherConfig | None = None) -> DispatcherService:
    config = config or DispatcherConfig()
    metrics = MetricsRecorder()
    id_store = MetricsIdempotencyStore(
        LoggingIdempotencyStore(InMemoryIdempotencyStore()), recorder=metrics
    )
    assignment_store = MetricsAssignmentStore(
        LoggingAssignmentStore(InMemoryAssignmentStore()), recorder=metrics
    )
    availability = MockAvailabilityClient(default_mock_data())
    rules = MockRulesClient()
    return DispatcherService(
        idempotency_store=id_store,
        assignment_store=assignment_store,
        availability_client=availability,
        rules_client=rules,
        config=config,
    )


def create_app(dispatcher: DispatcherService | None = None) -> FastAPI:
    dispatcher = dispatcher or create_dispatcher_service()
    app = FastAPI(title="Tour Dispatcher")

    @app.on_event("startup")
    async def _startup() -> None:
        await dispatcher.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await dispatcher.stop()

    @app.post("/dispatch", response_model=DispatchResponse)
    async def submit(payload: DispatchPayload) -> DispatchResponse:
        async with with_timeout(dispatcher.config.request_deadline_seconds) as ctx:
            request = DispatchRequest(
                request_id=payload.request_id,
                client_id=payload.client_id,
                zipcode=payload.zipcode,
                start_time=payload.start_time,
                end_time=payload.end_time,
                party_size=payload.party_size,
                notes=payload.notes,
            )
            try:
                assignment = await dispatcher.submit(ctx, request)
            except ValidationError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except ResourceExhaustedError as exc:
                raise HTTPException(status_code=429, detail=str(exc)) from exc
            response = _assignment_to_response(assignment)
            return response

    return app


def _assignment_to_response(assignment: Assignment) -> DispatchResponse:
    return DispatchResponse(
        request_id=assignment.request_id,
        status=assignment.status,
        agent_id=assignment.agent_id,
        reason=assignment.reason,
        latency_ms=assignment.latency_ms,
    )
