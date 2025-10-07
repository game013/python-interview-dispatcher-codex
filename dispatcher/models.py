from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class AssignmentStatus(str, Enum):
    ASSIGNED = "ASSIGNED"
    NO_AGENT = "NO_AGENT"
    TIMEOUT = "TIMEOUT"
    REJECTED = "REJECTED"
    ERROR = "ERROR"


@dataclass(slots=True)
class DispatchRequest:
    request_id: str
    client_id: str
    zipcode: str
    start_time: datetime
    end_time: datetime
    party_size: int = 1
    notes: Optional[str] = None


@dataclass(slots=True)
class AgentSlot:
    agent_id: str
    zipcode: str
    start_time: datetime
    end_time: datetime


@dataclass(slots=True)
class Assignment:
    request_id: str
    status: AssignmentStatus
    agent_id: Optional[str] = None
    reason: Optional[str] = None
    latency_ms: Optional[int] = None


class DispatcherError(Exception):
    """Base class for dispatcher related errors."""


class ResourceExhaustedError(DispatcherError):
    """Raised when a shard queue is full under backpressure"""


class DeadlineExceededError(DispatcherError):
    """Raised when the deadline is exceeded before enqueueing"""


class ValidationError(DispatcherError):
    """Raised when the incoming request is invalid."""
