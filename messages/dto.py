from dataclasses import dataclass
from uuid import UUID

from .events import Event
from domain.enums import JobState
from domain.demo_events import DemoEvents


class DTO(Event):
    pass


@dataclass(frozen=True, repr=False)
class JobSelectable(DTO):
    job_id: UUID
    job_inter: bytes
    demo_events: DemoEvents


@dataclass(frozen=True, repr=False)
class JobFailed(DTO):
    job_id: UUID
    job_inter: bytes
    reason: str


@dataclass(frozen=True, repr=False)
class JobDemoProcessing(DTO):
    job_id: UUID
    job_inter: bytes