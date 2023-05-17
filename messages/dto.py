from dataclasses import dataclass
from uuid import UUID

from domain.match import Match

from .events import Event


class DTO(Event):
    pass


@dataclass(frozen=True, repr=False)
class JobSelectable(DTO):
    job_id: UUID
    job_inter: bytes
    match: Match


@dataclass(frozen=True, repr=False)
class JobSuccess(DTO):
    job_id: UUID
    job_inter: bytes


@dataclass(frozen=True, repr=False)
class JobFailed(DTO):
    job_id: UUID
    job_inter: bytes
    reason: str


@dataclass(frozen=True, repr=False)
class JobWaiting(DTO):
    job_id: UUID
    job_inter: bytes


@dataclass(frozen=True, repr=False)
class JobRecording(DTO):
    job_id: UUID
    job_inter: bytes
    infront: int
