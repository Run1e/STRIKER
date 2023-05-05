from dataclasses import dataclass
from uuid import UUID

from bot import config
from messages.deco import consume, publish


class Event:
    pass


# demo events


@dataclass(frozen=True)
class DemoProcessing(Event):
    demo_id: int


@dataclass(frozen=True)
class DemoReady(Event):
    demo_id: int


@dataclass(frozen=True)
class DemoFailure(Event):
    demo_id: int
    reason: str


# job events


@dataclass(frozen=True)
class JobSelecting(Event):
    job_id: UUID


@dataclass(frozen=True)
class JobWaiting(Event):
    job_id: UUID
    job_inter: bytes


@dataclass(frozen=True)
class JobFailed(Event):
    job_id: UUID
    reason: str


@dataclass(frozen=True)
class JobAborted(Event):
    job_id: UUID


# demoparse


# repr removed because it caused a fuckton of
# console spam
@dataclass(frozen=True, repr=config.DUMP_EVENTS)
@consume()
@publish()
class DemoParseSuccess(Event):
    origin: str
    identifier: str
    data: str
    version: int


@dataclass(frozen=True)
@consume()
@publish()
class DemoParseFailure(Event):
    origin: str
    identifier: str
    reason: str


@dataclass(frozen=True)
class DemoParseDL(Event):
    command: None
    reason: str


# recorder


@dataclass(frozen=True)
class RecorderSuccess(Event):
    id: UUID


@dataclass(frozen=True)
class RecorderFailure(Event):
    id: UUID
    reason: str


# uploader
@dataclass(frozen=True)
class UploaderSuccess(Event):
    id: UUID


@dataclass(frozen=True)
class UploaderFailure(Event):
    id: UUID
    reason: str


# archiver


@dataclass(frozen=True)
class ArchiveSuccess(Event):
    id: UUID
    removed_demos: int
    removed_videos: int
    # current_matchids: list


@dataclass(frozen=True)
class ArchiveFailure(Event):
    id: UUID
    reason: str
