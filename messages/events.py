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


@dataclass(frozen=True, repr=False)
class JobWaiting(Event):
    job_id: UUID


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
class DemoUploaded(Event):
    origin: str
    identifier: str


@dataclass(frozen=True)
@consume()
@publish()
class PresignedUrlGenerated(Event):
    origin: str
    identifier: str
    presigned_url: str


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
@consume()
@publish()
class RecordingProgression(Event):
    job_id: str
    infront: int | None  # > 0: queued, == 0: recording, is None: send from commands.Record handler


@dataclass(frozen=True)
@consume()
@publish()
class RecorderSuccess(Event):
    job_id: str


@dataclass(frozen=True)
@consume()
@publish()
class RecorderFailure(Event):
    job_id: str
    reason: str


@dataclass(frozen=True)
class RecorderDL(Event):
    command: None
    reason: str


# # uploader
# @dataclass(frozen=True)
# class UploaderSuccess(Event):
#     id: UUID


# @dataclass(frozen=True)
# class UploaderFailure(Event):
#     id: UUID
#     reason: str
