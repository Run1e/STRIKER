from dataclasses import dataclass
from uuid import UUID

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
@dataclass(frozen=True, repr=False)
@publish()
@consume(
    dispatch_err=lambda e, r: DemoParseFailure(e.origin, e.identifier, "Failed handling response.")
)
class DemoParseSuccess(Event):
    origin: str
    identifier: str
    data: str
    version: int


@dataclass(frozen=True)
@publish(ttl=6.0)
@consume()
class PresignedUrlGenerated(Event):
    origin: str
    identifier: str
    presigned_url: str


@dataclass(frozen=True)
@publish()
@consume()
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
@publish(ttl=60.0)  # not stritcly a good ttl but I don't want these events to heap up I guess?
@consume()
class RecordingProgression(Event):
    job_id: str
    infront: int | None  # > 0: queued, == 0: recording, is None: send from commands.Record handler


@dataclass(frozen=True)
@publish()
@consume()
class RecorderSuccess(Event):
    job_id: str


@dataclass(frozen=True)
@publish()
@consume()
class RecorderFailure(Event):
    job_id: str
    reason: str


@dataclass(frozen=True)
class RecorderDL(Event):
    command: None
    reason: str


# uploader


@dataclass(frozen=True)
@publish()
@consume()
class UploaderSuccess(Event):
    job_id: str


@dataclass(frozen=True)
@publish()
@consume()
class UploaderFailure(Event):
    job_id: str
    reason: str


@dataclass(frozen=True, repr=False)
@publish(ttl=12.0)
@consume()
class Tokens(Event):
    tokens: list


@dataclass(frozen=True, repr=False)
@publish(ttl=32.0)
@consume()
class UploadData(Event):
    job_id: str = None
    video_title: str = None
    user_id: int = None
    channel_id: int = None
