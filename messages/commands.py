from dataclasses import dataclass
from uuid import UUID

from . import events
from .deco import consume, publish


class Command:
    def __repr__(self):
        return f"{self.__class__.__name__}(...)"


@dataclass(frozen=True, repr=False)
class CreateJob(Command):
    guild_id: int
    channel_id: int
    user_id: int
    inter_payload: bytes
    origin: str = None
    identifier: str = None
    sharecode: str = None
    demo_id: int = None
    demo_url: str = None


@dataclass(frozen=True)
class AbortJob(Command):
    job_id: UUID


@dataclass(frozen=True)
@publish(ttl=60.0, dead_event=events.DemoParseDL)
@consume(
    publish_err=lambda m, e: events.DemoParseFailure(
        m.origin, m.identifier, e or "The demo parser encountered an error."
    ),
    requeue=True,
)
class RequestDemoParse(Command):
    origin: str
    identifier: str
    download_url: str
    data_version: int


@dataclass(frozen=True)
@publish(ttl=6.0)
@consume()  # uses wait_for and raises ServiceError itself
class RequestPresignedUrl(Command):
    origin: str
    identifier: str
    expires_in: int


@dataclass(frozen=True)
class GetPresignedUrlDTO(Command):
    origin: str
    identifier: str


@dataclass(frozen=True)
class Restore(Command):
    pass


@dataclass(frozen=True)
class Record(Command):
    job_id: UUID
    player_xuid: int
    round_id: int
    tier: int


@dataclass(frozen=True)
@publish(ttl=120.0, dead_event=events.RecorderDL)
@consume(
    publish_err=lambda m, e: events.RecorderFailure(
        m.job_id, e or "Gateway failed processing request."
    ),
    requeue=False,  # False because rabbitmq won't redeliver to same consumer, and we only have one
)
class RequestRecording(Command):
    job_id: str
    game: str
    demo_origin: str
    demo_identifier: str
    demo_url: str
    upload_url: str
    player_xuid: int
    tickrate: int
    start_tick: int
    end_tick: int
    skips: list
    fps: int
    video_bitrate: int
    audio_bitrate: int
    fragmovie: bool
    color_filter: bool
    righthand: bool
    crosshair_code: str
    use_demo_crosshair: bool
    hq: bool


@dataclass(frozen=True)
@publish(ttl=12.0)
@consume()
class RequestTokens(Command):
    pass


@dataclass(frozen=True)
@publish(ttl=32.0)
@consume()
class RequestUploadData(Command):
    job_id: str


@dataclass(frozen=True)
class UpdateUserSettings(Command):
    user_id: int
    data: dict
