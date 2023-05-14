from dataclasses import dataclass
from uuid import UUID

from . import events
from .deco import consume, publish


class Command:
    pass


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


@dataclass(frozen=True, repr=False)
class AbortJob(Command):
    job_id: UUID


@dataclass(frozen=True)
class RequestMatchInfo(Command):
    sharecode: str


@dataclass(frozen=True)
@publish(ttl=60.0, dead_event=events.DemoParseDL)
@consume(
    error_factory=lambda m, e: events.DemoParseFailure(
        m.origin, m.identifier, e or "Failed processsing demo."
    ),
    requeue=True,
    raise_on_ok=False,
)
class RequestDemoParse(Command):
    origin: str
    identifier: str
    download_url: str


@dataclass(frozen=True)
@publish(ttl=12.0, dead_event=events.DemoParseDL)
@consume(
    error_factory=lambda m, e: events.DemoParseFailure(
        m.origin, m.identifier, e or "Failed processsing demo."
    ),
    requeue=True,
    raise_on_ok=False,
)
class RequestPresignedUrl(Command):
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
@publish(ttl=60.0 * 20, dead_event=events.RecorderDL)
@consume(
    error_factory=lambda m, e: events.RecorderFailure(m.job_id, e or "Gateway timed out."),
    requeue=False,  # False because rabbitmq won't redeliver to same consumer, and we only have one
    raise_on_ok=False,
)
class RequestRecording(Command):
    job_id: str
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


@dataclass(frozen=True)
@publish(ttl=32.0)
@consume()
class RequestTokens(Command):
    pass


@dataclass(frozen=True, repr=False)
@publish(ttl=32.0)
@consume()
class RequestUploadData(Command):
    job_id: str


@dataclass(frozen=True)
class UpdateUser(Command):
    user_id: int
    data: dict
