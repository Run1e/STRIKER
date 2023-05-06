from dataclasses import dataclass
from uuid import UUID

from . import events
from .deco import publish, consume


class Command:
    pass


@dataclass(frozen=True, repr=False)
class CreateJob(Command):
    guild_id: int
    channel_id: int
    user_id: int
    inter_payload: bytes
    sharecode: str = None
    demo_id: int = None


@dataclass(frozen=True, repr=False)
class AbortJob(Command):
    job_id: UUID


@dataclass(frozen=True)
class RequestMatchInfo(Command):
    sharecode: str


@dataclass(frozen=True)
@publish(ttl=4.0, dead_event=events.DemoParseDL)
@consume(
    error_factory=lambda message, error: events.DemoParseFailure(
        message.origin, message.identifier, error or "Unable to download/parse demo."
    ),
    requeue=True,
    raise_on_ok=False,
)
class RequestDemoParse(Command):
    origin: str
    identifier: str
    download_url: str




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
class RequestRecording(Command):
    job_id: str
    demo: str
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
