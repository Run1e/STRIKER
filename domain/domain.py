import json
from datetime import datetime, timezone
from typing import List
from uuid import uuid4
from bot.config import DEMO_URL_FORMAT

from messages import events
from shared.const import DEMOPARSE_VERSION

from .enums import DemoGame, DemoOrigin, DemoState, JobState, RecordingType

demoevents_cache = dict()


class Entity:
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={self.id}>"

    def add_event(self, event: events.Event):
        self.events.append(event)

    @property
    def events(self):
        if not hasattr(self, "_events"):
            self._events = []
        return self._events


class Demo(Entity):
    def __init__(
        self,
        game: DemoGame,
        origin: DemoOrigin,
        state: DemoState,
        identifier: str = None,
        sharecode: str = None,
        time: datetime = None,
        download_url: str = None,
        map: str = None,
        score: List[int] = None,
        downloaded_at: datetime = None,
        data_version: int = None,
        data: dict = None,
    ):
        self.game = game
        self.origin = origin
        self.state = state
        self.identifier = identifier
        self.sharecode = sharecode
        self.time = time
        self.download_url = download_url
        self.map = map
        self.score = score
        self.downloaded_at = downloaded_at
        self.data_version = data_version
        self.data = data

    def has_download_url(self):
        return self.download_url is not None

    def has_data(self):
        return self.data is not None

    def is_up_to_date(self):
        # I don't like how this uses an external constant at all
        return self.data_version == DEMOPARSE_VERSION

    def is_ready(self):
        return self.has_data() and self.is_up_to_date() and self.state is DemoState.READY

    def failed(self, reason):
        self.state = DemoState.FAILED
        self.add_event(events.DemoFailure(self.id, reason))

    def processing(self):
        self.state = DemoState.PROCESSING

    def ready(self):
        self.state = DemoState.READY
        self.add_event(events.DemoReady(self.id))

    def set_demo_data(self, data, version):
        self.data = data
        self.data_version = version
        self.downloaded_at = datetime.now(timezone.utc)

        demoheader = data["demoheader"]
        self.map = demoheader["mapname"]

        self.score = data["score"]

        self.ready()


class Recording(Entity):
    def __init__(self, recording_type: RecordingType, player_xuid: int, round_id: int = None):
        self.recording_type = recording_type
        self.player_xuid = player_xuid
        self.round_id = round_id


class Job(Entity):
    demo: Demo
    recording: Recording

    def __init__(
        self,
        state: JobState,
        guild_id: int,
        channel_id: int,
        user_id: int,
        started_at: datetime,
        inter_payload: bytes,
        completed_at: datetime = None,
        upload_token: str = None,
    ):
        self.state = state
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.user_id = user_id
        self.started_at = started_at
        self.inter_payload = inter_payload
        self.completed_at = completed_at
        self.upload_token = upload_token

    def generate_upload_token(self):
        token = "".join(str(uuid4()).split("-"))
        self.upload_token = token
        return token

    def set_completed(self):
        self.state = JobState.SUCCESS

    def set_demo(self, demo: Demo):
        self.demo = demo

        if demo.is_ready() and self.state is JobState.WAITING:
            self.demo_ready()
        elif demo.state is DemoState.PROCESSING:
            self.demo_processing()

    def demo_ready(self):
        self.state = JobState.SELECTING
        self.add_event(events.JobSelecting(self.id))

    def demo_processing(self):
        self.state = JobState.WAITING
        self.add_event(events.JobWaiting(self.id, self.inter_payload))

    def aborted(self):
        self.state = JobState.ABORTED
        self.add_event(events.JobAborted(self.id))

    def failed(self, reason: str):
        self.state = JobState.FAILED
        self.add_event(events.JobFailed(self.id, reason))

    def set_recording(self):
        self.state = JobState.RECORDING

    def uploading(self):
        self.state = JobState.UPLOADING


class User(Entity):
    modifiable_fields = (
        "crosshair_code",
        "fragmovie",
        "color_filter",
        "righthand",
        "use_demo_crosshair",
    )

    def __init__(
        self,
        user_id: int,
        crosshair_code: str = None,
        fragmovie: bool = None,
        color_filter: bool = None,
        righthand: bool = None,
        use_demo_crosshair: bool = None,
    ) -> None:
        self.user_id = user_id
        self.crosshair_code = crosshair_code
        self.fragmovie = fragmovie
        self.color_filter = color_filter
        self.righthand = righthand
        self.use_demo_crosshair = use_demo_crosshair

    def get(self, key):
        return self.all_recording_settings(False).get(key)

    def set(self, key, value):
        if key in self.all_recording_settings(False):
            setattr(self, key, value)
        else:
            raise ValueError

    def all_recording_settings(self, only_toggleable=True):
        default = lambda v, d: v if v is not None else d

        d = dict(
            fragmovie=default(self.fragmovie, False),
            color_filter=default(self.color_filter, True),
            righthand=default(self.righthand, True),
            use_demo_crosshair=default(self.use_demo_crosshair, False),
        )

        if not only_toggleable:
            d["crosshair_code"] = default(self.crosshair_code, "CSGO-SG5dx-aAeRk-dnoAc-TwqMh-yTSFE")

        return d

    def update_recorder_settings(self):
        d = dict()

        for attr in self.modifiable_fields:
            val = getattr(self, attr)
            if val is not None:
                d[attr] = val

        return d


def calculate_bitrate(
    duration: float, bitrate_scalar=0.7, max_bitrate_mbit=10, max_file_size_mb=25
):
    max_bitrate = max_bitrate_mbit * 1024 * 1024
    max_file_size = max_file_size_mb * 8 * 1024 * 1024
    return min(max_bitrate, int((max_file_size / duration) * bitrate_scalar))


def build_demo_url(origin, identifier):
    return DEMO_URL_FORMAT.format(
        origin=origin.lower(),
        identifier=identifier,
    )
