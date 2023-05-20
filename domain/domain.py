from datetime import datetime, timezone
from typing import List

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

    def is_selectable(self):
        return self.has_data() and self.is_up_to_date()

    def is_ready(self):
        return self.is_selectable() and self.state is DemoState.READY

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


class Job(Entity):
    demo: Demo

    def __init__(
        self,
        state: JobState,
        guild_id: int,
        channel_id: int,
        user_id: int,
        started_at: datetime,
        inter_payload: bytes,
        completed_at: datetime = None,
        video_title: str = None,
        recording_type: RecordingType = None,
        recording_data: dict = None,
    ):
        self.state = state
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.user_id = user_id
        self.started_at = started_at
        self.inter_payload = inter_payload
        self.completed_at = completed_at
        self.video_title = video_title
        self.recording_type = recording_type
        self.recording_data = recording_data

    def set_demo(self, demo: Demo):
        self.demo = demo

        if demo.is_selectable():
            self.selecting()
        else:
            self.add_event(events.JobWaiting(self.id))

    def selecting(self):
        self.state = JobState.SELECTING
        self.add_event(events.JobSelecting(self.id))

    def aborted(self):
        self.state = JobState.ABORTED
        # self.add_event(events.JobAborted(self.id))

    def failed(self, reason: str):
        self.state = JobState.FAILED
        self.add_event(events.JobFailed(self.id, reason))

    def recording(self):
        self.state = JobState.RECORDING

    def success(self):
        self.state = JobState.SUCCESS


class UserSettings(Entity):
    toggleable_values = {
        "righthand": True,
        "color_filter": True,
        "hq": False,
        "fragmovie": False,
        "use_demo_crosshair": False,
    }

    text_values = {
        "crosshair_code": None,
    }

    value_tiers = {
        "righthand": 0,
        "color_filter": 0,
        "hq": 2,
        "fragmovie": 1,
        "use_demo_crosshair": 1,
        "crosshair_code": 1,
    }

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

    @classmethod
    def defaults(cls):
        return {**cls.toggleable_values, **cls.text_values}

    def update(self, **d):
        for k, v in d.items():
            if k in self.toggleable_values or k in self.text_values:
                setattr(self, k, v)

    def filled(self, tier: int):
        # returns default settings, but updated with .unfilled()
        settings = self.defaults()
        settings.update(self.unfilled(tier))
        return settings

    def unfilled(self, tier: int):
        # only returns user-specified settings that match tier level or above

        settings = dict()
        for key in self.defaults().keys():
            if tier < self.value_tiers[key]:
                continue

            user_value = getattr(self, key)
            if user_value is not None:
                settings[key] = user_value

        return settings


def calculate_bitrate(
    duration: float,
    bitrate_scalar=0.7,
    max_bitrate_mbit=10,
    max_file_size_mb=25,
):
    max_bitrate = max_bitrate_mbit * 1024 * 1024
    max_file_size = max_file_size_mb * 8 * 1024 * 1024
    return min(max_bitrate, int((max_file_size / duration) * bitrate_scalar))
