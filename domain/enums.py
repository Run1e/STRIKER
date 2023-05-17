from enum import Enum, auto


class JobState(Enum):
    WAITING = auto()  # associated demo is currently processing
    SELECTING = auto()  # job is or can have user selecting
    RECORDING = auto()  # job is on record queue
    ABORTED = auto()  # job was aborted by the user, view timeout, or job limit
    FAILED = auto()  # job failed for some reason
    SUCCESS = auto()  # job completed successfully


class DemoGame(Enum):
    CSGO = auto()
    CS2 = auto()


class DemoOrigin(Enum):
    VALVE = auto()
    FACEIT = auto()
    UPLOAD = auto()


class DemoState(Enum):
    PROCESSING = auto()
    FAILED = auto()  # demo failed, not recordable
    READY = auto()  # demo successful, can be used by jobs
    DELETED = auto()  # demo unavailable, archived/delete probably


class RecordingType(Enum):
    PLAYER_ROUND = auto()
