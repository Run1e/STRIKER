from dataclasses import dataclass


class Command:
    pass


@dataclass
class CreateJob(Command):
    guild_id: int
    channel_id: int
    user_id: int
    inter_payload: bytes
    sharecode: str = None
    demo_id: int = None


@dataclass
class Restore(Command):
    pass