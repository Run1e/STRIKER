from json import loads
from random import randint
import pytest

from domain.domain import Demo, DemoGame, DemoOrigin, DemoState, Job, JobState
from shared.const import DEMOPARSE_VERSION
from shared.utils import utcnow


def create_job(state):
    job = Job(
        state=state,
        guild_id=0,
        channel_id=0,
        user_id=0,
        started_at=utcnow(),
        inter_payload=bytes((0,)),
        completed_at=None,
    )

    return job


def random_matchid():
    return randint(1, 10_000)


def new_demo(
    game=DemoGame.CSGO,
    origin=DemoOrigin.VALVE,
    state=DemoState.PROCESSING,
    add_matchinfo=False,
    add_valve_data=False,
    **kwargs,
):
    if add_matchinfo:
        kwargs["identifier"] = str(random_matchid())
        kwargs["time"] = utcnow()
        kwargs["download_url"] = "not a real url"

    if add_valve_data:
        with open("tests/data/valve.json", "r") as f:
            kwargs["data"] = loads(f.read())
        kwargs["data_version"] = DEMOPARSE_VERSION

    return Demo(game=game, origin=origin, state=state, **kwargs)


@pytest.fixture
def new_job_junk():
    return dict(
        guild_id=0,
        channel_id=0,
        user_id=0,
        inter_payload=bytes((0,)),
    )


@pytest.fixture
def demo_job():
    return create_job(JobState.WAITING)


@pytest.fixture
def select_job():
    return create_job(JobState.SELECTING)


@pytest.fixture
def record_job():
    return create_job(JobState.RECORDING)


@pytest.fixture
def success_job():
    return create_job(JobState.SUCCESS)


@pytest.fixture
def valve():
    with open("tests/data/valve.json", "r") as f:
        return f.read()


@pytest.fixture
def faceit():
    with open("tests/data/faceit.json", "r") as f:
        return f.read()
