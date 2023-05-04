from random import randint
from unittest.mock import AsyncMock, Mock

import pytest

from domain.domain import Demo, DemoGame, DemoOrigin, DemoState, Job, JobState
from shared.const import DEMOPARSE_VERSION
from shared.utils import utcnow

from .data import demo_data


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
    add_data=False,
    **kwargs,
):
    if add_matchinfo:
        kwargs["identifier"] = str(random_matchid())
        kwargs["time"] = utcnow()
        kwargs["download_url"] = "not a real url"

    if add_data:
        kwargs["data"] = demo_data[0]
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
