from random import randint
from unittest.mock import AsyncMock, Mock

import pytest
from domain.domain import Demo, DemoState, Job, JobState
from shared.const import DEMOPARSE_VERSION
from shared.utils import utcnow


def new_job(state):
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


def new_demo(state, queued, sharecode, has_matchinfo, data=None):
    demo = Demo(
        state=state,
        queued=queued,
        sharecode=sharecode,
    )

    if has_matchinfo:
        demo.matchid = random_matchid()
        demo.matchtime = utcnow()
        demo.url = "not a real url"

    if data:
        demo.data = data
        demo.version = DEMOPARSE_VERSION

    return demo


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
    return new_job(JobState.DEMO)


@pytest.fixture
def select_job():
    return new_job(JobState.SELECT)


@pytest.fixture
def record_job():
    return new_job(JobState.RECORD)


@pytest.fixture
def success_job():
    return new_job(JobState.SUCCESS)


@pytest.fixture
def mock_send_raise(mocker):
    return mocker.patch("adapters.broker.send", side_effect=Exception)


@pytest.fixture
def mock_call(mocker):
    return mocker.patch("services.bus.call", side_effect=AsyncMock())


@pytest.fixture
def mock_call_raises(mocker):
    return mocker.patch("services.bus.call", side_effect=AsyncMock(side_effect=Exception))


@pytest.fixture
def mock_dispatch(mocker):
    return mocker.patch("services.bus.dispatch", side_effect=Mock())
