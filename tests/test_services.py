from asyncio import events
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from json import loads
from unittest.mock import ANY, AsyncMock
from uuid import uuid4

import pytest

from adapters import broker
from domain import events
from domain.domain import DemoState, JobState, Recording, RecordingType
from services import services
from shared.const import DEMOPARSE_VERSION
from tests.data import demo_data
from tests.testutils import *


class FakeRepository:
    def __init__(self, instances=None):
        self.store = {}
        self.rollback_store = None
        for instance in instances:
            self.add(instance)

    def commit(self):
        self.rollback_store = None

    def rollback(self):
        remove_ids = []

        for current_id in self.store.keys():
            if current_id not in self.rollback_store:
                remove_ids.append(current_id)

        for id in remove_ids:
            self.store.pop(id)

        for current_id, current_ins in self.store.items():
            for rollback_id, rollback_dict in self.rollback_store.items():
                if current_id == rollback_id:
                    for k, v in rollback_dict.items():
                        setattr(current_ins, k, v)
                    break

    def make_rollback_point(self):
        self.rollback_store = {}
        for id, ins in self.store.items():
            self.rollback_store[id] = deepcopy(ins.__dict__)

    def add(self, instance):
        self.store[instance.id] = instance
        if self.rollback_store is not None:
            self.rollback_store[instance.id] = deepcopy(instance.__dict__)

    async def get(self, _id):
        return self.store.get(_id, None)

    async def where(self, **kwargs):
        instances = []
        for instance in self.store.values():
            if all(getattr(instance, k) == v for k, v in kwargs.items()):
                instances.append(instance)
        return instances


class FakeJobRepository(FakeRepository):
    def add(self, instance):
        if not hasattr(instance, "id"):
            instance.id = uuid4()
        if not hasattr(instance, "demo_id"):
            instance.demo_id = None

        return super().add(instance)

    async def get_recording(self):
        jobs = []
        for job in self.store.values():
            if job.state is JobState.RECORD:
                jobs.append(job)

        return jobs

    async def get_restart(self):
        jobs = []
        for job in self.store.values():
            if job.state is JobState.SELECT:
                jobs.append(job)

        return jobs

    async def waiting_for_demo(self, demo_id):
        jobs = []
        for job in self.store.values():
            if (
                job.state is JobState.DEMO
                and job.started_at > datetime.now(timezone.utc) - timedelta(minutes=14)
                and job.demo_id == demo_id
            ):
                jobs.append(job)

        return jobs


class FakeDemoRepository(FakeRepository):
    def __init__(self, instances=None):
        self._demo_counter = 1
        super().__init__(instances)

    def add(self, instance):
        if not hasattr(instance, "id"):
            instance.id = self._demo_counter
            self._demo_counter += 1
        return super().add(instance)

    async def from_sharecode(self, sharecode):
        for instance in self.store:
            if instance.sharecode == sharecode:
                return instance
        return None

    async def queued(self):
        demos = []
        for demo in self.store.values():
            if demo.queued and demo.state in (DemoState.MATCH, DemoState.PARSE):
                demos.append(demo)

        return demos

    async def unqueued(self):
        demos = []
        for demo in self.store.values():
            if not demo.queued and demo.state in (DemoState.MATCH, DemoState.PARSE):
                demos.append(demo)

        return demos

    async def set_failed(self, _id):
        demo: Demo = await self.get(_id)
        demo.state = DemoState.FAILED
        demo.queued = False


class FakeUnitOfWork:
    def __init__(self, jobs=None, demos=None) -> None:
        self.jobs = FakeJobRepository(jobs or [])
        self.demos = FakeDemoRepository(demos or [])
        self.commit_count = 0
        self.rollback_count = 0

    async def __aenter__(self):
        self.jobs.make_rollback_point()
        self.demos.make_rollback_point()
        self.events = list()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback_count += 1
            self.jobs.rollback()
            self.demos.rollback()

    def add_event(self, event):
        self.events.append(event)

    async def flush(self):
        pass

    async def commit(self):
        self.jobs.commit()
        self.demos.commit()
        self.commit_count += 1


def get_first(repo):
    return next((ins for ins in repo.store.values()))


@pytest.fixture
def matchinfo_send(mocker):
    return mocker.patch("adapters.broker.matchinfo.send", side_effect=AsyncMock())


@pytest.fixture
def matchinfo_send_raises(mocker):
    return mocker.patch(
        "adapters.broker.matchinfo.send", side_effect=AsyncMock(side_effect=Exception)
    )


@pytest.fixture
def demoparse_send(mocker):
    return mocker.patch("adapters.broker.demoparse.send", side_effect=AsyncMock())


@pytest.fixture
def demoparse_send_raises(mocker):
    return mocker.patch(
        "adapters.broker.demoparse.send", side_effect=AsyncMock(side_effect=Exception)
    )


@pytest.fixture
def recorder_send(mocker):
    return mocker.patch("adapters.broker.recorder.send", side_effect=AsyncMock())


@pytest.fixture
def recorder_send_raises(mocker):
    return mocker.patch(
        "adapters.broker.recorder.send", side_effect=AsyncMock(side_effect=Exception)
    )


@pytest.fixture
def uploader_send(mocker):
    return mocker.patch("adapters.broker.uploader.send", side_effect=AsyncMock())


@pytest.mark.asyncio
async def test_new_job_sharecode(matchinfo_send, new_job_junk):
    uow = FakeUnitOfWork()

    await services.new_job(
        uow=uow,
        sharecode="sharecode",
        **new_job_junk,
    )

    job = get_first(uow.jobs)
    demo = get_first(uow.demos)

    assert job.id is not None
    assert demo.id is not None
    assert demo.state is DemoState.MATCH
    assert demo.queued is True
    assert demo.sharecode == "sharecode"

    assert job.state is JobState.DEMO

    matchinfo_send.assert_awaited_once_with(
        id=demo.id, sharecode=demo.sharecode, dispatcher=ANY
    )


@pytest.mark.asyncio
async def test_new_job_demo_id_can_record(mock_dispatch, new_job_junk):
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=True,
        data=demo_data[0],
    )

    uow = FakeUnitOfWork(demos=[demo])

    await services.new_job(uow, demo_id=demo.id, **new_job_junk)

    job = get_first(uow.jobs)

    assert uow.commit_count == 1

    assert demo.can_record()
    assert not demo.queued
    assert job.state is JobState.SELECT

    event = mock_dispatch.call_args[0][0]
    assert event.job is job


@pytest.mark.asyncio
async def test_new_job_demo_id_no_data(demoparse_send, new_job_junk):
    demo = new_demo(
        state=DemoState,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=True,
    )

    uow = FakeUnitOfWork(demos=[demo])

    await services.new_job(uow, demo_id=demo.id, **new_job_junk)

    job = get_first(uow.jobs)

    assert not demo.can_record()
    assert not demo.is_up_to_date()
    assert demo.queued
    assert job.state is JobState.DEMO

    demoparse_send.assert_awaited_once_with(
        id=demo.id, dispatcher=ANY, matchid=demo.matchid, url=demo.url
    )


@pytest.mark.asyncio
async def test_new_job_demo_id_not_up_to_date(demoparse_send, new_job_junk):
    # what I'm really doing here is testing handle_demo step, so the new_job
    # stuff is really unnecessary

    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=True,
        data=demo_data[0],
    )

    demo.version -= 1

    uow = FakeUnitOfWork(demos=[demo])

    await services.new_job(uow, demo_id=demo.id, **new_job_junk)

    job = get_first(uow.jobs)

    assert not demo.can_record()
    assert not demo.is_up_to_date()
    assert demo.state is DemoState.PARSE
    assert demo.queued
    assert job.state is JobState.DEMO

    demoparse_send.assert_awaited_once_with(
        id=demo.id, dispatcher=ANY, matchid=demo.matchid, url=demo.url
    )


@pytest.mark.asyncio
async def test_new_job_demo_id_no_matchinfo(matchinfo_send, new_job_junk):
    # what I'm really doing here is testing handle_demo step, so the new_job
    # stuff is really unnecessary

    sharecode = "sharecode"
    demo = new_demo(
        state=DemoState.PARSE,  # should be changed to .MATCH
        queued=False,
        sharecode=sharecode,
        has_matchinfo=False,
    )

    uow = FakeUnitOfWork(demos=[demo])

    await services.new_job(uow, demo_id=demo.id, **new_job_junk)

    job = get_first(uow.jobs)

    assert not demo.can_record()
    assert not demo.is_up_to_date()
    assert not demo.has_matchinfo()
    assert demo.state is DemoState.MATCH
    assert demo.queued
    assert job.state is JobState.DEMO

    matchinfo_send.assert_awaited_once_with(
        id=demo.id, dispatcher=ANY, sharecode=sharecode
    )


@pytest.mark.asyncio
async def test_demo_step_matchinfo_failure(matchinfo_send_raises):
    # test case: new job was started with demo which has queued=False
    # and state=MATCH

    sharecode = "sharecode"
    demo = new_demo(
        state=DemoState.MATCH,
        queued=False,
        sharecode=sharecode,
        has_matchinfo=False,
    )

    uow = FakeUnitOfWork()

    with pytest.raises(Exception):
        async with uow:
            uow.demos.add(demo)
            await services.handle_demo_step(demo=demo)

    assert uow.commit_count == 0
    assert uow.rollback_count == 1

    assert not demo.has_matchinfo()
    assert not demo.can_record()
    assert not demo.is_up_to_date()

    assert demo.state is DemoState.MATCH
    assert not demo.queued

    matchinfo_send_raises.assert_awaited_once_with(
        id=demo.id, dispatcher=ANY, sharecode=sharecode
    )


@pytest.mark.asyncio
async def test_demo_step_demoparse_failure(demoparse_send_raises):
    # test case: matchinfo was received earlier but the demoparse broker failed
    # when matchinfo_success called it
    # meaning that the demo was rolled back to state=PARSE and queued=False

    sharecode = "sharecode"
    demo = new_demo(
        state=DemoState.PARSE,
        queued=False,
        sharecode=sharecode,
        has_matchinfo=True,
    )

    uow = FakeUnitOfWork(demos=[demo])

    with pytest.raises(Exception):
        async with uow:
            tmp_demo = await uow.demos.get(demo.id)
            await services.handle_demo_step(demo=tmp_demo)

    assert uow.commit_count == 0
    assert uow.rollback_count == 1

    assert not demo.can_record()
    assert not demo.is_up_to_date()

    assert demo.state is DemoState.PARSE
    assert not demo.queued

    demoparse_send_raises.assert_awaited_once_with(
        id=demo.id, dispatcher=ANY, matchid=demo.matchid, url=demo.url
    )


@pytest.mark.asyncio
async def test_demo_step_can_record():
    # test case: demo is can_record=True but has wrong state
    # this should fix it

    sharecode = "sharecode"
    demo = new_demo(
        state=DemoState.PARSE,
        queued=False,
        sharecode=sharecode,
        has_matchinfo=True,
        data=demo_data[0],
    )

    uow = FakeUnitOfWork(demos=[demo])

    async with uow:
        await services.handle_demo_step(demo=demo)

    assert uow.commit_count == 0
    assert uow.rollback_count == 0

    assert demo.can_record()
    assert demo.is_up_to_date()

    assert demo.state is DemoState.SUCCESS
    assert not demo.queued


@pytest.mark.asyncio
async def test_matchinfo_success(demoparse_send):
    demo = new_demo(
        state=DemoState.MATCH,
        queued=True,
        sharecode="sharecode",
        has_matchinfo=False,
    )

    uow = FakeUnitOfWork(demos=[demo])

    matchid = random_matchid()
    matchtime = 1657154816
    url = "not a real url"
    event = events.MatchInfoSuccess(
        id=demo.id, matchid=matchid, matchtime=matchtime, url=url
    )

    await services.matchinfo_success(uow, event)

    assert demo.state is DemoState.PARSE
    assert demo.queued
    assert demo.matchid == matchid
    assert isinstance(demo.matchtime, datetime)
    assert demo.url == url

    demoparse_send.assert_awaited_once_with(
        id=demo.id, dispatcher=ANY, matchid=matchid, url=url
    )


@pytest.mark.asyncio
async def test_matchinfo_success_broker_failure(demoparse_send_raises):
    demo = new_demo(
        state=DemoState.MATCH,
        queued=True,
        sharecode="sharecode",
        has_matchinfo=False,
    )

    job = new_job(state=JobState.DEMO)
    job.demo = demo

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])
    job.demo_id = demo.id

    matchid = random_matchid()
    matchtime = 1657154816
    url = "not a real url"
    event = events.MatchInfoSuccess(
        id=demo.id, matchid=matchid, matchtime=matchtime, url=url
    )

    with pytest.raises(Exception):
        await services.matchinfo_success(uow, event)

    assert demo.state is DemoState.PARSE
    assert not demo.queued
    assert demo.matchid == matchid
    assert isinstance(demo.matchtime, datetime)
    assert demo.url == url

    demoparse_send_raises.assert_awaited_once_with(
        id=demo.id, dispatcher=ANY, matchid=matchid, url=url
    )

    assert len(uow.events) == 1
    dispatch_event = uow.events[0]
    assert isinstance(dispatch_event, events.JobDemoParseFailed)
    assert dispatch_event.job is job


@pytest.mark.asyncio
async def test_matchinfo_failure():
    demo = new_demo(
        state=DemoState.MATCH,
        queued=True,
        sharecode="sharecode",
        has_matchinfo=False,
    )

    job = new_job(state=JobState.DEMO)

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])

    job.demo_id = demo.id

    reason = "some reason"
    event = events.MatchInfoFailure(id=demo.id, reason=reason)

    await services.matchinfo_failure(uow, event)

    assert uow.commit_count == 1

    assert demo.state is DemoState.FAILED
    assert not demo.queued

    assert job.state is JobState.FAILED

    assert len(uow.events) == 1
    dispatch_event = uow.events[0]
    assert dispatch_event.job is job
    assert dispatch_event.reason == reason


@pytest.mark.asyncio
async def test_demoparse_success():
    demo = new_demo(
        state=DemoState.PARSE,
        queued=True,
        sharecode="sharecode",
        has_matchinfo=True,
    )

    job = new_job(state=JobState.DEMO)

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])

    job.demo_id = demo.id

    version = 1
    event = events.DemoParseSuccess(id=demo.id, data=demo_data[0], version=version)
    await services.demoparse_success(uow, event)

    assert uow.commit_count == 1

    assert demo.state is DemoState.SUCCESS
    assert not demo.queued
    assert isinstance(demo.data, dict)
    assert demo.version == version
    assert isinstance(demo.downloaded_at, datetime)
    assert len(demo.score) == 2

    assert job.state is JobState.SELECT

    assert len(uow.events) == 1
    dispatch_event = uow.events[0]
    assert isinstance(dispatch_event, events.JobReadyForSelect)
    assert dispatch_event.job is job


@pytest.mark.asyncio
async def test_demoparse_success_outdated(demoparse_send):
    demo = new_demo(
        state=DemoState.PARSE,
        queued=True,
        sharecode="sharecode",
        has_matchinfo=True,
    )

    job = new_job(state=JobState.DEMO)

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])

    job.demo_id = demo.id

    version = DEMOPARSE_VERSION - 1
    event = events.DemoParseSuccess(id=demo.id, data=demo_data[0], version=version)
    await services.demoparse_success(uow, event)

    assert uow.commit_count == 1

    assert demo.state is DemoState.PARSE
    assert demo.queued

    demoparse_send.assert_awaited_once_with(
        id=demo.id, dispatcher=ANY, matchid=demo.matchid, url=demo.url
    )


@pytest.mark.asyncio
async def test_demoparse_failure():
    demo = new_demo(
        state=DemoState.PARSE,
        queued=True,
        sharecode="sharecode",
        has_matchinfo=True,
    )

    job = new_job(state=JobState.DEMO)

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])

    job.demo_id = demo.id

    reason = "some reason"
    event = events.DemoParseFailure(id=demo.id, reason=reason)
    await services.demoparse_failure(uow, event)

    assert uow.commit_count == 1

    assert demo.state is DemoState.FAILED
    assert not demo.queued

    assert job.state is JobState.FAILED

    assert len(uow.events) == 1
    dispatch_event = uow.events[0]
    assert isinstance(dispatch_event, events.JobDemoParseFailed)
    assert dispatch_event.job is job
    assert dispatch_event.reason == reason


@pytest.mark.asyncio
async def test_record(recorder_send):
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=True,
        sharecode="sharecode",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    demo.parse()

    round_id = 10

    job = new_job(state=JobState.SELECT)
    job.demo = demo

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])

    job.demo_id = demo.id
    player = demo.get_player_by_id(6)

    await services.record(uow, job=job, player=player, round_id=round_id)

    assert job.state is JobState.RECORD
    assert isinstance(job.recording, Recording)
    assert job.recording.recording_type is RecordingType.HIGHLIGHT
    assert job.recording.player_xuid == player.xuid
    assert job.recording.round_id == round_id

    recorder_send.assert_awaited_once()


@pytest.mark.asyncio
async def test_record_broker_failure():
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    demo.parse()

    round_id = 10

    job = new_job(state=JobState.SELECT)
    job.demo = demo

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])

    job.demo_id = demo.id
    player = demo.get_player_by_id(6)

    with pytest.raises(Exception):
        await services.record(uow, job=job, player=player, round_id=round_id)

    assert job.state is JobState.FAILED

    assert len(uow.events) == 1
    dispatch_event = uow.events[0]
    assert isinstance(dispatch_event, events.JobRecordingFailed)
    assert dispatch_event.job is job


@pytest.mark.asyncio
async def test_recorder_success(uploader_send):
    job = new_job(state=JobState.RECORD)
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    job.demo = demo
    job.recording = Recording(
        RecordingType.HIGHLIGHT, player_xuid=76561198044195953, round_id=10
    )

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])

    event = events.RecorderSuccess(job.id)
    await services.recorder_success(uow, event)

    assert uow.commit_count == 1
    assert job.state is JobState.UPLOAD
    uploader_send.assert_awaited_once_with(
        id=job.id,
        user_id=job.user_id,
        channel_id=job.channel_id,
        file_name=ANY,
    )


@pytest.mark.asyncio
async def test_recorder_failure():
    job = new_job(state=JobState.DEMO)

    uow = FakeUnitOfWork(jobs=[job])

    reason = "some reason"
    event = events.RecorderFailure(id=job.id, reason=reason)
    await services.recorder_failure(uow, event)

    assert uow.commit_count == 1

    assert job.state is JobState.FAILED

    assert len(uow.events) == 1
    dispatch_event = uow.events[0]
    assert isinstance(dispatch_event, events.JobRecordingFailed)
    assert dispatch_event.job is job
    assert dispatch_event.reason == reason


@pytest.mark.asyncio
async def test_uploader_success():
    job = new_job(state=JobState.UPLOAD)

    uow = FakeUnitOfWork(jobs=[job])

    event = events.UploaderSuccess(id=job.id)
    await services.uploader_success(uow, event)

    assert uow.commit_count == 1
    assert len(uow.events) == 1
    assert isinstance(uow.events[0], events.JobUploadSuccess)
    assert uow.events[0].job is job


@pytest.mark.asyncio
async def test_uploader_failure():
    job = new_job(state=JobState.UPLOAD)

    uow = FakeUnitOfWork(jobs=[job])

    reason = "oof"
    event = events.UploaderFailure(id=job.id, reason=reason)
    await services.uploader_failure(uow, event)

    assert uow.commit_count == 1
    assert len(uow.events) == 1
    assert isinstance(uow.events[0], events.JobUploadFailed)
    assert uow.events[0].job is job
    assert uow.events[0].reason == reason


@pytest.mark.asyncio
async def test_abort_job(demo_job):
    uow = FakeUnitOfWork(jobs=[demo_job])

    await services.abort_job(uow, demo_job)

    assert uow.commit_count == 1
    assert demo_job.state is JobState.ABORTED


@pytest.mark.asyncio
async def test_restore_restart_jobs():
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    demo.parse()

    job = new_job(state=JobState.SELECT)
    job.demo = demo
    uow = FakeUnitOfWork(jobs=[job], demos=[demo])
    job.demo_id = demo.id

    await services.restore(uow)

    event = uow.events[0]
    assert isinstance(event, events.JobReadyForSelect)
    assert event.job is job


@pytest.mark.asyncio
async def test_restore_unqueued_demos_matchinfo(matchinfo_send):
    demo = new_demo(
        state=DemoState.MATCH,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=False,
    )

    uow = FakeUnitOfWork(demos=[demo])

    await services.restore(uow)

    matchinfo_send.assert_awaited_once()


@pytest.mark.asyncio
async def test_restore_unqueued_demos_demoparse(demoparse_send):
    demo = new_demo(
        state=DemoState.PARSE,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=True,
    )

    uow = FakeUnitOfWork(demos=[demo])

    await services.restore(uow)

    demoparse_send.assert_awaited_once()


@pytest.mark.asyncio
async def test_restore_queued_demos(matchinfo_send, demoparse_send):
    demo_one = new_demo(
        state=DemoState.MATCH, queued=False, sharecode="sharecode", has_matchinfo=False
    )

    demo_two = new_demo(
        state=DemoState.PARSE,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=True,
    )

    demo_three = new_demo(
        state=DemoState.PARSE,
        queued=True,
        sharecode="sharecode",
        has_matchinfo=True,
    )

    uow = FakeUnitOfWork(demos=[demo_one, demo_two, demo_three])

    await services.restore(uow)

    matchinfo_send.assert_awaited_once()
    demoparse_send.assert_awaited_once()


@pytest.mark.asyncio
async def test_restore_get_recording():
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=True,
        sharecode="sharecode",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    job = new_job(state=JobState.RECORD)
    job.demo = demo
    uow = FakeUnitOfWork(jobs=[job], demos=[demo])
    job.demo_id = demo.id

    await services.restore(uow)

    assert broker.recorder._queue[0] == job.id
