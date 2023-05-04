import asyncio
from collections import deque
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from json import loads
from unittest.mock import ANY, AsyncMock
from uuid import uuid4

import pytest

from domain.domain import DemoGame, DemoOrigin, DemoState, JobState, Recording, RecordingType
from messages import bus as eventbus
from messages import commands, events
from services import services
from shared.const import DEMOPARSE_VERSION
from tests.testutils import *


class FakeRepository:
    def __init__(self, instances=None):
        self.instances = {}
        for instance in instances:
            self.add(instance)

    @property
    def seen(self):
        return list(self.instances.values())

    def add(self, instance):
        self.instances[instance.id] = instance

    async def get(self, _id):
        return self.instances.get(_id, None)


class FakeJobRepository(FakeRepository):
    def add(self, instance):
        if not hasattr(instance, "id"):
            instance.id = uuid4()
        if not hasattr(instance, "demo_id"):
            instance.demo_id = None

        return super().add(instance)

    async def get_recording(self):
        jobs = []
        for job in self.instances.values():
            if job.state is JobState.RECORDING:
                jobs.append(job)

        return jobs

    async def get_restart(self):
        jobs = []
        for job in self.instances.values():
            if job.state is JobState.SELECTING:
                jobs.append(job)

        return jobs

    async def waiting_for_demo(self, demo_id):
        jobs = []
        for job in self.instances.values():
            if (
                job.state is JobState.WAITING
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
        for instance in self.instances.values():
            if instance.sharecode == sharecode:
                return instance
        return None

    async def from_identifier(self, origin, identifier):
        for instance in self.instances.values():
            if instance.origin is origin and instance.identifier == identifier:
                return instance
        return None


class FakeUnitOfWork:
    def __init__(self, jobs=None, demos=None) -> None:
        self.jobs = FakeJobRepository(jobs or [])
        self.demos = FakeDemoRepository(demos or [])
        self.committed = False

    async def __aenter__(self):
        self.messages = deque()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for repo in (self.jobs, self.demos):
            for model in repo.seen:
                self.messages.extend(model.events)

                # strictly a fake uow thing as we reuse the same uow
                model.events.clear()

    def add_message(self, event):
        self.messages.append(event)

    async def flush(self):
        pass

    async def commit(self):
        self.committed = True


def get_first(repo):
    return next((ins for ins in repo.instances.values()))


from messages.bus import MessageBus


async def create_bus(uow: FakeUnitOfWork, dependencies=None) -> MessageBus:
    messagebus = eventbus.MessageBus(dependencies=dependencies or dict(), uow_factory=lambda: uow)
    messagebus.add_decos()
    return messagebus


@pytest.mark.asyncio
async def test_new_job_sharecode(new_job_junk):
    matchid = 1337
    matchtime = 1520689874
    url = ("http://replay184.valve.net/730/003265661444162584623_2064223309.dem.bz2",)
    sharecode = "not a sharecode"

    uow = FakeUnitOfWork()
    publish = AsyncMock()
    sharecode_resolver = AsyncMock(return_value=(matchid, matchtime, url))

    bus = await create_bus(uow, dict(publish=publish, sharecode_resolver=sharecode_resolver))

    await bus.dispatch(commands.CreateJob(sharecode=sharecode, **new_job_junk))

    job = get_first(uow.jobs)
    demo = job.demo

    assert demo.game == DemoGame.CSGO
    assert demo.origin is DemoOrigin.VALVE
    assert demo.state is DemoState.PROCESSING
    assert demo.sharecode == sharecode
    assert demo.identifier == str(matchid)
    assert isinstance(demo.time, datetime)
    assert demo.download_url == url


@pytest.mark.asyncio
async def test_new_job_demo_id_can_record(new_job_junk):
    demo = new_demo(
        state=DemoState.READY,
        add_matchinfo=True,
        add_data=True,
    )

    uow = FakeUnitOfWork(demos=[demo])
    publish = AsyncMock()
    sharecode_resolver = AsyncMock()
    bus = await create_bus(uow, dict(publish=publish, sharecode_resolver=sharecode_resolver))

    await bus.dispatch(commands.CreateJob(demo_id=demo.id, **new_job_junk))

    job = get_first(uow.jobs)

    assert uow.committed

    assert demo.is_ready()
    assert job.state is JobState.SELECTING

    # TODO: could also check frontend call here


@pytest.mark.asyncio
async def test_new_job_do_not_request_again(new_job_junk):
    # creating a new job on a demo that's currently processing
    # should NOT cause another microserve publish
    demo = new_demo(
        state=DemoState.PROCESSING,
        add_matchinfo=True,
    )

    uow = FakeUnitOfWork(demos=[demo])
    publish = AsyncMock()
    sharecode_resolver = AsyncMock()
    bus = await create_bus(uow, dict(publish=publish, sharecode_resolver=sharecode_resolver))

    await bus.dispatch(commands.CreateJob(demo_id=demo.id, **new_job_junk))

    job = get_first(uow.jobs)

    assert not demo.is_ready()
    assert not demo.is_up_to_date()
    assert job.state is JobState.WAITING

    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_new_job_demo_id_not_up_to_date(new_job_junk):
    # new job on existing demo with outdated parsed version
    # should cause a new RequestDemoParse publish

    demo = new_demo(
        state=DemoState.READY,
        add_matchinfo=True,
        add_data=True,
    )

    demo.data_version -= 1

    uow = FakeUnitOfWork(demos=[demo])
    publish = AsyncMock()
    sharecode_resolver = AsyncMock()
    bus = await create_bus(uow, dict(publish=publish, sharecode_resolver=sharecode_resolver))

    await bus.dispatch(commands.CreateJob(demo_id=demo.id, **new_job_junk))

    job = get_first(uow.jobs)

    assert not demo.is_ready()
    assert not demo.is_up_to_date()
    assert demo.state is DemoState.PROCESSING
    assert job.state is JobState.WAITING

    publish.assert_awaited_once_with(
        commands.RequestDemoParse(origin=ANY, identifier=ANY, download_url=ANY)
    )


@pytest.mark.asyncio
async def test_new_job_demo_id_no_matchinfo(new_job_junk):
    # what I'm really doing here is testing handle_demo step, so the new_job
    # stuff is really unnecessary

    sharecode = "sharecode"
    demo = new_demo(
        state=DemoState.PROCESSING,  # should be changed to .MATCH
        sharecode=sharecode,
    )

    uow = FakeUnitOfWork(demos=[demo])
    publish = AsyncMock()
    sharecode_resolver = AsyncMock()
    bus = await create_bus(uow, dict(publish=publish, sharecode_resolver=sharecode_resolver))

    await bus.dispatch(commands.CreateJob(demo_id=demo.id, **new_job_junk))

    job = get_first(uow.jobs)

    assert not demo.is_ready()
    assert not demo.is_up_to_date()
    assert not demo.has_download_url()
    assert demo.state is DemoState.PROCESSING
    assert job.state is JobState.WAITING


@pytest.mark.asyncio
async def test_demo_step_can_record():
    # test case: demo is can_record=True but has wrong state
    # this should fix it

    sharecode = "sharecode"
    demo = new_demo(
        state=DemoState.PROCESSING,
        sharecode=sharecode,
        add_matchinfo=True,
        add_data=True,
    )

    uow = FakeUnitOfWork(demos=[demo])

    async with uow:
        await services.handle_demo_step(demo=demo, publish=AsyncMock())

    assert not uow.committed

    assert demo.is_ready()
    assert demo.is_up_to_date()

    assert demo.state is DemoState.READY


@pytest.mark.asyncio
async def test_demoparse_success():
    demo = new_demo(
        state=DemoState.PROCESSING,
        add_matchinfo=True,
    )

    job = create_job(state=JobState.WAITING)

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])

    publish = AsyncMock()
    bus = await create_bus(uow, dict(publish=publish))

    job.demo = demo
    job.demo_id = demo.id

    version = 1
    event = events.DemoParseSuccess(
        origin=demo.origin.name, identifier=demo.identifier, data=demo_data[0], version=version
    )
    await bus.dispatch(event)

    assert uow.committed

    assert demo.state is DemoState.READY
    assert isinstance(demo.data, dict)
    assert demo.data_version == version
    assert isinstance(demo.downloaded_at, datetime)
    assert len(demo.score) == 2
    assert demo.map is not None

    assert job.state is JobState.SELECTING


@pytest.mark.asyncio
async def test_demoparse_success_outdated():
    demo = new_demo(
        state=DemoState.PROCESSING,
        sharecode="sharecode",
        add_matchinfo=True,
    )

    job = create_job(state=JobState.WAITING)
    uow = FakeUnitOfWork(jobs=[job], demos=[demo])
    publish = AsyncMock()

    bus = await create_bus(uow, dict(publish=publish))

    job.demo_id = demo.id

    version = DEMOPARSE_VERSION - 1

    event = events.DemoParseSuccess(
        origin=demo.origin.name, identifier=demo.identifier, data=demo_data[0], version=version
    )

    await bus.dispatch(event)

    assert uow.committed
    assert demo.state is DemoState.PROCESSING

    publish.assert_awaited_once_with(
        commands.RequestDemoParse(origin=ANY, identifier=ANY, download_url=ANY)
    )

    assert job.state is JobState.WAITING


@pytest.mark.asyncio
async def test_demoparse_failure():
    demo = new_demo(
        state=DemoState.PROCESSING,
        sharecode="sharecode",
        add_matchinfo=True,
    )

    job = create_job(state=JobState.WAITING)

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])

    bus = await create_bus(uow)

    job.demo_id = demo.id

    reason = "demo failed sadge"
    event = events.DemoParseFailure(origin=demo.origin.name, identifier=demo.identifier, reason=reason)
    await bus.dispatch(event)

    assert uow.committed

    assert demo.state is DemoState.FAILED
    assert job.state is JobState.FAILED


@pytest.mark.asyncio
async def test_record():
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=True,
        sharecode="sharecode",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    demo.parse()

    round_id = 10

    job = create_job(state=JobState.SELECTING)
    job.demo = demo

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])
    bus = await create_bus(uow)

    job.demo_id = demo.id
    player = demo.get_player_by_id(6)

    await bus.dispatch(
        commands.Record(job_id=job.id, player_xuid=player.xuid, round_id=round_id, tier=0)
    )

    assert job.state is JobState.RECORDING


@pytest.mark.asyncio
async def test_recorder_success():
    job = create_job(state=JobState.RECORDING)
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    job.demo = demo
    job.recording = Recording(
        RecordingType.PLAYER_ROUND, player_xuid=76561198044195953, round_id=10
    )

    uow = FakeUnitOfWork(jobs=[job], demos=[demo])

    event = events.RecorderSuccess(job.id)
    await services.recorder_success(uow, event)

    assert uow.commit_count == 1
    assert job.state is JobState.UPLOADING
    # uploader_send.assert_awaited_once_with(
    #     id=job.id,
    #     user_id=job.user_id,
    #     channel_id=job.channel_id,
    #     file_name=ANY,
    # )


@pytest.mark.asyncio
async def test_recorder_failure():
    job = create_job(state=JobState.WAITING)

    uow = FakeUnitOfWork(jobs=[job])

    reason = "some reason"
    event = events.RecorderFailure(id=job.id, reason=reason)
    await services.recorder_failure(uow, event)

    assert uow.commit_count == 1

    assert job.state is JobState.FAILED

    assert len(uow.messages) == 1
    dispatch_event = uow.messages[0]
    assert isinstance(dispatch_event, events.JobRecordingFailed)
    assert dispatch_event.job is job
    assert dispatch_event.reason == reason


@pytest.mark.asyncio
async def test_uploader_success():
    job = create_job(state=JobState.UPLOADING)

    uow = FakeUnitOfWork(jobs=[job])

    event = events.UploaderSuccess(id=job.id)
    await services.uploader_success(uow, event)

    assert uow.commit_count == 1
    assert len(uow.messages) == 1
    assert isinstance(uow.messages[0], events.JobUploadSuccess)
    assert uow.messages[0].job is job


@pytest.mark.asyncio
async def test_uploader_failure():
    job = create_job(state=JobState.UPLOADING)

    uow = FakeUnitOfWork(jobs=[job])

    reason = "oof"
    event = events.UploaderFailure(id=job.id, reason=reason)
    await services.uploader_failure(uow, event)

    assert uow.commit_count == 1
    assert len(uow.messages) == 1
    assert isinstance(uow.messages[0], events.JobUploadFailed)
    assert uow.messages[0].job is job
    assert uow.messages[0].reason == reason


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

    job = create_job(state=JobState.SELECTING)
    job.demo = demo
    uow = FakeUnitOfWork(jobs=[job], demos=[demo])
    job.demo_id = demo.id

    command = commands.Restore()
    await services.restore(command=command, uow=uow)

    event = uow.messages[0]
    assert isinstance(event, events.JobReadyForSelect)
    assert event.job is job


@pytest.mark.asyncio
async def test_restore_unqueued_demos_matchinfo():
    demo = new_demo(
        state=DemoState.MATCH,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=False,
    )

    uow = FakeUnitOfWork(demos=[demo])

    command = commands.Restore()
    await services.restore(command=command, uow=uow)

    # matchinfo_send.assert_awaited_once()


@pytest.mark.asyncio
async def test_restore_unqueued_demos_demoparse():
    demo = new_demo(
        state=DemoState.PARSE,
        queued=False,
        sharecode="sharecode",
        has_matchinfo=True,
    )

    uow = FakeUnitOfWork(demos=[demo])

    command = commands.Restore()
    await services.restore(command=command, uow=uow)

    # demoparse_send.assert_awaited_once()


@pytest.mark.asyncio
async def test_restore_queued_demos():
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

    command = commands.Restore()
    await services.restore(command=command, uow=uow)

    # matchinfo_send.assert_awaited_once()
    # demoparse_send.assert_awaited_once()
