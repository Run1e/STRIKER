import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import List
from uuid import UUID

from adapters import broker
from bot import sequencer
from domain import events
from domain.domain import (
    Demo,
    DemoState,
    Job,
    JobState,
    Player,
    Recording,
    RecordingType,
)
from shared.const import DEMOPARSE_VERSION
from shared.utils import utcnow

from services import bus
from services.uow import SqlUnitOfWork

DEMO_LOCK = asyncio.Lock()
log = logging.getLogger(__name__)


async def new_job(
    uow: SqlUnitOfWork,
    guild_id: int,
    channel_id: int,
    user_id: int,
    inter_payload: bytes,
    sharecode: str = None,
    demo_id: int = None,
):
    async with DEMO_LOCK:
        async with uow:
            if sharecode is not None:
                # see if demo already exists
                demo = await uow.demos.from_sharecode(sharecode)

                if demo is None:
                    demo = Demo(
                        state=DemoState.MATCH, queued=False, sharecode=sharecode
                    )
                    uow.demos.add(demo)

            elif demo_id is not None:
                demo = await uow.demos.get(demo_id)

            can_record = demo.can_record()

            job = Job(
                state=JobState.SELECT if can_record else JobState.DEMO,
                guild_id=guild_id,
                channel_id=channel_id,
                user_id=user_id,
                started_at=utcnow(),
                inter_payload=inter_payload,
            )

            job.demo = demo

            uow.jobs.add(job)
            await uow.commit()

        # future me: this has to be outside the above ouw
        # because it NEEDS AN ID BEFORE BEING SENT
        # THROUGH THE BROKER
        # also: no try catch since it causes an exception
        # which goes to the slash command error handler
        # also 2: we could .flush() to get the id but this is fine
        # also 3: is it really? it should fail the command if it fails here
        if not can_record:
            async with uow:
                uow.demos.add(demo)
                await handle_demo_step(demo, dispatcher=uow.add_event)
                await uow.commit()

        # if can_record was false above, it might've gotten "fixed" by
        # having its state set to success, in which case we can actually gogogo
        # of course the demo could literally also just be ready to record
        if demo.can_record():
            bus.dispatch(events.JobReadyForSelect(job=job))
        else:
            pass  # TODO: uuuhhh


async def handle_demo_step(demo: Demo, dispatcher=None):
    # if demo is already queued, do nothing
    if demo.queued:
        log.info('Demo %s already queued, not finding next step', demo.id)
        return

    log.info('Demo %s not queued, finding next step', demo.id)

    if not demo.has_matchinfo():
        log.info('New demo, need to fetch matchinfo first')
        await broker.matchinfo.send(
            id=demo.id, dispatcher=dispatcher, sharecode=demo.sharecode
        )
        demo.state = DemoState.MATCH
        demo.queued = True

    elif not demo.is_up_to_date():
        # the demo version is not what we expect
        # this can be one of two reasons:
        # 1. the demo has not been parsed yet
        # 2. the demo has been parsed but is out of date
        # in both cases we need to send it on to the demoparser
        log.info(
            'Demo exists but has no data or is out of date, dispatching to demoparse'
        )
        await broker.demoparse.send(
            id=demo.id,
            dispatcher=dispatcher,
            matchid=demo.matchid,
            url=demo.url,
        )
        demo.state = DemoState.PARSE
        demo.queued = True

    else:
        # otherwise, demo *should* be fine
        log.info('Demo is seemingly ready to use')
        demo.state = DemoState.SUCCESS
        demo.queued = False


async def restore(uow: SqlUnitOfWork):
    # restores jobs and demos that were cut off during last restart
    async with uow:
        jobs = await uow.jobs.where(state=JobState.SELECT)
        for job in jobs:
            uow.add_event(events.JobReadyForSelect(job))

        demos = await uow.demos.unqueued()
        for demo in demos:
            # on broker failure this will except
            # which is probably what we want to know
            # on startup
            await handle_demo_step(demo, dispatcher=uow.add_event)

        await uow.commit()


async def get_user_demos(uow: SqlUnitOfWork, user_id: int) -> List[Demo]:
    async with uow:
        demos = await uow.demos.user_associated(user_id)

    return demos


async def abort_job(uow: SqlUnitOfWork, job: Job):
    async with uow:
        uow.jobs.add(job)
        job.state = JobState.ABORTED
        await uow.commit()


async def record(
    uow: SqlUnitOfWork,
    job: Job,
    player: Player,
    round_id: int,
):
    demo = job.demo
    all_kills = demo.get_player_kills(player)
    kills = all_kills[round_id]
    BITRATE_SCALAR = 0.8
    MAX_VIDEO_BITRATE = 4 * 1024 * 1024
    MAX_FILE_SIZE = 8 * 1024 * 1024 * 8  # 8 MB

    start_tick, end_tick, skips, total_seconds = sequencer.single_highlight(
        demo.tickrate, kills
    )

    # video_bitrate = 20 * 1024 * 1024
    video_bitrate = min(
        MAX_VIDEO_BITRATE, int((MAX_FILE_SIZE / total_seconds) * BITRATE_SCALAR)
    )

    data = dict(
        job_id=str(job.id),
        matchid=demo.matchid,
        xuid=player.xuid,
        start_tick=start_tick,
        end_tick=end_tick,
        skips=skips,
        fps=60,
        color_correction=True,
        resolution=(1280, 854),  # (960, 640),
        video_bitrate=video_bitrate,
        audio_bitrate=192,
    )

    async with uow:
        uow.jobs.add(job)

        try:
            await broker.recorder.send(id=job.id, dispatcher=uow.add_event, **data)
        except:
            job.state = JobState.FAILED
            uow.add_event(
                events.JobRecordingFailed(
                    job=job, reason='Failed communicating with message broker'
                )
            )

            raise
        else:
            job.state = JobState.RECORD
            job.recording = Recording(
                recording_type=RecordingType.HIGHLIGHT,
                player_xuid=player.xuid,
                round_id=round_id,
            )
        finally:
            await uow.commit()


@bus.listen(events.MatchInfoSuccess)
async def matchinfo_success(uow: SqlUnitOfWork, event: events.MatchInfoSuccess):
    async with DEMO_LOCK:
        async with uow:
            demo = await uow.demos.get(event.id)

            if demo is None:
                raise ValueError(
                    f'Received MatchInfoSuccess for nonexistent demo with id {event.id}'
                )

            demo.state = DemoState.PARSE
            demo.queued = False
            demo.matchid = event.matchid
            demo.matchtime = datetime.fromtimestamp(event.matchtime, timezone.utc)
            demo.url = event.url

            try:
                await handle_demo_step(demo=demo, dispatcher=uow.add_event)
            except:
                jobs = await uow.jobs.waiting_for_demo(demo.id)
                for job in jobs:
                    uow.add_event(
                        events.JobDemoParseFailed(
                            job=job, reason='Failed communicating with message broker'
                        )
                    )
                raise
            finally:
                await uow.commit()


@bus.listen(events.MatchInfoFailure)
async def matchinfo_failure(uow: SqlUnitOfWork, event: events.MatchInfoFailure):
    async with uow:
        await uow.demos.update(
            event.id,
            state=DemoState.FAILED,
            queued=False,
        )

        # get jobs related to demo and set them to failed
        jobs = await uow.jobs.waiting_for_demo(event.id)
        for job in jobs:
            job.state = JobState.FAILED
            uow.add_event(events.JobMatchInfoFailed(job, event.reason))

        await uow.commit()


async def get_jobs_waiting_for_demo(uow: SqlUnitOfWork, demo_id: int):
    async with uow:
        return await uow.jobs.waiting_for_demo(demo_id=demo_id)


async def get_job(uow: SqlUnitOfWork, job_id: UUID):
    async with uow:
        return await uow.jobs.get(job_id)


@bus.listen(events.DemoParseSuccess)
async def demoparse_success(uow: SqlUnitOfWork, event: events.DemoParseSuccess):
    if event.version != DEMOPARSE_VERSION:
        async with uow:
            demo = await uow.demos.get(event.id)

            if demo is None:
                raise ValueError(
                    f'Received success from demoparse for non-existent demo with id: {event.id}'
                )

            demo.queued = False

            try:
                await handle_demo_step(demo=demo, dispatcher=uow.add_event)
            except:
                jobs = await uow.jobs.waiting_for_demo(demo_id=demo.id)
                for job in jobs:
                    job.state = JobState.FAILED
                    uow.add_event(
                        events.JobDemoParseFailed(
                            job=job, reason='Failed communicating with message broker'
                        )
                    )
                raise
            finally:
                await uow.commit()

        return

    async with uow:
        demo = await uow.demos.get(event.id)

        if demo is None:
            raise ValueError(
                f'Received success from demoparse for non-existent demo with id: {event.id}'
            )

        demo.state = DemoState.SUCCESS
        demo.queued = False
        demo.data = json.loads(event.data)
        demo.version = event.version
        demo.downloaded_at = utcnow()

        # parse the demo to ensure .score is set
        demo.parse()

        # get jobs waiting for this demo
        jobs = await uow.jobs.waiting_for_demo(demo_id=event.id)

        # these jobs can now start the select process
        for job in jobs:
            job.state = JobState.SELECT
            uow.add_event(events.JobReadyForSelect(job=job))

        await uow.commit()


@bus.listen(events.DemoParseFailure)
async def demoparse_failure(uow: SqlUnitOfWork, event: events.DemoParseFailure):
    async with uow:
        await uow.demos.update(
            event.id,
            state=DemoState.FAILED,
            queued=False,
        )

        jobs = await uow.jobs.waiting_for_demo(demo_id=event.id)

        for job in jobs:
            job.state = JobState.FAILED
            uow.add_event(events.JobDemoParseFailed(job, event.reason))

        await uow.commit()


@bus.listen(events.RecorderSuccess)
async def recorder_success(uow: SqlUnitOfWork, event: events.RecorderSuccess):
    async with uow:
        job = await uow.jobs.get(event.id)

        if job is None:
            raise ValueError(
                f'Recorder success references job that does not exist: {event.id}'
            )

        # since we're doing a bus.call below, we need to set this before
        # firing JobRecordingComplete
        # if we did it afterwards, that listener would have the wrong job state
        job.state = JobState.SUCCESS

        try:
            await bus.call(events.JobRecordingComplete(job=job))
        except:
            job.state = JobState.FAILED
            uow.add_event(
                events.JobRecordingUploadFailed(job=job, reason='Job upload failed.')
            )
            raise
        finally:
            await uow.commit()


@bus.listen(events.RecorderFailure)
async def recorder_failure(uow: SqlUnitOfWork, event: events.RecorderFailure):
    async with uow:
        job = await uow.jobs.get(event.id)
        if job is None:
            raise ValueError(
                f'Recorder failure references job that does not exist: {event.id}'
            )

        job.state = JobState.FAILED

        uow.add_event(events.JobRecordingFailed(job=job, reason=event.reason))
        await uow.commit()
