import asyncio
import logging
from typing import List
from uuid import UUID, uuid4

from bot import sequencer
from domain.domain import Demo, Job, User
from domain.enums import DemoGame, DemoOrigin, DemoState, JobState
from domain.demo_events import DemoEvents
from messages import commands, dto, events
from messages.deco import handler, listener
from services.uow import SqlUnitOfWork
from shared.const import DEMOPARSE_VERSION
from shared.utils import utcnow

DEMO_LOCK = asyncio.Lock()
log = logging.getLogger(__name__)


class ServiceError(Exception):
    pass


@handler(commands.CreateJob)
async def create_job(
    command: commands.CreateJob,
    uow: SqlUnitOfWork,
    publish,
    sharecode_resolver,
):
    async with DEMO_LOCK:
        async with uow:
            new_demo = False

            if command.demo_id is not None:
                demo = await uow.demos.get(command.demo_id)

            elif command.sharecode is not None:
                # see if demo already exists
                demo = await uow.demos.from_sharecode(command.sharecode)

                if demo is None:
                    matchid, matchtime, url = await sharecode_resolver(command.sharecode)

                    new_demo = True
                    demo = Demo(
                        game=DemoGame.CSGO,
                        origin=DemoOrigin.VALVE,
                        state=DemoState.PROCESSING,
                        sharecode=command.sharecode,
                        identifier=str(matchid),  # hacky
                        time=matchtime,
                        download_url=url,
                    )

                    uow.demos.add(demo)

                    # gives the new demo an autoincremented id
                    # which is needed in handle_demo_step
                    await uow.flush()

                    log.info(
                        "Demo with sharecode %s created with id %s", command.sharecode, demo.id
                    )

            job = Job(
                state=JobState.WAITING,
                guild_id=command.guild_id,
                channel_id=command.channel_id,
                user_id=command.user_id,
                started_at=utcnow(),
                inter_payload=command.inter_payload,
            )

            uow.jobs.add(job)

            # force id for job
            await uow.flush()

            job.set_demo(demo)

            # new demo? check next step
            # old demo and not currenly processing? double check!
            # basically has the effect of not rehandling demos that aren't new but are already processing
            # another way of thinking about it, there's two cases where nothing has been queued:
            # 1. the demo is new
            # 2. the demo is not processing anymore (READY (might have old parsed version), or FAILED)
            if new_demo or demo.state is not DemoState.PROCESSING:
                await handle_demo_step(demo, publish=publish)

            await uow.commit()


async def handle_demo_step(demo: Demo, publish):
    if not demo.is_up_to_date():
        # the demo version is not what we expect
        # this can be one of two reasons:
        # 1. the demo has not been parsed yet
        # 2. the demo has been parsed but is out of date
        # in both cases we need to send it on to the demoparser
        log.info("Demo %s needs to be parsed, requesting demo to be parsed", demo.id)

        demo.processing()

        await publish(
            commands.RequestDemoParse(
                origin=demo.origin.name,
                identifier=demo.identifier,
                download_url=demo.download_url,
            )
        )

    else:
        # otherwise, demo *should* be fine
        log.info("Demo %s seems fine to use", demo.id)
        demo.ready()


@listener(events.DemoReady)
async def demo_ready(event: events.DemoReady, uow: SqlUnitOfWork):
    async with uow:
        jobs = await uow.jobs.waiting_for_demo(demo_id=event.demo_id)

        for job in jobs:
            job.demo_ready()

        await uow.commit()


@listener(events.DemoFailure)
async def demo_failure(event: events.DemoFailure, uow: SqlUnitOfWork):
    async with uow:
        jobs = await uow.jobs.waiting_for_demo(demo_id=event.demo_id)

        for job in jobs:
            job.failed(event.reason)

        await uow.commit()


@listener(events.JobSelecting)
async def job_selecting(event: events.JobSelecting, uow: SqlUnitOfWork):
    async with uow:
        job = await uow.jobs.get(event.job_id)
        if job is None:
            return

        uow.add_message(
            dto.JobSelectable(job.id, job.state, job.inter_payload, DemoEvents(job.demo.data))
        )


@listener(events.DemoParseSuccess)
async def demoparse_success(event: events.DemoParseSuccess, uow: SqlUnitOfWork, publish):
    if event.version != DEMOPARSE_VERSION:
        await demoparse_success_out_of_date(event, uow, publish)
    else:
        await demoparse_success_up_to_date(event, uow)


async def demoparse_success_out_of_date(
    event: events.DemoParseSuccess, uow: SqlUnitOfWork, publish
):
    async with uow:
        demo = await uow.demos.from_identifier(DemoOrigin[event.origin], event.identifier)
        if demo is None:
            return

        await handle_demo_step(demo=demo, publish=publish)
        await uow.commit()


async def demoparse_success_up_to_date(event: events.DemoParseSuccess, uow: SqlUnitOfWork):
    async with uow:
        demo = await uow.demos.from_identifier(DemoOrigin[event.origin], event.identifier)
        if demo is None:
            return

        demo.set_demo_data(event.data, event.version)
        await uow.commit()


@listener(events.DemoParseFailure)
async def demoparse_failure(event: events.DemoParseFailure, uow: SqlUnitOfWork):
    async with uow:
        demo: Demo = await uow.demos.from_identifier(DemoOrigin[event.origin], event.identifier)
        if demo is None:
            return

        demo.failed(event.reason)
        await uow.commit()


async def restore(command: commands.Restore, uow: SqlUnitOfWork):
    # restores jobs and demos that were cut off during last restart
    async with uow:
        jobs = await uow.jobs.get_restart()
        for job in jobs:
            uow.add_message(events.JobReadyForSelect(job))

        await uow.commit()


async def get_user_demos(uow: SqlUnitOfWork, user_id: int) -> List[Demo]:
    async with uow:
        demos = await uow.demos.user_associated(user_id)

    return demos


async def get_jobs_waiting_for_demo(uow: SqlUnitOfWork, demo_id: int):
    async with uow:
        return await uow.jobs.waiting_for_demo(demo_id=demo_id)


async def get_job(uow: SqlUnitOfWork, job_id: UUID):
    async with uow:
        return await uow.jobs.get(job_id)


async def abort_job(uow: SqlUnitOfWork, job: Job):
    async with uow:
        uow.jobs.add(job)
        job.state = JobState.ABORTED
        await uow.commit()


async def user_recording_count(uow: SqlUnitOfWork, user_id: int):
    async with uow:
        return await uow.jobs.recording_count(user_id)


@handler(commands.Record)
async def record(
    command: commands.Record,
    uow: SqlUnitOfWork,
):
    async with uow:
        job = await uow.jobs.get(command.job_id)
        demo = job.demo

        demo.parse()

        player = demo.get_player_by_xuid(command.player_xuid)

        all_kills = demo.get_player_kills(player)
        kills = all_kills[command.round_id]

        BITRATE_SCALAR = 0.7
        MAX_VIDEO_BITRATE = 10 * 1024 * 1024
        MAX_FILE_SIZE = 25 * 8 * 1024 * 1024

        start_tick, end_tick, skips, total_seconds = sequencer.single_highlight(
            demo.tickrate, kills
        )

        # video_bitrate = 20 * 1024 * 1024
        video_bitrate = min(
            MAX_VIDEO_BITRATE, int((MAX_FILE_SIZE / total_seconds) * BITRATE_SCALAR)
        )

        data = dict(
            job_id=str(job.id),
            demo=demo.matchid,
            player_xuid=player.xuid,
            tickrate=demo.tickrate,
            start_tick=start_tick,
            end_tick=end_tick,
            skips=skips,
            fps=60,
            video_bitrate=video_bitrate,
            audio_bitrate=192,
            # user controlled
            fragmovie=False,
            color_filter=True,
            righthand=True,
            crosshair_code="CSGO-SG5dx-aAeRk-dnoAc-TwqMh-yTSFE",
            use_demo_crosshair=False,
        )

        if command.tier > 0:
            user = await uow.users.get_user(job.user_id)
            if user is not None:
                data.update(**user.update_recorder_settings())

        cmd = commands.RequestRecording(**data)
        uow.add_message(cmd)

        job.state = JobState.RECORDING

        await uow.commit()


async def archive(uow: SqlUnitOfWork, max_active_demos: int, dry_run: bool):
    async with uow:
        overflowed_demos = await uow.demos.least_recently_used_matchids(keep_count=max_active_demos)
        ignore_videos = await uow.jobs.active_ids()

        overflowed_demo_ids = [tup[0] for tup in overflowed_demos]
        overflowed_matchids = [tup[1] for tup in overflowed_demos]

        _uuid = uuid4()
        task = asyncio.create_task(
            bus.wait_for(
                events=[events.ArchiveSuccess, events.ArchiveFailure],
                check=lambda event: event.id == _uuid,
                timeout=20.0,
            )
        )

        # await broker.archive.send(
        #     id=_uuid,
        #     dry_run=dry_run,
        #     remove_matchids=overflowed_matchids,
        #     ignore_videos=[str(_id) for _id in ignore_videos],
        # )

        result = await task

        if result is None:
            raise ServiceError("Archive microservice did not respond in time.")

        if isinstance(result, events.ArchiveFailure):
            raise ServiceError(result.reason)

        # otherwise, we succeeded and can go about changing some heckin' demo states
        await uow.demos.bulk_set_deleted(overflowed_demo_ids)

        if not dry_run:
            await uow.commit()

        return dict(
            removed_demos=result.removed_demos,
            removed_videos=result.removed_videos,
        )


async def get_user(uow: SqlUnitOfWork, user_id: int) -> User:
    async with uow:
        user = await uow.users.get_user(user_id)

        if user is None:
            user = User(user_id)

        uow.users.add(user)
        await uow.commit()

    return user


async def store_user(uow: SqlUnitOfWork, user: User):
    async with uow:
        uow.users.add(user)
        await uow.commit()


# @bus.listen(events.RecorderSuccess)
async def recorder_success(uow: SqlUnitOfWork, event: events.RecorderSuccess):
    async with uow:
        job = await uow.jobs.get(event.id)

        if job is None:
            raise ValueError(f"Recorder success references job that does not exist: {event.id}")

        job.state = JobState.UPLOADING
        demo = job.demo
        recording = job.recording

        try:
            demo.parse()

            player = demo.get_player_by_xuid(recording.player_xuid)
            round_id = recording.round_id
            kills = demo.get_player_kills_round(player, round_id)

            info = demo.kills_info(recording.round_id, kills)
            file_name = " ".join([info[0], player.name, info[1]])
        except Exception:
            # the above is not *that* important
            # if anything in there fails, just revert to the job id
            file_name = str(job.id)

        # await broker.uploader.send(
        #     id=job.id,
        #     user_id=job.user_id,
        #     channel_id=job.channel_id,
        #     file_name=file_name,
        # )

        await uow.commit()


# @bus.listen(events.RecorderFailure)
async def recorder_failure(uow: SqlUnitOfWork, event: events.RecorderFailure):
    async with uow:
        job = await uow.jobs.get(event.id)
        if job is None:
            raise ValueError(f"Recorder failure references job that does not exist: {event.id}")

        job.state = JobState.FAILED

        uow.add_message(events.JobRecordingFailed(job=job, reason=event.reason))
        await uow.commit()


# @bus.listen(events.UploaderSuccess)
async def uploader_success(uow: SqlUnitOfWork, event: events.UploaderSuccess):
    async with uow:
        job = await uow.jobs.get(event.id)
        if job is None:
            raise ValueError(f"Upload success references job that does not exist: {event.id}")

        job.state = JobState.SUCCESS

        uow.add_message(events.JobUploadSuccess(job))
        await uow.commit()


# @bus.listen(events.UploaderFailure)
async def uploader_failure(uow: SqlUnitOfWork, event: events.UploaderFailure):
    async with uow:
        job = await uow.jobs.get(event.id)
        if job is None:
            raise ValueError(f"Upload failure references job that does not exist: {event.id}")

        job.state = JobState.FAILED

        uow.add_message(events.JobUploadFailed(job, reason=event.reason))
        await uow.commit()
