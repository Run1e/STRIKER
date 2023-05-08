import asyncio
import logging
from json import loads
from typing import List
from uuid import UUID, uuid4

from bot import sequencer
from domain.demo_events import DemoEvents
from domain.domain import Demo, Job, User, build_demo_url, calculate_bitrate
from domain.enums import DemoGame, DemoOrigin, DemoState, JobState
from messages import commands, dto, events
from messages.deco import handler, listener
from services import views
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


@handler(commands.AbortJob)
async def abort_job(command: commands.AbortJob, uow: SqlUnitOfWork):
    async with uow:
        job = await uow.jobs.get(command.job_id)
        if job is None:
            return

        job.aborted()
        await uow.commit()


@listener(events.JobWaiting)
async def job_waiting(event: events.JobWaiting, uow: SqlUnitOfWork):
    async with uow:
        inter = await uow.jobs.get_inter(event.job_id)
        uow.add_message(dto.JobDemoProcessing(event.job_id, inter))


@listener(events.JobSelecting)
async def job_selecting(event: events.JobSelecting, uow: SqlUnitOfWork):
    async with uow:
        job = await uow.jobs.get(event.job_id)
        if job is None:
            return

        demo_events = DemoEvents.from_demo(job.demo)
        demo_events.parse()

        uow.add_message(dto.JobSelectable(job.id, job.inter_payload, demo_events))


@listener(events.DemoReady)
async def demo_ready(event: events.DemoReady, uow: SqlUnitOfWork):
    async with uow:
        jobs = await uow.jobs.waiting_for_demo(demo_id=event.demo_id)

        for job in jobs:
            job.selecting()

        await uow.commit()


@listener(events.DemoFailure)
async def demo_failure(event: events.DemoFailure, uow: SqlUnitOfWork):
    async with uow:
        jobs = await uow.jobs.waiting_for_demo(demo_id=event.demo_id)

        for job in jobs:
            job.failed(event.reason)

        await uow.commit()


@listener(events.JobFailed)
async def job_failure(event: events.JobFailed, uow: SqlUnitOfWork):
    inter_payload = await views.get_job_inter(event.job_id, uow)
    uow.add_message(dto.JobFailed(event.job_id, inter_payload, event.reason))


@listener(events.DemoParsed)
async def demoparse_success(event: events.DemoParsed, uow: SqlUnitOfWork, publish):
    async with uow:
        demo = await uow.demos.from_identifier(DemoOrigin[event.origin], event.identifier)
        if demo is None:
            return

        if event.version != DEMOPARSE_VERSION:
            await handle_demo_step(demo=demo, publish=publish)
        else:
            demo.set_demo_data(loads(event.data), event.version)

            jobs = await uow.jobs.waiting_for_demo(demo.id)
            for job in jobs:
                job.selecting()

        await uow.commit()


@listener(events.DemoParseFailure)
async def demoparse_failure(event: events.DemoParseFailure, uow: SqlUnitOfWork):
    async with uow:
        demo: Demo = await uow.demos.from_identifier(DemoOrigin[event.origin], event.identifier)
        if demo is None:
            return

        demo.failed(event.reason)
        await uow.commit()


@listener(events.DemoParseDL)
async def demoparse_died(event: events.DemoParseDL, uow: SqlUnitOfWork):
    async with uow:
        command: commands.RequestDemoParse = event.command
        reason = event.reason

        prose_version = dict(
            rejected="The demo parse service was unable to process your request.",
            expired="The demo parse service request timed out.",
        ).get(reason, "The demo parse service request failed.")

        uow.add_message(
            events.DemoParseFailure(command.origin, command.identifier, reason=prose_version)
        )


@handler(commands.Record)
async def record(command: commands.Record, uow: SqlUnitOfWork, publish):
    async with uow:
        job = await uow.jobs.get(command.job_id)
        job.set_recording()

        demo = job.demo

        upload_token = job.generate_upload_token()

        demo_events = DemoEvents.from_demo(demo)
        demo_events.parse()

        player = demo_events.get_player_by_xuid(command.player_xuid)

        all_kills = demo_events.get_player_kills(player)
        kills = all_kills[command.round_id]

        start_tick, end_tick, skips, total_seconds = sequencer.single_highlight(
            demo_events.tickrate, kills
        )

        video_bitrate = calculate_bitrate(total_seconds)

        data = dict(
            job_id=str(job.id),
            demo_origin=demo.origin.name,
            demo_identifier=demo.identifier,
            demo_url=build_demo_url(demo.origin.name, demo.identifier),
            upload_token=upload_token,
            player_xuid=player.xuid,
            tickrate=demo_events.tickrate,
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

        # if command.tier > 0:
        #     user = await uow.users.get_user(job.user_id)
        #     if user is not None:
        #         data.update(**user.update_recorder_settings())

        cmd = commands.RequestRecording(**data)
        await publish(cmd)

        await uow.commit()


@listener(events.RecorderSuccess)
async def recorder_success(event: events.RecorderSuccess, uow: SqlUnitOfWork):
    async with uow:
        job = await uow.jobs.get(event.job_id)
        if job is None:
            return

        job.uploading()
        uow.add_message(dto.JobUploading(job.id, job.inter_payload))
        await uow.commit()


@listener(events.RecorderFailure)
async def recorder_failure(event: events.RecorderFailure, uow: SqlUnitOfWork):
    async with uow:
        job = await uow.jobs.get(event.job_id)
        if job is None:
            return

        job.failed(event.reason)
        await uow.commit()


@listener(events.RecorderDL)
async def recorder_died(event: events.RecorderDL, uow: SqlUnitOfWork):
    async with uow:
        command: commands.RequestRecording = event.command
        job = await uow.jobs.get(command.job_id)
        if job is None:
            return

        job.failed(event.reason)
        await uow.commit()


@listener(events.RecordingQueued)
@listener(events.RecordingStarted)
async def recording_progression(event, uow: SqlUnitOfWork):
    async with uow:
        job_id = UUID(event.job_id)
        inter_payload = await uow.jobs.get_inter(job_id)
        infront = event.infront if isinstance(event, events.RecordingQueued) else None
        uow.add_message(dto.JobRecording(job_id, inter_payload, infront))


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


# # @bus.listen(events.UploaderSuccess)
# async def uploader_success(uow: SqlUnitOfWork, event: events.UploaderSuccess):
#     async with uow:
#         job = await uow.jobs.get(event.id)
#         if job is None:
#             raise ValueError(f"Upload success references job that does not exist: {event.id}")

#         job.state = JobState.SUCCESS

#         uow.add_message(events.JobUploadSuccess(job))
#         await uow.commit()


# # @bus.listen(events.UploaderFailure)
# async def uploader_failure(uow: SqlUnitOfWork, event: events.UploaderFailure):
#     async with uow:
#         job = await uow.jobs.get(event.id)
#         if job is None:
#             raise ValueError(f"Upload failure references job that does not exist: {event.id}")

#         job.state = JobState.FAILED

#         uow.add_message(events.JobUploadFailed(job, reason=event.reason))
#         await uow.commit()
