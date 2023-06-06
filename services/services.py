import asyncio
import logging
from json import loads
from uuid import UUID

from adapters.faceit import HTTPException, NotFound
from domain import sequencer
from domain.domain import Demo, Job, UserSettings, calculate_bitrate
from domain.enums import DemoGame, DemoOrigin, DemoState, JobState, RecordingType
from domain.match import Match
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
    faceit_resolver,
):
    async with DEMO_LOCK:
        async with uow:
            new_demo = False

            if command.demo_id is not None:
                demo = await uow.demos.get(command.demo_id)
            else:
                if command.sharecode is not None:
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

                elif command.origin == "FACEIT":
                    origin = DemoOrigin.FACEIT
                    demo = await uow.demos.from_identifier(origin, command.identifier)

                    if demo is None:
                        try:
                            data = await faceit_resolver(command.identifier)
                        except NotFound:
                            raise ServiceError("Could not find that FACEIT match.")
                        except HTTPException:
                            raise ServiceError("FACEIT API did not respond in time.")

                        url = data["demo_url"][0]

                        new_demo = True
                        demo = Demo(
                            game=DemoGame.CSGO,
                            origin=origin,
                            state=DemoState.PROCESSING,
                            identifier=command.identifier,
                            download_url=url,
                        )

                        uow.demos.add(demo)

                elif command.origin == "VALVE":
                    # if no sharecode but origin is still valve, the identifier holds a demo url
                    origin = DemoOrigin.VALVE
                    demo = await uow.demos.from_identifier(origin, command.identifier)

                    if demo is None:
                        new_demo = True
                        demo = Demo(
                            game=DemoGame.CSGO,
                            origin=origin,
                            state=DemoState.PROCESSING,
                            identifier=command.identifier,
                            download_url=command.demo_url,
                        )

                        uow.demos.add(demo)

            if not new_demo and demo.state is DemoState.DELETED:
                raise ServiceError("Demo has been deleted.")

            job = Job(
                state=JobState.WAITING,
                guild_id=command.guild_id,
                channel_id=command.channel_id,
                started_at=utcnow(),
                user_id=command.user_id,
                inter_payload=command.inter_payload,
            )

            uow.jobs.add(job)

            # force id for job
            # not sure this is needed anymore?
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
        if inter is None:
            return

        uow.add_message(dto.JobWaiting(event.job_id, inter))


@listener(events.JobSelecting)
async def job_selecting(event: events.JobSelecting, uow: SqlUnitOfWork):
    async with uow:
        job = await uow.jobs.get(event.job_id)
        if job is None:
            return

        match = Match.from_demo(job.demo)
        match.parse()

        uow.add_message(dto.JobSelectable(job.id, job.inter_payload, match))


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
    inter_payload = await views.job_inter(event.job_id, uow)
    uow.add_message(dto.JobFailed(event.job_id, inter_payload, event.reason))


@listener(events.DemoParseSuccess)
async def demoparse_success(event: events.DemoParseSuccess, uow: SqlUnitOfWork, publish):
    async with uow:
        demo = await uow.demos.from_identifier(DemoOrigin[event.origin], event.identifier)
        if demo is None:
            return

        if event.version != DEMOPARSE_VERSION:
            await handle_demo_step(demo=demo, publish=publish)
        else:
            demo.set_demo_data(loads(event.data), event.version)
            demo.ready()

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


@handler(commands.GetPresignedUrlDTO)
async def get_presigned_url_dto(
    command: commands.GetPresignedUrlDTO, wait_for, publish, uow: SqlUnitOfWork
):
    async with uow:
        waiter = wait_for(
            events.PresignedUrlGenerated,
            check=lambda e: e.origin == command.origin and e.identifier == command.identifier,
            timeout=4.0,
        )

        await publish(commands.RequestPresignedUrl(command.origin, command.identifier, 60 * 5))
        result = await waiter

        if result is not None:
            uow.add_message(
                dto.PresignedUrlReceived(
                    origin=command.origin,
                    identifier=command.identifier,
                    presigned_url=result.presigned_url,
                )
            )


@handler(commands.Record)
async def record(command: commands.Record, uow: SqlUnitOfWork, publish, wait_for, video_upload_url):
    # a lot of the stuff in here is not orchestration
    # it should be majorly refactored
    async with uow:
        job = await uow.jobs.get(command.job_id)
        job.recording()

        job.recording_type = RecordingType.PLAYER_ROUND
        job.recording_data = dict(player_xuid=command.player_xuid, round_id=command.round_id)

        demo = job.demo

        match = Match.from_demo(demo)
        match.parse()

        # get all player kills
        player = match.get_player_by_xuid(command.player_xuid)

        half = match.halves[command.half]
        kills = half.get_player_kills_round(player, command.round_id)

        # get the kills info to make video title
        info = half.kills_info(command.round_id, kills)
        job.video_title = " ".join([info[0], player.name, info[1]])

        start_tick, end_tick, skips, total_seconds = sequencer.single_highlight(
            match.tickrate, kills
        )

        video_bitrate = calculate_bitrate(total_seconds)

        job_id = str(job.id)

        data = dict(
            job_id=job_id,
            demo_origin=demo.origin.name,
            demo_identifier=demo.identifier,
            upload_url=video_upload_url,
            player_xuid=command.player_xuid,
            tickrate=match.tickrate,
            start_tick=start_tick,
            end_tick=end_tick,
            skips=skips,
            fps=60,
            video_bitrate=video_bitrate,
            audio_bitrate=192,
            **UserSettings.toggleable_values,
            **UserSettings.text_values,
        )

        user = await uow.users.get_user(job.user_id)
        if user is not None:
            data.update(**user.unfilled(command.tier))

        task = wait_for(
            events.PresignedUrlGenerated,
            check=lambda m: m.origin == demo.origin.name and m.identifier == demo.identifier,
            timeout=4.0,
        )

        await publish(commands.RequestPresignedUrl(demo.origin.name, demo.identifier, 60 * 60))
        result: events.PresignedUrlGenerated | None = await task

        if result is None:
            uow.add_message(
                events.JobFailed(job.id, reason="Unable to get archive link from demo parser.")
            )
            return

        data["demo_url"] = result.presigned_url

        task = wait_for(
            events.RecordingProgression, check=lambda e: e.job_id == job_id, timeout=2.0
        )

        await publish(commands.RequestRecording(**data))

        # I think this is a sensible place to put the commit
        await uow.commit()

        progression: events.RecordingProgression | None = await task

        if progression is None:
            uow.add_message(events.RecordingProgression(job_id, None))


@listener(events.RecorderSuccess)
async def recorder_success(event: events.RecorderSuccess):
    return  # nice to know I guess but not much to do here


@listener(events.RecorderFailure)
async def recorder_failure(event: events.RecorderFailure, uow: SqlUnitOfWork):
    async with uow:
        job = await uow.jobs.get(UUID(event.job_id))
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

        prose_version = dict(
            rejected="Gateway was unable to process the request.",
            expired="Gateway timed out.",
        ).get(event.reason, "The gateway request failed.")

        job.failed(prose_version)
        await uow.commit()


@listener(events.RecordingProgression)
async def recording_progression(event: events.RecordingProgression, uow: SqlUnitOfWork):
    async with uow:
        job_id = UUID(event.job_id)

        inter_payload = await uow.jobs.get_inter(job_id)
        if inter_payload is None:
            return

        uow.add_message(dto.JobRecording(job_id, inter_payload, event.infront))


@listener(events.UploaderSuccess)
async def uploader_success(event: events.UploaderSuccess, uow: SqlUnitOfWork):
    async with uow:
        job = await uow.jobs.get(UUID(event.job_id))
        if job is None:
            return

        job.success()
        uow.add_message(dto.JobSuccess(job.id, job.inter_payload))
        await uow.commit()


@listener(events.UploaderFailure)
async def upload_failure(event: events.UploaderFailure, uow: SqlUnitOfWork):
    async with uow:
        job = await uow.jobs.get(UUID(event.job_id))
        if job is None:
            return

        job.failed(event.reason)
        await uow.commit()


@handler(commands.RequestTokens)
async def request_tokens(command: commands.RequestTokens, publish, tokens):
    await publish(events.Tokens(list(tokens)))


@handler(commands.RequestUploadData)
async def validate_upload(command: commands.RequestUploadData, uow: SqlUnitOfWork, publish):
    async with uow:
        job = await uow.jobs.get(UUID(command.job_id))
        if job is None:
            return

        e = events.UploadData(
            job_id=command.job_id,
            video_title=job.video_title,
            channel_id=job.channel_id,
            user_id=job.user_id,
        )

        await publish(e)


@handler(commands.Restore)
async def restore(command: commands.Restore, uow: SqlUnitOfWork):
    # restores jobs and demos that were cut off during last restart
    async with uow:
        jobs = await uow.jobs.get_restart()
        for job in jobs:
            job.selecting()

        await uow.commit()


@handler(commands.UpdateUserSettings)
async def update_user_settings(command: commands.UpdateUserSettings, uow: SqlUnitOfWork):
    async with uow:
        user = await uow.users.get_user(command.user_id)
        if user is None:
            user = UserSettings(command.user_id)
            uow.users.add(user)

        user.update(**command.data)
        await uow.commit()
