import asyncio
import logging
import pickle
import re
from functools import partial

import disnake
from rapidfuzz import fuzz, process
from disnake.ext import commands
from domain import events
from domain.domain import Job, JobState, Player
from services import bus, services
from services.uow import SqlUnitOfWork
from shared.utils import TimedDict, timer
from tabulate import tabulate

from bot.sharecode import is_valid_sharecode

from . import config
from .ui import PlayerView, RoundView

log = logging.getLogger(__name__)


def patched_init(original):
    def patched(self, *, data, state):
        self._payload = data
        return original(self, data=data, state=state)

    return patched


disnake.ApplicationCommandInteraction.__init__ = patched_init(
    disnake.ApplicationCommandInteraction.__init__
)


class RecorderCog(commands.Cog):
    def __init__(self, bot):
        self.bot: commands.InteractionBot = bot
        self.job_tasks = dict()  # Job.id -> (task, cancellable)

        # holds values for 10 seconds between get and sets
        self._demo_cache = TimedDict(10.0)  # user.id: List[Demo]
        self._autocomplete_mapping = dict()  # desc_desc: demo.id
        self._autocomplete_user_mapping = dict()  # user.id: demo_desc

        bus.register_instance(self)

    @commands.slash_command(description='Record again from a previous demo')
    async def demos(self, inter: disnake.AppCmdInter, search: str):
        aum = self._autocomplete_user_mapping[inter.author.id]
        fuzzed = process.extract(
            query=search,
            choices=aum,
            scorer=fuzz.ratio,
            processor=None,
            limit=1,
        )

        if fuzzed is None:
            raise commands.CommandError('Demo not found, please try again.')

        demo_id = self._autocomplete_mapping.get(fuzzed[0][0], None)

        await inter.response.defer(ephemeral=True)

        await services.new_job(
            uow=SqlUnitOfWork(),
            guild_id=inter.guild.id,
            channel_id=inter.channel.id,
            user_id=inter.user.id,
            inter_payload=pickle.dumps(inter._payload),
            demo_id=demo_id,
        )

    @demos.autocomplete('search')
    async def demos_autocomplete(self, inter: disnake.AppCmdInter, search: str):
        demos = self._demo_cache.get(inter.author.id, None)

        if demos is None:
            aum = []
            self._autocomplete_user_mapping[inter.author.id] = aum
            demos = await services.get_user_demos(
                uow=SqlUnitOfWork(), user_id=inter.author.id
            )
            self._demo_cache[inter.author.id] = demos
            for demo in demos:
                fmt = demo.format()
                self._autocomplete_mapping[fmt] = demo.id
                aum.append(fmt)

        else:
            aum = self._autocomplete_user_mapping[inter.author.id]

        if search:
            fuzzed = process.extract(
                query=search,
                choices=aum,
                scorer=fuzz.ratio,
                processor=None,
                limit=8,
            )

            aum = [v[0] for v in fuzzed]

        # TODO: fix this it ain't right
        # this gets all the autocompleted demo names
        return aum

    @commands.slash_command(description='Record a CS:GO highlight')
    async def record(self, inter: disnake.AppCmdInter, sharecode: str):
        sharecode = re.sub(
            r'^steam://rungame/730/\d*/\+csgo_download_match%20', '', sharecode.strip()
        )

        if not is_valid_sharecode(sharecode):
            raise commands.UserInputError("Sorry, that's not a valid sharecode!")

        await inter.response.defer(ephemeral=True)

        await services.new_job(
            uow=SqlUnitOfWork(),
            guild_id=inter.guild.id,
            channel_id=inter.channel.id,
            user_id=inter.user.id,
            inter_payload=pickle.dumps(inter._payload),
            sharecode=sharecode,
        )

    @bus.mark(events.MatchInfoEnqueued)
    @bus.mark(events.MatchInfoProcessing)
    @bus.mark(events.DemoParseEnqueued)
    @bus.mark(events.DemoParseProcessing)
    async def demo_queue_event(self, event: events.Event):
        jobs = await services.get_jobs_waiting_for_demo(
            uow=SqlUnitOfWork(), demo_id=event.id
        )

        for job in jobs:
            await self.job_event(job, event)

    @bus.mark(events.RecorderEnqueued)
    @bus.mark(events.RecorderProcessing)
    async def job_queue_event(self, event: events.Event):
        job = await services.get_job(uow=SqlUnitOfWork(), job_id=event.id)
        await self.job_event(job, event)

    async def job_event(self, job: Job, event: events.JobEvent):
        # we only care about these enqueued/processing events if the
        # job state is *actually* DEMO or RECORD.
        # if they're not, more important stuff is likely happening
        if job.state not in (JobState.DEMO, JobState.RECORD):
            log.warn('Ignoring event because of state %s: %s', job.state, event)
            return

        inter = job.get_inter(self.bot)
        message = await inter.original_message()

        embed = job.embed(self.bot)

        coro = {
            events.MatchInfoEnqueued: self.event_enqueued,
            events.MatchInfoProcessing: self.event_processing,
            events.DemoParseEnqueued: self.event_enqueued,
            events.DemoParseProcessing: self.event_processing,
            events.RecorderEnqueued: self.event_enqueued,
            events.RecorderProcessing: self.event_processing,
        }.get(event.__class__, None)

        # get current enqueued/processing task
        # if it's cancellable, cancel it before running this task
        current_task: asyncio.Task
        current_task_tuple = self.job_tasks.get(job.id, None)
        if current_task_tuple is not None:
            current_task, current_event = self.job_tasks[job.id]
            is_cancellable = isinstance(current_event, events.CancellableEvent)
            current_task_done = current_task.done()

            if not current_task_done and is_cancellable:
                log.warn('%s cancelled by %s for job %s', current_event, event, job.id)
                current_task.cancel()

        self.job_tasks[job.id] = (
            asyncio.create_task(coro(message, embed, event)),
            event,
        )

    async def event_enqueued(
        self,
        message: disnake.InteractionMessage,
        embed: disnake.Embed,
        event: events.CancellableEvent,
    ):
        # await asyncio.sleep(2.0)

        infront = event.infront

        description = {
            events.MatchInfoEnqueued: f'#{infront} in queue for match info',
            events.DemoParseEnqueued: f'#{infront} in queue for demo parser',
            events.RecorderEnqueued: f'#{infront} in queue for recording',
        }.get(event.__class__)

        embed.description = f'{config.SPINNER} {description}'
        await message.edit(content=None, embed=embed, view=None)

    async def event_processing(
        self,
        message: disnake.InteractionMessage,
        embed: disnake.Embed,
        event: events.CancellableEvent,
    ):
        description = {
            events.MatchInfoProcessing: 'Asking Steam Coordinator for match info',
            events.DemoParseProcessing: 'Downloading and parsing demo',
            events.RecorderProcessing: 'Recording your highlight right now!',
        }.get(event.__class__)

        assert description is not None

        embed.description = f'{config.SPINNER} {description}'
        await message.edit(content=None, embed=embed, view=None)

    @bus.mark(events.JobMatchInfoFailed)
    @bus.mark(events.JobDemoParseFailed)
    @bus.mark(events.JobRecordingFailed)
    @bus.mark(events.JobRecordingUploadFailed)
    async def job_failed(self, event: events.Event):
        job = event.job
        inter: disnake.AppCmdInter = job.get_inter(self.bot)
        embed = job.embed(self.bot)

        message = await inter.original_message()

        embed.description = event.reason
        await message.edit(content=None, embed=embed, view=None)

    @bus.mark(events.JobReadyForSelect)
    async def start_select(self, event: events.JobReadyForSelect):
        job = event.job
        inter = job.get_inter(self.bot)

        # ensure demo has been parsed
        job.demo.parse()

        # also clear this users demo cache
        if inter.author.id in self._demo_cache:
            del self._demo_cache[inter.author.id]

        await self.select_player(job, inter)

    async def select_player(self, job: Job, inter: disnake.Interaction):
        view = PlayerView(
            job=job,
            player_callback=partial(self.select_round, job),
            abort_callback=partial(self.abort_job, job),
            timeout_callback=partial(self.view_timeout, job),
            timeout=300.0,
        )

        embed = job.embed(self.bot)
        embed.description = 'Select a player you want to record a highlight from below.'

        data = (
            ('Map', job.demo.map),
            ('Score', job.demo.score_string),
            ('Date', job.demo.matchtime_string),
        )
        data_str = tabulate(
            tabular_data=data,
            colalign=('left', 'left'),
            tablefmt='plain',
        )

        embed.description += f'\n```\n{data_str}\n```'

        edit_kwargs = dict(content=None, embed=embed, view=view)

        # depends on whether we came here from an appcmdinter
        # or from a button interaction
        if isinstance(inter, disnake.MessageInteraction):
            await inter.response.edit_message(**edit_kwargs)
        elif isinstance(inter, disnake.AppCmdInter):
            message = await inter.original_message()
            await message.edit(**edit_kwargs)

    async def abort_job(self, job: Job, inter: disnake.Interaction, reason=None):
        await services.abort_job(uow=SqlUnitOfWork(), job=job)

        embed = job.embed(self.bot)
        embed.description = reason or 'Aborted.'

        await inter.response.edit_message(content=None, embed=embed, view=None)

    async def view_timeout(self, job: Job):
        await services.abort_job(uow=SqlUnitOfWork(), job=job)

        embed = job.embed(self.bot)
        embed.description = 'Command timed out.'

        inter = job.get_inter(self.bot)
        message = await inter.original_message()
        await message.edit(content=None, embed=embed, view=None)

    async def select_round(self, job: Job, inter: disnake.AppCmdInter, player: Player):
        view = RoundView(
            round_callback=partial(self.record_highlight, job, player),
            reselect_callback=partial(self.select_player, job),
            abort_callback=partial(self.abort_job, job),
            timeout_callback=partial(self.view_timeout, job),
            job=job,
            embed_factory=partial(job.embed, self.bot),
            player=player,
            timeout=300.0,
        )

        embed = await view.set_half(True)

        await inter.response.edit_message(content=None, embed=embed, view=view)

    async def record_highlight(
        self,
        job: Job,
        player: Player,
        inter: disnake.AppCmdInter,
        round_id: int,
    ):
        await inter.response.defer()

        await services.record(
            uow=SqlUnitOfWork(), job=job, player=player, round_id=round_id
        )

    @bus.mark(events.JobRecordingComplete)
    async def job_recording_complete(self, event: events.JobRecordingComplete):
        job: Job = event.job
        inter = job.get_inter(self.bot)

        buttons = []

        if config.GITHUB_URL is not None:
            buttons.append(
                disnake.ui.Button(
                    style=disnake.ButtonStyle.url,
                    label='Star the project on GitHub',
                    url=config.GITHUB_URL,
                )
            )

        end = timer(f'Upload for job {job.id}')
        log.info('Starting upload for job %s', job.id)
        await inter.channel.send(
            content=inter.author.mention,
            file=disnake.File(f'{config.VIDEO_DIR}/{job.id}.mp4'),
            components=disnake.ui.ActionRow(*buttons),
        )

        log.info(end())

        try:
            message = await inter.original_message()
        except disnake.HTTPException:
            return

        embed = job.embed(self.bot)
        embed.author.name = 'Upload complete!'
        embed.description = 'Enjoy!'

        try:
            await message.edit(content=None, embed=embed, view=None)
        except:
            pass


def setup(bot: commands.InteractionBot):
    bot.add_cog(RecorderCog(bot))
