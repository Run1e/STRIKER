import asyncio
import logging
import pickle
import re
from functools import partial

import disnake
from disnake.ext import commands
from rapidfuzz import fuzz, process
from tabulate import tabulate

from bot.sharecode import is_valid_sharecode
from domain import events
from domain.domain import Job, JobState, Player
from services import bus, services
from services.uow import SqlUnitOfWork
from shared.utils import TimedDict

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

    @commands.slash_command(name="help", description="How to use the bot!")
    @commands.bot_has_permissions(embed_links=True)
    async def _help(self, inter: disnake.AppCmdInter):
        await self.bot.wait_until_ready()
        await self._send_help_embed(inter)

    async def _send_help_embed(self, inter: disnake.Interaction):
        e = disnake.Embed(
            color=disnake.Color.orange(),
        )

        e.set_author(name="How to use the bot!", icon_url=self.bot.user.display_avatar)

        e.description = (
            "This bot can record and upload CS:GO clips from matchmaking games straight to Discord. "
            "To do so you will need to give the bot a sharecode from one of your matchmaking matches.\n\n"
            "The below image shows how to find and copy a matchmaking sharecode from inside CS:GO.\n\n"
            "To record a highlight, run the `/record` command and paste the sharecode you copied.\n\n"
            "To record another highlight from the same match, use `/demos`.\n\n"
            "Have fun!"
        )

        e.set_image(url=config.SHARECODE_IMG_URL)

        await inter.send(embed=e, ephemeral=True)

    @commands.slash_command(description="Record again from a previous demo")
    @commands.bot_has_permissions(embed_links=True, attach_files=True)
    async def demos(self, inter: disnake.AppCmdInter, search: str):
        await self.bot.wait_until_ready()

        aum = self._autocomplete_user_mapping[inter.author.id]
        fuzzed = process.extract(
            query=search,
            choices=aum,
            scorer=fuzz.ratio,
            processor=None,
            limit=1,
        )

        if fuzzed is None:
            raise commands.CommandError("Demo not found, please try again.")

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

    @demos.autocomplete("search")
    async def demos_autocomplete(self, inter: disnake.AppCmdInter, search: str):
        demos = self._demo_cache.get(inter.author.id, None)

        if demos is None:
            aum = []
            self._autocomplete_user_mapping[inter.author.id] = aum
            demos = await services.get_user_demos(uow=SqlUnitOfWork(), user_id=inter.author.id)
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

    @commands.Cog.listener()
    async def on_button_click(self, inter: disnake.MessageInteraction):
        if inter.component.custom_id == "howtouse":
            await self._send_help_embed(inter)

    @commands.slash_command(description="Record a CS:GO highlight")
    @commands.bot_has_permissions(embed_links=True, attach_files=True)
    async def record(self, inter: disnake.AppCmdInter, sharecode: str):
        await self.bot.wait_until_ready()

        sharecode = re.sub(
            r"^steam://rungame/730/\d*/\+csgo_download_match%20", "", sharecode.strip()
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

    @bus.mark(events.MatchInfoProgression)
    @bus.mark(events.DemoParseProgression)
    async def demo_progression(self, event: events.Event):
        jobs = await services.get_jobs_waiting_for_demo(uow=SqlUnitOfWork(), demo_id=event.id)
        for job in jobs:
            await self.job_event(job, event)

    @bus.mark(events.RecorderProgression)
    async def job_progression(self, event: events.Event):
        job = await services.get_job(uow=SqlUnitOfWork(), job_id=event.id)
        await self.job_event(job, event)

    async def job_event(self, job: Job, event: events.Event):
        # we only care about these enqueued/processing events if the
        # job state is *actually* RECORD.
        # if they're not, more important stuff is likely happening
        if job.state not in (JobState.DEMO, JobState.RECORD):
            log.warn("Ignoring event because of state %s: %s", job.state, event)
            return

        inter = job.make_inter(self.bot)
        message = await inter.original_message()

        embed = job.embed(self.bot)

        # get current enqueued/processing task
        # if it's cancellable, cancel it before running this task
        current_task: asyncio.Task
        current_task_tuple = self.job_tasks.get(job.id, None)
        if current_task_tuple is not None:
            current_task, current_event = self.job_tasks[job.id]
            current_task_done = current_task.done()

            if not current_task_done:
                log.warn("%s cancelled by %s for job %s", current_event, event, job.id)
                current_task.cancel()

        self.job_tasks[job.id] = (
            asyncio.create_task(self.event_progression(message, embed, event)),
            event,
        )

    async def event_progression(
        self,
        message: disnake.InteractionMessage,
        embed: disnake.Embed,
        event: events.Event,
    ):
        infront = event.infront

        desc = {
            events.MatchInfoProgression: "Fetching match information...",
            events.DemoParseProgression: "Downloading and parsing demo...",
            events.RecorderProgression: (
                "Recording now!",
                f"#{event.infront + 1} in the recording queue",
            )[int(infront > 0)],
        }.get(event.__class__)

        embed.description = f"{config.SPINNER} {desc}"
        await message.edit(content=None, embed=embed, view=None)

    @bus.mark(events.JobMatchInfoFailed)
    @bus.mark(events.JobDemoParseFailed)
    @bus.mark(events.JobRecordingFailed)
    @bus.mark(events.JobUploadFailed)
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
        inter = job.make_inter(self.bot)

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
        embed.description = "Select a player you want to record a highlight from below."

        data = (
            ("Map", job.demo.map),
            ("Score", job.demo.score_string),
            ("Date", job.demo.matchtime_string),
        )
        data_str = tabulate(
            tabular_data=data,
            colalign=("left", "left"),
            tablefmt="plain",
        )

        embed.description += f"\n```\n{data_str}\n```"

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
        embed.description = reason or "Aborted."

        await inter.response.edit_message(content=None, embed=embed, view=None)

    async def view_timeout(self, job: Job):
        await services.abort_job(uow=SqlUnitOfWork(), job=job)

        embed = job.embed(self.bot)
        embed.description = "Command timed out."

        inter = job.make_inter(self.bot)
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

        await services.record(uow=SqlUnitOfWork(), job=job, player=player, round_id=round_id)

    @bus.mark(events.JobUploadSuccess)
    async def job_upload_success(self, event: events.JobUploadSuccess):
        job: Job = event.job
        inter = job.make_inter(self.bot)

        try:
            message = await inter.original_message()
        except disnake.HTTPException:
            return

        embed = job.embed(self.bot)
        embed.author.name = "Upload complete!"
        embed.description = (
            "Enjoy!\n\n"
            "If you want to record another highlight from a previously used demo, "
            "use the `/demos` command and select the the demo from the list."
        )

        try:
            await message.edit(content=None, embed=embed, view=None)
        except:
            pass


def setup(bot: commands.InteractionBot):
    bot.add_cog(RecorderCog(bot))
