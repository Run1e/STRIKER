import asyncio
import logging
import pickle
import re
from functools import partial

import disnake
from disnake.ext import commands, tasks
from rapidfuzz import fuzz, process
from tabulate import tabulate

from bot.sharecode import is_valid_sharecode
from domain.demo_events import Player
from domain.domain import Job, User
from domain.enums import JobState
from messages import commands as cmds
from messages import dto
from messages.bus import MessageBus
from services import services
from services.uow import SqlUnitOfWork
from shared.utils import TimedDict

from . import config
from .errors import SponsorRequired
from .ui import ConfigView, PlayerView, RoundView

log = logging.getLogger(__name__)


def patched_init(original):
    def patched(self, *, data, state):
        self._payload = data
        return original(self, data=data, state=state)

    return patched


# monkey patch appcmdinter's init so it stores its payload data
# so we can store it ourselves later
disnake.ApplicationCommandInteraction.__init__ = patched_init(
    disnake.ApplicationCommandInteraction.__init__
)


def make_inter(inter_payload: bytes, bot: commands.InteractionBot) -> disnake.AppCommandInteraction:
    return disnake.ApplicationCommandInteraction(
        data=pickle.loads(inter_payload), state=bot._connection
    )


class EmbedBuilder:
    def __init__(self, bot) -> None:
        self.bot = bot

    def build(self, title, color=disnake.Color.orange(), job_id=None):
        e = disnake.Embed(color=color)
        e.set_author(name=title, icon_url=self.bot.user.display_avatar)
        if job_id is not None:
            e.set_footer(text=f"ID: {job_id}")
        return e

    def waiting(self, job_id):
        return self.build("Processing demo", disnake.Color.orange(), job_id)

    def selecting(self, job_id):
        return self.build("Select what you want to record", disnake.Color.blurple(), job_id)

    def recording(self, job_id):
        return self.build("Recording queued!", disnake.Color.orange(), job_id)

    def success(self, job_id):
        return self.build("Job completed, enjoy!", disnake.Color.green(), job_id)

    def failed(self, job_id):
        return self.build("Oops!", disnake.Color.red(), job_id)

    def aborted(self, job_id):
        return self.build("Job aborted", disnake.Color.red(), job_id)


def not_maintenance():
    async def checker(inter: disnake.AppCmdInter):
        if not inter.bot.maintenance:
            return True

        raise commands.CheckFailure("Bot is under maintenance! Check back in a bit!")

    return commands.check(checker)


async def job_limit_checker(inter: disnake.AppCmdInter, limit: int):
    job_count = await services.user_recording_count(uow=SqlUnitOfWork(), user_id=inter.author.id)

    if job_count < limit:
        return True

    job_word = "job" if limit == 1 else "jobs"

    raise commands.CheckFailure(
        f"You can only have {limit} {job_word} queued at a time. "
        "Please wait for one of your previous jobs to complete before starting a new one."
    )


job_limit = lambda limit: commands.check(partial(job_limit_checker, limit=limit))


async def get_tier(bot: commands.InteractionBot, user_id):
    guild = bot.get_guild(config.STRIKER_GUILD_ID)
    if guild is None:
        return 0

    try:
        member = await guild.fetch_member(user_id)
    except disnake.HTTPException:
        return 0

    for level, role_ids in reversed(config.PATREON_TIERS.items()):
        if any(role.id in role_ids for role in member.roles):
            return level

    return 0


async def tier_checker(inter: disnake.AppCmdInter, required_tier: int):
    actual_level = await get_tier(inter.bot, inter.author.id)

    if actual_level == 0:
        raise SponsorRequired(
            f"A [Patreon subscription]({config.PATREON_LINK}) is required to run this command.\n\n"
            "If you're already a Patron, "
            "make sure your Discord account is linked to your Patreon account "
            f"and that you've joined the [STRIKER Community Discord]({config.DISCORD_INVITE_URL})."
        )

    if actual_level < required_tier:
        raise SponsorRequired(f"This command requires a Tier {required_tier} Patreon subscription.")

    return True


tier = lambda tier: commands.check(partial(tier_checker, required_tier=tier))

job_perms = dict(send_messages=True, read_messages=True, embed_links=True, attach_files=True)


class RecorderCog(commands.Cog):
    def __init__(self, bot):
        self.bot: commands.InteractionBot = bot
        self.bus: MessageBus = bot.bus
        self.job_tasks = dict()  # Job.id -> (task, cancellable)

        self.embed = EmbedBuilder(bot)

        # holds values for 10 seconds between get and sets
        self._demo_cache = TimedDict(10.0)  # user.id: List[Demo]
        self._autocomplete_mapping = dict()  # desc_desc: demo.id
        self._autocomplete_user_mapping = dict()  # user.id: demo_desc

        self.bus.add_event_listener(dto.JobSelectable, self.start_select)
        self.bus.add_event_listener(dto.JobFailed, self.job_failed)
        self.bus.add_event_listener(dto.JobDemoProcessing, self.job_processing)

    @commands.slash_command(name="help", description="How to use the bot!", dm_permission=False)
    @commands.bot_has_permissions(embed_links=True)
    async def _help(self, inter: disnake.AppCmdInter):
        await self.bot.wait_until_ready()
        await self._send_help_embed(inter)

    @commands.Cog.listener()
    async def on_button_click(self, inter: disnake.MessageInteraction):
        custom_id = inter.component.custom_id
        if custom_id == "howtouse":
            await self._send_help_embed(inter)
        elif custom_id == "donatebutton":
            await self._send_donate(inter)

    async def _send_help_embed(self, inter: disnake.Interaction):
        e = self.embed.build("How to use the bot!")

        e.description = (
            "This bot can record and upload CS:GO clips from matchmaking games straight to Discord. "
            "To do so you will need to give the bot a sharecode from one of your matchmaking matches.\n\n"
            "The below image shows how to find and copy a matchmaking sharecode from inside CS:GO.\n\n"
            "To record a highlight, run the `/record` command and paste the sharecode you copied.\n\n"
            "To record another highlight from the same match, use `/demos`.\n\n"
            "Have fun!"
        )

        e.set_image(url=config.SHARECODE_IMG_URL)

        buttons = []

        buttons.append(
            disnake.ui.Button(
                style=disnake.ButtonStyle.url,
                label="Invite the bot to another server",
                emoji="🎉",
                url=self.bot.craft_invite_link(),
            )
        )

        await inter.send(embed=e, components=disnake.ui.ActionRow(*buttons), ephemeral=True)

    async def _send_donate(self, inter: disnake.Interaction):
        e = self.embed.build("Donate to support the project!")

        e.description = (
            "Thanks for your interest in supporting the project!\n\n"
            "Below are all the options for donating."
        )

        buttons = []

        if config.DONATE_URL is not None:
            buttons.append(
                disnake.ui.Button(
                    style=disnake.ButtonStyle.url,
                    label="Support through Ko-fi",
                    url=config.DONATE_URL,
                )
            )

        if config.TRADELINK_URL is not None:
            buttons.append(
                disnake.ui.Button(
                    style=disnake.ButtonStyle.url,
                    label="Send me some skins",
                    url=config.TRADELINK_URL,
                )
            )

        await inter.send(embed=e, components=disnake.ui.ActionRow(*buttons), ephemeral=True)

    @commands.slash_command(
        name="config",
        description="Tweak the recording settings",
        dm_permission=False,
    )
    @tier(2)
    async def _config(self, inter: disnake.AppCmdInter):
        user: User = await services.get_user(uow=SqlUnitOfWork(), user_id=inter.author.id)

        view = ConfigView(
            inter=inter,
            user=user,
            store_callback=self._store_config,
            abort_callback=self._abort_config,
            timeout=180.0,
        )

        await inter.send(embed=view.embed(), view=view, ephemeral=True)

    async def _store_config(self, inter: disnake.MessageInteraction, user: User):
        await services.store_user(uow=SqlUnitOfWork(), user=user)

        e = self.embed.build("STRIKER")
        e.description = "Configuration saved."

        await inter.response.edit_message(view=None, embed=e)

    async def _abort_config(self, inter: disnake.MessageInteraction):
        e = self.embed.build("STRIKER")
        e.description = "Configurator aborted."

        await inter.response.edit_message(view=None, embed=e)

    @commands.slash_command(
        name="maintenance",
        description="Set bot in maintenance mode",
        dm_permission=False,
        guild_ids=[config.STRIKER_GUILD_ID],
    )
    @commands.is_owner()
    async def maintenance(self, inter: disnake.AppCmdInter, enable: bool):
        await self.bot.wait_until_ready()

        self.bot.maintenance = enable
        await inter.send(
            "Bot now in maintenance mode!" if enable else "Bot now accepting new commands!"
        )

        if enable:
            await self.bot.change_presence(activity=disnake.Game(name="🛠 maintenance"))
        else:
            await self.bot.normal_presence()

    @commands.slash_command(description="Record again from a previous demo", dm_permission=False)
    @commands.bot_has_permissions(**job_perms)
    @not_maintenance()
    @job_limit(config.JOB_LIMIT)
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

        await services.create_job(
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

    @commands.slash_command(description="Record a CS:GO highlight", dm_permission=False)
    @commands.bot_has_permissions(**job_perms)
    @not_maintenance()
    @job_limit(config.JOB_LIMIT)
    async def record(self, inter: disnake.AppCmdInter, sharecode: str):
        await self.bot.wait_until_ready()

        sharecode = re.sub(
            r"^steam://rungame/730/\d*/\+csgo_download_match(%20| )", "", sharecode.strip()
        )

        if not is_valid_sharecode(sharecode):
            raise commands.UserInputError("Sorry, that's not a valid sharecode!")

        await inter.response.defer(ephemeral=True)

        await self.bus.dispatch(
            cmds.CreateJob(
                guild_id=inter.guild.id,
                channel_id=inter.channel.id,
                user_id=inter.user.id,
                inter_payload=pickle.dumps(inter._payload),
                sharecode=sharecode,
            )
        )

    @commands.slash_command(name="about", description="About the bot", dm_permission=False)
    async def about(self, inter: disnake.AppCmdInter):
        e = self.embed.build("STRIKER")

        e.add_field(
            name="Developer",
            value="runie#0001",
        )

        e.add_field(
            name="Shard count",
            value=self.bot.shard_count,
        )

        latencies = ", ".join(str(f"{t[1]:.3f}") for t in self.bot.latencies)
        e.add_field(name="Shard latencies", value=f"`{latencies}`")

        e.add_field(
            name="Guilds",
            value=f"{len(self.bot.guilds):,d}",
        )

        e.add_field(name="Channels", value=f"{sum(len(g.channels) for g in self.bot.guilds):,d}")

        e.add_field(
            name="Members",
            value=f"{sum(g.member_count for g in self.bot.guilds):,d}",
        )

        buttons = []

        buttons.append(
            disnake.ui.Button(
                style=disnake.ButtonStyle.url,
                label="Invite the bot",
                emoji="🎉",
                url=self.bot.craft_invite_link(),
            )
        )

        buttons.append(
            disnake.ui.Button(
                style=disnake.ButtonStyle.url,
                label="Discord",
                emoji=":discord:1099362254731882597",
                url=config.DISCORD_INVITE_URL,
            )
        )

        buttons.append(
            disnake.ui.Button(
                style=disnake.ButtonStyle.url,
                label="GitHub",
                emoji=":github:1099362911077544007",
                url=config.GITHUB_URL,
            )
        )

        buttons.append(
            disnake.ui.Button(
                style=disnake.ButtonStyle.secondary,
                label="Donate",
                emoji="\N{Hot Beverage}",
                custom_id="donatebutton",
            )
        )

        await inter.send(embed=e, components=disnake.ui.ActionRow(*buttons))

    # DTOs

    async def job_processing(self, event: dto.DTO):
        inter = make_inter(event.job_inter, self.bot)
        embed = self.embed.waiting(event.job_id)

        embed.description = {
            dto.JobDemoProcessing: "Processing demo...",
        }.get(type(event))

        original_message = await inter.original_response()
        await original_message.edit(embed=embed, content=None, components=None)

    async def job_failed(self, event: dto.JobFailed):
        inter = make_inter(event.job_inter, self.bot)

        embed = self.embed.failed(event.job_id)
        embed.description = event.reason

        original_message = await inter.original_response()
        await original_message.edit(embed=embed, content=None, components=None)

    async def start_select(self, event: dto.JobSelectable):
        inter = make_inter(event.job_inter, self.bot)

        # also clear this users demo cache
        if inter.author.id in self._demo_cache:
            del self._demo_cache[inter.author.id]

        await self.select_player(event, inter)

    async def select_player(self, event: dto.JobSelectable, inter: disnake.AppCmdInter):
        view = PlayerView(
            demo_events=event.demo_events,
            player_callback=partial(self.select_round, event),
            abort_callback=partial(self.abort_job, event),
            timeout_callback=partial(self.view_timeout, event),
            timeout=300.0,
        )

        embed = self.embed.selecting(event.job_id)
        embed.description = "Select a player you want to record a highlight from below."

        data = (
            ("Map", event.demo_events.map),
            ("Score", event.demo_events.score_str),
            ("Date", event.demo_events.time_str),
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

    async def abort_job(self, event, inter: disnake.Interaction):
        await self.bus.dispatch(cmds.AbortJob(event.job_id))

        embed = self.embed.aborted(event.job_id)
        embed.description = "Aborted."

        await inter.response.edit_message(content=None, embed=embed, view=None)

    async def view_timeout(self, event: dto.JobSelectable, inter: disnake.Interaction):
        await self.bus.dispatch(cmds.AbortJob(event.job_id))

        embed = self.embed.aborted(event.job_id)
        embed.description = "Command timed out."

        message = await inter.original_message()
        await message.edit(content=None, embed=embed, view=None)

    async def select_round(
        self, event: dto.JobSelectable, inter: disnake.Interaction, player: Player
    ):
        view = RoundView(
            demo_events=event.demo_events,
            round_callback=partial(self.record_highlight, event, player),
            reselect_callback=partial(self.select_player, event),
            abort_callback=partial(self.abort_job, event),
            timeout_callback=partial(self.view_timeout, event),
            embed_factory=partial(self.embed.selecting, job_id=event.job_id),
            player=player,
            timeout=300.0,
        )

        embed = await view.set_half(True)

        await inter.response.edit_message(content=None, embed=embed, view=view)

    async def record_highlight(
        self,
        event: dto.JobSelectable,
        player: Player,
        inter: disnake.AppCmdInter,
        round_id: int,
    ):
        await inter.response.defer()

        try:
            await job_limit_checker(inter=inter, limit=config.JOB_LIMIT)
        except commands.CheckFailure as exc:
            self.bot.dispatch("slash_command_error", inter, exc)
            await self.bus.dispatch(cmds.AbortJob(event.job_id))
            return

        tier = await get_tier(self.bot, inter.author.id)

        await self.bus.dispatch(
            cmds.Record(job_id=event.job_id, player_xuid=player.xuid, round_id=round_id, tier=tier)
        )

    # @bus.mark(events.JobUploadSuccess)
    # async def job_upload_success(self, event: events.JobUploadSuccess):
    #     job: Job = event.job
    #     inter = job.make_inter(self.bot)

    #     try:
    #         message = await inter.original_message()
    #     except disnake.HTTPException:
    #         return

    #     embed = job.embed(self.bot)
    #     embed.description = (
    #         "If you want to record another highlight from a previously used demo, "
    #         "use the `/demos` command and select the demo from the list."
    #     )

    #     try:
    #         await message.edit(content=None, embed=embed, view=None)
    #     except:
    #         pass


def setup(bot: commands.InteractionBot):
    bot.add_cog(RecorderCog(bot))
