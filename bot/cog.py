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
from domain.match import Player
from messages import commands as cmds
from messages import dto
from messages.bus import MessageBus
from services import views
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

    def waiting(self, job_id: str = None, title: str = None):
        return self.build(title or "Preparing demo", disnake.Color.orange(), job_id)

    def selecting(self, job_id: str = None, title: str = None):
        return self.build(
            title or "Select what you want to record", disnake.Color.blurple(), job_id
        )

    def recording(self, job_id: str = None, title: str = None):
        return self.build(title or "Recording queued!", disnake.Color.orange(), job_id)

    def success(self, job_id: str = None, title: str = None):
        return self.build(title or "Job completed, enjoy!", disnake.Color.green(), job_id)

    def failed(self, job_id: str = None, title: str = None):
        return self.build(title or "Oops!", disnake.Color.red(), job_id)

    def aborted(self, job_id: str = None, title: str = None):
        return self.build(title or "Job aborted", disnake.Color.red(), job_id)


def not_maintenance():
    async def checker(inter: disnake.AppCmdInter):
        if not inter.bot.maintenance:
            return True

        raise commands.CheckFailure("Bot is under maintenance! Check back in a bit!")

    return commands.check(checker)


async def job_limit_checker(inter: disnake.AppCmdInter, limit: int):
    job_count = await views.user_recording_count(user_id=inter.author.id, uow=SqlUnitOfWork())

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

    if actual_level < required_tier:
        raise SponsorRequired("This command", tier=required_tier)

    return True


tier = lambda tier: commands.check(partial(tier_checker, required_tier=tier))

job_perms = dict(send_messages=True, read_messages=True, embed_links=True, attach_files=True)


class RecorderCog(commands.Cog):
    def __init__(self, bot):
        self.bot: commands.InteractionBot = bot
        self.bus: MessageBus = bot.bus

        self.embed = EmbedBuilder(bot)

        self._demo_view_cache = TimedDict(10.0)

        self.bus.add_event_listener(dto.JobSelectable, self.job_selectable)
        self.bus.add_event_listener(dto.JobFailed, self.job_failed)
        self.bus.add_event_listener(dto.JobWaiting, self.job_processing)
        self.bus.add_event_listener(dto.JobRecording, self.job_recording)
        self.bus.add_event_listener(dto.JobSuccess, self.job_success)

        self.bot._error_actionrow = self.make_actionrow(discord=True)
        self.bot._error_sponsor_actionrow = self.make_actionrow(patreon=True)

    async def cog_slash_command_check(self, inter: disnake.Interaction):
        if not self.bot.is_ready() or not self.bot.gather.is_set():
            raise commands.CheckFailure(
                "Bot is currently booting up. Please wait a bit then try again."
            )

        return True

    @commands.slash_command(
        description="Record a matchmaking or FACEIT highlight", dm_permission=False
    )
    @commands.bot_has_permissions(**job_perms)
    @not_maintenance()
    @job_limit(config.JOB_LIMIT)
    async def record(self, inter: disnake.AppCmdInter, sharecode_or_url: str):
        demo_dict = dict()
        sharecode_or_url = sharecode_or_url.strip()
        user_id = inter.author.id

        # https://stackoverflow.com/questions/11384589/what-is-the-correct-regex-for-matching-values-generated-by-uuid-uuid4-hex
        faceit_match = re.match(
            r"^https:\/\/www.faceit.com\/\w{2}\/csgo\/room\/(\d-[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12})(\/scoreboard)?$",
            sharecode_or_url,
        )

        replay_match = re.match(
            r"^http:\/\/replay\d{1,3}\.valve\.net\/730\/(\d*)_(\d*)\.dem\.bz2$",
            sharecode_or_url,
        )

        if faceit_match:
            tier = await get_tier(self.bot, user_id)
            if tier < 2:
                raise SponsorRequired("Recording FACEIT demos", tier=2)
            demo_dict = dict(origin="FACEIT", identifier=faceit_match.group(1))
        elif replay_match:
            demo_dict = dict(
                origin="VALVE",
                identifier=replay_match.group(1).strip("0"),
                demo_url=sharecode_or_url,
            )

        else:
            # last resort is sharecode
            sharecode = re.sub(
                r"^steam://rungame/730/\d*/\+csgo_download_match(%20| )",
                "",
                sharecode_or_url,
            )

            if not is_valid_sharecode(sharecode):
                raise commands.UserInputError(
                    "Sorry, that's not a valid sharecode or FACEIT url.\n\n"
                    "You can give the bot matchmaking sharecodes:\n"
                    "`steam://rungame/730/76561202255233023/+csgo_download_match%20CSGO-3VocL-obGr4-SjkBU-DjHhz-KWtrD`\n\n"
                    "Or FACEIT links:\n"
                    "`https://www.faceit.com/en/csgo/room/1-9fa1db69-5f1a-4ea3-a37c-3ab84fbd416a`"
                )

            demo_dict = dict(origin="VALVE", sharecode=sharecode)

        await inter.response.defer(ephemeral=True)

        await self.bus.dispatch(
            cmds.CreateJob(
                guild_id=inter.guild.id,
                channel_id=inter.channel.id,
                user_id=user_id,
                inter_payload=pickle.dumps(inter._payload),
                **demo_dict,
            )
        )

    @commands.slash_command(description="Record again from a previous demo", dm_permission=False)
    @commands.bot_has_permissions(**job_perms)
    @not_maintenance()
    @job_limit(config.JOB_LIMIT)
    async def demos(self, inter: disnake.AppCmdInter, search: str):
        not_found_exc = commands.CommandError(
            "Your search query did not match any demos available."
        )

        found = await self._search_demos(inter.author.id, search, limit=1)

        if not found:
            raise not_found_exc

        found_desc = found[0]
        found_demo_id = None
        for demo_id, demo_desc in self._demo_view_cache.get(inter.author.id).items():
            if demo_desc == found_desc:
                found_demo_id = demo_id

        if found_demo_id is None:
            raise not_found_exc

        await inter.response.defer(ephemeral=True)

        user_id = inter.author.id
        demo_origin = await views.get_demo_origin(found_demo_id, uow=SqlUnitOfWork())

        if demo_origin == "FACEIT":
            tier = await get_tier(self.bot, user_id)
            if tier < 2:
                raise SponsorRequired("Recording FACEIT demos", tier=2)

        await self.bus.dispatch(
            cmds.CreateJob(
                guild_id=inter.guild.id,
                channel_id=inter.channel.id,
                user_id=user_id,
                inter_payload=pickle.dumps(inter._payload),
                demo_id=found_demo_id,
            )
        )

    async def _search_demos(self, user_id: int, search: str, limit: int = 5):
        demos = self._demo_view_cache.get(user_id, None)

        if demos is None:
            demos = await views.get_user_demo_formats(user_id, SqlUnitOfWork())
            self._demo_view_cache[user_id] = demos

        choices = list(demos.values())

        if not choices:
            return ["No demos available! Use /record to add one!"]

        if not search:
            return choices[:limit]

        fuzzed = process.extract(
            query=search,
            choices=choices,
            scorer=fuzz.ratio,
            processor=None,
            limit=limit,
        )

        return list(t[0] for t in fuzzed)

    @demos.autocomplete("search")
    async def demos_autocomplete(self, inter: disnake.AppCmdInter, search: str):
        return await self._search_demos(inter.author.id, search, limit=5)

    # DTOs

    async def job_processing(self, event: dto.DTO):
        inter = make_inter(event.job_inter, self.bot)

        embed = self.embed.waiting(event.job_id)
        embed.description = f"{config.SPINNER} Please wait..."

        await inter.edit_original_response(embed=embed, content=None, components=None)

    async def job_failed(self, event: dto.JobFailed):
        inter = make_inter(event.job_inter, self.bot)

        embed = self.embed.failed(event.job_id)
        embed.description = event.reason

        await inter.edit_original_response(embed=embed, content=None, components=None)

    async def job_recording(self, event: dto.JobRecording):
        inter = make_inter(event.job_inter, self.bot)

        embed = self.embed.recording(event.job_id)
        if event.infront is None:  # gateway nonresponsive or no getters in gateway
            embed.description = "Waiting for gateway..."
        elif event.infront == 0:  # currently recording
            embed.description = f"{config.SPINNER} Recording your highlight now!"
        else:  # queued
            embed.description = f"{config.SPINNER} #{event.infront} in queue..."

        embed.add_field(
            name="Get perks by becoming a Patreon member!",
            value=(
                "* Record FACEIT demos\n"
                "* Change cl_righthand, remove HUD, change crosshair, and more\n"
                "* Bot keeps your demos for an entire month"
            ),
        )

        actionrow = self.make_actionrow(patreon=True)
        await inter.edit_original_response(embed=embed, content=None, components=actionrow)

    async def job_success(self, event: dto.JobSuccess):
        inter = make_inter(event.job_inter, self.bot)

        embed = self.embed.success(event.job_id)
        embed.description = "Uploaded! Enjoy the clip!"

        await inter.edit_original_response(embed=embed, content=None, components=None)

    async def job_selectable(self, event: dto.JobSelectable):
        inter = make_inter(event.job_inter, self.bot)

        # also clear this users demo cache
        if inter.author.id in self._demo_view_cache:
            del self._demo_view_cache[inter.author.id]

        await self.select_player(event, inter)

    async def select_player(self, event: dto.JobSelectable, inter: disnake.AppCmdInter):
        view = PlayerView(
            match=event.match,
            player_callback=partial(self.select_round, event),
            abort_callback=partial(self.abort_job, event),
            timeout_callback=partial(self.view_timeout, event),
            timeout=300.0,
        )

        embed = self.embed.selecting(event.job_id)
        embed.description = "Select a player you want to record a highlight from below."

        data = (
            ("Source", event.match.origin),
            ("Map", event.match.map),
            ("Score", event.match.score_str),
            ("Date", event.match.time_str),
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
        match = event.match

        view = RoundView(
            match=match,
            player=player,
            round_callback=partial(self.record_highlight, event, player),
            reselect_callback=partial(self.select_player, event),
            abort_callback=partial(self.abort_job, event),
            timeout_callback=partial(self.view_timeout, event),
            embed_factory=partial(self.embed.selecting, job_id=event.job_id),
            timeout=300.0,
        )

        embed = view.set_half(1 if match.has_knife_round else 0)
        await inter.response.edit_message(content=None, embed=embed, view=view)

    async def record_highlight(
        self,
        event: dto.JobSelectable,
        player: Player,
        inter: disnake.AppCmdInter,
        round_id: int,
        half: int,
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
            cmds.Record(
                job_id=event.job_id,
                player_xuid=player.xuid,
                half=half,
                round_id=round_id,
                tier=tier,
            )
        )

    @commands.slash_command(
        name="config",
        description="Tweak the recording settings",
        dm_permission=False,
    )
    # @tier(2)
    async def _config(self, inter: disnake.AppCmdInter):
        tier = await get_tier(self.bot, inter.author.id)
        user_settings, value_tiers = await views.get_user_settings(inter.author.id, tier, uow=SqlUnitOfWork())

        view = ConfigView(
            inter=inter,
            tier=tier,
            user_settings=user_settings,
            value_tiers=value_tiers,
            store_callback=self._store_config,
            abort_callback=self._abort_config,
            timeout=300.0,
        )

        await inter.send(embed=view.embed(), view=view, ephemeral=True)

    async def _store_config(self, inter: disnake.MessageInteraction, updates):
        await self.bus.dispatch(cmds.UpdateUserSettings(inter.author.id, updates))

        e = self.embed.success(title="STRIKER Configurator")
        e.description = "Configuration saved."

        await inter.response.edit_message(view=None, embed=e)

    async def _abort_config(self, inter: disnake.MessageInteraction):
        e = self.embed.failed(title="STRIKER Configurator")
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
        self.bot.maintenance = enable
        await inter.send(
            "Bot now in maintenance mode!" if enable else "Bot now accepting new commands!"
        )

        if enable:
            await self.bot.change_presence(activity=disnake.Game(name="🛠 maintenance"))
        else:
            await self.bot.normal_presence()

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

        actionrow = self.make_actionrow(invite=True, discord=True, github=True, patreon=True)
        await inter.send(embed=e, components=actionrow)

    @commands.slash_command(name="help", description="How to use the bot!", dm_permission=False)
    @commands.bot_has_permissions(embed_links=True)
    async def _help(self, inter: disnake.AppCmdInter):
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
            "This bot can record and upload CS:GO clips from matchmaking and FACEIT games straight to Discord. "
            "To do so you will need to give the bot a sharecode from one of your matchmaking matches, or a FACEIT room link.\n\n"
            "The below image shows how to find and copy a matchmaking sharecode from inside CS:GO.\n\n"
            "To record a highlight, run the `/record` command and paste the sharecode you copied.\n\n"
            "To record another highlight from the same match, use `/demos`.\n\n"
            "To record FACEIT matches, give `/record` a link like:\n"
            "`https://www.faceit.com/en/csgo/room/1-9fa1db69-5f1a-4ea3-a37c-3ab84fbd416a`\n\n"
            "Have fun!"
        )

        e.set_image(url=config.SHARECODE_IMG_URL)

        actionrow = self.make_actionrow(invite=True, discord=True)
        await inter.send(embed=e, components=actionrow, ephemeral=True)

    async def _send_donate(self, inter: disnake.Interaction):
        e = self.embed.build("Donate to support the project!")

        e.description = (
            "Thanks for your interest in supporting the project!\n\n"
            "Below are all the options for donating."
        )

        actionrow = self.make_actionrow(patreon=True, kofi=True, tradelink=True)

        await inter.send(embed=e, components=actionrow, ephemeral=True)

    def make_actionrow(
        self, invite=False, discord=False, github=False, patreon=False, kofi=False, tradelink=False
    ):
        buttons = []

        if invite:
            buttons.append(
                disnake.ui.Button(
                    style=disnake.ButtonStyle.url,
                    label="Invite the bot",
                    emoji="🎉",
                    url=self.bot.craft_invite_link(),
                )
            )

        if discord:
            buttons.append(
                disnake.ui.Button(
                    style=disnake.ButtonStyle.url,
                    label="Discord",
                    emoji=":discord:1099362254731882597",
                    url=config.DISCORD_INVITE_URL,
                )
            )

        if github:
            buttons.append(
                disnake.ui.Button(
                    style=disnake.ButtonStyle.url,
                    label="GitHub",
                    emoji=":github:1099362911077544007",
                    url=config.GITHUB_URL,
                )
            )

        if patreon:
            buttons.append(
                disnake.ui.Button(
                    style=disnake.ButtonStyle.url,
                    label="Patreon",
                    emoji=config.PATREON_EMOJI,
                    url=config.PATREON_URL,
                )
            )

        if kofi:
            buttons.append(
                disnake.ui.Button(
                    style=disnake.ButtonStyle.url,
                    label="Support through Ko-fi",
                    url=config.DONATE_URL,
                )
            )

        if tradelink:
            buttons.append(
                disnake.ui.Button(
                    style=disnake.ButtonStyle.url,
                    label="Send me some skins",
                    url=config.TRADELINK_URL,
                )
            )

        return disnake.ui.ActionRow(*buttons)


def setup(bot: commands.InteractionBot):
    bot.add_cog(RecorderCog(bot))
