import asyncio
import logging
import pickle
import re
from functools import partial
from uuid import UUID

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

disnake.MessageInteraction.__init__ = patched_init(disnake.MessageInteraction.__init__)


def make_inter(
    inter_payload: bytes, bot: commands.InteractionBot
) -> disnake.AppCommandInteraction | disnake.MessageInteraction:
    data = pickle.loads(inter_payload)
    type_value = data["type"]

    if type_value == 2:
        inter = disnake.ApplicationCommandInteraction(data=data, state=bot._connection)
    elif type_value == 3:
        inter = disnake.MessageInteraction(data=data, state=bot._connection)

    return inter


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
        return self.build(title or "Recording queued", disnake.Color.orange(), job_id)

    def success(self, job_id: str = None, title: str = None):
        return self.build(title or "Job completed", disnake.Color.green(), job_id)

    def failed(self, job_id: str = None, title: str = None):
        return self.build(title or "Oops!", disnake.Color.red(), job_id)

    def aborted(self, job_id: str = None, title: str = None):
        return self.build(title or "Job aborted", disnake.Color.red(), job_id)


def not_maintenance():
    async def checker(inter: disnake.AppCmdInter):
        if not inter.bot.maintenance:
            return True

        if await inter.bot.is_owner(inter.author):
            return True

        raise commands.CheckFailure("Bot is under maintenance! Check back in a bit!")

    return commands.check(checker)


async def job_limit_checker(inter: disnake.AppCmdInter, limit: int):
    job_count = await views.user_recording_count(user_id=inter.author.id, uow=SqlUnitOfWork())

    if job_count < limit:
        return True

    job_word = "jobs" if limit > 1 else "job"
    one_of = "one of " if limit > 1 else ""

    raise commands.CheckFailure(
        f"You can only have {limit} {job_word} queued at a time. "
        f"Please wait for {one_of}your previous {job_word} to complete before starting a new one."
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

        tier = await get_tier(self.bot, user_id)

        if faceit_match:
            if tier < 2:
                raise SponsorRequired("Recording FACEIT demos", tier=2)

            demo_dict = dict(origin="FACEIT", identifier=faceit_match.group(1))
        elif replay_match:
            if tier < 1:
                raise SponsorRequired("Direct Valve demo links", tier=1)

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
                    "You can give the bot matchmaking sharecodes from the CS:GO client:\n"
                    "`steam://rungame/730/76561202255233023/+csgo_download_match%20CSGO-3VocL-obGr4-SjkBU-DjHhz-KWtrD`\n\n"
                    "(or just the sharecode itself: `CSGO-3VocL-obGr4-SjkBU-DjHhz-KWtrD`)\n\n"
                    "The bot also supports FACEIT matches:\n"
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
        demo_origin, _ = await views.get_demo_origin_and_identifier(
            found_demo_id, uow=SqlUnitOfWork()
        )

        # this data probably belongs in a dict somewhere
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

    async def _record_another(self, inter: disnake.MessageInteraction):
        message = inter.message

        if not message.embeds:
            return

        embed = message.embeds[0]
        job_id = embed.footer.text[4:]

        demo_id = await views.get_job_demo_id(UUID(job_id), uow=SqlUnitOfWork())
        if demo_id is None:
            return

        await self.bus.dispatch(
            cmds.CreateJob(
                guild_id=inter.guild.id,
                channel_id=inter.channel.id,
                user_id=inter.author.id,
                inter_payload=pickle.dumps(inter._payload),
                demo_id=demo_id,
            )
        )

    async def edit_inter(self, inter: disnake.Interaction, **kwargs):
        try:
            await inter.edit_original_response(**kwargs)
        except disnake.HTTPException as e:
            log.info("Editing interaction message failed with code %s: %s", e.code, e.text)

    # DTOs

    async def job_processing(self, event: dto.DTO):
        inter = make_inter(event.job_inter, self.bot)

        embed = self.embed.waiting(event.job_id)
        embed.description = f"{config.SPINNER} Please wait..."

        await self.edit_inter(inter, embed=embed, content=None, components=None)

    async def job_failed(self, event: dto.JobFailed):
        inter = make_inter(event.job_inter, self.bot)

        embed = self.embed.failed(event.job_id)
        embed.description = event.reason

        await self.edit_inter(inter, embed=embed, content=None, components=None)

    async def job_recording(self, event: dto.JobRecording):
        inter = make_inter(event.job_inter, self.bot)

        embed = self.embed.recording(event.job_id)

        if event.infront is None:  # gateway nonresponsive or no getters in gateway
            embed.description = "Waiting for gateway/recorder..."
        elif event.infront == 0:  # currently recording
            embed.description = f"{config.SPINNER} Recording your highlight now!"
        else:  # queued
            embed.description = f"{config.SPINNER} #{event.infront} in queue..."

        tier = await get_tier(self.bot, inter.author.id)

        if tier == 0:
            embed.add_field(
                name="Get perks by becoming a Patreon member!",
                value=(
                    "- Record FACEIT demos\n"
                    "- Record scrimmage/wingman demos\n"
                    "- High quality 1080p video\n"
                    "- Support the project! :partying_face:"
                ),
            )

            components = self.make_actionrow(patreon=True)
        else:
            components = None

        await self.edit_inter(inter, embed=embed, content=None, components=components)

    async def job_success(self, event: dto.JobSuccess):
        inter = make_inter(event.job_inter, self.bot)

        embed = self.embed.success(event.job_id)
        embed.description = (
            "Enjoy the clip!\n\n"
            "Check out `/config` to tweak your recording settings!\n"
            "To record another highlight from a previously used demo, use `/demos`"
        )

        button = disnake.ui.Button(
            style=disnake.ButtonStyle.secondary,
            label="Record another one?",
            emoji="\N{Clapper Board}",
            custom_id="recordanother",
        )

        await self.edit_inter(inter, embed=embed, content=None, components=[button])

    async def job_selectable(self, event: dto.JobSelectable):
        inter = make_inter(event.job_inter, self.bot)

        # also clear this users demo cache
        if inter.author.id in self._demo_view_cache:
            del self._demo_view_cache[inter.author.id]

        await self.select_player(event, inter)

    # interactable views/embeds

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

    async def view_timeout(self, event: dto.JobSelectable):
        await self.bus.dispatch(cmds.AbortJob(event.job_id))

        embed = self.embed.aborted(event.job_id)
        embed.description = "Command timed out."

        inter = make_inter(event.job_inter, self.bot)
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
                round_id=round_id,
                tier=tier,
            )
        )

    @commands.slash_command(
        name="config",
        description="Tweak the recording settings",
        dm_permission=False,
    )
    async def _config(self, inter: disnake.AppCmdInter):
        tier = await get_tier(self.bot, inter.author.id)
        user_settings, value_tiers = await views.get_user_settings(
            inter.author.id, tier, uow=SqlUnitOfWork()
        )

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

    @commands.slash_command(
        name="download",
        description="Get demo download link",
        dm_permission=False,
        guild_ids=[config.STRIKER_GUILD_ID],
    )
    @commands.is_owner()
    async def download(self, inter: disnake.AppCmdInter, job_or_demo_id: str):
        job_id = None
        demo_id = None

        try:
            job_id = UUID(job_or_demo_id)
            demo_id = await views.get_job_demo_id(job_id, uow=SqlUnitOfWork())
        except ValueError:
            try:
                demo_id = int(job_or_demo_id)
            except ValueError:
                raise commands.BadArgument("Not a valid job or demo id.")

        row = await views.get_demo_origin_and_identifier(demo_id=demo_id, uow=SqlUnitOfWork())
        if row is None:
            raise commands.BadArgument("Could not find that job/demo.")

        demo_origin, demo_identifier = row
        command = cmds.GetPresignedUrlDTO(demo_origin, demo_identifier)

        waiter = self.bus.wait_for(
            dto.PresignedUrlReceived,
            check=lambda e: e.origin == demo_origin and e.identifier == demo_identifier,
            timeout=4.0,
        )

        await self.bus.dispatch(command)
        result = await waiter

        if result:
            e = self.embed.build(f"Demo #{demo_id}")
            e.description = (
                f"Download link valid for 5 minutes.\n\n[Download]({result.presigned_url})"
            )
            await inter.response.send_message(embed=e)
        else:
            await inter.response.send_message("Failed getting presigned url.", ephemeral=True)

    @commands.slash_command(name="donate", description="Support the project", dm_permission=False)
    async def donate(self, inter: disnake.AppCmdInter):
        await self._send_donate_embed(inter)

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

        actionrow = self.make_actionrow(
            invite=True, discord=True, github=True, patreon=True, kofi=True, tradelink=True
        )
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
        elif custom_id == "alternateoriginhelp":
            await self._send_alternate_origin_help(inter)
        elif custom_id == "recordanother":
            await self._record_another(inter)

    async def _send_help_embed(self, inter: disnake.Interaction):
        e = self.embed.build("STRIKER")

        e.description = (
            "STRIKER can record and upload CS:GO highlights from matchmaking, FACEIT, scrimmage, and wingman matches straight to Discord.\n\n"
            "### How do I use STRIKER?\n"
            "1. Follow the steps in the image below to copy a sharecode from inside CS:GO\n"
            "2. Run the `/record` command inside Discord and paste the sharecode\n"
            "3. Select a player and a round\n"
            "4. You're done! The bot will record the highlight and upload it."
        )

        e.set_image(url=config.SHARECODE_IMG_URL)

        actionrow = self.make_actionrow(invite=True)
        actionrow.append_item(
            disnake.ui.Button(
                label="FACEIT/scrimmage/wingman matches",
                custom_id="alternateoriginhelp",
                emoji="\N{Black Question Mark Ornament}",
            )
        )

        await inter.send(embed=e, components=actionrow, ephemeral=True)

    async def _send_alternate_origin_help(self, inter: disnake.Interaction):
        e = self.embed.build("STRIKER")

        e.description = (
            "Patreon supporters can record highlights from additional sources.\n\n"
            "### FACEIT matches (Patreon Tier 2)\n"
            "1. Open https://faceit.com/ and and go to your profile\n"
            '2. Click on the "Stats" tab and scroll down to your match history\n'
            "3. Click one of the matches and copy the url\n"
            "4. Give that url to `/record`\n"
            "### Scrimmage/wingman matches (Patreon Tier 1)\n"
            "1. Go to https://steamcommunity.com/my/gcpd/730\n"
            '2. Click on "Scrimmage Matches" or "Wingman matches"\n'
            '3. Right click "Download GOTV Replay" and select "Copy link address"\n'
            "4. Give that url to `/record`\n"
        )

        actionrow = self.make_actionrow(invite=True, patreon=True)
        await inter.send(embed=e, components=actionrow, ephemeral=True)

    async def _send_donate_embed(self, inter: disnake.Interaction):
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
                    label="Donate",
                    emoji=config.KOFI_EMOJI,
                    url=config.DONATE_URL,
                )
            )

        if tradelink:
            buttons.append(
                disnake.ui.Button(
                    style=disnake.ButtonStyle.url,
                    label="Gift some skins",
                    emoji=config.STEAM_EMOJI,
                    url=config.TRADELINK_URL,
                )
            )

        # dumb thing to hardcode but there's never gonna be > 10 buttons
        # so it's fine
        if len(buttons) > 5:
            return [
                disnake.ui.ActionRow(*buttons[0:3]),
                disnake.ui.ActionRow(*buttons[3:]),
            ]

        return disnake.ui.ActionRow(*buttons)


def setup(bot: commands.InteractionBot):
    bot.add_cog(RecorderCog(bot))
