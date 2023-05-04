import asyncio
import logging.handlers

import disnake
from disnake.ext import commands

from bot import config
from messages.bus import MessageBus

EXTENSIONS = ("checks", "errors", "cog", "owner")
log = logging.getLogger(__name__)


class Bot(commands.AutoShardedInteractionBot):
    def __init__(self, bus, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bus: MessageBus = bus

        self.maintenance = False
        self.invite_permissions = disnake.Permissions(274878286912)
        self.invite_scopes = {"applications.commands", "bot"}

    async def on_ready(self):
        await self.change_presence()
        await self.normal_presence()

    async def normal_presence(self):
        await self.change_presence(activity=disnake.Game(name="/help"))

    def craft_invite_link(self):
        return disnake.utils.oauth_url(
            self.user.id,
            permissions=self.invite_permissions,
            scopes=self.invite_scopes,
        )

    def craft_guild_invite_link(self, guild):
        return disnake.utils.oauth_url(
            self.user.id,
            disable_guild_select=True,
            guild=guild,
            permissions=self.invite_permissions,
            scopes=self.invite_scopes,
        )


def start_bot(bus: MessageBus):
    log.info("Initializing bot")

    logging.getLogger("disnake").setLevel(logging.INFO)

    intents = disnake.Intents(guilds=True, guild_messages=True)

    command_sync_flags = commands.CommandSyncFlags(
        allow_command_deletion=config.DEBUG,
        sync_commands=True,
        sync_commands_debug=True,
        sync_global_commands=True,
        sync_guild_commands=True,
        sync_on_cog_actions=False,
    )

    bot_kwargs = dict(
        max_messages=None,
        intents=intents,
        allowed_mentions=disnake.AllowedMentions(everyone=False),
        command_sync_flags=command_sync_flags,
    )

    if config.TEST_GUILDS:
        bot_kwargs["test_guilds"] = config.TEST_GUILDS

    bot = Bot(bus=bus, **bot_kwargs)

    for name in EXTENSIONS:
        log.info("Loading extension %s", name)
        bot.load_extension(f"bot.{name}")

    asyncio.create_task(bot.start(config.BOT_TOKEN))

    log.info("Bot start task created")

    return bot
