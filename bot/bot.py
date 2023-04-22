import asyncio
import logging.handlers

import disnake
from disnake.ext import commands

from bot import config

EXTENSIONS = ("cog", "error_handler")
log = logging.getLogger(__name__)


class Bot(commands.AutoShardedInteractionBot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._maintenance = False
        self.log = log

    async def on_ready(self):
        await self.change_presence()
        await self.normal_presence()

    async def normal_presence(self):
        await self.change_presence(activity=disnake.Game(name="/help"))


def start_bot():
    log.info("Initializing bot")

    logging.getLogger("disnake").setLevel(logging.INFO)

    intents = disnake.Intents.default()
    intents.typing = False
    intents.messages = False
    intents.voice_states = False
    intents.dm_messages = False
    intents.reactions = False

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

    bot = Bot(**bot_kwargs)

    for name in EXTENSIONS:
        log.info("Loading extension %s", name)
        bot.load_extension(f"bot.{name}")

    asyncio.create_task(bot.start(config.BOT_TOKEN))

    log.info("Bot start task created")

    return bot
