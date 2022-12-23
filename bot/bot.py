import asyncio
import logging.handlers

import disnake
from disnake.ext import commands

from bot import config

EXTENSIONS = ('cog', 'error_handler')
log = logging.getLogger(__name__)


class Bot(commands.InteractionBot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.log = log
        self._started = False

    async def on_ready(self):
        if self._started is False:
            for name in EXTENSIONS:
                log.info('Loading extension %s', name)
                self.load_extension(f'bot.{name}')

            self._started = True
            self.log.info('Bot initialized')

        await self.change_presence()
        await self.change_presence(activity=disnake.Game(name='try /help'))

    @property
    def invite_link(self):
        return disnake.utils.oauth_url(
            self.user.id,
            permissions=disnake.Permissions(387136),
            scopes=['bot', 'applications.commands'],
        )


def start_bot():
    log.info('Initializing bot')

    logging.getLogger('disnake').setLevel(logging.INFO)

    intents = disnake.Intents.default()
    intents.messages = False

    bot = Bot(
        max_messages=None,
        intents=intents,
        test_guilds=config.TEST_GUILDS,
    )

    asyncio.create_task(bot.start(config.BOT_TOKEN))

    log.info('Bot start task created')

    return bot
