import asyncio
import logging

from adapters import orm, steam
from adapters.faceit import FACEITAPI
from bot import bot, config
from messages import commands, events
from messages.broker import Broker
from messages.bus import MessageBus
from services.uow import SqlUnitOfWork
from shared.log import logging_config
from shared.utils import sentry_init

logging_config(config.DEBUG)

log = logging.getLogger(__name__)


async def bootstrap(
    uow_type,
    start_orm: bool,
    start_steam: bool,
    start_faceit: bool,
    start_bot: bool,
    restore: bool,
) -> MessageBus:
    if start_orm:
        await orm.start_orm()

    bus = MessageBus(
        dependencies=dict(video_upload_url=config.VIDEO_UPLOAD_URL, tokens=config.TOKENS),
        factories=dict(uow=uow_type),
    )

    broker = Broker(
        bus=bus,
        identifier="bot",
        publish_commands={
            commands.RequestDemoParse,
            commands.RequestPresignedUrl,
            commands.RequestRecording,
        },
        consume_events={
            events.PresignedUrlGenerated,
        },
    )

    gather = asyncio.Event()
    waiters = list()

    if start_steam:
        fetcher, steam_waiter = await steam.get_match_fetcher(config.STEAM_REFRESH_TOKEN)
        bus.add_dependencies(sharecode_resolver=fetcher)
        waiters.append(steam_waiter)
    else:
        # TODO: remove this
        bus.add_dependencies(sharecode_resolver=None)

    if start_faceit:
        faceit_api = FACEITAPI(api_key=config.FACEIT_API_KEY)
        bus.add_dependencies(faceit_resolver=faceit_api.match)

    bus.register_decos()

    if start_bot:
        bot_instance = bot.start_bot(bus, gather)
        waiters.append(bot_instance.wait_until_ready())

    await asyncio.gather(*waiters)
    await broker.start(config.RABBITMQ_HOST)
    gather.set()

    log.info("Ready to bot!")

    # this restarts any jobs that were in selectland
    # within the last 12 (at the time of writing, anyway)
    # minutes last we restarted
    if restore:
        await bus.dispatch(commands.Restore())

    return bus


def main():
    if config.SENTRY_DSN:
        sentry_init(config.SENTRY_DSN)

    loop = asyncio.get_event_loop()

    logging.getLogger("aio_pika").setLevel(logging.INFO)
    logging.getLogger("aiormq.connection").setLevel(logging.INFO)

    coro = bootstrap(
        uow_type=SqlUnitOfWork,
        start_orm=True,
        start_steam=True,
        start_faceit=True,
        start_bot=True,
        restore=True,
    )

    loop.create_task(coro)
    loop.run_forever()


if __name__ == "__main__":
    main()
