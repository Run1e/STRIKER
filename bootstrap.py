import asyncio
import logging

from adapters import orm, steam
from bot import bot, config
from messages import commands
from messages.broker import Broker
from messages.bus import MessageBus
from services.uow import SqlUnitOfWork
from shared.log import logging_config

logging_config(config.DEBUG)

log = logging.getLogger(__name__)


async def bootstrap(
    uow_type,
    start_orm: bool,
    start_steam: bool,
    start_bot: bool,
    restore: bool,
) -> MessageBus:
    if start_orm:
        await orm.start_orm()

    bus = MessageBus(
        uow_factory=lambda: uow_type(),
        dependencies=dict(video_upload_url=config.VIDEO_UPLOAD_URL),
    )

    broker = Broker(
        bus=bus,
        identifier="bot",
        publish_commands={
            commands.RequestDemoParse,
            commands.RequestPresignedUrl,
            commands.RequestRecording,
        },
    )

    bus.add_dependencies(publish=broker.publish, wait_for=bus.wait_for)

    if start_steam:
        fetcher = await steam.get_match_fetcher(config.STEAM_REFRESH_TOKEN)
        bus.add_dependencies(sharecode_resolver=fetcher)

    bus.register_decos()

    if start_bot:
        bot_instance = bot.start_bot(bus)
        await bot_instance.wait_until_ready()

    log.info("Ready to bot!")

    await broker.start(config.RABBITMQ_HOST)

    # this restarts any jobs that were in selectland
    # within the last 12 (at the time of writing, anyway)
    # minutes last we restarted
    # if restore:
    #     await messagebus.dispatch(commands.Restore())

    return bus


def main():
    loop = asyncio.get_event_loop()

    logging.getLogger("aiormq.connection").setLevel(logging.INFO)

    coro = bootstrap(
        uow_type=SqlUnitOfWork,
        start_orm=True,
        start_steam=True,
        start_bot=True,
        restore=True,
    )

    loop.create_task(coro)
    loop.run_forever()


if __name__ == "__main__":
    main()
