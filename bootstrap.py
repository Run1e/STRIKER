import asyncio
import logging

from adapters import broker, orm
from bot import bot, config
from services import services
from services.uow import SqlUnitOfWork
from shared.log import logging_config

logging_config(config.DEBUG)

log = logging.getLogger(__name__)


async def bootstrap(
    start_orm: bool,
    start_bot: bool,
    start_broker: bool,
    restore: bool,
):
    if start_orm:
        await orm.start_orm()

    if start_bot:
        bot_instance = bot.start_bot()
        await bot_instance.wait_until_ready()

    if start_broker:
        await broker.start_brokers()

    log.info("Ready to bot!")

    # this restarts any jobs that were in selectland
    # within the last 12 (at the time of writing, anyway)
    # minutes last we restarted
    if restore:
        await services.restore(uow=SqlUnitOfWork())


def main():
    loop = asyncio.get_event_loop()

    coro = bootstrap(
        start_orm=True,
        start_bot=True,
        start_broker=True,
        restore=True,
    )

    loop.create_task(coro)
    loop.run_forever()


if __name__ == "__main__":
    main()
