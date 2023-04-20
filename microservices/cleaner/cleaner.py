# top-level module include hack for shared :|
import sys

sys.path.append("../..")

from base64 import b64decode, b64encode
import os
import asyncio
import logging

import aiormq
from shared.log import logging_config
from shared.message import RPCServer

import config

logging_config(config.DEBUG)
log = logging.getLogger(__name__)


async def check_space():
    pass



async def main():
    log.info("Starting up...")

    logging.getLogger("aiormq").setLevel(logging.INFO)

    mq = await aiormq.connect(config.RABBITMQ_HOST)
    channel = await mq.channel()

    await channel.basic_qos(prefetch_count=1)

    s = RPCServer(channel, config.CLEANER_QUEUE)
    s.register(check_space)
    await s.start()

    log.info("Ready to clean!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
