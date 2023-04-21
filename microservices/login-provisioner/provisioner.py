# top-level module include hack for shared :|
import config
from shared.message import RPCServer
from shared.log import logging_config
import aiormq
import logging
import asyncio
import os
from base64 import b64decode, b64encode
import sys

sys.path.append("../..")


logging_config(config.DEBUG)
log = logging.getLogger(__name__)


async def get_login():
    return config.LOGINS[0]


async def load_sentry(username):
    file = f"sentries/{username}_sentry.bin"

    if not os.path.isfile(file):
        return None

    with open(file, "rb") as f:
        _bytes = f.read()

    data = b64encode(_bytes).decode("ascii")
    return data


async def store_sentry(username, sentry):
    log.info(f"Storing sentry for user {username}")

    sentry = b64decode(sentry)

    file = f"sentries/{username}_sentry.bin"

    with open(file, "wb") as f:
        f.write(sentry)


async def main():
    log.info("Starting up...")

    logging.getLogger("aiormq").setLevel(logging.INFO)

    mq = await aiormq.connect(config.RABBITMQ_HOST)
    channel = await mq.channel()

    await channel.basic_qos(prefetch_count=1)

    s = RPCServer(channel, config.LOGIN_PROVISIONER_QUEUE)
    s.register(get_login)
    s.register(load_sentry)
    s.register(store_sentry)
    await s.start()

    log.info("Ready to provision!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
