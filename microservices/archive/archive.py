# top-level module include hack for shared :|
import sys

sys.path.append("../..")

import asyncio
import logging
import os

import aiormq
import config

from shared.log import logging_config
from shared.message import MessageWrapper

logging_config(config.DEBUG)
log = logging.getLogger(__name__)


async def on_archive(message: aiormq.channel.DeliveredMessage):
    wrap = MessageWrapper(
        message=message,
        default_error="An error occurred while archiving.",
        ack_on_failure=True,
    )

    async with wrap as ctx:
        data = ctx.data
        dry_run = data["dry_run"]
        remove_matchids = data["remove_matchids"]
        ignore_videos = data["ignore_videos"]

        log.info(remove_matchids)
        log.info(ignore_videos)

        removed_demos = 0
        removed_videos = 0
        current_matchids = []

        for entry in os.listdir(config.VIDEO_DIR):
            _id = entry[:-4]
            if _id not in ignore_videos:
                if not dry_run:
                    log.info("Removing %s", entry)
                    os.remove(f"{config.VIDEO_DIR}/{entry}")
                removed_videos += 1
            else:
                log.info("Active video %s", entry)

        for entry in os.listdir(config.DEMO_DIR):
            matchid = int(entry[:-4])
            if matchid in remove_matchids:
                if not dry_run:
                    log.info("Removing %s", entry)
                    os.remove(f"{config.DEMO_DIR}/{entry}")
                removed_demos += 1
            else:
                current_matchids.append(matchid)

        await ctx.success(
            removed_demos=removed_demos,
            removed_videos=removed_videos,
            current_matchids=current_matchids,
        )


async def main():
    log.info("Starting up...")

    logging.getLogger("aiormq").setLevel(logging.INFO)

    mq = await aiormq.connect(config.RABBITMQ_HOST)
    channel = await mq.channel()

    await channel.basic_qos(prefetch_count=1)
    await channel.basic_consume(
        queue=config.ARCHIVE_QUEUE, consumer_callback=on_archive, no_ack=False
    )

    log.info("Ready to archive!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
