# top-level module include hack for shared :|
from shared.utils import timer
from shared.message import MessageError, MessageWrapper
from shared.log import logging_config
from shared.const import DEMOPARSE_VERSION
import config
import aiormq
import aiohttp
from subprocess import run
from concurrent.futures import ProcessPoolExecutor
from bz2 import BZ2Decompressor
import os
import logging
import asyncio
import sys

sys.path.append("../..")


CHUNK_SIZE = 4 * 1024 * 1024

logging_config(config.DEBUG)
log = logging.getLogger(__name__)

executor = ProcessPoolExecutor(max_workers=3)
session = aiohttp.ClientSession()
loop = None

if os.name == "nt" and not config.WSL:
    splitter = "\r\n"
else:
    splitter = "\n"


def decompress(archive, file):
    with open(file, "wb") as new_file, open(archive, "rb") as file:
        decompressor = BZ2Decompressor()
        for data in iter(lambda: file.read(1024 * 1024), b""):
            try:
                chunk = decompressor.decompress(data)
            except OSError as exc:
                raise MessageError("Demo corrupted.") from exc

            new_file.write(chunk)


def parse_demo(demofile):
    p = run(["node", "parse/index.js", demofile], capture_output=True)
    if p.returncode != 0:
        log.error(p.stdout)
        raise MessageError("Failed parsing demo.")
    return p.stdout


async def on_demoparse(message: aiormq.channel.DeliveredMessage):
    wrap = MessageWrapper(
        message=message,
        default_error="An error occurred while getting the demo.",
        ack_on_failure=True,
    )

    loop = asyncio.get_running_loop()

    async with wrap as ctx:
        data = ctx.data

        matchid = data["matchid"]
        url = data["url"]

        log.info(f"Processing {matchid=} with {url=}")

        archive_path = rf"{config.ARCHIVE_FOLDER}/{matchid}.dem.bz2"
        archive_path_temp = rf"{config.TEMP_FOLDER}/{matchid}.dem.bz2"
        demo_path = rf"{config.DEMO_FOLDER}/{matchid}.dem"
        demo_path_temp = rf"{config.TEMP_FOLDER}/{matchid}.dem"

        # if archive and demo does not exist, download the archive
        if not os.path.isfile(demo_path):
            if not os.path.isfile(archive_path):
                log.info(f"{matchid} downloading demo")

                end = timer("download")
                # down the the demo
                async with session.get(url) as resp:
                    # deleted from valve servers
                    if resp.status == 404:
                        raise MessageError("Demo has been deleted from Valve servers.")

                    # misc error
                    elif resp.status != 200:
                        raise MessageError("Failed to download demo from Valve servers.")

                    # write to file
                    with open(archive_path_temp, "wb") as f:
                        f.write(await resp.read())

                # after successful download, move the archive to the archive folder
                os.rename(archive_path_temp, archive_path)

                log.info(end())
            else:
                log.info(f"{matchid} archive already exist")

            log.info(f"{matchid} extracting demo")
            end = timer("extraction")

            await loop.run_in_executor(executor, decompress, archive_path, demo_path_temp)
            os.rename(demo_path_temp, demo_path)
            log.info(end())
        else:
            log.info(f"{matchid} demo already exists")

        log.info(f"{matchid} parsing demo")
        end = timer("parsing")

        data = await loop.run_in_executor(executor, parse_demo, demo_path)
        if not data:
            raise ValueError("demoinfogo returned no data")

        log.info(end())

        await ctx.success(data=data.decode("utf-8"), version=DEMOPARSE_VERSION)


async def main():
    logging.getLogger("aiormq").setLevel(logging.INFO)

    mq = await aiormq.connect(config.RABBITMQ_HOST)
    chan = await mq.channel()

    await chan.basic_qos(prefetch_count=3)

    # await chan.queue_declare(config.DEMOPARSE_QUEUE)
    await chan.basic_consume(
        queue=config.DEMOPARSE_QUEUE, consumer_callback=on_demoparse, no_ack=False
    )

    log.info("Ready to parse!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
