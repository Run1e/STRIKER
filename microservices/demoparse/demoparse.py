# top-level module include hack for shared :|
from json import dumps
import sys


sys.path.append("../..")

import asyncio
import logging
import os
from bz2 import BZ2Decompressor
from concurrent.futures import ProcessPoolExecutor
from subprocess import run

import aiohttp
import aiormq
import config

from messages import events
from messages.broker import Broker
from messages.bus import MessageBus
from messages.commands import RequestDemoParse
from messages.deco import handler
from shared.const import DEMOPARSE_VERSION
from shared.log import logging_config
from shared.message import MessageError, MessageWrapper
from shared.utils import timer

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


@handler(RequestDemoParse)
async def on_demoparse(command: RequestDemoParse, publish):
    loop = asyncio.get_running_loop()

    origin = command.origin
    identifier = command.identifier
    download_url = command.download_url
    path = f"{origin.lower()}/{identifier}"

    log.info("Processing %s url %s", path, download_url)

    archive_path = f"{config.ARCHIVE_FOLDER}/{path}.dem.bz2"
    archive_path_temp = f"{config.TEMP_FOLDER}/{identifier}.dem.bz2"
    demo_path = f"{config.DEMO_FOLDER}/{path}.dem"
    demo_path_temp = f"{config.TEMP_FOLDER}/{identifier}.dem"

    # if archive and demo does not exist, download the archive
    if not os.path.isfile(archive_path):
        log.info("downloading %s", path)

        end = timer("download")

        # down the demo
        async with session.get(download_url) as resp:
            # deleted from valve servers
            if resp.status == 404:
                raise MessageError("Demo is not available at the given URL.")

            # misc error
            elif resp.status != 200:
                raise MessageError("Unable to download demo.")

            # write to file
            with open(archive_path_temp, "wb") as f:
                f.write(await resp.read())

        # after successful download, move the archive to the archive folder
        os.rename(archive_path_temp, archive_path)

        log.info(end())
    else:
        log.info("archive exists %s", path)

    if not os.path.isfile(demo_path):
        log.info("extracting %s", path)
        end = timer("extraction")

        await loop.run_in_executor(executor, decompress, archive_path, demo_path_temp)
        os.rename(demo_path_temp, demo_path)

        log.info(end())
    else:
        log.info("demo exists %s", path)

    log.info("parsing %s", path)
    end = timer("parsing")

    data = await loop.run_in_executor(executor, parse_demo, demo_path)
    if not data:
        raise ValueError("demofile returned no data")

    log.info(end())

    await publish(
        events.DemoParseSuccess(
            origin=origin, identifier=identifier, data=data.decode("utf-8"), version=DEMOPARSE_VERSION
        )
    )


async def main():
    logging.getLogger("aiormq").setLevel(logging.INFO)

    bus = MessageBus()
    broker = Broker(bus)
    bus.add_dependencies(publish=broker.publish)
    bus.register_decos()
    await broker.start(config.RABBITMQ_HOST, prefetch=3)


    log.info("Ready to parse!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
