# top-level module include hack for shared :|
import sys

sys.path.append("../..")

import asyncio
import logging
import os
from bz2 import BZ2Decompressor
from concurrent.futures import ProcessPoolExecutor
from subprocess import run

import aioboto3
import aiohttp
import config

from messages import events
from messages.broker import Broker, MessageError
from messages.bus import MessageBus
from messages.commands import RequestDemoParse
from messages.deco import handler
from shared.const import DEMOPARSE_VERSION
from shared.log import logging_config
from shared.utils import DemoCorrupted, decompress, timer

CHUNK_SIZE = 4 * 1024 * 1024

logging_config(config.DEBUG)
log = logging.getLogger(__name__)

executor = ProcessPoolExecutor(max_workers=3)
session = aiohttp.ClientSession()
loop = None

if os.name == "nt":
    splitter = "\r\n"
else:
    splitter = "\n"


class DemoStorage:
    def __init__(self, bucket, endpoint_url, region_name, keyID, applicationKey) -> None:
        self.bucket = bucket
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.keyID = keyID
        self.applicationKey = applicationKey

        self.session = aioboto3.Session()

    def make_client(self):
        return self.session.client(
            service_name="s3",
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.keyID,
            aws_secret_access_key=self.applicationKey,
        )

    @staticmethod
    def _build_key(origin, identifier):
        return f"{origin.lower()}/{identifier}.dem.bz2"

    async def upload_demo(self, origin, identifier):
        key = self._build_key(origin, identifier)

        log.info("Uploading demo %s", key)

        with open(f"data/{key}", "rb") as fp:
            async with self.make_client() as client:
                await client.upload_fileobj(fp, self.bucket, key)

    async def get_url(self, origin, identifier):
        async with self.make_client() as client:
            return await client.generate_presigned_url(
                "get_object",
                Params=dict(Bucket=self.bucket, Key=self._build_key(origin, identifier)),
            )


def parse_demo(demofile):
    p = run(["node", "parse/index.js", demofile], capture_output=True)
    if p.returncode != 0:
        log.error(p.stdout)
        raise MessageError("Failed parsing demo.")
    return p.stdout


@handler(RequestDemoParse)
async def on_demoparse(command: RequestDemoParse, publish, upload_demo):
    loop = asyncio.get_running_loop()

    origin = command.origin
    identifier = command.identifier
    download_url = command.download_url

    folder = f"data/{origin.lower()}"

    if not os.path.isdir(folder):
        os.makedirs(folder)

    log.info("Processing origin %s identifier %s url %s", origin, identifier, download_url)

    archive_path = f"{folder}/{identifier}.dem.bz2"
    demo_path = f"{folder}/{identifier}.dem"

    # if archive and demo does not exist, download the archive
    if not os.path.isfile(archive_path):
        log.info("downloading %s", archive_path)

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
            with open(archive_path, "wb") as f:
                f.write(await resp.read())

        log.info(end())
    else:
        log.info("archive exists %s", archive_path)

    if not os.path.isfile(demo_path):
        log.info("extracting %s", demo_path)
        end = timer("extraction")

        try:
            await loop.run_in_executor(executor, decompress, archive_path, demo_path)
        except DemoCorrupted:
            raise MessageError("Demo corrupted.")

        log.info(end())
    else:
        log.info("demo exists %s", demo_path)

    log.info("parsing %s", demo_path)
    end = timer("parsing")

    data = await loop.run_in_executor(executor, parse_demo, demo_path)
    os.remove(demo_path)

    if not data:
        raise ValueError("demofile returned no data")

    log.info(end())

    await publish(
        events.DemoParsed(
            origin=origin,
            identifier=identifier,
            data=data.decode("utf-8"),
            version=DEMOPARSE_VERSION,
        )
    )

    end = timer("upload")
    await upload_demo(origin, identifier)
    log.info(end())

    os.remove(archive_path)


async def main():
    logging.getLogger("aiormq").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.INFO)
    logging.getLogger("aiobotocore").setLevel(logging.INFO)
    logging.getLogger("aioboto3").setLevel(logging.INFO)

    s3 = DemoStorage(
        bucket=config.DEMO_BUCKET,
        endpoint_url=config.ENDPOINT_URL,
        region_name=config.REGION_NAME,
        keyID=config.KEY_ID,
        applicationKey=config.APPLICATION_KEY,
    )

    bus = MessageBus()
    broker = Broker(bus)
    bus.add_dependencies(publish=broker.publish, upload_demo=s3.upload_demo, get_url=s3.get_url)
    bus.register_decos()
    await broker.start(config.RABBITMQ_HOST, prefetch_count=3)

    log.info("Ready to parse!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
