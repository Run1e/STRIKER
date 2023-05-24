# top-level module include hack for shared :|
import sys

sys.path.append("../..")

import asyncio
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from subprocess import run

import aioboto3
import aiofiles
import aiohttp
import config

from messages import events
from messages.broker import Broker, MessageError
from messages.bus import MessageBus
from messages.commands import RequestDemoParse, RequestPresignedUrl
from messages.deco import handler
from shared.const import DEMOPARSE_VERSION
from shared.log import logging_config
from shared.utils import decompress, sentry_init, timer

CHUNK_SIZE = 4 * 1024 * 1024

logging_config(config.DEBUG)
log = logging.getLogger(__name__)

executor = ProcessPoolExecutor(max_workers=3)
session = aiohttp.ClientSession()

if os.name == "nt":
    splitter = "\r\n"
else:
    splitter = "\n"


# eehhh kinda idiotic solution but whatever for now
def origin_to_ext(origin):
    return dict(
        VALVE="bz2",
        FACEIT="gz",
    ).get(origin)


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
        return f"{origin.lower()}/{identifier}.dem.{origin_to_ext(origin)}"

    async def upload_demo(self, file, origin, identifier):
        key = self._build_key(origin, identifier)

        # this shit just has to be sync smh
        with open(file, "rb") as fp:
            async with self.make_client() as client:
                await client.upload_fileobj(fp, self.bucket, key)

    async def get_url(self, origin, identifier):
        async with self.make_client() as client:
            return await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": self._build_key(origin, identifier)},
                ExpiresIn=3600,  # an hour
            )


def parse_demo(demofile):
    p = run(["node", "parse/index.js", demofile], capture_output=True)
    if p.returncode != 0:
        log.error(p.stdout)
        raise MessageError("Failed parsing demo.")
    return p.stdout


@handler(RequestDemoParse)
async def request_demo_parse(command: RequestDemoParse, publish, upload_demo):
    loop = asyncio.get_running_loop()

    origin = command.origin
    identifier = command.identifier
    download_url = command.download_url

    if not config.DATA_FOLDER.is_dir():
        os.makedirs(config.DATA_FOLDER)

    log.info("Processing origin %s identifier %s url %s", origin, identifier, download_url)

    ext = origin_to_ext(origin)

    demo_path = config.DATA_FOLDER / f"{origin.lower()}_{identifier}.dem"
    archive_path = config.DATA_FOLDER / f"{origin.lower()}_{identifier}.dem.{ext}"

    # if archive and demo does not exist, download the archive
    if not archive_path.is_file():
        log.info("downloading %s", archive_path)

        end = timer("download")

        # down the demo
        try:
            async with session.get(download_url, timeout=aiohttp.ClientTimeout(total=32.0)) as resp:
                # deleted from valve servers
                if resp.status == 404:
                    raise MessageError("Demo is not available at the given URL.")

                # misc error
                elif resp.status != 200:
                    raise MessageError("Unable to download demo.")

                # write to file
                async with aiofiles.open(archive_path, "wb") as f:
                    await f.write(await resp.read())

        except asyncio.TimeoutError as exc:
            raise MessageError("Fetching demo timed out.") from exc

        log.info(end())
    else:
        log.info("archive exists %s", archive_path)

    if not demo_path.is_file():
        log.info("extracting %s", demo_path)
        end = timer("extraction")

        try:
            await loop.run_in_executor(executor, decompress, archive_path, demo_path)
        except OSError as exc:
            raise MessageError("Unable to extract demo archive.") from exc

        log.info(end())
    else:
        log.info("demo exists %s", demo_path)

    async def parser():
        log.info("parsing %s", demo_path)
        end = timer("parsing")
        result = await loop.run_in_executor(executor, parse_demo, demo_path)
        log.info(end())
        return result

    async def uploader():
        log.info("uploading %s", archive_path)
        end = timer("upload")
        await upload_demo(archive_path, origin, identifier)
        log.info(end())

    # parse and upload demo
    async with asyncio.TaskGroup() as tg:
        parse_task = tg.create_task(parser())
        tg.create_task(uploader())

    if not config.DEBUG:
        # only remove when in prod...
        os.remove(demo_path)
        os.remove(archive_path)

    data = parse_task.result()
    if not data:
        raise MessageError("Failed parsing demo.")

    await publish(
        events.DemoParseSuccess(
            origin=origin,
            identifier=identifier,
            data=data.decode("utf-8"),
            version=DEMOPARSE_VERSION,
        )
    )


@handler(RequestPresignedUrl)
async def request_presigned_url(command: RequestPresignedUrl, publish, get_url):
    presigned_url = await get_url(command.origin, command.identifier)
    await publish(events.PresignedUrlGenerated(command.origin, command.identifier, presigned_url))


async def main():
    if config.SENTRY_DSN:
        sentry_init(config.SENTRY_DSN)

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
    await broker.start(config.RABBITMQ_HOST, prefetch_count=1)

    log.info("Ready to parse!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
