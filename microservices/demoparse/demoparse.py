# top-level module include hack for shared :|
import sys

sys.path.append("../..")

import asyncio
import logging
import os

import aioboto3
import aiohttp
import config

from messages import events
from messages.broker import Broker, MessageError
from messages.bus import MessageBus
from messages.commands import RequestDemoParse, RequestPresignedUrl
from messages.deco import handler
from shared.const import CSGO_DEMOPARSE_VERSION
from shared.log import logging_config
from shared.utils import (
    CurlError,
    RunError,
    decompress,
    delete_file,
    download_file,
    make_folder,
    run,
    sentry_init,
    timer,
)

CHUNK_SIZE = 4 * 1024 * 1024

logging_config(config.DEBUG)
log = logging.getLogger(__name__)

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

    async def get_url(self, origin: str, identifier: str, expires_in: int):
        async with self.make_client() as client:
            return await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": self._build_key(origin, identifier)},
                ExpiresIn=expires_in,
            )


async def parse_demo(demofile) -> str:
    code, stdout, stderr = await run("node", "parse/index.js", demofile, timeout=32.0)
    if code != 0:
        raise RunError(code=code)  # node code lmao
    return stdout


@handler(RequestDemoParse)
async def request_demo_parse(command: RequestDemoParse, publish, upload_demo, get_url):
    origin = command.origin
    identifier = command.identifier
    download_url = command.download_url

    if not config.DATA_FOLDER.is_dir():
        make_folder(config.DATA_FOLDER)

    log.info("Processing origin %s identifier %s url %s", origin, identifier, download_url)

    ext = origin_to_ext(origin)

    demo_path = config.DATA_FOLDER / f"{origin.lower()}_{identifier}.dem"
    archive_path = config.DATA_FOLDER / f"{origin.lower()}_{identifier}.dem.{ext}"

    # clear if they already exist
    delete_file(demo_path)
    delete_file(archive_path)

    if command.data_version:
        # if the caller says they already have a version,
        # that means we already have the demo in our s3 bucket,
        # and we basically want to reparse it (likely DEMOPARSE_VERSION increment)
        # as such, set the download_url to the output of s3.get_url
        log.info("Setting download_url to own s3 presigned url")
        download_url = await get_url(origin, identifier, 60 * 60)

    # if archive and demo does not exist, download the archive
    log.info("downloading %s", archive_path)
    end = timer("download")

    # if not archive_path.is_file():
    try:
        await download_file(download_url, archive_path, timeout=45.0)
    except asyncio.TimeoutError as exc:
        raise MessageError("Fetching demo timed out.") from exc
    except CurlError as exc:
        raise MessageError(
            ("Demo not found (404)." if exc.http_code == 404 else "Failed fetching demo.") + "\n\n"
            "If you just finished this match, please wait a couple of minutes and try again.\n"
            "Otherwise, this demo might have been deleted."
        ) from exc

    log.info(end())

    log.info("extracting %s", demo_path)
    end = timer("extraction")

    try:
        await decompress(archive_path, demo_path)
    except (asyncio.TimeoutError, RunError) as exc:
        raise MessageError("Failed extracting demo archive.") from exc

    log.info(end())

    async def parser():
        log.info("parsing %s", demo_path)
        end = timer("parsing")
        result = await parse_demo(demo_path)
        log.info(end())
        return result

    async def uploader():
        log.info("uploading %s", archive_path)
        end = timer("upload")
        await upload_demo(archive_path, origin, identifier)
        log.info(end())

    tasks = [asyncio.create_task(parser())]

    if command.data_version is None:
        tasks.append(asyncio.create_task(uploader()))
    else:
        log.info("Skipping demo upload since data_version=%s", command.data_version)

    try:
        parser_result, *junk = await asyncio.gather(*tasks)
    except Exception:
        for task in tasks:
            task.cancel()
        raise

    delete_file(demo_path)
    delete_file(archive_path)

    data = parser_result
    if not data:
        raise MessageError("Failed parsing demo.")

    await publish(
        events.DemoParseSuccess(
            origin=origin,
            identifier=identifier,
            data=data,
            version=CSGO_DEMOPARSE_VERSION,
        )
    )


@handler(RequestPresignedUrl)
async def request_presigned_url(command: RequestPresignedUrl, publish, get_url):
    presigned_url = await get_url(command.origin, command.identifier, command.expires_in)
    await publish(events.PresignedUrlGenerated(command.origin, command.identifier, presigned_url))


async def main():
    if config.SENTRY_DSN:
        sentry_init(config.SENTRY_DSN)

    logging.getLogger("aio_pika").setLevel(logging.INFO)
    logging.getLogger("aiormq.connection").setLevel(logging.INFO)
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
    await broker.start(config.RABBITMQ_HOST, prefetch_count=2)

    log.info("Ready to parse!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
