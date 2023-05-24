import sys


sys.path.append("../..")

import asyncio
import logging
import os
import subprocess
from concurrent.futures import ProcessPoolExecutor
from distutils.dir_util import copy_tree
from json import dumps, loads
from pathlib import Path
from shutil import rmtree
from urllib import parse

import aiofiles
import aiohttp
import config
from ipc import CSGO, RecordingError, SandboxedCSGO, random_string
from resource_semaphore import ResourcePool, ResourceRequest
from sandboxie import Sandboxie
from sandboxie_config import make_config
from script_builder import make_script
from websockets import client, InvalidStatusCode
from websockets.exceptions import ConnectionClosed

from messages import commands
from shared.log import logging_config
from shared.utils import decompress

logging_config(config.DEBUG)
log = logging.getLogger(__name__)

executor = ProcessPoolExecutor(max_workers=3)


def count(start: int):
    while True:
        yield start
        start += 1


new_port = iter(count(41920))
CHUNK_SIZE = 1024 * 1024

sb = Sandboxie(config.SANDBOXIE_START)


class RecordingError(Exception):
    pass


async def record(
    csgo: CSGO,
    demo: Path,
    command: commands.RequestRecording,
):
    output = config.TEMP_DIR / f"{command.job_id}.mp4"
    capture_dir = config.TEMP_DIR
    video_filters = config.VIDEO_FILTERS if command.color_filter else None

    if not demo.is_file():
        raise ValueError(f"Demo {demo} does not exist.")

    if command.end_tick < command.start_tick:
        raise ValueError("End tick must be after start tick")

    log.info(
        f"Recording player {command.player_xuid} from tick {command.start_tick} to {command.end_tick} with skips {command.skips}"
    )

    unblock_string = random_string()

    cmds = make_script(
        tickrate=command.tickrate,
        start_tick=command.start_tick,
        end_tick=command.end_tick,
        skips=command.skips,
        xuid=command.player_xuid,
        fps=command.fps,
        bitrate=command.video_bitrate,
        fragmovie=command.fragmovie,
        righthand=command.righthand,
        # is this really the right place to set this default?
        # E: yeah pretty much
        crosshair_code=command.crosshair_code or "CSGO-CRGTh-TOq2d-nhbkC-doNM6-tzioE",
        use_demo_crosshair=command.use_demo_crosshair,
        capture_dir=capture_dir,
        video_filters=video_filters,
        unblock_string=unblock_string,
    )

    script_file = config.TEMP_DIR / f"{command.job_id}.xml"
    cmds.save(script_file)

    preplay_commands = (
        "mirv_cmd clear",
        f'mirv_cmd load "{script_file}"',
        "mirv_deathmsg lifetime 0",
        # f"mirv_pov {command.player_entityid}",
    )

    await csgo.run(";".join(preplay_commands))

    if command.hq:
        await csgo.set_resolution(1920, 1080)
    else:
        await csgo.set_resolution(1280, 854)

    take_folder = await csgo.playdemo(
        demo=demo,
        unblock_string=unblock_string,
        start_at=command.start_tick - (6 * command.tickrate),
    )

    delete_file(script_file)

    log.info("Muxing in folder %s", take_folder)

    take_folder = config.TEMP_DIR / take_folder

    # mux audio
    wav = list(take_folder.glob("*.wav"))[0]
    subprocess.run(
        [
            config.FFMPEG_BIN,
            "-i",
            take_folder / "normal/video.mp4",
            "-i",
            wav,
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-b:a",
            f"{command.audio_bitrate}k",
            "-y",
            output,
        ],
        capture_output=True,
    )

    delete_folder(take_folder)
    return output


def craft_hlae_args(port):
    return (
        config.HLAE_BIN,
        "-csgoLauncher",
        "-noGui",
        "-autoStart",
        "-csgoExe",
        config.CSGO_BIN,
        "-gfxEnabled",
        "true",
        "-gfxWidth",
        str(1280),
        "-gfxHeight",
        str(854),
        "-gfxFull",
        "false",
        "-mmcfgEnabled",
        "true",
        "-mmcfg",
        config.MMCFG_DIR,
        "-customLaunchOptions",
        f"-netconport {port} -console -novid",
    )


async def make_sandboxed_csgo(sb: Sandboxie, box: str, sleep) -> CSGO:
    await sb.cleanup(box)
    await asyncio.sleep(3.0)

    if sleep is not None:
        await asyncio.sleep(sleep)

    sb.run(
        config.STEAM_BIN,
        "-nofriendsui",
        "-silent",
        "-offline",
        "-clearbeta",
        box=box,
    )

    await asyncio.sleep(32.0)

    port = next(new_port)

    sb.run(
        *craft_hlae_args(port),
        box=box,
    )

    return SandboxedCSGO(host="localhost", port=port, box=box)


def make_csgo(port):
    args = craft_hlae_args(port)
    subprocess.run(args)
    return CSGO("localhost", port=port)


async def on_csgo_error(pool: ResourcePool, csgo: CSGO, exc: Exception):
    if not isinstance(csgo, SandboxedCSGO):
        log.error("Recovering CSGO instances is only supported for sandboxed CSGO instances.")
        return

    box_name = csgo.box

    # cleanup the box
    await sb.cleanup(box_name)

    new_csgo = await make_sandboxed_csgo(sb, box=box_name, sleep=None)
    await prepare_csgo(new_csgo)

    pool.add(new_csgo)


async def prepare_csgo(csgo: CSGO):
    await csgo.connect()

    # causes issues with resolution changes
    # csgo.minimize()

    startup_commands = ('mirv_block_commands add 5 "\*"', "exec stream")
    for command in startup_commands:
        await csgo.run(command)
        await asyncio.sleep(0.5)


async def file_reader(file_name):
    async with aiofiles.open(file_name, "rb") as f:
        chunk = await f.read(CHUNK_SIZE)
        while chunk:
            yield chunk
            chunk = await f.read(CHUNK_SIZE)


def make_folder(path: Path):
    try:
        os.makedirs(path, exist_ok=True)
    except:
        pass


def delete_file(path: Path):
    if path.is_file():
        try:
            os.remove(path)
            log.info("Deleted file: %s", path)
        except:
            pass


def delete_folder(path: Path):
    try:
        rmtree(path)
        log.info("Deleted folder: %s", path)
    except:
        pass


async def handle_recording_request(
    command: commands.RequestRecording,
    session: aiohttp.ClientSession,
    pool: ResourcePool,
):
    origin_lower = command.demo_origin.lower()
    archive_name = os.path.basename(parse.urlparse(command.demo_url).path)

    archive_path = config.DEMO_DIR / f"{origin_lower}_{archive_name}"
    temp_archive_path = config.TEMP_DIR / f"{command.job_id}.dem.{archive_path.suffix}"
    demo_path = config.TEMP_DIR / f"{command.job_id}.dem"

    if not archive_path.is_file():
        log.info("Downloading archive...")
        try:
            async with session.get(
                url=command.demo_url, timeout=aiohttp.ClientTimeout(20.0)
            ) as resp:
                if resp.status == 200:
                    async with aiofiles.open(temp_archive_path, "wb") as f:
                        while not resp.content.at_eof():
                            await f.write(await resp.content.read(CHUNK_SIZE))
                else:
                    raise RecordingError("Failed fetching demo archive.")

                if not archive_path.is_file():
                    try:
                        os.rename(temp_archive_path, archive_path)
                    except OSError:
                        log.info(
                            "Failed renaming archive? %s -> %s", temp_archive_path, archive_path
                        )
        except (asyncio.TimeoutError, aiohttp.ClientConnectionError):
            raise RecordingError("Unable to download demo archive.")

    # decompress temp archive to temp demo file
    log.info("Decompressing archive...")
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(executor, decompress, archive_path, demo_path)
    except OSError:
        raise RecordingError("Demo corrupted.")

    log.info("Getting CSGO instance and starting recording...")
    async with ResourceRequest(pool) as csgo:
        log.info("Got CSGO instance: %s", csgo)
        video_file = await record(csgo, demo_path, command)

    log.info("Uploading to uploader service...")

    try:
        async with session.post(
            url=command.upload_url,
            params=dict(job_id=command.job_id),
            headers={"Authorization": config.API_TOKEN},
            data=file_reader(video_file),
            timeout=aiohttp.ClientTimeout(total=32.0),
        ) as resp:
            log.info("Upload for job %s status %s", command.job_id, resp.status)
            if resp.status != 200:
                raise RecordingError("Uploader service failed.")

    except (asyncio.TimeoutError, aiohttp.ClientConnectionError):
        raise RecordingError("Upload service did not respond to the upload request.")

    # delete stuff that is now junk...
    for path in (demo_path, temp_archive_path, video_file):
        delete_file(path)

    # delete the least recently used demo(s) that fall outside our cache size
    oldest = list(sorted(config.DEMO_DIR.iterdir(), key=lambda f: f.stat().st_atime))
    for file in oldest[: len(oldest) - config.KEEP_DEMO_COUNT]:
        delete_file(file)


class GatewayClient:
    def __init__(self, pool: ResourcePool) -> None:
        self.pool = pool
        self.session = aiohttp.ClientSession()

        self.queue = asyncio.Queue()

        self.websocket = None
        self.connected_event = asyncio.Event()

        self.job_ids = set()

    async def looper(self):
        while True:
            future = asyncio.Future()
            await self.queue.put(future)

            command: commands.RequestRecording = await future
            log.info("Received recording request %s", command)

            self.job_ids.add(command.job_id)

            try:
                await handle_recording_request(command, self.session, self.pool)
            except Exception as exc:
                log.exception(exc)
                reason = str(exc) if isinstance(exc, RecordingError) else "Recorder failed."
                await self.send("failure", dict(job_id=command.job_id, reason=reason))
            else:
                await self.send("success", dict(job_id=command.job_id))
            finally:
                try:
                    self.job_ids.remove(command.job_id)
                except KeyError:
                    pass

    async def send(self, *data):
        if not self.connected_event.is_set():
            await self.connected_event.wait()

        await self.websocket.send(dumps(data))

    async def connect(self, endpoint):
        for _ in self.pool:
            asyncio.create_task(self.looper())

        while True:
            self.connected_event.clear()

            try:
                websocket = await client.connect(
                    endpoint, extra_headers=dict(Authorization=config.API_TOKEN)
                )
            except ConnectionRefusedError as exc:
                log.warn("Timed out trying to connect...")
                continue
            except InvalidStatusCode as exc:
                log.warn("Connect returned code %s", exc.status_code)
                await asyncio.sleep(1.0)
                continue

            log.info("Connected!")

            self.websocket = websocket
            self.connected_event.set()

            # tell the gateway which job id's we're currently working on
            await websocket.send(dumps(list(self.job_ids)))

            try:
                while True:
                    getter = asyncio.create_task(self.queue.get())
                    waiter = asyncio.create_task(websocket.wait_closed())

                    finished, _ = await asyncio.wait(
                        [getter, waiter], return_when=asyncio.FIRST_COMPLETED
                    )

                    if waiter in finished:
                        # websocket closed, cancel getter
                        getter.cancel()
                        # and this should raise ConnectionClosed
                        await websocket.ensure_open()
                    else:
                        # getter finished first, cancel waiter
                        waiter.cancel()

                    future = getter.result()

                    await self.send("request", dict())
                    message = await websocket.recv()

                    data = loads(message)
                    command = commands.RequestRecording(**data)
                    future.set_result(command)
            except ConnectionClosed as exc:
                if exc.code == 1008:
                    raise  # invalid auth
                log.warn("Lost connection to gateway!")

            # requeue the current future if we .get()'d it and then failed
            if not future.done():
                log.info("Putting current future back in queue...")
                await self.queue.put(future)


async def main():
    global sb

    if not config.DEBUG:
        logging.getLogger("websockets").setLevel(logging.INFO)

    # copy over csgo config files
    copy_tree("cfg", str(config.CSGO_DIR / "cfg"))

    # delete temp dir
    delete_folder(config.TEMP_DIR)

    # ensure data folders exist
    for path in (config.TEMP_DIR, config.DEMO_DIR):
        make_folder(path)

    if config.SANDBOXED:
        sb.terminate_all()

        cfg = make_config(
            user=config.SANDBOXIE_USER,
            demo_dir=config.DEMO_DIR,
            temp_dir=config.TEMP_DIR,
            boxes=config.BOXES,
        )

        with open(config.SANDBOXIE_INI, "w", encoding="utf-16") as f:
            f.write(cfg)

        await asyncio.sleep(1.0)

        sb.reload()

        await asyncio.sleep(2.0)

        setups = [
            make_sandboxed_csgo(sb, box=box_name, sleep=idx * 5)
            for idx, box_name in enumerate(config.BOXES)
        ]

        csgos = await asyncio.gather(*setups)
    else:
        csgos = [make_csgo(next(new_port))]

    await asyncio.gather(*[prepare_csgo(csgo) for csgo in csgos])

    pool = ResourcePool(on_removal=on_csgo_error)

    for csgo in csgos:
        pool.add(csgo)
        csgo.set_connection_lost_callback(pool.on_removal)

    g = GatewayClient(pool)

    await g.connect(config.API_ENDPOINT)

    log.info("Ready to record!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
