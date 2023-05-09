import sys

sys.path.append("../..")

import asyncio
import logging
import os
import subprocess
from concurrent.futures import ProcessPoolExecutor
from distutils.dir_util import copy_tree
from functools import partial
from glob import glob
from json import dumps, loads
from shutil import rmtree

import aiofiles
import aiohttp
import config
from ipc import CSGO, RecordingError, SandboxedCSGO, random_string
from resource_semaphore import ResourcePool, ResourceRequest
from sandboxie import Sandboxie
from sandboxie_config import make_config
from script_builder import make_script
from websockets import client
from websockets.exceptions import ConnectionClosed

from messages import commands
from shared.log import logging_config
from shared.utils import DemoCorrupted, decompress

logging_config(config.DEBUG)
log = logging.getLogger(__name__)

executor = ProcessPoolExecutor(max_workers=3)


def count(start: int):
    while True:
        yield start
        start += 1


new_port = iter(count(41920))
# CHUNK_SIZE = 4 * 1024 * 1024
CHUNK_SIZE = 64 * 1024

sb = Sandboxie(config.SANDBOXIE_START)


class RecordingError(Exception):
    pass


async def record(
    csgo: CSGO,
    demo: str,
    command: commands.RequestRecording,
):
    output = f"{config.TEMP_DIR}/{command.job_id}.mp4"
    capture_dir = config.TEMP_DIR
    video_filters = config.VIDEO_FILTERS if command.color_filter else None

    if not os.path.isfile(demo):
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
        crosshair_code=command.crosshair_code,
        use_demo_crosshair=command.use_demo_crosshair,
        capture_dir=capture_dir,
        video_filters=video_filters,
        unblock_string=unblock_string,
    )

    script_file = f"{config.TEMP_DIR}/{command.job_id}.xml"
    cmds.save(script_file)

    await csgo.run(f'mirv_cmd clear; mirv_cmd load "{script_file}"')

    # change res
    await csgo.set_resolution(1280, 852)

    # make sure deathmsg doesn't fill up and clear lock spec
    await csgo.run(f"mirv_deathmsg lifetime 0")

    take_folder = await csgo.playdemo(
        demo=demo,
        unblock_string=unblock_string,
        start_at=command.start_tick - (6 * command.tickrate),
    )

    # mux audio
    wav = glob(take_folder + r"\*.wav")[0]
    subprocess.run(
        [
            config.FFMPEG_BIN,
            "-i",
            take_folder + r"\normal\video.mp4",
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

    rmtree(take_folder)

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
        # '-nocache',
        "-nofriendsui",
        "-silent",
        "-offline",
        # '-login',
        # config.STEAM_USER,
        # config.STEAM_PASS,
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
    subprocess.run(craft_hlae_args(port))
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
    csgo.minimize()

    startup_commands = ('mirv_block_commands add 5 "\*"', "exec stream")
    for command in startup_commands:
        await csgo.run(command)
        await asyncio.sleep(0.5)


async def file_sender(file_name):
    async with aiofiles.open(file_name, "rb") as f:
        chunk = await f.read(CHUNK_SIZE)
        while chunk:
            yield chunk
            chunk = await f.read(CHUNK_SIZE)


async def handle_recording_request(
    command: commands.RequestRecording,
    session: aiohttp.ClientSession,
    pool: ResourcePool,
):
    demo_dir = f"{config.TEMP_DIR}/{command.demo_origin.lower()}"
    archive_dir = f"{config.DEMO_DIR}/{command.demo_origin.lower()}"

    # ensure this demos origin type has a folder
    for _dir in (demo_dir, archive_dir):
        try:
            os.makedirs(_dir)
        except FileExistsError:
            pass

    archive_path = f"{archive_dir}/{command.demo_identifier}.dem.bz2"
    demo_path = f"{demo_dir}/{command.demo_identifier}.dem"

    has_archive = os.path.isfile(archive_path)
    has_demo = os.path.isfile(demo_path)

    if not has_archive:
        timeout = aiohttp.ClientTimeout(20.0)
        async with session.get(command.demo_url, timeout=timeout) as resp:
            if resp.status == 200:
                async with aiofiles.open(archive_path, "wb") as f:
                    while not resp.content.at_eof():
                        await f.write(await resp.content.read(CHUNK_SIZE))
            else:
                pass  # TODO: handle

    if not has_archive or not has_demo:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(executor, decompress, archive_path, demo_path)
        except DemoCorrupted:
            raise RecordingError("Demo corrupted.")

    async with ResourceRequest(pool) as csgo:
        video_file = await record(csgo, demo_path, command)

    timeout = aiohttp.ClientTimeout(total=32.0)
    async with aiofiles.open(video_file, "rb") as f:
        async with session.post(
            command.upload_url,
            params=dict(job_id=command.job_id, upload_token=command.upload_token),
            data=file_sender(video_file),
            timeout=timeout,
        ) as resp:
            if resp.status != 200:
                return  # TODO: handle


async def start_ws(recording_request_handler):
    async def send(c: str, d: dict = None):
        await websocket.send(dumps([c, d or {}]))

    # retry connection forever
    while True:
        try:
            websocket = await client.connect(
                config.API_ENDPOINT, extra_headers=dict(Authorization=config.API_TOKEN)
            )
        except ConnectionRefusedError as exc:
            log.warn("Timed out trying to connect...")
            continue

        log.info("Connected!")

        while True:
            try:
                await send("request")
                request = await websocket.recv()
                action, data = loads(request)

                if action == "record":
                    try:
                        command = commands.RequestRecording(**data)
                        log.info("Received recording request %s", command)
                        await recording_request_handler(command)
                    except Exception as exc:
                        reason = (
                            str(exc) if isinstance(exc, RecordingError) else "Recording failed :("
                        )
                        await send("failure", dict(job_id=command.job_id, reason=reason))
                    else:
                        await send("success", dict(job_id=command.job_id))

            except ConnectionClosed as exc:
                if exc.code == 1008:
                    raise  # invalid auth
                log.warn("Lost connection to gateway!")
                break  # retry


async def main():
    global sb

    # logging.getLogger("websockets").setLevel(logging.INFO)

    # copy over csgo config files
    copy_tree("cfg", config.CSGO_DIR + "/cfg")

    # delete temp dir
    try:
        rmtree(config.TEMP_DIR)
    except FileNotFoundError:
        pass

    # ensure data folders exist
    for path in (config.TEMP_DIR, config.DEMO_DIR):
        try:
            os.makedirs(path)
        except FileExistsError:
            pass

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

    for _ in csgos:
        asyncio.create_task(
            start_ws(
                partial(
                    handle_recording_request,
                    session=aiohttp.ClientSession(),
                    pool=pool,
                )
            )
        )

    log.info("Ready to record!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
