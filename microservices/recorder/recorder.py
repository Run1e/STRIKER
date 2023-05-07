import sys

sys.path.append("../..")

import asyncio
import logging
import os
import subprocess
from distutils.dir_util import copy_tree
from glob import glob
from json import dumps, loads
from shutil import rmtree

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

logging_config(config.DEBUG)
log = logging.getLogger(__name__)


def count(start: int):
    while True:
        yield start
        start += 1


new_port = iter(count(41920))

sb = Sandboxie(config.SANDBOXIE_START)


class RecordingError(Exception):
    pass


async def record(
    csgo: CSGO,
    demo: str,
    command: commands.RequestRecording,
):
    output = f"video/{command.job_id}.mp4"
    capture_dir = config.TEMP_DIR
    video_filters = config.VIDEO_FILTERS if command.color_filter else None

    if not os.path.isfile(demo):
        raise ValueError(f"Demo {demo} does not exist.")

    if command.end_tick < command.start_tick:
        raise ValueError("End tick must be after start tick")

    log.info(
        f"Recording player {command.xuid} from tick {command.start_tick} to {command.end_tick} with skips {command.skips}"
    )

    unblock_string = random_string()

    cmds = make_script(
        tickrate=command.tickrate,
        start_tick=command.start_tick,
        end_tick=command.end_tick,
        skips=command.skips,
        xuid=command.xuid,
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


async def start_ws(pool):
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
            except ConnectionClosed as exc:
                if exc.code == 1008:
                    raise
                log.warn("Lost connection to gateway!")
                break

            command, data = loads(request)
            cmd = commands.RequestRecording(**data)
            async with ResourceRequest(pool) as csgo:
                await record(csgo, cmd)


async def main():
    global sb

    logging.getLogger("websockets").setLevel(logging.INFO)

    # copy over csgo config files
    copy_tree("cfg", config.CSGO_DIR + "/cfg")

    # delete temp dir
    rmtree(config.TEMP_DIR)

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
        asyncio.create_task(start_ws(pool))

    log.info("Ready to record!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
