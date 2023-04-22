import sys

sys.path.append("../..")

import asyncio
import logging
import os
import subprocess
from distutils.dir_util import copy_tree
from glob import glob
from shutil import rmtree

import aiormq
import aiormq.types
import config
from ipc import CSGO, RecordingError, SandboxedCSGO, random_string
from resource_semaphore import ResourcePool, ResourceRequest
from sandboxie import Sandboxie
from sandboxie_config import make_config
from script_builder import make_script

from shared.log import logging_config
from shared.message import MessageError, MessageWrapper

logging_config(config.DEBUG)
log = logging.getLogger(__name__)

if config.SANDBOXED:
    sb = Sandboxie(config.SANDBOXIE_START)

    current_port = config.PORT_START

    def new_port():
        global current_port

        return_port = current_port
        current_port += 1
        return return_port


async def on_message(message: aiormq.channel.DeliveredMessage):
    wrap = MessageWrapper(
        message=message,
        default_error="An error occurred while recording.",
        ack_on_failure=False,
        raise_on_message_error=True,
        requeue_on_nack=True,
    )

    async with ResourceRequest(pool) as csgo, wrap as ctx:
        await record(csgo, **ctx.data)
        await ctx.success()


async def record(
    csgo: CSGO,
    job_id: str,
    matchid: int,
    tickrate: int,
    start_tick: int,
    end_tick: int,
    xuid: int,
    fps: int,
    resolution: tuple,
    video_bitrate: int,
    audio_bitrate,
    skips: list,
    color_correction: bool,
    **kwargs,
):
    demo = rf"{config.DEMO_DIR}\{matchid}.dem"
    output = rf"{config.VIDEO_DIR}\{job_id}.mp4"
    capture_dir = config.TEMP_DIR
    video_filters = config.VIDEO_FILTERS if color_correction else None

    if not os.path.isfile(demo):
        raise ValueError(f"Demo {demo} does not exist.")

    if end_tick < start_tick:
        raise ValueError("End tick must be after start tick")

    log.info(f"Recording player {xuid} from tick {start_tick} to {end_tick} with skips {skips}")

    unblock_string = random_string()

    commands = make_script(
        tickrate=tickrate,
        start_tick=start_tick,
        end_tick=end_tick,
        skips=skips,
        xuid=xuid,
        fps=fps,
        bitrate=video_bitrate,
        capture_dir=capture_dir,
        video_filters=video_filters,
        unblock_string=unblock_string,
    )

    script_file = f"{config.SCRIPT_DIR}/{job_id}.xml"
    commands.save(script_file)

    await csgo.run(f'mirv_cmd clear; mirv_cmd load "{script_file}"')

    # change res
    await csgo.set_resolution(*resolution)

    # make sure deathmsg doesn't fill up and clear lock spec
    await csgo.run(f"mirv_deathmsg lifetime 0")

    try:
        take_folder = await csgo.playdemo(
            demo=demo,
            unblock_string=unblock_string,
            start_at=start_tick - (6 * tickrate),
        )
    except RecordingError as exc:
        raise MessageError(exc.args[0])

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
            f"{audio_bitrate}k",
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

    port = new_port()

    sb.run(
        *craft_hlae_args(port),
        box=box,
    )

    return SandboxedCSGO(host="localhost", port=port, box=box)


def make_csgo(port):
    subprocess.run(craft_hlae_args(config.PORT_START))

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

    log_name = csgo.box if isinstance(csgo, SandboxedCSGO) else "csgo"

    def return_checker():
        f = open(rf"{config.CSGO_LOG_DIR}\{log_name}.log", "w")

        def checker(line):
            f.write(line + "\n")
            f.flush()

        return checker

    csgo.checks[return_checker()] = asyncio.Event()


pool = ResourcePool(on_removal=on_csgo_error)


async def main():
    global sb

    logging.getLogger("aiormq").setLevel(logging.INFO)

    # copy over csgo config files
    copy_tree("cfg", config.CSGO_DIR + "/cfg")

    # empty temp dir
    for entry in os.listdir(config.TEMP_DIR):
        try:
            rmtree(f"{config.TEMP_DIR}/{entry}")
            log.info("Removed %s", entry)
        except:
            pass

    if config.SANDBOXED:
        sb.terminate_all()

        cfg = make_config(
            user=config.SANDBOXIE_USER,
            data_dir=config.DATA_DIR,
            temp_dir=config.TEMP_DIR,
            log_dir=config.CSGO_LOG_DIR,
            boxes=config.BOXES,
        )

        with open(config.SANDBOXIE_INI, "w", encoding="utf-16") as f:
            f.write(cfg)

        sb.reload()

        setups = [
            make_sandboxed_csgo(sb, box=box_name, sleep=idx * 5)
            for idx, box_name in enumerate(config.BOXES)
        ]

        csgos = await asyncio.gather(*setups)
    else:
        csgos = [make_csgo(config.PORT_START)]

    await asyncio.gather(*[prepare_csgo(csgo) for csgo in csgos])

    for csgo in csgos:
        pool.add(csgo)
        csgo.set_connection_lost_callback(pool.on_removal)

    mq = await aiormq.connect(config.RABBITMQ_HOST)
    chan = await mq.channel()

    await chan.basic_qos(prefetch_count=len(config.BOXES) if config.SANDBOXED else 1)

    # await chan.queue_declare(config.RECORDER_QUEUE)
    await chan.basic_consume(
        queue=config.RECORDER_QUEUE, consumer_callback=on_message, no_ack=False
    )

    log.info("Ready to record!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
