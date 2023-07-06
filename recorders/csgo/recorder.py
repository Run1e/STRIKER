import sys

sys.path.append("../..")

import asyncio
import logging
import os
import re
import subprocess
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict
from distutils.dir_util import copy_tree
from json import dumps, loads
from pathlib import Path
from urllib import parse
from uuid import uuid4

import aiofiles
import aiohttp
import config
from ipc import CSGO, RecordingError, SandboxedCSGO, random_string
from sandboxie import Sandboxie
from sandboxie_config import make_config
from script_builder import make_script
from websockets import InvalidStatusCode, client
from websockets.exceptions import ConnectionClosed

from messages import commands, events
from messages.broker import MessageError
from messages.bus import MessageBus
from shared.log import logging_config
from shared.utils import (
    RunError,
    decompress,
    delete_file,
    delete_folder,
    download_file,
    make_folder,
    rename_file,
    run,
    sentry_init,
)

logging_config(config.DEBUG)
log = logging.getLogger(__name__)

executor = ProcessPoolExecutor(max_workers=3)


def count(start: int):
    while True:
        yield start
        start += 1


new_port = iter(count(41920))
CHUNK_SIZE = 1024 * 1024


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
        f'mirv_cmd load "{script_file.absolute()}"',
        "mirv_deathmsg lifetime 0",
        # f"mirv_pov {command.player_entityid}",
    )

    await csgo.run(";".join(preplay_commands))

    if command.hq:
        await csgo.set_resolution(1920, 1080)
    else:
        await csgo.set_resolution(1280, 854)

    error = None
    take_folders = list()

    def folder_checker(line):
        if line.startswith('Recording to "'):
            match = re.findall(r"Recording to \"(.*)\"\.", line)
            folder = match[0]
            log.info("Found take folder: %s", folder)
            take_folders.append(config.TEMP_DIR / folder)

        return False

    def checker(line):
        nonlocal error
        if re.match(r"^Missing map .*, disconnecting$", line):
            error = RecordingError("Demos that require old maps are not supported.")
            return True

        elif line == unblock_string + " ":
            return True

        return False

    take_folder_task = asyncio.create_task(csgo.wait_for(check=folder_checker, timeout=250.0))

    await csgo.playdemo(
        demo=demo.absolute(),
        start_at=command.start_tick - (6 * command.tickrate),
    )

    try:
        await csgo.wait_for(check=checker, timeout=240.0)
    except:
        raise
    finally:
        take_folder_task.cancel()

    if error:
        raise error

    delete_file(script_file)

    parts = list()
    coros = []

    # mux audio
    for idx, take_folder in enumerate(take_folders):
        take_output = take_folder / f"{command.job_id}-{idx}.mp4"
        parts.append(take_output)
        log.info("Muxing in %s", take_folder)
        wav = list(take_folder.glob("*.wav"))[0]
        coro = run(
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
            take_output,
        )

        coros.append(coro)

    await asyncio.gather(*coros)

    part_file = config.TEMP_DIR / f"{command.job_id}-parts.txt"
    with open(part_file, "w") as f:
        f.write("\n".join(f"file '{part.absolute()}'" for part in parts))

    await run(
        config.FFMPEG_BIN,
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        part_file.absolute(),
        "-c",
        "copy",
        output.absolute(),
    )

    delete_file(part_file)

    for take_folder in take_folders:
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


async def prepare_csgo(csgo: CSGO):
    await csgo.connect()

    # causes issues with resolution changes
    # csgo.minimize()

    startup_commands = (
        'mirv_block_commands add 5 "\*"',
        "exec recorder",
        "exec stream",
    )

    for command in startup_commands:
        await csgo.run(command)
        await asyncio.sleep(0.5)


async def file_reader(file_name):
    async with aiofiles.open(file_name, "rb") as f:
        chunk = await f.read(CHUNK_SIZE)
        while chunk:
            yield chunk
            chunk = await f.read(CHUNK_SIZE)


def ensure_cleanup(f):
    async def cleanup_wrapper(*args, **kwargs):
        files = []
        try:
            result = await f(*args, **kwargs, cleanup_files=files)
        finally:
            for file in files:
                delete_file(file)

            # delete the least recently used demo(s) that fall outside our cache size
            oldest = list(sorted(config.DEMO_DIR.iterdir(), key=lambda f: f.stat().st_atime))
            for file in oldest[: len(oldest) - config.KEEP_DEMO_COUNT]:
                delete_file(file)

        return result

    return cleanup_wrapper


@ensure_cleanup
async def handle_recording_request(
    command: commands.RequestRecording,
    session: aiohttp.ClientSession,
    csgo: CSGO,
    cleanup_files: list,
):
    origin_lower = command.demo_origin.lower()
    archive_name = os.path.basename(parse.urlparse(command.demo_url).path)

    archive_path = config.DEMO_DIR / f"{origin_lower}_{archive_name}"
    temp_archive_path = config.TEMP_DIR / f"{command.job_id}.dem{archive_path.suffix}"
    demo_path = config.TEMP_DIR / f"{command.job_id}.dem"

    cleanup_files.append(demo_path)
    cleanup_files.append(temp_archive_path)

    if not archive_path.is_file():
        try:
            log.info("Download demo archive...")
            await download_file(command.demo_url, temp_archive_path, timeout=32.0)
        except (asyncio.TimeoutError, RunError) as exc:
            raise MessageError("Failed downloading demo archive.") from exc

        if not archive_path.is_file():
            rename_file(temp_archive_path, archive_path)

    # decompress temp archive to temp demo file
    log.info("Decompressing archive...")
    try:
        await decompress(archive_path, demo_path)
    except (asyncio.TimeoutError, RunError) as exc:
        # if we fail decompressing, delete the archive as well
        cleanup_files.append(archive_path)
        raise MessageError("Failed extracting demo archive.") from exc

    log.info("CSGO instance: %s", csgo)
    video_file = await record(csgo, demo_path, command)
    cleanup_files.append(video_file)

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

    except (asyncio.TimeoutError, aiohttp.ClientConnectionError) as exc:
        raise RecordingError("Upload service did not respond to the upload request.") from exc


class GatewayClient:
    def __init__(self, sandboxed: bool) -> None:
        self.name = str(uuid4())[:8]
        self.sandboxed = sandboxed

        self.bus = MessageBus()
        self.bus.add_command_handler(commands.RequestRecording, self.request_recording)

        self.instances = defaultdict(set)
        self.instance_stop_event = defaultdict(asyncio.Event)

        self.recording_job_ids = set()

        self.sb: Sandboxie | None = Sandboxie(config.SANDBOXIE_START) if sandboxed else None

        self.session = aiohttp.ClientSession()
        self.queues: dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

        self.websocket = None
        self.connected_event = asyncio.Event()

        self.message_type_lookup = {
            "RequestRecording": commands.RequestRecording,
        }

    async def connect(self, endpoint):
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

            await self.send(
                events.GatewayClientHello(client_name=self.name, job_ids=[*self.recording_job_ids])
            )

            for game_name, queue in self.queues.items():
                for _ in queue._getters:
                    await self.send(
                        events.GatewayClientWaiting(client_name=self.name, game=game_name)
                    )

            try:
                async for message in self.websocket:
                    _type, data = loads(message)
                    message_type = self.message_type_lookup[_type]
                    msg = message_type(**data)
                    asyncio.create_task(self.bus.dispatch(msg))

            except ConnectionClosed as exc:
                if exc.code == 1008:
                    raise  # invalid auth
                log.warn("Lost connection to gateway!")

    async def send(self, message: commands.Command | events.Event):
        if not self.connected_event.is_set():
            await self.connected_event.wait()

        name = message.__class__.__name__
        dictified = asdict(message)
        await self.websocket.send(dumps([name, dictified]))

    async def start(self):
        if self.sandboxed:
            self.sb.terminate_all()

            cfg = make_config(
                user=config.SANDBOXIE_USER,
                demo_dir=config.DEMO_DIR,
                temp_dir=config.TEMP_DIR,
                boxes=config.BOXES,
            )

            with open(config.SANDBOXIE_INI, "w", encoding="utf-16") as f:
                f.write(cfg)

            await asyncio.sleep(1.0)

            self.sb.reload()

            await asyncio.sleep(2.0)

            setups = [
                make_sandboxed_csgo(self.sb, box=box_name, sleep=idx * 5)
                for idx, box_name in enumerate(config.BOXES)
            ]

            csgos = await asyncio.gather(*setups)
        else:
            csgos = [make_csgo(next(new_port))]

        await asyncio.gather(*[prepare_csgo(csgo) for csgo in csgos])

        for csgo in csgos:
            self.instance_add(csgo)

    def instance_add(self, instance: CSGO):
        log.info("Added game instance: %s", instance.name)

        self.instances[instance.name].add(instance)
        instance.set_connection_lost_callback(self.instance_remove)
        asyncio.create_task(self.instance_loop(instance))

    async def instance_remove(self, instance, exc):
        log.error("Removing instance for game %s", instance.name)
        log.exception(exc)

        event = self.instance_stop_event.pop(instance, None)
        if event:
            event.set()

        instances = self.instances[instance.name]
        try:
            instances.remove(instance)
        except KeyError:
            pass

        if not isinstance(instance, SandboxedCSGO):
            raise TypeError("Unable to recreate non-sandboxed CSGO instances!")

        new_instance = await make_sandboxed_csgo(self.sb, box=instance.box, sleep=None)
        await prepare_csgo(new_instance)

        self.instance_add(new_instance)

        log.info("New instance created!")

    async def instance_loop(self, instance):
        queue = self.queues[instance.name]
        event = self.instance_stop_event[instance]

        while True:
            await self.send(events.GatewayClientWaiting(client_name=self.name, game=instance.name))

            queue_task = asyncio.create_task(queue.get())
            event_task = asyncio.create_task(event.wait())
            done, _ = await asyncio.wait(
                [queue_task, event_task], return_when=asyncio.FIRST_COMPLETED
            )

            if event_task in done:
                log.info("Stopping instance loop (direct) %s", instance)
                queue_task.cancel()
                return

            if queue_task in done:
                command: commands.RequestRecording = await queue_task
                event_task.cancel()

            self.recording_job_ids.add(command.job_id)

            try:
                await self.send(events.RecordingProgression(command.job_id, infront=0))
                await handle_recording_request(command, self.session, instance)
            except Exception as exc:
                log.exception(exc)
                reason = str(exc) if isinstance(exc, RecordingError) else "Recorder failed."
                await self.send(events.RecorderFailure(job_id=command.job_id, reason=reason))
            else:
                await self.send(events.RecorderSuccess(job_id=command.job_id))
            finally:
                self.recording_job_ids.remove(command.job_id)

            if event.is_set():
                log.info("Stopping instance loop (post) %s", instance)
                return

    async def request_recording(self, command: commands.RequestRecording):
        queue = self.queues[command.game]
        await queue.put(command)


async def main():
    global sb

    if config.SENTRY_DSN:
        sentry_init(config.SENTRY_DSN)

    if not config.DEBUG:
        logging.getLogger("websockets").setLevel(logging.INFO)

    # copy over csgo config files
    copy_tree("cfg", str(config.CSGO_DIR / "cfg"))

    # delete temp dir
    delete_folder(config.TEMP_DIR)

    # ensure data folders exist
    for path in (config.TEMP_DIR, config.DEMO_DIR):
        make_folder(path)

    g = GatewayClient(sandboxed=config.SANDBOXED)
    await g.start()
    await g.connect(config.API_ENDPOINT)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
