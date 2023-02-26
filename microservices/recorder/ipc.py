import asyncio
import logging
import os
import random
import re
import string
from asyncio import shield, wait_for

log = logging.getLogger(__name__)

sep = "\r\n"


class RecordingError(Exception):
    pass


def random_string(length=32):
    return "".join(random.choices(string.ascii_letters, k=length))


class CSGO:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.checks = {}
        self.results = {}

        self.log(f"Starting CSGO on port {port}")

    def log(self, *msgs):
        for msg in msgs:
            log.info(msg)

    async def connect(self):
        self.log("Waiting for CSGO to launch...")

        while True:
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
                break
            except ConnectionRefusedError:
                continue

        self.reader = reader
        self.writer = writer

        asyncio.create_task(self.read_loop())

        self.log("Connected to CSGO!")

    async def read_loop(self):
        while True:
            line = await self.reader.readline()
            line = line[:-2].decode()
            to_remove = set()

            for check, event in self.checks.items():
                try:
                    val = check(line)
                except:
                    to_remove.add(check)

                if val:
                    self.results[check] = line
                    to_remove.add(check)
                    event.set()

            for remove in to_remove:
                self.checks.pop(remove)

    async def wait_for(self, check, timeout=60.0):
        event = asyncio.Event()
        self.checks[check] = event

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            self.checks.pop(check)
            raise exc

        results = self.results[check]
        self.results.pop(check)
        return results

    async def wait_for_many(self, timeout=60.0, **checks):
        matched = None

        def checker(line):
            nonlocal matched

            for name, check in checks.items():
                if check(line):
                    matched = name
                    return True

            return False

        result = await self.wait_for(check=checker, timeout=timeout)
        return matched, result

    async def run(self, command):
        self.log(command)

        start_token = random_string()
        end_token = random_string()

        listen = False
        output = list()

        def aggregator(line):
            nonlocal output, listen
            if not listen:
                if line.startswith(start_token):
                    listen = True
            else:
                if line.startswith(end_token):
                    return True
                output.append(line)
            return False

        task = asyncio.create_task(self.wait_for(check=aggregator, timeout=8.0))

        await self.send_commands([f"echo {start_token}", command, f"echo {end_token}"])
        await task

        return output

    async def send_commands(self, commands, timeout=4.0):
        to_send = ""
        for command in commands:
            to_send += command + sep
        self.writer.write(to_send.encode())
        await shield(wait_for(self.writer.drain(), timeout))

    async def set_resolution(self, w, h):
        await self.run(f"mat_setvideomode {w} {h} 1")

    async def playdemo(self, demo, vdm, unblock_string, start_at=None):
        # disconnect in case we're stuck in another demo playback
        await self.run("disconnect")

        vdm_path = demo[:-3] + "vdm"

        if os.path.isfile(vdm_path):
            os.remove(vdm_path)

        with open(vdm_path, "w") as f:
            f.write(vdm.dumps())

        command = f'playdemo "{demo}'

        if start_at is not None:
            command += f"@{start_at}"

        command += '"'

        task = asyncio.create_task(
            self.wait_for_many(
                rec=lambda line: line.startswith('Recording to "'),
                missingmap=lambda line: re.match(
                    r"^Missing map .*, disconnecting$", line
                ),
                timeout=60.0,
            )
        )

        await self.run(command)

        self.log("Waiting for recording to start...")
        result, line = await task

        take = None

        if result == "rec":
            match = re.findall(r"Recording to \"(.*)\"\.", line)
            take = match[0]
            self.log(f"Recording to {take}")

        elif result == "missingmap":
            raise RecordingError("Demos that require old maps are not supported.")

        await self.wait_for(lambda line: line == unblock_string + " ", timeout=120.0)

        self.log("Recording completed")

        try:
            os.remove(vdm_path)
        except OSError:
            pass

        return take

    def kill(self):
        self.process.kill()


class SandboxedCSGO(CSGO):
    def __init__(self, host, port, box):
        self.box = box
        super().__init__(host, port)

    def log(self, *msgs):
        for msg in msgs:
            msg = f"[{self.box}] " + msg
            log.info(msg)
