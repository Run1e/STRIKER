import asyncio
import logging
import os
import random
import re
import string
from asyncio import shield, wait_for

log = logging.getLogger(__name__)

sep = "\r\n"


def random_string(length=32):
    return "".join(random.choices(string.ascii_letters, k=length))


class CSGO:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    def __init__(self, host, port, box=None):
        self.host = host
        self.port = port
        self.box = box

        self.log(f"Listening on port {port}")

    def log(self, *msgs):
        for msg in msgs:
            if self.box is not None:
                msg = f"[{self.box}] " + msg

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

        await asyncio.sleep(2.0)

        self.log("Connected to CSGO!")

    async def run(self, command):
        self.log(command)

        start_token = random_string()
        end_token = random_string()

        await self.send_commands([f"echo {start_token}", command, f"echo {end_token}"])

        await self.readuntil(start_token, timeout=15.0)
        result = await self.readuntil(end_token, timeout=15.0)
        return result[0 : -len(end_token)].strip()

    async def readuntil(self, s, timeout):
        result = await shield(wait_for(self.reader.readuntil(s.encode()), timeout))
        return result.decode()

    async def send_commands(self, commands, timeout=10.0):
        to_send = ""
        for command in commands:
            to_send += command + sep
        self.writer.write(to_send.encode())
        await shield(wait_for(self.writer.drain(), timeout))

    async def set_resolution(self, w, h):
        await self.run(f"mat_setvideomode {w} {h} 1")

    async def playdemo(self, demo, vdm, unblock_at=None, start_at=None):
        vdm_path = demo[:-3] + "vdm"

        if os.path.isfile(vdm_path):
            os.remove(vdm_path)

        with open(vdm_path, "w") as f:
            f.write(vdm.dumps())

        command = f'playdemo "{demo}'

        if start_at is not None:
            command += f"@{start_at}"

        command += '"'

        await self.run(command)

        take = None

        self.log("Waiting for recording to start...")
        while True:
            line = await self.readuntil(sep, timeout=40.0)
            if line.startswith('Recording to "'):
                match = re.findall(r"Recording to \"(.*)\"\.", line)

                take = match[0]

                self.log(f"Recording to {take}")
                break

        self.log(f'Waiting for unblock string "{unblock_at}"')
        await self.readuntil(unblock_at, timeout=240.0)

        try:
            os.remove(vdm_path)
        except OSError:
            pass

        return take

    def kill(self):
        self.process.kill()
