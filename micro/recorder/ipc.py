import asyncio
import logging
import os
import re
import subprocess
import string
import random

log = logging.getLogger(__name__)

sep = '\r\n'


def random_string(length=32):
    return ''.join(random.choices(string.ascii_letters, k=length))


class CSGO:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter

    def __init__(
        self,
        hlae_exe,
        csgo_exe,
        mmcfg_dir,
        host='localhost',
        port='2121',
        width=1280,
        height=720,
    ):
        self.host = host
        self.port = port
        self.hlae_exe = hlae_exe
        self.csgo_exe = csgo_exe

        self.process = subprocess.Popen(
            [
                hlae_exe,
                '-csgoLauncher',
                '-noGui',
                '-autoStart',
                '-csgoExe',
                csgo_exe,
                '-gfxEnabled',
                'true',
                '-gfxWidth',
                str(width),
                '-gfxHeight',
                str(height),
                '-gfxFull',
                'false',
                '-mmcfgEnabled',
                'true',
                '-mmcfg',
                mmcfg_dir,
                '-customLaunchOptions',
                f'-netconport {port} -console -novid',
            ],
        )

    async def connect(self):
        log.info('Waiting for CSGO to launch...')
        while True:
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
                break
            except ConnectionRefusedError:
                continue

        self.reader = reader
        self.writer = writer

        await asyncio.sleep(2.0)
        log.info('Connected to CSGO!')

    async def run(self, command):
        log.info(command)

        start_token = random_string()
        end_token = random_string()

        await self.send_commands([f'echo {start_token}', command, f'echo {end_token}'])

        await self.readuntil(start_token)
        result = await self.readuntil(end_token)
        return result[0 : -len(end_token)].strip()

    async def readuntil(self, s):
        return (await self.reader.readuntil(s.encode())).decode()

    async def send_commands(self, commands):
        to_send = ''
        for command in commands:
            to_send += command + sep
        self.writer.write(to_send.encode())
        await self.writer.drain()

    async def set_resolution(self, w, h):
        await self.run(f'mat_setvideomode {w} {h} 1')

    async def playdemo(
        self, demo, vdm, return_take=False, unblock_at=None, start_at=None
    ):
        vdm_path = demo[:-3] + 'vdm'

        if os.path.isfile(vdm_path):
            os.remove(vdm_path)

        with open(vdm_path, 'w') as f:
            f.write(vdm.dumps())

        command = f'playdemo "{demo}'

        if start_at is not None:
            command += f'@{start_at}'

        command += '"'

        await self.run(command)

        take = None

        if return_take:
            while True:
                line = await self.readuntil(sep)
                if line.startswith('Recording to "'):
                    match = re.findall(r'Recording to \"(.*)\"\.', line)

                    take = match[0]
                    break

        if unblock_at is not None:
            await self.readuntil(unblock_at)
            os.remove(vdm_path)

        return take

    def kill(self):
        self.process.kill()
