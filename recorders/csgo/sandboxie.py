import logging
from asyncio import sleep
from subprocess import run

from psutil import pid_exists

log = logging.getLogger(__name__)


class Sandboxie:
    def __init__(self, start):
        self._start = start

    def run(self, *args, box=None) -> str:
        if box:
            args = [f"/box:{box}", *args]
        log.info(args)
        r = run([self._start, *args], capture_output=True)
        return r.stdout.decode("utf-8").strip()

    def start(self, *args, box=None):
        return self.run(*args, box=box)

    def listpids(self, box=None):
        box_pids = self.run("/listpids", box=box).split("\r\n")
        return [int(pid) for pid in box_pids[1:]]

    def terminateall(self, box=None):
        return self.run("/silent", "/terminateall", box=box)

    def terminate_all(self):
        return self.run("/silent", "/terminate_all")

    def reload(self):
        return self.run("/reload")

    async def cleanup(self, *boxes):
        log.info(f"Cleaning up boxes: {boxes}")

        pids = self._listpids_multiple(*boxes)

        if not pids:
            return

        log.info(f"Running pids: {pids}")

        for box in boxes:
            self.terminateall(box=box)

        await self._wait_pids_gone(pids)

    def _listpids_multiple(self, *boxes):
        pids = set()
        for box in boxes:
            pids.update(self.listpids(box=box))
        return pids

    async def _wait_pids_gone(self, pids):
        while True:
            await sleep(1)
            pids_running = [pid for pid in pids if pid_exists(pid)]
            if not pids_running:
                break
            log.info(f"Waiting for {len(pids_running)} to exit...")
            log.info(f"Remaining pids: {pids_running}")
