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
        return self.run("/listpids", box=box).split("\r\n")

    def terminateall(self, box=None):
        self.run("/terminateall", box=box)

    async def cleanup(self, *boxes):
        log.info(f"Cleaning up boxes: {boxes}")

        pids = set()
        for box in boxes:
            box_pids = self.listpids(box=box)[1:]
            pids.update(int(pid) for pid in box_pids)

        if not pids:
            return

        log.info(f"Running pids: {pids}")

        for box in boxes:
            self.terminateall(box=box)

        while True:
            await sleep(1)
            pids_running = [pid for pid in pids if pid_exists(pid)]
            if not pids_running:
                break
            log.info(f"Waiting for {len(pids_running)} to exit...")
            log.info(f"Remaining pids: {pids_running}")
