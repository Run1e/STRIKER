import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from shutil import rmtree
import signal
from time import monotonic

import sentry_sdk
from sentry_sdk.integrations.asyncio import AsyncioIntegration

log = logging.getLogger(__name__)


def sentry_init(dsn):
    sentry_sdk.init(
        dsn=dsn,
        integrations=[
            AsyncioIntegration(),
        ],
        traces_sample_rate=0.2,
    )


class RunError(Exception):
    def __init__(self, *args: object, code: int = None) -> None:
        super().__init__(*args)
        self.code = code


class CurlError(RunError):
    def __init__(self, *args: object, code: int = None, http_code: int = None) -> None:
        super().__init__(*args, code=code)
        self.http_code = http_code


async def run(program: str, *args, timeout: float = 8.0):
    proc = None

    try:
        async with asyncio.timeout(timeout):
            log.info("%s %s", program, " ".join(str(arg) for arg in args))

            proc = await asyncio.create_subprocess_exec(
                program,
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await proc.communicate()
            return proc.returncode, stdout.decode(), stderr.decode()
    except asyncio.TimeoutError:
        if proc is not None:
            proc.kill()
        raise


async def download_file(url: str, file: Path, timeout: float = 8.0) -> bool:
    code, stdout, stderr = await run(
        "curl",
        "--connect-timeout",
        "8",
        "--max-time",
        str(timeout),
        "-s",
        "-w",
        "%{http_code}",
        "-o",
        file,
        url,
        timeout=timeout,
    )

    if code != 0 or stdout != "200":
        try:
            http_code = int(stdout.strip())
        except ValueError:
            http_code = 0

        raise CurlError(code=code, http_code=http_code)


async def decompress(archive: Path, file: Path):
    suffix = archive.suffix

    if suffix == ".gz":
        program = "gzip"
    elif suffix == ".bz2":
        program = "bzip2"
    else:
        raise RunError("Unknown archive suffix")

    if file.is_file():
        delete_file(file)

    code, stdout, stderr = await run(program, "-dk", archive, timeout=32.0)

    if code != 0:
        raise RunError(code=code)

    decompressed = archive.parent / archive.stem
    rename_file(decompressed, file)


def rename_file(path: Path, dst: Path):
    try:
        path.rename(dst)
    except FileExistsError:
        delete_file(dst)
        path.rename(dst)


def delete_file(path: Path):
    if path.is_file():
        try:
            path.unlink(missing_ok=True)
            log.info("Deleted file: %s", path)
        except:
            pass


def make_folder(path: Path):
    if not path.is_dir():
        try:
            os.makedirs(path, exist_ok=True)
            log.info("Created directory: %s", path)
        except:
            pass


def delete_folder(path: Path):
    if path.is_dir():
        try:
            rmtree(path)
            log.info("Deleted folder: %s", path)
        except:
            pass


def utcnow():
    return datetime.now(timezone.utc)


def timer(name):
    start = monotonic()
    return lambda: f"{name} took {monotonic() - start:0.2f} seconds"


def add_signal_handlers():
    if os.name == "nt":
        return []

    loop = asyncio.get_event_loop()
    close_tasks = []

    async def shutdown(sig: signal.Signals) -> None:
        """
        Cancel all running async tasks (other than this one) when called.
        By catching asyncio.CancelledError, any running task can perform
        any necessary cleanup when it's cancelled.
        """
        nonlocal close_tasks

        log.info("Got shutdown signal: %s", sig)
        log.info("Running %s close tasks", len(close_tasks))

        await asyncio.gather(*[coro() for coro in close_tasks], return_exceptions=True)

        log.info("Finished running close tasks")

        tasks = []
        for task in asyncio.all_tasks(loop):
            if task is not asyncio.current_task(loop):
                task.cancel()
                tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        log.info("Finished awaiting tasks, stopping loop")
        loop.stop()

    for sig in [signal.SIGINT, signal.SIGTERM]:
        log.info("Adding signal handler for %s", sig)
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(sig)))

    return close_tasks


ordinal = lambda n: "%d%s" % (
    n,
    "tsnrhtdd"[(n // 10 % 10 != 1) * (n % 10 < 4) * n % 10 :: 4],
)


class MISSING:
    pass


class TimedDict(dict):
    def __init__(self, ttl=10.0):
        self._ttl = ttl
        self._used_at = dict()

    def get(self, key, default=MISSING):
        try:
            return self.__getitem__(key)
        except KeyError:
            if default is MISSING:
                raise

            return default

    def __getitem__(self, key):
        set_at = self._used_at.get(key, None)
        now = monotonic()

        if set_at is not None and now - set_at >= self._ttl:
            del self[key]

        val = dict.__getitem__(self, key)
        self._used_at[key] = now

        return val

    def __setitem__(self, key, val):
        self._used_at[key] = monotonic()
        dict.__setitem__(self, key, val)
