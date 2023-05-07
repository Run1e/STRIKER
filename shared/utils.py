from bz2 import BZ2Decompressor
from datetime import datetime, timezone
from time import monotonic


def utcnow():
    return datetime.now(timezone.utc)


def timer(name):
    start = monotonic()
    return lambda: f"{name} took {monotonic() - start:0.2f} seconds"


ordinal = lambda n: "%d%s" % (
    n,
    "tsnrhtdd"[(n // 10 % 10 != 1) * (n % 10 < 4) * n % 10 :: 4],
)


class DemoCorrupted(Exception):
    pass


def decompress(archive, file):
    with open(file, "wb") as new_file, open(archive, "rb") as file:
        decompressor = BZ2Decompressor()
        for data in iter(lambda: file.read(1024 * 1024), b""):
            try:
                chunk = decompressor.decompress(data)
            except OSError as exc:
                raise DemoCorrupted("Demo corrupted.") from exc

            new_file.write(chunk)


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
