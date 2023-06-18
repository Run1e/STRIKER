import asyncio
import logging
from collections import defaultdict
from typing import TypeVar

K = TypeVar("K")
log = logging.getLogger(__name__)


class EphemeralLock:
    def __init__(self, locks: dict[K, asyncio.Lock], key: K) -> None:
        self.locks = locks
        self.key = key
        self.lock = None

    async def __aenter__(self):
        self.lock = self.locks[self.key]
        if self.lock.locked():
            log.warn("Lock acquire needs waiter for lock key %s", self.key)
        await self.lock.acquire()

    async def __aexit__(self, *junk):
        self.lock.release()
        if not self.lock._waiters:
            del self.locks[self.key]


class LockStore:
    def __init__(self) -> None:
        self.locks: dict[K, asyncio.Lock] = defaultdict(asyncio.Lock)

    def get(self, key: K):
        return EphemeralLock(self.locks, key)
