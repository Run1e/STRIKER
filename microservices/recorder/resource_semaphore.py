import asyncio
from collections import deque

from logging import getLogger

log = getLogger(__name__)


class ResourcePool:
    def __init__(self, on_removal=None) -> None:
        self._on_removal = on_removal

        self.queue = deque()
        self.event = asyncio.Event()
        self.event.clear()

        self._removed = set()

    def __iter__(self):
        yield from self.queue

    def __len__(self):
        return len(self.queue)

    def add(self, resource):
        should_set = not self.queue
        self.queue.append(resource)

        log.info("Adding %s, total: %s", resource, len(self.queue))

        if should_set and not self.event.is_set():
            self.event.set()

    def remove(self, resource):
        h = hash(resource)

        if h in self._removed:
            log.info("%s already removed", resource)
            return False

        try:
            self.queue.remove(resource)
        except ValueError:
            # we need to check for this, as the resource
            # might be in a ResourceRequest at this stage
            pass

        log.info("Removing resource: %s, total: %s", resource, len(self.queue))
        self._removed.add(h)
        return True

    async def get(self):
        # while queue is empty, wait for event
        while not self.queue:
            await self.event.wait()

        resource = self.queue.popleft()

        # if queue was depleted, clear the event
        if not self.queue:
            self.event.clear()

        return resource

    def release(self, resource):
        if hash(resource) in self._removed:
            log.info(
                "Previously removed resource attempted released back to pool %s",
                resource,
            )
            return

        self.queue.append(resource)
        if not self.event.is_set():
            self.event.set()

    async def on_removal(self, resource, exc_val):
        if self.remove(resource) and self._on_removal is not None:
            await self._on_removal(self, resource, exc_val)


class ResourceRequest:
    def __init__(self, pool: ResourcePool) -> None:
        self.pool = pool
        self.resource = None

    async def __aenter__(self):
        resource = await self.pool.get()
        self.resource = resource
        return resource

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        resource = self.resource

        if exc_type is None:
            self.pool.release(resource)
        else:
            asyncio.create_task(self.pool.on_removal(resource, exc_val))
