import asyncio
from collections import deque


class ResourcePool:
    def __init__(self, on_removal=None) -> None:
        self._on_removal = on_removal

        self.queue = deque()
        self.event = asyncio.Event()
        self.event.clear()

    def add(self, resource):
        should_set = not self.queue
        self.queue.append(resource)

        if should_set and not self.event.is_set():
            self.event.set()

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
        self.queue.append(resource)
        if not self.event.is_set():
            self.event.set()

    async def on_removal(self, resource, exc_val):
        if self._on_removal is not None:
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
