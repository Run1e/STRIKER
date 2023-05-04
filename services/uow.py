from collections import deque

from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction

from adapters.orm import Session
from adapters.repo import DemoRepository, JobRepository, UserRepository


class SqlUnitOfWork:
    session: AsyncSession
    transaction: AsyncSessionTransaction
    jobs: JobRepository
    demos: DemoRepository

    async def __aenter__(self):
        self.session: AsyncSession = Session()

        self.messages = []
        self.seen = set()
        self.committed: bool = False

        self.jobs = JobRepository(self.session)
        self.demos = DemoRepository(self.session)
        self.users = UserRepository(self.session)

        self.transaction: AsyncSessionTransaction = await self.session.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and not self.committed:
            await self.transaction.rollback()

        # collect events from persistent session objects.
        # since this is done at the context manager exit,
        # events created by domain models after uow end
        # will not be caught. even if the events get dispatched
        # *after* the service call.
        # in other words, don't do things to domain models
        # that might create an event after closing the
        # uow but before the service call ends
        # https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.Session.identity_map
        for persistent_object in self.session.identity_map.values():
            self.messages.extend(persistent_object.events)

        await self.session.close()

    def add_message(self, event):
        self.messages.append(event)

    async def flush(self):
        await self.session.flush()

    async def commit(self):
        await self.transaction.commit()
        self.committed = True
