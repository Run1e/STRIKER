from adapters.orm import Session
from adapters.repo import DemoRepository, JobRepository, UserRepository
from sqlalchemy.ext.asyncio import AsyncSession, AsyncSessionTransaction

from services.bus import dispatch


class SqlUnitOfWork:
    session: AsyncSession
    transaction: AsyncSessionTransaction
    jobs: JobRepository
    demos: DemoRepository

    async def __aenter__(self):
        self.session: AsyncSession = Session()

        self.events = list()
        self._committed: bool = False

        self.jobs = JobRepository(self.session)
        self.demos = DemoRepository(self.session)
        self.users = UserRepository(self.session)

        self.transaction: AsyncSessionTransaction = await self.session.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and not self._committed:
            await self.transaction.rollback()

        await self.session.close()

        for event in self.events:
            dispatch(event)

    def add_event(self, event):
        self.events.append(event)

    async def flush(self):
        await self.session.flush()

    async def commit(self):
        await self.transaction.commit()
        self._committed = True
