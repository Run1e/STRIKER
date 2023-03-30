from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy import func, inspect, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from domain.domain import Demo, DemoState, Job, JobState

INTERACTION_MINUTES = 13


class SqlRepository:
    def __init__(self, session: AsyncSession, _type):
        self.session = session
        self._type = _type
        self._primary_key = inspect(self._type).primary_key[0]

    def add(self, instance):
        self.session.add(instance)

    async def _get(self, _id):
        return await self.session.get(self._type, _id)

    async def _where(self, **kwargs):
        stmt = select(self._type).filter_by(**kwargs)
        result = await self.session.execute(stmt)
        return result.scalars().all()


class JobRepository(SqlRepository):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Job)

    async def get(self, _id) -> Job:
        return await self._get(_id)

    async def where(self, **kwargs) -> List[Job]:
        return await self._where(**kwargs)

    async def waiting_for_demo(self, demo_id):
        stmt = select(Job).where(
            Job.state == JobState.DEMO,
            Job.started_at > datetime.now(timezone.utc) - timedelta(minutes=INTERACTION_MINUTES),
            Job.demo_id == demo_id,
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def get_restart(self, minutes=INTERACTION_MINUTES) -> List[Job]:
        """Gets jobs that were made within the last {minutes} minutes and have state JobState.SELECT"""

        stmt = select(Job).where(
            Job.state == JobState.SELECT,
            Job.started_at > datetime.now(timezone.utc) - timedelta(minutes=minutes),
        )

        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def get_recording(self, minutes=INTERACTION_MINUTES) -> List[Job]:
        """Gets jobs that were made within the last {minutes} minutes and have state JobState.SELECT"""

        stmt = select(Job).where(
            Job.state == JobState.RECORD,
            Job.started_at > datetime.now(timezone.utc) - timedelta(minutes=minutes),
        )

        result = await self.session.execute(stmt)
        return result.scalars().all()


class DemoRepository(SqlRepository):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Demo)

    async def get(self, _id) -> Demo:
        return await self._get(_id)

    async def where(self, **kwargs) -> List[Demo]:
        return await self._where(**kwargs)

    async def from_sharecode(self, sharecode: str) -> Demo:
        """Gets demo from a sharecode"""
        stmt = select(Demo).where(Demo.sharecode == sharecode).limit(1)
        return await self.session.scalar(stmt)

    async def unqueued(self):
        stmt = select(Demo).where(
            Demo.queued == False, Demo.state.in_((DemoState.MATCH, DemoState.PARSE))
        )

        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def queued(self):
        stmt = select(Demo).where(
            Demo.queued == True, Demo.state.in_((DemoState.MATCH, DemoState.PARSE))
        )

        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def user_associated(self, user_id: int) -> List[Demo]:
        """Gets demos associated with a user (which requires a join on Job for the user_id)"""

        stmt = (
            select(Demo)
            .join(Job)
            .where(Job.user_id == user_id, Demo.state == DemoState.SUCCESS)
            .group_by(Demo.id)
            .order_by(func.max(Job.started_at).desc())
        )

        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def set_failed(self, _id):
        stmt = update(Demo).where(Demo.id == _id).values(state=DemoState.FAILED, queued=False)
        await self.session.execute(stmt)
