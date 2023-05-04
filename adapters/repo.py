from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy import func, inspect, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from domain.domain import Demo, Job, User
from domain.enums import DemoOrigin, DemoState, JobState

INTERACTION_MINUTES = 15


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

    async def waiting_for_demo(self, demo_id, minutes=INTERACTION_MINUTES) -> List[Job]:
        stmt = select(Job).where(
            Job.state == JobState.WAITING,
            Job.started_at > datetime.now(timezone.utc) - timedelta(minutes=minutes),
            Job.demo_id == demo_id,
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def get_restart(self, minutes=INTERACTION_MINUTES) -> List[Job]:
        """Gets jobs that were made within the last {minutes} minutes and have state JobState.SELECT"""

        stmt = select(Job).where(
            Job.state == JobState.SELECTING,
            Job.started_at > datetime.now(timezone.utc) - timedelta(minutes=minutes),
        )

        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def recording_count(self, user_id: int, minutes: int = INTERACTION_MINUTES):
        stmt = (
            select(func.count())
            .select_from(Job)
            .where(
                Job.user_id == user_id,
                Job.state == JobState.RECORDING,
                Job.started_at > datetime.now(timezone.utc) - timedelta(minutes=minutes),
            )
        )

        result = await self.session.execute(stmt)
        return result.scalar()

    async def active_ids(self):
        stmt = (
            select(Job.id, Job.state)
            .select_from(Job)
            .where(Job.state.in_((JobState.SELECTING, JobState.RECORDING, JobState.UPLOADING)))
        )

        result = await self.session.execute(stmt)
        return result.scalars().all()


class DemoRepository(SqlRepository):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Demo)

    async def get(self, _id) -> Demo:
        return await self._get(_id)

    async def from_sharecode(self, sharecode: str) -> Demo:
        """Gets demo from a sharecode"""
        stmt = select(Demo).where(Demo.sharecode == sharecode).limit(1)
        return await self.session.scalar(stmt)

    async def from_identifier(self, origin: DemoOrigin, identifier: str) -> Demo:
        """Gets demo from an origin/identifier"""
        stmt = select(Demo).where(Demo.origin == origin, Demo.identifier == identifier).limit(1)
        return await self.session.scalar(stmt)

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

    async def bulk_set_deleted(self, ids: list):
        # if we set data=None and version=None, it's like we've "deparsed" the demo,
        # so if it gets given to /record again, handle_demo_step knows to send it
        # to the demoparse microservice
        # -- in reality, we could just remove "version" but data holds so much
        # potentially useless data I'd rather just clear that as well,
        # I mean the demoparse microservice will re-parse it anyway
        stmt = (
            update(Demo)
            .where(Demo.id.in_(ids))
            .values(state=DemoState.DELETED, data=None, version=None)
        )
        await self.session.execute(stmt)

    async def least_recently_used_matchids(self, keep_count):
        # bruh sql be like
        # this gets the matchids of the demos which has been least recently in use
        # the definition of "least recently" is sorting by the greatest of
        # the demos downloaded_at date and the demos latest used job started_at date
        #
        # there are also safety concerns with sqla when using raw sql like this,
        # but in this case we're only selecting so it *should* be fine
        stmt = text(
            """WITH demos AS (
                SELECT demo.id, demo.matchid, demo.state, demo.downloaded_at, MAX(job.started_at) AS started_at
                FROM demo LEFT JOIN job ON job.demo_id=demo.id
                WHERE demo.state='SUCCESS'
                GROUP BY demo.id
            )

            SELECT id, matchid, GREATEST(downloaded_at, started_at)
            FROM demos AS d
            WHERE NOT EXISTS(SELECT 1 FROM job WHERE demo_id=d.id AND state IN ('DEMO', 'SELECT', 'RECORD') LIMIT 1)
            ORDER BY greatest
            LIMIT (SELECT GREATEST(0, COUNT(*) - :keep) FROM demos)"""
        ).bindparams(keep=keep_count)

        result = await self.session.execute(stmt)
        return result.all()


class UserRepository(SqlRepository):
    def __init__(self, session: AsyncSession):
        super().__init__(session, User)

    async def get(self, _id) -> User:
        return await self._get(_id)

    async def get_user(self, user_id: int) -> User:
        """Gets demo from a sharecode"""
        stmt = select(User).where(User.user_id == user_id).limit(1)
        return await self.session.scalar(stmt)
