from uuid import UUID

from sqlalchemy import text
from domain.domain import UserSettings

from services.uow import SqlUnitOfWork


async def job_inter(job_id: UUID, uow: SqlUnitOfWork):
    async with uow:
        return await uow.jobs.get_inter(job_id)


async def get_user_demo_formats(user_id: int, uow: SqlUnitOfWork):
    async with uow:
        stmt = text(
            "SELECT d.id, d.game, d.origin, d.time, d.map, d.score, d.downloaded_at "
            "FROM demo AS d JOIN job AS j ON j.demo_id=d.id "
            "WHERE d.state='READY' AND j.user_id=:user_id "
            "ORDER BY j.started_at DESC"
        ).bindparams(user_id=user_id)
        result = await uow.session.execute(stmt)
        rows = result.all()

        def formatter(row):
            time_str = row.time.strftime(f" %Y/%m/%d at %I:%M") if row.time else ""
            score_str = "-".join(str(val) for val in row.score)
            return f"[{row.origin}] {row.map} {score_str}{time_str}"

        return {row.id: formatter(row) for row in rows}


async def user_recording_count(user_id: int, uow: SqlUnitOfWork):
    async with uow:
        return await uow.jobs.recording_count(user_id=user_id)


async def get_user_settings(user_id: int, tier: int, uow: SqlUnitOfWork):
    async with uow:
        user = await uow.users.get_user(user_id)
        if user is None:
            user = UserSettings(user_id)
            uow.users.add(user)

        await uow.commit()
        return user.filled(tier), UserSettings.value_tiers


async def get_demo_origin(demo_id: int, uow: SqlUnitOfWork):
    async with uow:
        stmt = text("SELECT origin FROM demo WHERE id=:demo_id").bindparams(demo_id=demo_id)
        result = await uow.session.execute(stmt)
        return result.scalar()
