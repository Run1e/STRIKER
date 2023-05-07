from uuid import UUID
from services.uow import SqlUnitOfWork


async def get_job_inter(job_id: UUID, uow: SqlUnitOfWork):
    async with uow:
        return await uow.jobs.get_inter(job_id)
