import logging
from uuid import uuid4

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as aio
import sqlalchemy.orm as orm
from bot import config
from domain.domain import Demo, DemoState, Job, JobState, RecordingType, Recording, User
from sqlalchemy.dialects import postgresql as pg

log = logging.getLogger(__name__)

meta = sa.MetaData()

demo_table = sa.Table(
    "demo",
    meta,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("state", pg.ENUM(DemoState), unique=False, nullable=False),
    sa.Column("queued", sa.Boolean, nullable=False),
    sa.Column("sharecode", sa.TEXT, unique=True),
    sa.Column("matchid", sa.BigInteger, nullable=True),
    sa.Column("matchtime", sa.DateTime(timezone=True), nullable=True),
    sa.Column("url", sa.TEXT, nullable=True),
    sa.Column("map", sa.TEXT, nullable=True),
    sa.Column("protocol", sa.Integer, nullable=True),
    sa.Column("version", sa.SmallInteger, nullable=True),
    sa.Column("downloaded_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("score", pg.ARRAY(sa.SmallInteger), nullable=True),
    sa.Column("data", sa.JSON, nullable=True),
)

job_table = sa.Table(
    "job",
    meta,
    sa.Column("id", pg.UUID(as_uuid=True), primary_key=True, default=uuid4),
    sa.Column("state", pg.ENUM(JobState), unique=False, nullable=False),
    sa.Column("guild_id", sa.BigInteger),
    sa.Column("channel_id", sa.BigInteger),
    sa.Column("user_id", sa.BigInteger),
    sa.Column("demo_id", sa.ForeignKey("demo.id")),
    sa.Column("started_at", sa.DateTime(timezone=True)),
    sa.Column("inter_payload", sa.LargeBinary),
    sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("recording_id", sa.ForeignKey("recording.id"), nullable=True),
)

recording_table = sa.Table(
    "recording",
    meta,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("recording_type", pg.ENUM(RecordingType), unique=False, nullable=False),
    sa.Column("player_xuid", sa.BigInteger, nullable=False),
    sa.Column("round_id", sa.Integer, nullable=True),
)

user_table = sa.Table(
    "striker_user", # a dumb name really but "user" has ns collision in pg
    meta,
    sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
    sa.Column("user_id", sa.BigInteger, nullable=False),
    sa.Column("crosshair_code", sa.TEXT, nullable=True),
    sa.Column("fragmovie", sa.Boolean, nullable=True),
    sa.Column("color_filter", sa.Boolean, nullable=True),
    sa.Column("righthand", sa.Boolean, nullable=True),
    sa.Column("sixteen_nine", sa.Boolean, nullable=True),
    sa.Column("use_demo_crosshair", sa.Boolean, nullable=True),
)

engine: aio.AsyncEngine = aio.create_async_engine(
    config.DB_BIND,
    execution_options={"isolation_options": "REPEATABLE READ"},
    future=True,
)

Session = orm.sessionmaker(
    engine, autocommit=False, expire_on_commit=False, class_=aio.AsyncSession
)


async def start_orm():
    log.info("Initializing ORM")

    if config.DEBUG:
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

        # redirects sqlalchemy info records to debug
        def wrap_info(original):
            def patched_info(self, msg, *args, **kwargs):
                if self.name.startswith("sqlalchemy"):
                    self._log(logging.DEBUG, msg, args, **kwargs)
                else:
                    original(self, msg, *args, **kwargs)

            return patched_info

        logging.Logger.info = wrap_info(logging.Logger.info)

    registry = orm.registry()

    registry.map_imperatively(Demo, demo_table)

    registry.map_imperatively(
        Job,
        job_table,
        properties=dict(
            demo=orm.relationship(Demo, lazy="joined"),
            recording=orm.relationship(Recording, lazy="joined"),
        ),
    )

    registry.map_imperatively(Recording, recording_table)

    registry.map_imperatively(User, user_table)

    async with engine.begin() as conn:
        if config.DROP_TABLES:
            await conn.run_sync(meta.drop_all)
            pass

        log.info("Creating tables if necessary")
        await conn.run_sync(meta.create_all)

    log.info("ORM initialized")
