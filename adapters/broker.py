import json
import logging
from collections import deque
from time import monotonic
from uuid import UUID

import aiormq
import aiormq.abc

from bot import config
from domain import events
from domain.events import Event
from services import bus

log = logging.getLogger(__name__)
broker_map = dict()


class Broker:
    def __init__(
        self,
        prefix: str,
        id_type_cast: type,
        success_event: Event,
        failure_event: Event,
        ttl: float = None,
        progression_event: Event = None,
        update_count: int = None,
        update_spacing: float = None,
    ):
        self.prefix = prefix
        self.id_type_cast = id_type_cast
        self.success_event = success_event
        self.failure_event = failure_event
        self.ttl = ttl
        self.progression_event = progression_event
        self.update_count = update_count
        self.update_spacing = update_spacing

        self._queue = deque()
        self._progression_updates = dict()  # correlation_id: monotonic
        self._dont_update = set()
        self._send_queue = f"{self.prefix}-send"
        self._recv_queue = f"{self.prefix}-recv"
        self._dlx_queue = f"{self.prefix}-dlx"

    async def start(self, channel: aiormq.Channel):
        self.channel = channel

        await self.channel.queue_declare(self._dlx_queue)
        await self.channel.queue_declare(
            self._send_queue,
            arguments={
                "x-dead-letter-exchange": "",  # should pick default exchange?
                "x-dead-letter-routing-key": self._dlx_queue,
            },
        )
        await self.channel.queue_declare(self._recv_queue)

        await self.channel.basic_consume(self._recv_queue, self.recv)
        await self.channel.basic_consume(self._dlx_queue, self.recv_dlx)

    async def send(self, id, dispatcher=None, **kwargs) -> aiormq.abc.ConfirmationFrameType:
        log.info(f"Broker sending on queue {self._send_queue}: {kwargs}")

        conf = await self.channel.basic_publish(
            body=json.dumps(kwargs).encode("utf-8"),
            routing_key=self._send_queue,
            properties=aiormq.spec.Basic.Properties(
                content_type="application/json",
                correlation_id=str(id),
                reply_to=self._recv_queue,
                expiration=str(int(self.ttl * 1000)) if self.ttl is not None else None,
            ),
        )

        if self.progression_event:
            self.send_progression(self.id_type_cast(id), dispatcher=dispatcher)

        return conf

    async def recv(self, message: aiormq.abc.DeliveredMessage):
        body = json.loads(message.body)
        success = body.pop("success")
        data = body.pop("data")

        correlation_id = message.header.properties.correlation_id
        correlation_id = self.id_type_cast(correlation_id)

        event = self.success_event if success else self.failure_event
        await bus.call(event(id=correlation_id, **data))

        # move above dispatch for testing to quickly "clear" recv queues
        await self.channel.basic_ack(message.delivery.delivery_tag)

        if self.progression_event:
            self.recv_progression(correlation_id)

    async def recv_dlx(self, message: aiormq.abc.DeliveredMessage):
        correlation_id = message.header.properties.correlation_id
        correlation_id = self.id_type_cast(correlation_id)
        reason = message.header.properties.headers["x-first-death-reason"]

        long_reason = dict(
            rejected="The service was unable to fulfill the request.",
            expired="The service request timed out.",
        ).get(reason, "A service error occurred.")

        bus.dispatch(self.failure_event(id=correlation_id, reason=long_reason))

        await self.channel.basic_ack(message.delivery.delivery_tag)

        if self.progression_event:
            self.recv_progression(correlation_id)

    def send_progression(self, correlation_id, dispatcher=None):
        infront = len(self._queue)
        self._queue.append(correlation_id)
        self.dispatch_progression(correlation_id, infront, dispatcher=dispatcher)

    def recv_progression(self, correlation_id):
        try:
            self._queue.remove(correlation_id)
        except ValueError:
            pass

        self._progression_updates.pop(correlation_id, None)

        try:
            self._dont_update.remove(correlation_id)
        except KeyError:
            pass

        updates = list(sorted(enumerate(self._progression_updates.items()), key=lambda t: t[1][1]))

        now = monotonic()
        updated = 0
        for infront, (corr_id, last_update) in updates:
            if updated >= self.update_count:
                break

            if now - last_update >= self.update_spacing and corr_id not in self._dont_update:
                self.dispatch_progression(corr_id, infront)
                updated += 1

    def dispatch_progression(self, correlation_id, infront, dispatcher=None):
        dispatcher = dispatcher or bus.dispatch
        dispatcher(self.progression_event(correlation_id, infront))
        self._progression_updates[correlation_id] = monotonic()
        if infront == 0:
            self._dont_update.add(correlation_id)


matchinfo = Broker(
    prefix="matchinfo",
    id_type_cast=int,
    success_event=events.MatchInfoSuccess,
    failure_event=events.MatchInfoFailure,
    ttl=16.0,
    progression_event=events.MatchInfoProgression,
)

demoparse = Broker(
    prefix="demoparse",
    id_type_cast=int,
    success_event=events.DemoParseSuccess,
    failure_event=events.DemoParseFailure,
    ttl=60.0,
    progression_event=events.DemoParseProgression,
)

recorder = Broker(
    prefix="recorder",
    id_type_cast=lambda uuid: UUID(str(uuid)),
    success_event=events.RecorderSuccess,
    failure_event=events.RecorderFailure,
    ttl=600.0,
    progression_event=events.RecorderProgression,
    update_count=3,
    update_spacing=100.0
)

uploader = Broker(
    prefix="uploader",
    id_type_cast=lambda uuid: UUID(str(uuid)),
    success_event=events.UploaderSuccess,
    failure_event=events.UploaderFailure,
    ttl=300.0,
)


async def start_brokers():
    log.info("Initializing brokers")

    logging.getLogger("aiormq").setLevel(logging.WARN)

    mq = await aiormq.connect(config.RABBITMQ_HOST)
    channel: aiormq.Channel = await mq.channel()

    for broker in (matchinfo, demoparse, recorder, uploader):
        await broker.start(channel)

    log.info("Brokers initialized")
