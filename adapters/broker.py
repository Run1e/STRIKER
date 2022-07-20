import json
import logging
import queue
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
        queue_prefix: str,
        send_queue: str,
        recv_queue: str,
        success_event: Event,
        failure_event: Event,
        enqueue_event: Event,
        processing_event: Event,
        id_type_cast: type,
        update_interval: float = 8.0,
        max_updates: int = 3,
    ):
        self.send_queue = queue_prefix + send_queue
        self.recv_queue = queue_prefix + recv_queue
        self.success_event = success_event
        self.failure_event = failure_event
        self.enqueue_event = enqueue_event
        self.processing_event = processing_event
        self.id_type_cast = id_type_cast
        self.update_interval = update_interval
        self.max_updates = max_updates

        self.queue = deque()
        self.enqueued_updates = dict()  # correlation_id: monotonic
        self.processing_sent = set()

    async def start(self, channel: aiormq.Channel):
        self.channel = channel
        await self.channel.queue_declare(self.send_queue)
        await self.channel.queue_declare(self.recv_queue)
        await self.channel.basic_consume(self.recv_queue, self.recv)

    async def send(
        self, id, dispatcher=None, **kwargs
    ) -> aiormq.abc.ConfirmationFrameType:
        log.info(f'Broker sending on queue {self.send_queue}: {kwargs}')

        conf = await self.channel.basic_publish(
            body=json.dumps(kwargs).encode('utf-8'),
            routing_key=self.send_queue,
            properties=aiormq.spec.Basic.Properties(
                content_type='application/json',
                correlation_id=str(id),
                reply_to=self.recv_queue,
            ),
        )

        self._handle_send_event(self.id_type_cast(id), dispatcher=dispatcher)
        return conf

    async def recv(self, message: aiormq.abc.DeliveredMessage):
        body = json.loads(message.body)
        success = body.pop('success')

        correlation_id = message.header.properties.correlation_id
        correlation_id = self.id_type_cast(correlation_id)

        body['id'] = correlation_id
        self._handle_recv_event(correlation_id)

        event = self.success_event if success else self.failure_event
        await bus.call(event(**body))

        # move above dispatch for testing to quickly "clear" recv queues
        await self.channel.basic_ack(message.delivery.delivery_tag)

    def _handle_send_event(self, correlation_id, dispatcher=None):
        infront = len(self.queue)
        self.queue.append(correlation_id)

        # since .restore() doesn't provide a dispatcher
        # it will not dispatch these events when restoring
        if dispatcher is None:
            return

        if not infront:
            self._dispatch_processing(corr_id=correlation_id, dispatcher=dispatcher)
        else:
            self._dispatch_enqueue(
                corr_id=correlation_id, infront=infront, dispatcher=dispatcher
            )

    def _handle_recv_event(self, correlation_id):
        # this method looks very messy, however it has to be messy
        # as it's dealing with a quite messy piece of logic
        # of when, which, and how many events should be fired
        # off when a new recv event comes in

        # response received, it can be removed from the queue
        next_cid = self._get_next_in_queue()
        self._set_finished(correlation_id)

        queue_len = len(self.queue)
        # if queue is not empty there's nothing to do
        if not queue_len:
            return

        # if we need to send a new processing event
        # if next_cid == correlation_id:
        for elem in self.queue:
            if elem not in self.processing_sent:
                self._dispatch_processing(corr_id=elem)
                break

        # no enqueue updates to be done if len is less than 2
        if queue_len < 2:
            return

        now = monotonic()
        corr_updates = list(
            sorted(enumerate(self.enqueued_updates.items()), key=lambda t: t[1][1])
        )

        updates = 0
        # otherwise, iterate through the enqueued_updates,
        # oldest first, and send enqueued updates
        for infront, (corr_id, last_update) in corr_updates:
            if corr_id in self.processing_sent:
                continue

            if now - last_update >= self.update_interval:
                self._dispatch_enqueue(corr_id=corr_id, infront=infront + 1, now=now)
                updates += 1

            if updates >= self.max_updates:
                return

    def _get_next_in_queue(self):
        if not self.queue:
            return None

        cid = self.queue.popleft()
        self.queue.appendleft(cid)
        return cid

    def _dispatch_processing(self, corr_id, dispatcher=None):
        disp = dispatcher or bus.dispatch
        disp(self.processing_event(id=corr_id))
        self.processing_sent.add(corr_id)
        self.enqueued_updates.pop(corr_id, None)

    def _dispatch_enqueue(self, corr_id, infront, now=None, dispatcher=None):
        disp = dispatcher or bus.dispatch
        disp(self.enqueue_event(id=corr_id, infront=infront))
        self.enqueued_updates[corr_id] = now or monotonic()

    def _set_finished(self, corr_id):
        try:
            self.queue.remove(corr_id)
        except ValueError:
            pass

        try:
            self.processing_sent.remove(corr_id)
        except KeyError:
            pass

        self.enqueued_updates.pop(corr_id, None)


_queue_prefix = config.QUEUE_PREFIX or ''

matchinfo = Broker(
    queue_prefix=_queue_prefix,
    send_queue='matchinfo_send',
    recv_queue='matchinfo_recv',
    success_event=events.MatchInfoSuccess,
    failure_event=events.MatchInfoFailure,
    enqueue_event=events.MatchInfoEnqueued,
    processing_event=events.MatchInfoProcessing,
    id_type_cast=int,
    update_interval=8.0,
    max_updates=2,
)

demoparse = Broker(
    queue_prefix=_queue_prefix,
    send_queue='demoparse_send',
    recv_queue='demoparse_recv',
    success_event=events.DemoParseSuccess,
    failure_event=events.DemoParseFailure,
    enqueue_event=events.DemoParseEnqueued,
    processing_event=events.DemoParseProcessing,
    id_type_cast=int,
    update_interval=12.0,
    max_updates=2,
)

recorder = Broker(
    queue_prefix=_queue_prefix,
    send_queue='recorder_send',
    recv_queue='recorder_recv',
    success_event=events.RecorderSuccess,
    failure_event=events.RecorderFailure,
    enqueue_event=events.RecorderEnqueued,
    processing_event=events.RecorderProcessing,
    id_type_cast=lambda uuid: UUID(str(uuid)),
    update_interval=20.0,
    max_updates=3,
)


async def start_brokers():
    log.info('Initializing brokers')

    logging.getLogger('aiormq').setLevel(logging.WARN)

    mq = await aiormq.connect(config.RABBITMQ_HOST)
    channel: aiormq.Channel = await mq.channel()

    for broker in (matchinfo, demoparse, recorder):
        await broker.start(channel)

    log.info('Brokers initialized')
