import logging
from dataclasses import asdict
from functools import partial
from json import dumps, loads
from uuid import uuid4

import aiormq

from . import bus, commands, deco, events

log = logging.getLogger(__name__)


class MessageError(Exception):
    pass


class Broker:
    def __init__(
        self,
        bus: bus.MessageBus,
        identifier=None,
        publish_commands: set = None,
        extra_events: set = None,
    ) -> None:
        self.bus = bus

        self.channel: aiormq.Channel = None
        self.identifier = identifier or str(uuid4())[:8]

        self._can_publish = publish_commands or set()
        self._extra_events = extra_events or set()
        self._identified = bool(identifier)

    async def start(self, url: str, prefetch_count=0):
        mq = await aiormq.connect(url)
        self.channel = await mq.channel()

        await self.prefetch(prefetch_count=prefetch_count)
        for exchange_name in ("command", "event", "dead"):
            await self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type="direct",
                durable=True,
                auto_delete=False,
            )

        has_deco = self.bus.has_deco
        also_has_consume_args = has_deco.intersection(set(deco.consume_args.keys()))

        for message_type in also_has_consume_args:
            if issubclass(message_type, commands.Command):
                await self.prepare_command(message_type, as_consumer=True)
            elif issubclass(message_type, events.Event):
                await self.prepare_event(message_type)

        if self._can_publish:
            for command_type in self._can_publish:
                await self.prepare_command(command_type, as_consumer=False)

        if self._extra_events:
            for event_type in self._extra_events:
                await self.prepare_event(event_type)

    async def prefetch(self, prefetch_count):
        await self.channel.basic_qos(prefetch_count=prefetch_count)

    def message_type_to_queue_name(self, message_type):
        if issubclass(message_type, events.Event):
            return f"event-{message_type.__name__}-{self.identifier}"
        elif issubclass(message_type, commands.Command):
            return f"cmd-{message_type.__name__}"

    async def prepare_event(self, message_type):
        routing_key = message_type.__name__
        queue_name = self.message_type_to_queue_name(message_type)

        consume_args = deco.consume_args.get(message_type, None)

        await self.channel.queue_declare(
            queue=queue_name,
            durable=self._identified,
            exclusive=not self._identified,
            auto_delete=not self._identified,
        )

        # bind the queue to its related exchange
        await self.channel.queue_bind(queue=queue_name, exchange="event", routing_key=routing_key)

        # consume from the queue
        log.info("Consuming queue (event) %s", queue_name)
        await self.channel.basic_consume(
            queue_name,
            partial(self.recv, message_type=message_type, **consume_args),
        )

    async def prepare_command(self, message_type, as_consumer: bool):
        routing_key = message_type.__name__
        queue_name = self.message_type_to_queue_name(message_type)
        dlx_queue_name = f"dead-{routing_key}"
        queue_args = dict()

        publish_args = deco.publish_args.get(message_type, None)
        dead_event = publish_args["dead_event"]

        if dead_event:
            await self.channel.queue_declare(
                queue=dlx_queue_name,
                durable=True,
                exclusive=False,
                auto_delete=False,
            )

            queue_args["x-dead-letter-exchange"] = "dead"
            queue_args["x-dead-letter-routing-key"] = routing_key

            # bind the dlx queue to its related exchange
            await self.channel.queue_bind(
                queue=dlx_queue_name,
                exchange="dead",
                routing_key=routing_key,
            )

            if not as_consumer:
                # we're publisher, so consume dead letter queue
                log.info("Consuming queue (dlx) %s", dlx_queue_name)
                await self.channel.basic_consume(
                    dlx_queue_name,
                    partial(self.recv_dead, message_type=message_type, dead_event=dead_event),
                )

        # create the message queue
        await self.channel.queue_declare(
            queue=queue_name,
            arguments=queue_args,
            durable=True,
            exclusive=False,
            auto_delete=False,
        )

        # bind the queue to its related exchange
        await self.channel.queue_bind(queue=queue_name, exchange="command", routing_key=routing_key)

        if as_consumer:
            consume_args = deco.consume_args.get(message_type, None)
            if consume_args is None:
                # not really a possible branch but nice as a sanity check I guess
                raise ValueError(
                    "Tried to consume command handler for type %s but no consume args configured",
                    message_type,
                )

            # we're consumer, so consume the main queue
            log.info("Consuming queue (cmd) %s", queue_name)
            await self.channel.basic_consume(
                queue_name, partial(self.recv, message_type=message_type, **consume_args)
            )

    async def publish(self, message):
        message_type = type(message)

        args = deco.publish_args.get(message_type, None)
        if not args:
            raise ValueError(
                "Attempting to publish message of type %s but no publish args set up", message_type
            )

        queue_name = message_type.__name__
        data = asdict(message)
        ttl = args["ttl"]

        if isinstance(message, commands.Command):
            if message_type not in self._can_publish:
                raise ValueError(
                    "Attemping to publish command of type %s but command not specified on broker creation",
                    message_type,
                )

            exchange = "command"
        else:
            exchange = "event"

        log.info("Publishing to '%s': %s", exchange, message)

        conf = await self.channel.basic_publish(
            body=dumps(data).encode("utf-8"),
            exchange=exchange,
            routing_key=queue_name,
            properties=aiormq.spec.Basic.Properties(
                expiration=str(int(ttl * 1000)) if ttl is not None else None
            ),
        )

        return conf

    async def recv_dead(self, message: aiormq.abc.DeliveredMessage, message_type, dead_event):
        log.info("Consuming dead letter: %s", message)

        command = message_type(**loads(message.body))
        event = dead_event(
            command=command, reason=message.header.properties.headers["x-first-death-reason"]
        )

        await self.bus.dispatch(event)
        await self.ack(message)

    async def recv(
        self,
        message: aiormq.abc.DeliveredMessage,
        message_type,
        error_factory: callable,
        requeue: bool,
        raise_on_ok: bool,
    ):
        try:
            msg = message_type(**loads(message.body))
            log.info("Consuming on '%s': %s", message.exchange, msg)
            await self.bus.dispatch(msg)
        except Exception as exc:
            is_ok = isinstance(exc, MessageError)

            if requeue and not message.redelivered:
                await self.nack(message, requeue=True)
            else:
                if error_factory:
                    await self.publish(error_factory(msg, str(exc) if is_ok else None))
                await self.ack(message)

            if not is_ok or raise_on_ok:
                raise exc

        else:
            await self.ack(message)

    async def ack(self, message: aiormq.abc.DeliveredMessage):
        await self.channel.basic_ack(message.delivery.delivery_tag)

    async def nack(self, message: aiormq.abc.DeliveredMessage, requeue: bool):
        await self.channel.basic_nack(
            message.delivery.delivery_tag,
            requeue=requeue,
        )
