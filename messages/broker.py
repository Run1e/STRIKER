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
    def __init__(self, bus: bus.MessageBus) -> None:
        self.bus = bus

        self.channel: aiormq.Channel = None
        self.publish_setup_completed = set()

    async def start(self, connect_uri: str, prefetch=1):
        mq = await aiormq.connect(connect_uri)
        channel = await mq.channel()

        await channel.basic_qos(prefetch_count=prefetch)

        self.channel = channel
        await self.setup(channel)

    async def setup(self, channel: aiormq.abc.AbstractChannel):
        for exchange_name in ("command", "event", "dead"):
            await channel.exchange_declare(
                exchange=exchange_name,
                exchange_type="direct",
                durable=True,
                auto_delete=False,
            )

        instance_id = str(uuid4())[:8]

        for message_type in self.bus.consuming_messages:
            args = deco.consume_args.get(message_type, None)
            if args is None:
                continue

            routing_key = message_type.__name__

            if issubclass(message_type, commands.Command):
                publish_args = deco.publish_args.get(message_type, None)
                await self._prepare_command_queue(
                    message_type, dead_event=publish_args["dead_event"], consume_args=args
                )
            elif issubclass(message_type, events.Event):
                queue_name = f"{routing_key}-{instance_id}"

                await self.channel.queue_declare(
                    queue=queue_name,
                    exclusive=True,
                    auto_delete=True,
                )

                # bind the queue to its related exchange
                await self.channel.queue_bind(
                    queue=queue_name, exchange="event", routing_key=routing_key
                )

                # consume from the queue
                await self.channel.basic_consume(
                    queue_name,
                    partial(self.recv, message_type=message_type, **args),
                )

    async def _prepare_command_queue(self, message_type, dead_event, consume_args: dict = None):
        queue_name = message_type.__name__
        dlx_queue_name = f"{queue_name}-dead"
        queue_args = dict()

        if dead_event:
            await self.channel.queue_declare(
                queue=dlx_queue_name,
                exclusive=False,
                auto_delete=False,
            )

            queue_args["x-dead-letter-exchange"] = "dead"
            queue_args["x-dead-letter-routing-key"] = dlx_queue_name

            # bind the dlx queue to its related exchange
            await self.channel.queue_bind(
                queue=dlx_queue_name, exchange="dead", routing_key=dlx_queue_name
            )

            if not consume_args:
                # we're publisher, so consume dead letter queue
                await self.channel.basic_consume(
                    dlx_queue_name, partial(self.recv_dead, message_type=message_type, dead_event=dead_event)
                )

        # create the message queue
        await self.channel.queue_declare(
            queue=queue_name,
            arguments=queue_args,
            exclusive=False,
            auto_delete=False,
        )

        # bind the queue to its related exchange
        await self.channel.queue_bind(queue=queue_name, exchange="command", routing_key=queue_name)

        if consume_args:
            # we're consumer, so consume the main queue
            await self.channel.basic_consume(
                queue_name, partial(self.recv, message_type=message_type, **consume_args)
            )

    async def publish(self, message):
        message_type = type(message)

        args = deco.publish_args.get(message_type, None)
        if not args:
            raise ValueError(
                "Attempted to publish message of type %s but no publish args set up", message_type
            )

        queue_name = message_type.__name__
        data = asdict(message)

        ttl = args["ttl"]
        dead_event = args["dead_event"]

        if isinstance(message, commands.Command):
            if message_type not in self.publish_setup_completed:
                await self._prepare_command_queue(
                    message_type=message_type,
                    dead_event=dead_event,
                )
                self.publish_setup_completed.add(message_type)

            exchange = "command"
        else:
            exchange = "event"

        log.info("Publishing to exchange '%s': %s", exchange, message)

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
        log.info(
            "Consuming dead letter of type %s from exchange '%s'", message_type, message.exchange
        )

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
            log.info("Consuming type %s on exchange '%s'", message_type, message.exchange)
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
