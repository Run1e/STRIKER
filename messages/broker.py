from dataclasses import asdict
from functools import partial
from json import dumps, loads
from uuid import uuid4

import aiormq

from . import bus, commands, events

queue_args = dict()


def queue(ttl=None, timeout_event=None):
    def inner(message):
        queue_args[message] = dict(ttl=ttl, timeout_event=timeout_event)
        return message

    return inner


class Broker:
    def __init__(self, bus: bus.MessageBus) -> None:
        self.bus = bus
        self.publish_props = dict()

    async def channel_setup(self, channel: aiormq.abc.AbstractChannel, messages: list):
        self.channel = channel

        await self.channel.exchange_declare(
            exchange="command",
            exchange_type="direct",
            durable=True,
            auto_delete=False,
        )

        await self.channel.exchange_declare(
            exchange="event",
            exchange_type="direct",
            durable=True,
            auto_delete=False,
        )

        for message in self.bus.in_use_messages:
            args = queue_args.get(message, None)
            if args is None:
                continue

            ttl = args["ttl"]
            self.publish_props[message] = dict(expiration=None if ttl is None else int(ttl * 1000))

            if isinstance(message, commands.Command):
                exchange = "command"
            elif isinstance(message, events.Event):
                exchange = "event"

            await self._create_queue(message_type=message, exchange=exchange, **args)

    async def _create_queue(self, message_type, exchange=None, ttl=None, timeout_event=None):
        # message type has a handler/listener in this process,
        # so we need to make sure the queue exists,
        # and then make the consume call

        queue_name = message_type.__name__
        dlx_queue_name = f"{queue_name}-dlx"
        arguments = dict()

        if timeout_event:
            arguments["x-dead-letter-exchange"] = exchange
            arguments["x-dead-letter-routing-key"] = dlx_queue_name

        # declare the main queue with dlx if timeout_event is specified
        await self.channel.queue_declare(
            queue=queue_name,
            arguments=arguments,
            exclusive=True,
            auto_delete=True,
        )

        await self.channel.queue_bind(queue=queue_name, exchange=exchange, routing_key=queue_name)

        # since this is called if we have a handler for the message, just start consuming
        await self.channel.basic_consume(queue_name, partial(self.recv, message_type))

        # also consume from the dlx if we configured out
        if timeout_event:
            await self.channel.basic_consume(dlx_queue_name, partial(self.recv, message_type))

    async def publish(self, message):
        queue_name = message.__name__
        data = asdict(message)

        if isinstance(message, commands.Command):
            # if message not in self.publish_setup_completed:
            #     await self._pre_publish_setup(message, **queue_args.get(message, {}))

            exchange = "command"
        else:
            exchange = "event"

        conf = await self.channel.basic_publish(
            body=dumps(data).encode("utf-8"),
            exchange=exchange,
            routing_key=queue_name,
            properties=aiormq.spec.Basic.Properties(**self.publish_args.get(type(message), {})),
        )

        return conf

    async def recv(self, message_type, message: aiormq.abc.DeliveredMessage):
        body = loads(message.body)
        await self.bus.dispatch(message_type(**body))
