import logging
from dataclasses import asdict
from functools import partial
from json import JSONDecodeError, dumps, loads
from typing import Mapping
from uuid import uuid4

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
)
from pamqp.commands import Basic

from . import bus, commands, deco, events

log = logging.getLogger(__name__)


class MessageError(Exception):
    pass


class Broker:
    def __init__(
        self,
        bus: bus.MessageBus,
        identifier: str = None,
        publish_commands: set = None,  # mainly to set up dlx queue for published commands
        consume_events: set = None,  # set up consumers for events we're .wait_for'ing
    ) -> None:
        self.bus = bus
        self.bus.add_dependencies(publish=self.publish)

        self.connection: AbstractConnection = None
        self.channel: AbstractChannel = None

        self.exchanges: Mapping[str, AbstractExchange] = {}
        self.queues: Mapping[str, AbstractQueue] = {}
        self.identifier = identifier or str(uuid4())[:8]

        self._publish_commands = publish_commands or set()
        self._consume_events = consume_events or set()
        self._identified = bool(identifier)

    async def start(self, url: str, prefetch_count=None):
        self.connection = await aio_pika.connect_robust(url)
        self.channel = await self.connection.channel()

        await self.channel.set_qos(prefetch_count=prefetch_count)

        for exchange_name in ("command", "event", "dead"):
            self.exchanges[exchange_name] = await self.channel.declare_exchange(
                name=exchange_name,
                type="direct",
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

        if self._publish_commands:
            for command_type in self._publish_commands:
                await self.prepare_command(command_type, as_consumer=False)

        if self._consume_events:
            for event_type in self._consume_events:
                await self.prepare_event(event_type)

    def message_type_to_queue_name(self, message_type):
        if issubclass(message_type, events.Event):
            return f"event-{message_type.__name__}-{self.identifier}"
        elif issubclass(message_type, commands.Command):
            return f"cmd-{message_type.__name__}"

    async def create_queue(self, name, *args, **kwargs):
        queue = await self.channel.declare_queue(name=name, *args, **kwargs)
        self.queues[name] = queue
        return queue

    async def prepare_event(self, message_type):
        routing_key = message_type.__name__
        queue_name = self.message_type_to_queue_name(message_type)

        queue = await self.create_queue(
            name=queue_name,
            durable=self._identified,
            exclusive=not self._identified,
            auto_delete=not self._identified,
        )

        # bind the queue to its related exchange
        await queue.bind(exchange="event", routing_key=routing_key)

        # consume from the queue
        log.info("Consuming queue (event) %s", queue_name)

        consume_args = deco.consume_args.get(message_type, None)
        if consume_args["requeue"]:
            raise ValueError("Requeueing not supported for events")

        await queue.consume(callback=partial(self.recv, message_type=message_type, **consume_args))

    async def prepare_command(self, message_type, as_consumer: bool):
        routing_key = message_type.__name__
        queue_name = self.message_type_to_queue_name(message_type)
        dlx_queue_name = f"dead-{routing_key}"
        queue_args = dict()
        publish_args = deco.publish_args.get(message_type, None)

        dead_event = publish_args["dead_event"]

        if dead_event:
            dead_queue = await self.create_queue(
                name=dlx_queue_name,
                durable=True,
                exclusive=False,
                auto_delete=False,
            )

            queue_args["x-dead-letter-exchange"] = "dead"
            queue_args["x-dead-letter-routing-key"] = routing_key

            # bind the dlx queue to its related exchange
            await dead_queue.bind(
                exchange="dead",
                routing_key=routing_key,
            )

            if not as_consumer:
                # we're publisher, so consume dead letter queue
                log.info("Consuming queue (dlx) %s", dlx_queue_name)
                await dead_queue.consume(
                    callback=partial(
                        self.recv_dead, message_type=message_type, dead_event=dead_event
                    ),
                )

        # create the message queue
        queue = await self.create_queue(
            name=queue_name,
            durable=True,
            exclusive=False,
            auto_delete=False,
            arguments=queue_args,
        )

        # bind the queue to its related exchange
        await queue.bind(exchange="command", routing_key=routing_key)

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
            await queue.consume(
                callback=partial(self.recv, message_type=message_type, **consume_args),
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
            if message_type not in self._publish_commands:
                raise ValueError(
                    "Attemping to publish command of type %s but command not specified on broker creation",
                    message_type,
                )

            exchange = "command"
        else:
            exchange = "event"

        log.info("Publishing %s", message)

        exchange = self.exchanges[exchange]

        message = aio_pika.Message(
            body=dumps(data).encode("utf-8"),
            headers=dict(
                expiration=str(int(ttl * 1000)) if ttl is not None else None,
                delivery_mode=2,  # persistent messages
            ),
        )

        # TODO: https://www.rabbitmq.com/confirms.html#publisher-confirms-latency
        result = await exchange.publish(message=message, routing_key=queue_name)

        log.info("Publish result: %s", result.name)
        assert isinstance(result, Basic.Ack)

    async def _load_message(self, message: AbstractIncomingMessage, message_type: type):
        log.info("Consuming %s", message_type)

        try:
            return message_type(**loads(message.body))
        except (JSONDecodeError, TypeError):
            log.error(
                "Tried to consume %s, but failed parsing json or loading into dataclass",
                message_type,
            )
            log.error(message.body)
            await message.ack()
            raise

    async def recv(
        self,
        message: AbstractIncomingMessage,
        message_type,
        publish_err: callable,
        dispatch_err: callable,
        requeue: bool,
    ):
        msg = await self._load_message(message, message_type)

        try:
            await self.bus.dispatch(msg)
        except Exception as exc:
            # MessageError is used to specify an error reason for the DTO, usually anyway
            is_ok = isinstance(exc, MessageError)

            # if this command is set up to requeue, only do it once (before it's redelivered)
            if requeue and not message.redelivered:
                await message.nack(requeue=True)

            # if we're unable to requeue...
            else:
                # publish an error event if an error factory is set up
                if publish_err:
                    await self.publish(publish_err(msg, str(exc) if is_ok else None))

                # or dispatch one if that's set up instead
                elif dispatch_err:
                    await self.bus.dispatch(dispatch_err(msg, str(exc) if is_ok else None))

                # and ack the command
                await message.ack()

            raise exc

        # if the command handler didn't except, ack the message
        else:
            await message.ack()

    async def recv_dead(self, message: AbstractIncomingMessage, message_type, dead_event):
        msg = await self._load_message(message, message_type)

        reason = message.properties.headers["x-first-death-reason"]
        event = dead_event(command=msg, reason=reason)

        log.info("Consuming dead letter %s", event)

        # immediately ack
        await message.ack()
        await self.bus.dispatch(event)
