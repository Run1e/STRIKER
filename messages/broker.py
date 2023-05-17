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
        identifier: str = None,
        publish_commands: set = None,  # mainly to set up dlx queue for published commands
        consume_events: set = None,  # set up consumers for events we're .wait_for'ing
    ) -> None:
        self.bus = bus
        self.bus.add_dependencies(publish=self.publish)

        self.channel: aiormq.Channel = None
        self.identifier = identifier or str(uuid4())[:8]

        self._publish_commands = publish_commands or set()
        self._consume_events = consume_events or set()
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

        if self._publish_commands:
            for command_type in self._publish_commands:
                await self.prepare_command(command_type, as_consumer=False)

        if self._consume_events:
            for event_type in self._consume_events:
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

        consume_args = deco.consume_args.get(message_type, None)
        if consume_args["requeue"]:
            raise ValueError("Requeueing not supported for events")

        await self.channel.basic_consume(
            queue=queue_name,
            consumer_callback=partial(self.recv, message_type=message_type, **consume_args),
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
                    queue=dlx_queue_name,
                    consumer_callback=partial(
                        self.recv_dead, message_type=message_type, dead_event=dead_event
                    ),
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
                queue=queue_name,
                consumer_callback=partial(self.recv, message_type=message_type, **consume_args),
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

    async def recv(
        self,
        message: aiormq.abc.DeliveredMessage,
        message_type,
        publish_err: callable,
        dispatch_err: callable,
        requeue: bool,
        raise_on_ok: bool,
    ):
        log.info("Consuming message of type %s", message_type)

        try:
            msg = message_type(**loads(message.body))
            await self.bus.dispatch(msg)
        except Exception as exc:
            # MessageError is used to specify an error reason for the DTO, usually anyway
            is_ok = isinstance(exc, MessageError)

            # if this command is set up to requeue, only do it once (before it's redelivered)
            if requeue and not message.redelivered:
                await self.nack(message, requeue=True)

            # if we're unable to requeue...
            else:
                # publish an error event if an error factory is set up
                if publish_err:
                    await self.publish(publish_err(msg, str(exc) if is_ok else None))

                # or dispatch one if that's set up instead
                elif dispatch_err:
                    await self.bus.dispatch(dispatch_err(msg, str(exc) if is_ok else None))

                # and ack the command
                await self.ack(message)

            # this is not a MessageError, or if it is and we want to raise on those, do so
            if not is_ok or raise_on_ok:
                raise exc

        # if the command handler didn't except, ack the message
        else:
            await self.ack(message)

    async def recv_dead(self, message: aiormq.abc.DeliveredMessage, message_type, dead_event):
        log.info("Consuming dead letter of type %s", message_type)

        command = message_type(**loads(message.body))
        reason = message.header.properties.headers["x-first-death-reason"]
        event = dead_event(command=command, reason=reason)

        # immediately ack
        await self.ack(message)
        await self.bus.dispatch(event)

    async def ack(self, message: aiormq.abc.DeliveredMessage):
        await self.channel.basic_ack(message.delivery.delivery_tag)

    async def nack(self, message: aiormq.abc.DeliveredMessage, requeue: bool):
        await self.channel.basic_nack(
            message.delivery.delivery_tag,
            requeue=requeue,
        )
