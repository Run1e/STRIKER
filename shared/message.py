import logging
import json
import asyncio
from string import ascii_letters
from random import choice
from json import dumps, loads

import aiormq
import aiormq.types

from shared.utils import timer

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


def random_string(l):
    return "".join(choice(ascii_letters) for _ in range(l))


class MessageError(Exception):
    pass


class RPCError(Exception):
    pass


class MessageWrapper:
    def __init__(
        self,
        message: aiormq.types.DeliveredMessage,
        default_error="An error occurred.",
        ack_on_failure=True,
        raise_on_message_error=False,
        requeue_on_nack=False,
    ):
        self.message: aiormq.types.DeliveredMessage = message
        self.default_error = default_error
        self.ack_on_failure = ack_on_failure
        self.raise_on_message_error = raise_on_message_error
        self.requeue_on_nack = requeue_on_nack
        self.data = loads(message.body.decode("utf-8"))
        self.correlation_id = message.header.properties.correlation_id
        self.end_timer = timer(f"perf {self.correlation_id}")

        log.info("start %s", self.correlation_id)

    async def ack(self):
        log.info("ack %s", self.correlation_id)
        await self.message.channel.basic_ack(self.message.delivery.delivery_tag)

    async def nack(self, requeue):
        log.info("nack %s requeue=%s", self.correlation_id, requeue)
        await self.message.channel.basic_nack(
            self.message.delivery.delivery_tag,
            requeue=requeue,
        )

    async def send(self, **kwargs):
        res = await self.message.channel.basic_publish(
            body=dumps(kwargs).encode("utf-8"),
            routing_key=self.message.header.properties.reply_to,
            properties=aiormq.spec.Basic.Properties(
                content_type="application/json",
                correlation_id=self.correlation_id,
            ),
        )

        log.info(self.end_timer())
        return res

    async def success(self, **kwargs):
        log.info("success %s", self.correlation_id)
        await self.send(success=1, data=kwargs)

    async def failure(self, **kwargs):
        log.error("failure %s", self.correlation_id)
        await self.send(success=0, data=kwargs)

    def should_requeue(self):
        return self.requeue_on_nack and not self.message.redelivered

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            is_ok = isinstance(exc_val, MessageError)
            reason = str(exc_val) if is_ok else self.default_error

            if self.ack_on_failure:
                await self.failure(reason=reason)
                await self.ack()
            else:
                requeue = self.should_requeue()
                if not requeue:
                    await self.failure(reason=reason)
                await self.nack(requeue)

            if is_ok and not self.raise_on_message_error:
                return True
        else:
            await self.ack()


class RPCServer:
    def __init__(self, channel: aiormq.abc.AbstractChannel, queue: str):
        self.channel = channel
        self.queue = queue
        self.commands = {}

    def register(self, f):
        self.commands[f.__name__] = f

    async def start(self):
        await self.channel.queue_declare(self.queue)
        await self.channel.basic_consume(
            self.queue,
            consumer_callback=self.recv,
            no_ack=False,
        )

    async def recv(self, message: aiormq.abc.DeliveredMessage):
        wraps = MessageWrapper(
            message=message,
            default_error="Server excepted while handling the RPC call.",
            ack_on_failure=True,
        )

        async with wraps as ctx:
            func = ctx.data.pop("func")
            command = self.commands.get(func, None)

            if command is None:
                raise MessageError(f"Command '{func}' not registered.")

            # no need for try/catch since exception
            # is handled by the context manager
            result = await command(**ctx.data)

            await ctx.success(result=result)


class RPCClient:
    def __init__(self, channel: aiormq.abc.AbstractChannel, queue: str):
        self.channel = channel
        self.queue = queue
        self.reply_queue = None
        self._events = {}
        self._results = {}

    async def setup(self):
        self.reply_queue = self.queue + "-" + random_string(16)
        await self.channel.queue_declare(
            queue=self.reply_queue,
            exclusive=True,
        )

        await self.channel.basic_consume(
            self.reply_queue,
            self.recv,
        )

    async def recv(self, message: aiormq.abc.DeliveredMessage):
        corr_id = message.header.properties.correlation_id

        event = self._events.get(corr_id, None)
        if event is None:
            return  # caller has stopped listening

        self._results[corr_id] = json.loads(message.body)

        await self.channel.basic_ack(message.delivery.delivery_tag)
        event.set()

    async def __call__(self, func, timeout=8.0, **kwargs):
        if self.reply_queue is None:
            await self.setup()

        corr_id = random_string(32)
        event = asyncio.Event()
        self._events[corr_id] = event
        kwargs["func"] = func

        await self.channel.basic_publish(
            body=dumps(kwargs).encode("utf-8"),
            routing_key=self.queue,
            properties=aiormq.spec.Basic.Properties(
                content_type="application/json",
                correlation_id=corr_id,
                reply_to=self.reply_queue,
            ),
        )

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            raise RPCError("RPC call timed out.")
        finally:
            self._events.pop(corr_id)

        result = self._results.pop(corr_id)

        if result["success"]:
            return result["data"]["result"]
        else:
            raise RPCError(result["reason"])
