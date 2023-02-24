import logging
from json import dumps, loads

import aiormq
import aiormq.types

from .utils import timer

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class MessageError(Exception):
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
        self.end_timer = timer(f"PERF {self.correlation_id}")

        log.info("INIT %s", self.correlation_id)

    async def ack(self):
        log.info("ACK %s", self.correlation_id)
        await self.message.channel.basic_ack(self.message.delivery.delivery_tag)

    async def nack(self, requeue):
        log.info("NACK %s", self.correlation_id)
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
        log.info("SUCCESS %s", self.correlation_id)
        await self.send(success=1, **kwargs)

    async def failure(self, **kwargs):
        log.info("FAILURE %s", self.correlation_id)
        await self.send(success=0, **kwargs)

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
