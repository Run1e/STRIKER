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
        default_error='An error occured.',
        ack_on_failure=True,
        requeue_on_nack=False,
    ):
        self.message: aiormq.types.DeliveredMessage = message
        self.default_error = default_error
        self.ack_on_failure = ack_on_failure
        self.requeue_on_nack = requeue_on_nack
        self.data = loads(message.body.decode('utf-8'))
        self.correlation_id = message.header.properties.correlation_id
        self.end_timer = timer(f'message processing for id {self.correlation_id}')

        log.info('INIT %s', self.correlation_id)

    async def ack(self):
        log.info('ACK %s', self.correlation_id)
        await self.message.channel.basic_ack(self.message.delivery.delivery_tag)

    async def nack(self):
        log.info('NACK %s', self.correlation_id)
        await self.message.channel.basic_nack(
            self.message.delivery.delivery_tag, requeue=self.requeue_on_nack
        )

    async def send(self, **kwargs):
        res = await self.message.channel.basic_publish(
            body=dumps(kwargs).encode('utf-8'),
            routing_key=self.message.header.properties.reply_to,
            properties=aiormq.spec.Basic.Properties(
                content_type='application/json',
                correlation_id=self.correlation_id,
            ),
        )

        log.info(self.end_timer())
        return res

    async def success(self, **kwargs):
        log.info('SUCCESS %s', self.correlation_id)
        await self.send(success=1, **kwargs)

    async def failure(self, **kwargs):
        log.info('FAILURE %s', self.correlation_id)
        await self.send(success=0, **kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            is_ok = isinstance(exc_val, MessageError)
            reason = str(exc_val) if is_ok else self.default_error

            await self.failure(reason=reason)

            if self.ack_on_failure:
                await self.ack()
            else:
                await self.nack()

            if is_ok:
                log.debug(f'Failed: {reason}')
                return True
            else:
                raise exc_val
        else:
            await self.ack()
