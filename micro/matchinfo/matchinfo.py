import asyncio
import logging
from multiprocessing import Event, Pipe, Process

import aiormq
from csgo.sharecode import decode
from shared.log import logging_config
from shared.message import MessageError, MessageWrapper

from . import config
from .cs_process import cs_process

logging_config(config.DEBUG)
log = logging.getLogger(__name__)


matches = dict()
listeners = dict()


async def on_message(message: aiormq.channel.DeliveredMessage):
    wraps = MessageWrapper(
        message=message,
        default_error='An error occurred while fetching match info from Steam.',
        ack_on_failure=True,
    )

    async with wraps as ctx:
        data = ctx.data
        sharecode = data['sharecode']

        log.info('Processing sharecode %s', sharecode)

        try:
            decoded = decode(sharecode)
        except ValueError:
            raise MessageError('Failed to decode sharecode.')

        matchid = decoded['matchid']

        parent_conn.send(decoded)

        event = asyncio.Event()
        listeners[matchid] = event

        await asyncio.wait_for(event.wait(), timeout=5.0)
        data = matches.pop(matchid)

        log.info('Returning %s', matchid)
        print(data)
        await ctx.success(**data)


async def main():
    global parent_conn

    log.info('Starting up...')

    logging.getLogger('aiormq').setLevel(logging.INFO)

    parent_conn, child_conn = Pipe()
    event = Event()

    p = Process(target=cs_process, args=(child_conn, event))
    p.start()

    event.wait()

    mq = await aiormq.connect(config.RABBITMQ_HOST)
    channel = await mq.channel()
    await channel.basic_qos(prefetch_count=3)
    await channel.basic_consume(
        queue=config.MATCHINFO_QUEUE, consumer_callback=on_message, no_ack=False
    )

    log.info('Ready to match!')

    while True:
        while parent_conn.poll():
            data = parent_conn.recv()

            matchid = data['matchid']
            event = listeners.pop(matchid, None)

            if event is not None:
                matches[matchid] = data
                event.set()

        await asyncio.sleep(0.05)
