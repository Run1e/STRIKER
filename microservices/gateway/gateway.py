import sys

sys.path.append("../..")

import asyncio
import logging
from dataclasses import asdict
from functools import partial
from json import dumps, loads

import config
from websockets import server
from websockets.exceptions import ConnectionClosed

from messages import commands, events
from messages.broker import Broker, MessageError
from messages.bus import MessageBus
from messages.deco import handler, listener
from shared.log import logging_config

logging_config(config.DEBUG)
log = logging.getLogger(__name__)


async def ws_connection_handler(
    websocket: server.WebSocketServerProtocol,
    queue: asyncio.Queue,
    clients: set,
):
    async def send(c, d):
        await websocket.send(dumps([c, d]))

    auth = websocket.request_headers.get("Authorization", None)
    if auth is None or auth not in config.TOKENS:
        await websocket.close(code=1008, reason="Missing or invalid token")

    # not needed to be a dict (can just be a single reference)
    # but if I move to a single websocket in the future this should work
    futures = dict()
    clients.add(websocket)

    try:
        async for message in websocket:
            action, data = loads(message)

            if action == "request":
                log.info("request")

                getter = asyncio.create_task(queue.get())
                waiter = asyncio.create_task(websocket.wait_closed())

                finished, _ = await asyncio.wait(
                    [getter, waiter], return_when=asyncio.FIRST_COMPLETED
                )

                if waiter in finished:
                    # websocket closed...
                    getter.cancel()
                    await websocket.ensure_open()
                else:
                    # getter finished first!
                    waiter.cancel()

                command, event, future = getter.result()

                futures[command.job_id] = future
                data = asdict(command)

                await send("record", data)

                event.set()

                log.info("request sent")

            else:
                log.info("recv %s: %s", action, data)

                future = futures.get(data["job_id"], None)
                future.set_result((action, data))

    except ConnectionClosed as exc:
        for future in futures:
            future.set_exception(exc)

    clients.remove(websocket)


@handler(commands.RequestRecording)
async def on_recording_request(
    command: commands.RequestRecording,
    broker: Broker,
    queue: asyncio.Queue,
    clients: set,
):
    queue_size = queue.qsize()
    getter_count = len(queue._getters)
    client_count = len(clients)

    future = asyncio.Future()
    event_type = asyncio.Event()

    await queue.put((command, event_type, future))

    if not client_count:  # no clients, send with None
        await broker.publish(events.RecordingProgression(command.job_id, None))
    elif queue_size >= getter_count:  # more waiting in queue than clients doing .get()
        await broker.publish(events.RecordingProgression(command.job_id, queue_size))

    # wait for connection handler to .get() this request
    await event_type.wait()

    # this request is currently being processed
    await broker.publish(events.RecordingProgression(command.job_id, 0))

    # wait for recording
    result, data = await future

    event_type = dict(
        success=events.RecorderSuccess,
        failure=events.RecorderFailure,
    ).get(result)

    event = event_type(**data)

    # publish it for the bot
    await broker.publish(event)


async def main():
    logging.getLogger("aiormq").setLevel(logging.INFO)
    # logging.getLogger("websockets").setLevel(logging.INFO)

    queue = asyncio.Queue()
    clients = set()

    bus = MessageBus(dependencies=dict(queue=queue, clients=clients))
    broker = Broker(bus)
    bus.add_dependencies(broker=broker)
    bus.register_decos()
    await broker.start(config.RABBITMQ_HOST, prefetch_count=0)

    await server.serve(
        ws_handler=partial(ws_connection_handler, queue=queue, clients=clients),
        host="localhost",
        port=9191,
    )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
