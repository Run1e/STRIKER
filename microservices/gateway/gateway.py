import sys

sys.path.append("../..")

import asyncio
import http
import logging
from collections import defaultdict
from dataclasses import asdict
from functools import partial
from json import dumps, loads

import config
from websockets import server
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosed

from messages import commands, events
from messages.broker import Broker, MessageError
from messages.bus import MessageBus
from shared.log import logging_config
from shared.utils import sentry_init

logging_config(config.DEBUG)
log = logging.getLogger(__name__)


class ClientMissingError(Exception):
    pass


class GatewayServer:
    def __init__(self, bus: MessageBus, publish, waiter: asyncio.Event) -> None:
        self.bus = bus
        self.publish = publish
        self.waiter = waiter

        self.clients: dict[str, server.WebSocketServerProtocol] = {}
        self.queues: dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self.futures: dict[str, asyncio.Future] = {}
        self.client_jobs: dict[str, set] = defaultdict(set)
        self.reported_recording_job_ids = set()

        self.bus.add_command_handler(commands.RequestRecording, self.request_recording)

        self.message_type_lookup: dict[str, events.Event] = {
            msg.__name__: msg
            for msg in [
                events.RecorderSuccess,
                events.RecorderFailure,
                events.RecordingProgression,
                events.GatewayClientWaiting,
            ]
        }

    def post_add_listeners(self):
        # this is in its own method since we register these listeners
        # after the broker has started, such that we
        # don't listen to these events from rabbitmq but only via our
        # internal .dispatch, which comes from the client
        self.bus.add_event_listener(events.RecorderSuccess, self.recorder_success)
        self.bus.add_event_listener(events.RecorderFailure, self.recorder_failure)
        self.bus.add_event_listener(events.RecordingProgression, self.recorder_progression)
        self.bus.add_event_listener(events.GatewayClientWaiting, self.client_waiting)

    async def new_connection(self, websocket: server.WebSocketServerProtocol):
        hello_pkt = await websocket.recv()
        _type, data = loads(hello_pkt)
        hello = events.GatewayClientHello(**data)
        client_name = hello.client_name

        self.reported_recording_job_ids.update(hello.job_ids)
        self.clients[client_name] = websocket

        log.info("Added client %s job_ids: %s", client_name, len(hello.job_ids))

        tasks = set()

        try:
            async for message in websocket:
                _type, data = loads(message)
                message_type = self.message_type_lookup[_type]
                msg = message_type(**data)
                task = asyncio.create_task(self.bus.dispatch(msg))
                tasks.add(task)
                task.add_done_callback(lambda task: tasks.remove(task))

        except ConnectionClosed:
            log.info("Lost connection to client %s", client_name)

            log.info("Clearing futures...")
            if client_name in self.client_jobs:
                for job_id in self.client_jobs[client_name]:
                    future = self.futures.get(job_id)
                    if future is None:
                        continue

                    log.info("Future state is %s", future._state)
                    if not future.done():
                        log.info("Excepting future for job %s", job_id)
                        future.set_exception(
                            RuntimeError(f"Client close exception for job {job_id}")
                        )

            log.info("Cancelling %s tasks...", len(tasks))

            for task in tasks:
                task.cancel()

            del self.clients[client_name]

            try:
                del self.client_jobs[client_name]
            except KeyError:
                pass

            log.info("Removed client %s", client_name)

    async def send(self, client_name: str, message: commands.Command | events.Event):
        websocket = self.clients.get(client_name)
        if websocket is None:
            raise ClientMissingError(
                f"Client {client_name} has disconnected, unable to send message"
            )

        name = message.__class__.__name__
        dictified = asdict(message)

        await websocket.send(dumps([name, dictified]))

    def get_future(self, job_id):
        if job_id not in self.futures:
            future = asyncio.Future()
            self.futures[job_id] = future

        return self.futures[job_id]

    def delete_future(self, job_id):
        return self.futures.pop(job_id, None)

    def forget_job(self, job_id):
        self.delete_future(job_id)

        for job_ids in self.client_jobs.values():
            if job_id in job_ids:
                job_ids.remove(job_id)

        try:
            self.reported_recording_job_ids.remove(job_id)
        except KeyError:
            pass

    async def client_waiting(self, event: events.GatewayClientWaiting):
        command: commands.RequestRecording

        queue = self.queues[event.game]

        log.info("Client %s waiting for %s job", event.client_name, event.game)
        command = await queue.get()

        log.info("Sending %s to client %s", command.job_id, event.client_name)

        try:
            await self.send(event.client_name, command)
        except ClientMissingError:
            log.info("Client %s removed, requeuing job")
            await queue.put(command)
            return

        self.client_jobs[event.client_name].add(command.job_id)

    async def request_recording(self, command: commands.RequestRecording, retry=True):
        if not self.waiter.is_set():
            await self.waiter.wait()

        game = command.game
        job_id = command.job_id
        queue = self.queues[game]
        future = self.get_future(job_id)

        client_count = len(self.clients)
        queue_size = queue.qsize()
        getter_count = len(queue._getters)

        if command.job_id not in self.reported_recording_job_ids:
            await queue.put(command)

            if not client_count:
                await self.publish(events.RecordingProgression(command.job_id, None))
            elif queue_size >= getter_count:
                await self.publish(
                    events.RecordingProgression(command.job_id, queue_size - getter_count + 1)
                )

        if retry:
            try:
                await future
            except Exception:
                log.info("Job %s failed once, requeueing it...", job_id)
                self.forget_job(job_id)
                await self.request_recording(command, retry=False)

        else:
            await future

    async def recorder_success(self, event: events.RecorderSuccess):
        await self.publish(event)
        future = self.get_future(event.job_id)
        future.set_result(None)
        self.forget_job(event.job_id)

    async def recorder_failure(self, event: events.RecorderFailure):
        future = self.get_future(event.job_id)
        future.set_exception(MessageError(event.reason))
        self.forget_job(event.job_id)

    async def recorder_progression(self, event: events.RecordingProgression):
        await self.publish(event)


async def process_request(path: str, request_headers: Headers, tokens: set):
    if path != "/gateway":
        return http.HTTPStatus.NOT_FOUND, [], b""

    token = request_headers.get("Authorization")

    if token is None or token not in tokens:
        return http.HTTPStatus.UNAUTHORIZED, [], b""

    # continue websocket handshake
    return None


async def main():
    if config.SENTRY_DSN:
        sentry_init(config.SENTRY_DSN)

    logging.getLogger("aio_pika").setLevel(logging.INFO)
    logging.getLogger("aiormq.connection").setLevel(logging.INFO)

    if not config.DEBUG:
        logging.getLogger("websockets").setLevel(logging.INFO)

    waiter = asyncio.Event()

    bus = MessageBus()
    broker = Broker(bus, publish_commands={commands.RequestTokens}, consume_events={events.Tokens})
    g = GatewayServer(bus, broker.publish, waiter)
    await broker.start(config.RABBITMQ_HOST)
    g.post_add_listeners()  # omggggg so fucking dumb

    token_waiter = bus.wait_for(events.Tokens, timeout=32.0)
    await broker.publish(commands.RequestTokens())
    event: events.Tokens | None = await token_waiter

    if event is None:
        log.info("Did not receive tokens in time. Closing in 5 seconds.")
        await asyncio.sleep(5.0)
        quit()

    tokens = set(event.tokens)
    log.info("Token count: %s", len(tokens))

    await server.serve(
        ws_handler=g.new_connection,
        host="0.0.0.0",
        port=9191,
        process_request=partial(process_request, tokens=tokens),
    )

    # the broker was fetching events faster than the clients could connect
    # so new recording events now wait for this instead
    await asyncio.sleep(5.0)
    waiter.set()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
