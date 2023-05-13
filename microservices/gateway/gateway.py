from collections import defaultdict
import sys

sys.path.append("../..")

import asyncio
import logging
from dataclasses import asdict
from json import dumps, loads

import config
from websockets import server
from websockets.exceptions import ConnectionClosed

from messages import commands, events
from messages.broker import Broker
from messages.bus import MessageBus
from shared.log import logging_config

logging_config(config.DEBUG)
log = logging.getLogger(__name__)


class GatewayServer:
    def __init__(self, bus: MessageBus, broker: Broker, waiter: asyncio.Event) -> None:
        self.bus = bus
        self.broker = broker
        self.waiter = waiter

        self.clients = set()

        self.queue = asyncio.Queue()
        self.futures = dict()
        self.client_futures = defaultdict(set)

        # job ids that newly connecting workers are already working on
        self.recording_job_ids = set()

        self.bus.add_command_handler(commands.RequestRecording, self.request_recording)

        self.action_handlers = dict(
            request=self.action_request,
            success=self.action_success,
            failure=self.action_failure,
        )

    async def new_connection(self, websocket: server.WebSocketServerProtocol):
        self.clients.add(websocket)

        hello_pkt = await websocket.recv()
        client_job_ids = loads(hello_pkt)

        self.recording_job_ids.update(client_job_ids)

        log.info("Client added (total: %s). Job count: %s", len(self.clients), len(client_job_ids))

        tasks = set()

        try:
            async for message in websocket:
                task = asyncio.create_task(self.recv(websocket, message))
                tasks.add(task)
                task.add_done_callback(lambda task: tasks.remove(task))
        except ConnectionClosed:
            pass  # cleanup comes after here, no need to

        self.clients.remove(websocket)
        log.info("Client removed (total: %s). Tasks: %s", len(self.clients), len(tasks))

        for task in tasks:
            if not task.done():
                log.info("Cancelling task")
                task.cancel()

        futures = self.client_futures.get(websocket, [])
        for future in futures:
            future.cancel()

    async def recv(self, client: server.WebSocketServerProtocol, message: str):
        action, data = loads(message)

        log.info("Recv action %s", action)

        handler = self.action_handlers.get(action)
        if not handler:
            raise ValueError(f"Action {action} has no handler")

        await handler(client, data)

    async def action_success(self, client, data):
        job_id = data["job_id"]
        await self.set_result(job_id, events.RecorderSuccess(**data))

    async def action_failure(self, client, data):
        job_id = data["job_id"]
        await self.set_result(job_id, events.RecorderFailure(**data))

    async def action_request(self, client, data):
        log.info("Waiting for job...")
        command, started, future = await self.queue.get()

        # I hate that I have to keep state here for this
        self.client_futures[client].add(future)

        log.info("Job found, sending")
        await client.send(dumps(asdict(command)))

        started.set()

    def get_future(self, job_id):
        future = self.futures.get(job_id)
        if future is None:
            future = asyncio.Future()
            self.futures[job_id] = future

        return future

    def remove_future(self, job_id):
        future = self.futures.pop(job_id, None)

        if not future:
            return

        for futures in self.client_futures.values():
            if future in futures:
                futures.remove(future)
                break

    async def set_result(self, job_id, event):
        log.info(f"Setting future: {event}")
        future = self.get_future(job_id)
        future.set_result(event)

    async def request_recording(self, command: commands.RequestRecording, retry=False):
        # this method runs for the entire lifecycle of the recording.
        # it's dispatched by the broker into here, and the asyncio.Event
        # that's created waits for the job to get picked up off the queue.
        # after that we wait for the future to be set, which should contain
        # either a events.RecordingSuccess or ...Failure.
        # we then publish that back to whatever is listening to
        # that event. (upd: with some retry logic as well)
        #
        # in the case where a recording node is working on a recording and
        # the gateway (this process) restarts, we rely on the GatewayClient
        # "hello" action packet to self-report which recordings its pool is
        # currently working on. that means we *DON'T* put this job on the queue
        # as that would make it be recorded twice. we do however still have to
        # make the future and await it, so that we can properly ack the
        # DeliveredMessage and publish a Recorder* event.
        # it's kind of dumb and messy, but it's what we have to do because of
        # the high level of decoupling here, and different states between
        # different services

        if not self.waiter.is_set():
            await self.waiter.wait()

        queue_size = self.queue.qsize()
        getter_count = len(self.queue._getters)
        client_count = len(self.clients)

        # print(queue_size, getter_count, client_count)

        future = self.get_future(command.job_id)

        if command.job_id in self.recording_job_ids:
            log.info(f"Job already being processed: {command.job_id}")
        else:
            started = asyncio.Event()

            await self.queue.put((command, started, future))

            if not client_count:  # no clients, send with None
                await self.broker.publish(events.RecordingProgression(command.job_id, None))
            elif queue_size >= getter_count:  # more waiting in queue than clients doing .get()
                await self.broker.publish(
                    events.RecordingProgression(command.job_id, queue_size - getter_count + 1)
                )

            # wait for connection handler to .get() this request
            await started.wait()

            # this request is currently being processed
            await self.broker.publish(events.RecordingProgression(command.job_id, 0))

        # wait for recording to finish, then ack the message
        try:
            event = await future
        except asyncio.CancelledError:
            # client most likely died, set a failure event
            # which might requeue depending on the value of retry
            event = events.RecorderFailure(command.job_id, "Recording node disconnected while recording.")

        # remove future
        self.remove_future(command.job_id)

        if isinstance(event, events.RecorderSuccess):
            await self.broker.publish(event)
        elif isinstance(event, events.RecorderFailure):
            if not retry:
                log.info("Retrying %s", command.job_id)
                await self.request_recording(command, retry=True)
            else:
                await self.broker.publish(event)


async def main():
    logging.getLogger("aiormq").setLevel(logging.INFO)
    # logging.getLogger("websockets").setLevel(logging.INFO)

    waiter = asyncio.Event()

    bus = MessageBus()
    broker = Broker(bus)
    g = GatewayServer(bus, broker, waiter)
    await broker.start(config.RABBITMQ_HOST, prefetch_count=0)

    await server.serve(
        ws_handler=g.new_connection,
        host="localhost",
        port=9191,
    )

    # the broker was fetching events faster than the clients could connect
    # so new recording events now wait for this instead
    await asyncio.sleep(5.0)
    waiter.set()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
