import inspect
import asyncio
import logging
from asyncio import create_task
from collections import defaultdict

from bot import config
from domain.events import Event


log = logging.getLogger(__name__)

listeners = defaultdict(set)


def listen(event: Event):
    def inner(listener: callable):
        needs_uow = False
        sig = inspect.signature(listener)
        for name in sig.parameters.keys():
            if name == "uow":
                needs_uow = True
                break

        listeners[event].add((listener, needs_uow))
        return listener

    return inner


def remove_listener(event, listener):
    remove = None
    event_listeners = listeners.get(event, None)
    if not event_listeners:
        return

    for inner_listener, needs_uow in event_listeners:
        if inner_listener == listener:
            remove = (inner_listener, needs_uow)
            break

    if remove is not None:
        event_listeners.remove(remove)


def mark(event: Event):
    def inner(listener: callable):
        if not hasattr(listener, "__bus_event__"):
            listener.__bus_event__ = []
        listener.__bus_event__.append(event)
        return listener

    return inner


def register_instance(instance):
    for _, member in inspect.getmembers(instance):
        if not inspect.iscoroutinefunction(member):
            continue

        event_list = getattr(member, "__bus_event__", None)
        if event_list is not None:
            for event in event_list:
                listen(event)(member)


def build_listeners(event):
    from services.uow import SqlUnitOfWork

    recv = listeners[event.__class__]

    result = dict()

    for listener, needs_uow in recv:
        kwargs = dict(event=event)

        if needs_uow:
            kwargs["uow"] = SqlUnitOfWork()

        result[listener] = kwargs

    return result


def store_event(event):
    with open("tests/eventdump", "a", encoding="utf-8") as f:
        f.write("\n\n")
        f.write(repr(event))


async def call(event: Event):
    listeners = build_listeners(event)
    listener_count = len(listeners)

    if listener_count == 0:
        return
    elif listener_count > 1:
        raise ValueError(f"Call dispatch can only call one listener, found {len(listeners)}")

    log.info("Call dispatch for %s", event)

    if config.DUMP_EVENTS:
        store_event(event)

    for listener, kwargs in listeners.items():
        await listener(**kwargs)


def dispatch(event: Event):
    listeners = build_listeners(event)
    log.info(f"Dispatching to {len(listeners)} - {event}")

    if config.DUMP_EVENTS:
        store_event(event)

    for listener, kwargs in listeners.items():
        create_task(listener(**kwargs))


async def wait_for(events, check, timeout: float):
    return_event = None
    asyncio_event = asyncio.Event()

    async def listener(event):
        nonlocal return_event, asyncio_event
        if check(event):
            return_event = event
            asyncio_event.set()

    for event in events:
        listen(event)(listener)

    try:
        await asyncio.wait_for(asyncio_event.wait(), timeout=timeout)
        return return_event
    except asyncio.TimeoutError:
        return None
    finally:
        for event in events:
            remove_listener(event, listener)
