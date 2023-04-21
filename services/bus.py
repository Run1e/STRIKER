import inspect
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

    if len(listeners) != 1:
        raise ValueError(f"Call dispatch can only call one listener, found {len(listeners)}")

    log.info(f"Call dispatch for {event}")

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
