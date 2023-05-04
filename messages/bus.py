import asyncio
import logging
from collections import defaultdict, deque
from functools import partial
from inspect import signature

from messages import commands, events

deco_command_handlers = dict()
deco_event_listeners = defaultdict(set)

log = logging.getLogger(__name__)


def handler(command: commands.Command):
    def wrapper(f):
        if f in deco_command_handlers:
            raise ValueError(f"Command {command} already has a handler")
        deco_command_handlers[command] = f
        return f

    return wrapper


def listener(event: events.Event):
    def wrapper(f):
        deco_event_listeners[event].add(f)
        return f

    return wrapper


class MessageBus:
    def __init__(self, dependencies: dict, uow_factory=None) -> None:
        self.dependencies = dependencies
        self.uow_factory = uow_factory

        self.command_handlers = dict()
        self.event_listeners = defaultdict(list)

    async def dispatch(self, message):
        if isinstance(message, commands.Command):
            await self.dispatch_command(message)
        elif isinstance(message, events.Event):
            await self.dispatch_event(message)

    async def dispatch_command(self, command: commands.Command):
        handler = self.command_handlers.get(type(command), None)

        if handler is None:
            log.warn("Command has no handler: %s", type(command))
            return

        log.info("Dispatching command: %s", command)
        await handler(command)

    async def dispatch_event(self, event: events.Event):
        listeners = self.event_listeners.get(type(event), None)
        if listeners is not None:
            log.info("Dispatching event to %s listeners: %s", len(listeners), event)

            for listener in listeners:
                # TODO: can be changed back to asyncio.gather later
                # this is just more useful for debugging as we can track down exceptions
                await listener(event)

    async def run_message(self, func, message, needs_uow, deps):
        if needs_uow:
            uow = self.uow_factory()
            deps["uow"] = uow

        await func(message, **deps)

        if needs_uow:
            for message in uow.messages:
                await self.dispatch(message)

    def add_decos(self):
        for command, handler in deco_command_handlers.items():
            self.add_command_handler(command, handler)

        for event, listeners in deco_event_listeners.items():
            for listener in listeners:
                self.add_event_listener(event, listener)

    def add_command_handler(self, command, handler):
        if command in self.command_handlers:
            raise ValueError(f"Handler already added for command {command}")

        needs_uow, deps = self.find_injectables(handler)
        self.command_handlers[command] = partial(
            self.run_message, handler, needs_uow=needs_uow, deps=deps
        )

    def add_event_listener(self, event, listener):
        needs_uow, deps = self.find_injectables(listener)
        self.event_listeners[event].append(
            partial(self.run_message, listener, needs_uow=needs_uow, deps=deps)
        )

    def find_injectables(self, func):
        params = signature(func).parameters
        needs_uow = "uow" in params

        return needs_uow, {
            name: dependency for name, dependency in self.dependencies.items() if name in params
        }

    @property
    def in_use_messages(self):
        return set(self.command_handlers) + set(self.event_listeners)


"""
Commands:
indicates that publisher wants *one* consumer to DO something
one queue per command
published by doing await bus.handle(command)
consumed by doing @handler(command)
queues: use reply_on thing to specify where the micro should reply to, autogen uuid4 for correlation_id

Events:
indicates that publisher wants to inform *any* consumer about an event
one queue per command
published by doing await bus.handle(event)
consumed by doing @listener(event)
queues: put on events' queue which is fanout type
"""
