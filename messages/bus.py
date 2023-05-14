import asyncio
import logging
from collections import defaultdict
from functools import partial
from inspect import signature

from . import commands, deco, events

log = logging.getLogger(__name__)


class MessageBus:
    def __init__(self, dependencies: dict = None, uow_factory=None) -> None:
        self.dependencies = dependencies or dict()
        self.uow_factory = uow_factory

        self.dependencies["wait_for"] = self.wait_for

        self.command_handlers = dict()
        self.event_listeners = defaultdict(list)
        self.checks = defaultdict(set)

    async def dispatch(self, message):
        if isinstance(message, commands.Command):
            await self.dispatch_command(message)
        elif isinstance(message, events.Event):
            await self.dispatch_event(message)

        # are there any checks
        message_type = type(message)
        checks = self.checks.get(message_type, None)
        if checks is None:
            return

        # set future and add to remove set if check succeeds
        to_remove = set()
        for tup in checks:
            check, fut = tup
            if check(message):
                # TODO: should I check for if not fut.cancelled() here?
                fut.set_result(message)
                to_remove.add(tup)

        self._remove_checks(message_type, to_remove)

    async def dispatch_command(self, command: commands.Command):
        handler = self.command_handlers.get(type(command), None)

        if handler is None:
            log.warn("Command has no handler: %s", type(command))
            return

        log.info("Dispatching command: %s", command)
        await handler(command)

    async def dispatch_event(self, event: events.Event):
        listeners = self.event_listeners.get(type(event), [])
        log.info("Dispatching event to %s listeners: %s", len(listeners), event)
        for listener in listeners:
            await listener(event)

    async def run_message(self, func, message, needs_uow, deps):
        if needs_uow:
            uow = self.uow_factory()
            deps["uow"] = uow

        await func(message, **deps)

        if needs_uow:
            for message in uow.messages:
                await self.dispatch(message)

    def wait_for(self, message_type, check=None, timeout=10.0):
        if check is None:
            check = lambda m: True

        fut = asyncio.Future()
        tup = (check, fut)
        self.checks[message_type].add(tup)

        async def waiter():
            try:
                async with asyncio.timeout(delay=timeout):
                    return await fut
            except asyncio.TimeoutError:
                return None
            finally:
                self._remove_checks(message_type, {tup})

        return waiter()

    def _remove_checks(self, message_type, to_remove):
        checks = self.checks.get(message_type, None)
        if checks is None:
            return

        if len(to_remove) == len(checks):
            del self.checks[message_type]
        elif to_remove:
            for remove_tup in to_remove:
                checks.remove(remove_tup)

    def register_decos(self):
        for command, handler in deco.command_handlers.items():
            self.add_command_handler(command, handler)

        for event, listeners in deco.event_listeners.items():
            for listener in listeners:
                self.add_event_listener(event, listener)

    def add_dependencies(self, **kwargs):
        self.dependencies.update(kwargs)

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
    def has_deco(self):
        return set(self.command_handlers).union(set(self.event_listeners))


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
