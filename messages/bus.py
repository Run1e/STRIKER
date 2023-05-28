import asyncio
import logging
from collections import defaultdict
from functools import partial
from inspect import signature

from . import commands, deco, events

log = logging.getLogger(__name__)


class MessageBus:
    def __init__(self, dependencies: dict = None, factories: dict = None) -> None:
        self.dependencies = dependencies or dict()
        self.factories = factories or dict()

        self.dependencies["wait_for"] = self.wait_for

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
        listeners = self.event_listeners.get(type(event), [])
        if not listeners:
            log.info("Event has no listeners: %s", event)
            return

        log.info("Dispatching to %s listeners: %s", len(listeners), event)
        for listener in listeners:
            await listener(event)

    async def run_message(self, func, message, dependencies, factories):
        built_factories = {key: factory() for key, factory in factories.items()}
        await func(message, **dependencies, **built_factories)

        if (uow := built_factories.get("uow")):
            for message in uow.messages:
                await self.dispatch(message)

    def wait_for(self, message_type, check=None, timeout=10.0):
        async def listener(message):
            if check is None or check(message):
                fut.set_result(message)
                self.remove_event_listener(message_type, added)
                task.cancel()

        async def sleeper():
            await asyncio.sleep(timeout)
            self.remove_event_listener(message_type, added)
            fut.set_result(None)

        added = self.add_event_listener(message_type, listener)
        fut = asyncio.Future()
        task = asyncio.create_task(sleeper())
        return fut

    def add_dependencies(self, **kwargs):
        self.dependencies.update(kwargs)

    def add_command_handler(self, command, handler):
        if command in self.command_handlers:
            raise ValueError(f"Handler already added for command {command}")

        dependencies, factories = self.find_injectables(handler)
        self.command_handlers[command] = partial(
            self.run_message, handler, dependencies=dependencies, factories=factories
        )

    def add_event_listener(self, event, listener):
        dependencies, factories = self.find_injectables(listener)
        added = partial(self.run_message, listener, dependencies=dependencies, factories=factories)
        self.event_listeners[event].append(added)
        return added

    def remove_event_listener(self, event, listener):
        try:
            self.event_listeners[event].remove(listener)
        except ValueError:
            pass

    def find_injectables(self, func):
        params = signature(func).parameters

        dependencies = {
            name: dependency for name, dependency in self.dependencies.items() if name in params
        }

        factories = {name: factory for name, factory in self.factories.items() if name in params}

        return dependencies, factories

    def register_decos(self):
        for command, handler in deco.command_handlers.items():
            self.add_command_handler(command, handler)

        for event, listeners in deco.event_listeners.items():
            for listener in listeners:
                self.add_event_listener(event, listener)

    @property
    def has_deco(self):
        return set(self.command_handlers).union(set(self.event_listeners))
