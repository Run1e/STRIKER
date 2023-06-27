import asyncio
import logging

from steam.ext.csgo import Client

from bot.sharecode import decode

log = logging.getLogger(__name__)


async def get_match_fetcher(refresh_token):
    logging.getLogger("steam").setLevel(logging.INFO)

    client = Client()

    async def dummy(*args, **kwargs):
        pass

    client._state.update_backpack = dummy

    asyncio.create_task(client.login(refresh_token=refresh_token))

    async def fetcher(sharecode):
        v = decode(sharecode)
        match = await client.fetch_match(**v)
        return match.id, match.created_at, match.rounds[-1].map

    return client, fetcher, client.wait_for_gc_ready()
