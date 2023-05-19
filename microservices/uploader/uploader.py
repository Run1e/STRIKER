# top-level module include hack for shared :|
import sys

sys.path.append("../..")

import asyncio
import logging
from functools import partial
from io import BytesIO

import config
import disnake
from aiohttp import web

from messages import commands, events
from messages.broker import Broker
from messages.bus import MessageBus
from shared.log import logging_config
from shared.utils import timer

CHUNK_SIZE = 4 * 1024 * 1024

logging_config(config.DEBUG)
log = logging.getLogger(__name__)


async def perform_upload(
    client: disnake.Client, job_id: str, upload_data: events.UploadData, binary_data: bytes
):
    log.info("Uploading job %s", job_id)

    channel_id = upload_data.channel_id
    user_id = upload_data.user_id

    # strip backticks because of how we display this string
    video_title = upload_data.video_title.replace("`", "")

    channel = await client.fetch_channel(channel_id)

    buttons = list()

    buttons.append(
        disnake.ui.Button(
            style=disnake.ButtonStyle.secondary,
            label="How to use the bot",
            emoji="\N{Black Question Mark Ornament}",
            custom_id="howtouse",
        )
    )

    if config.DISCORD_INVITE_URL is not None:
        buttons.append(
            disnake.ui.Button(
                style=disnake.ButtonStyle.url,
                label="Discord",
                emoji=config.DISCORD_EMOJI,
                url=config.DISCORD_INVITE_URL,
            )
        )

    buttons.append(
        disnake.ui.Button(
            style=disnake.ButtonStyle.url,
            label="Patreon",
            emoji=config.PATREON_EMOJI,
            url=config.PATREON_URL,
        )
    )

    b = BytesIO(binary_data)

    await channel.send(
        content=f"<@{user_id}> `{video_title}`",
        file=disnake.File(fp=b, filename=job_id + ".mp4"),
        allowed_mentions=disnake.AllowedMentions(users=[disnake.Object(id=user_id)]),
        components=disnake.ui.ActionRow(*buttons),
    )


async def upload(
    request: web.Request, client: disnake.Client, bus: MessageBus, broker: Broker, tokens: set
) -> web.Response:
    args = request.query

    job_id = args.get("job_id")
    token = request.headers.get("Authorization")

    if job_id is None:
        return web.Response(status=400)
    if token is None or token not in tokens:
        return web.Response(status=401)

    task = bus.wait_for(events.UploadData, check=lambda e: e.job_id == job_id, timeout=32.0)
    await broker.publish(commands.RequestUploadData(job_id))
    upload_data: events.UploadData | None = await task

    if upload_data is None:
        await broker.publish(events.UploaderFailure(job_id, reason="Unable to upload."))
        return web.Response(status=503)

    await perform_upload(client, job_id, upload_data, await request.read())

    await broker.publish(events.UploaderSuccess(job_id))
    return web.Response(status=200)


async def main(injectables):
    logging.getLogger("aiormq").setLevel(logging.INFO)

    client = disnake.AutoShardedClient(intents=disnake.Intents.none())
    bus = MessageBus()
    broker = Broker(
        bus=bus,
        publish_commands={
            commands.RequestUploadData,
            commands.RequestTokens,
        },
        consume_events={
            events.UploadData,
            events.Tokens,
        },
    )

    asyncio.create_task(client.start(config.BOT_TOKEN))
    await client.wait_until_ready()
    await broker.start(config.RABBITMQ_HOST, prefetch_count=2)

    token_waiter = bus.wait_for(events.Tokens, timeout=12.0)
    await broker.publish(commands.RequestTokens())
    event: events.Tokens | None = await token_waiter

    if event is None:
        log.info("Did not receive tokens in time. Closing in 5 seconds.")
        await asyncio.sleep(5.0)
        quit()

    tokens = set(event.tokens)
    log.info("Token count: %s", len(tokens))

    injectables.update(dict(client=client, bus=bus, broker=broker, tokens=tokens))


if __name__ == "__main__":
    # I hate the way I'm getting this data out of main().
    injectables = dict()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(injectables))
    app = web.Application(client_max_size=30 * 1024 * 1024)
    app.add_routes([web.post("/uploader", partial(upload, **injectables))])
    web.run_app(app, host="0.0.0.0", port=9000, loop=loop)
