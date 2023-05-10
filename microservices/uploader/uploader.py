# top-level module include hack for shared :|
from io import BytesIO
import sys

sys.path.append("../..")

import asyncio
import logging

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


client = disnake.AutoShardedClient(intents=disnake.Intents.none())
bus = MessageBus()
broker = Broker(
    bus=bus,
    publish_commands={
        commands.ValidateUploadArgs,
    },
    extra_events={
        events.UploadArgsValidated,
    },
)


async def upload(request: web.Request) -> web.Response:

    await request.read()

    args = request.query

    job_id = args.get("job_id")
    upload_token = args.get("upload_token")

    if job_id is None or upload_token is None:
        return web.Response(status=400)

    task = asyncio.create_task(
        bus.wait_for(events.UploadArgsValidated, check=lambda e: e.job_id == job_id, timeout=20.0)
    )

    await broker.publish(commands.ValidateUploadArgs(job_id, upload_token))

    validated: events.UploadArgsValidated = await task
    if validated is None:
        return web.Response(status=500)

    log.info("Uploading job %s", job_id)

    channel_id = validated.channel_id
    user_id = validated.user_id

    # strip backticks because of how we display this string
    video_title = validated.video_title.replace("`", "")

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
                emoji=":discord:1099362254731882597",
                url=config.DISCORD_INVITE_URL,
            )
        )

    if config.GITHUB_URL is not None:
        buttons.append(
            disnake.ui.Button(
                style=disnake.ButtonStyle.url,
                label="GitHub",
                emoji=":github:1099362911077544007",
                url=config.GITHUB_URL,
            )
        )

    buttons.append(
        disnake.ui.Button(
            style=disnake.ButtonStyle.secondary,
            label="Donate",
            emoji="\N{Hot Beverage}",
            custom_id="donatebutton",
        )
    )

    b = BytesIO(await request.read())

    await channel.send(
        content=f"<@{user_id}> `{video_title}`",
        file=disnake.File(fp=b, filename=job_id + ".mp4"),
        allowed_mentions=disnake.AllowedMentions(users=[disnake.Object(id=user_id)]),
        components=disnake.ui.ActionRow(*buttons),
    )

    return web.Response(status=200)


async def main():
    logging.getLogger("aiormq").setLevel(logging.INFO)

    asyncio.create_task(client.start(config.BOT_TOKEN))
    await client.wait_until_ready()
    await broker.start(config.RABBITMQ_HOST, prefetch_count=1)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    app = web.Application(client_max_size=30 * 1024 * 1024)
    app.add_routes([web.post("/upload", upload)])
    web.run_app(app, host="0.0.0.0", port=9000, loop=loop)
