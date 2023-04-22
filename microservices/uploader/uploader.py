# top-level module include hack for shared :|
import sys

sys.path.append("../..")


import asyncio
import logging

import aiormq
import config
import disnake

from shared.log import logging_config
from shared.message import MessageError, MessageWrapper

CHUNK_SIZE = 4 * 1024 * 1024

logging_config(config.DEBUG)
log = logging.getLogger(__name__)

loop = None

client = disnake.Client(intents=disnake.Intents.none())


async def on_upload(message: aiormq.channel.DeliveredMessage):
    wrap = MessageWrapper(
        message=message,
        default_error="An error occurred while uploading.",
        ack_on_failure=True,
    )

    async with wrap as ctx:
        data = ctx.data
        job_id = ctx.correlation_id
        user_id = data["user_id"]
        channel_id = data["channel_id"]
        file_name = data["file_name"]

        log.info("Uploading job %s", job_id)

        channel = await client.fetch_channel(channel_id)
        if channel is None:
            raise MessageError("Uploader failed finding channel.")

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

        await channel.send(
            content=f"<@{user_id}> `{file_name}`",
            file=disnake.File(fp=f"{config.VIDEO_DIR}/{job_id}.mp4", filename=file_name + ".mp4"),
            components=disnake.ui.ActionRow(*buttons),
        )

        await ctx.success()


async def main():
    logging.getLogger("aiormq").setLevel(logging.INFO)

    asyncio.create_task(client.start(config.BOT_TOKEN))
    await client.wait_until_ready()

    mq = await aiormq.connect(config.RABBITMQ_HOST)
    chan = await mq.channel()

    await chan.basic_qos(prefetch_count=1)

    # await chan.queue_declare(config.UPLOAD_QUEUE)
    await chan.basic_consume(queue=config.UPLOAD_QUEUE, consumer_callback=on_upload, no_ack=False)

    log.info("Ready to upload!")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
