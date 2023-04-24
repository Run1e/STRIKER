import io
import logging
import re
import textwrap
import traceback
from collections import Counter
from contextlib import redirect_stdout
from pprint import pprint

import disnake
from disnake.ext import commands
from tabulate import tabulate

from . import config

log = logging.getLogger(__name__)


class Owner(commands.Cog):
    """Commands accessible only to the bot owner."""

    def __init__(self, bot):
        self.bot = bot
        self.event_counter = Counter()

    @commands.Cog.listener()
    async def on_socket_event_type(self, event_type):
        self.event_counter[event_type] += 1

    @commands.Cog.listener()
    async def on_message(self, message: disnake.Message):
        match = re.search(rf"^<@{self.bot.user.id}>\s*```(py)?([\s\S]*)```$", message.content)
        if match is None:
            return

        if not await self.bot.is_owner(message.author):
            return

        await self._eval(message, match.group(2).strip())

    async def _eval(self, message: disnake.Message, code: str):
        """Evaluates some code."""

        env = {
            "disnake": disnake,
            "bot": self.bot,
            "channel": message.channel,
            "author": message.author,
            "guild": message.guild,
            "message": message,
            "pp": pprint,
            "tabulate": tabulate,
        }

        env.update(globals())

        stdout = io.StringIO()

        to_compile = f'async def func():\n{textwrap.indent(code, "  ")}'

        try:
            exec(to_compile, env)
        except Exception as e:
            return await message.channel.send(f"```py\n{e.__class__.__name__}: {e}\n```")

        func = env["func"]
        try:
            with redirect_stdout(stdout):
                ret = await func()
        except Exception as e:
            value = stdout.getvalue()
            await message.channel.send(f"```py\n{value}{traceback.format_exc()}\n```")
        else:
            value = stdout.getvalue()
            try:
                await message.add_reaction("\u2705")
            except:
                pass

            if ret is None:
                if value:
                    if len(value) > 1990:
                        fp = io.BytesIO(value.encode("utf-8"))
                        await message.channel.send(
                            "Log too large...", file=disnake.File(fp, "results.txt")
                        )
                    else:
                        await message.channel.send(f"```py\n{value}\n```")

    @commands.slash_command(
        name="gateway",
        description="See gateway event counters",
        dm_permission=False,
        guild_ids=[config.STRIKER_GUILD_ID],
    )
    @commands.is_owner()
    async def gateway(self, inter: disnake.AppCmdInter):
        """Print gateway event counters."""

        table = tabulate(
            tabular_data=[
                (name, format(count, ",d")) for name, count in self.event_counter.most_common()
            ],
            headers=("Event", "Count"),
        )

        paginator = commands.Paginator()
        for line in table.split("\n"):
            paginator.add_line(line)

        for page in paginator.pages:
            await inter.send(page)


def setup(bot):
    bot.add_cog(Owner(bot))
