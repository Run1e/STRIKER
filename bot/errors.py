import disnake
from disnake.ext import commands

from services.services import ServiceError


class BotAccountRequired(commands.CheckFailure):
    pass


class SponsorRequired(commands.CheckFailure):
    pass


class ErrorHandler(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_slash_command_error(self, inter: disnake.AppCmdInter, exc):
        """Handle command errors."""

        if not inter.response.is_done():
            await inter.response.defer(ephemeral=True)

        title = "Oops!"
        desc = "Some undefined error occurred, sorry about that!"

        is_ok = True

        if isinstance(exc, commands.UserInputError):
            desc = str(exc)
        elif isinstance(exc, commands.BotMissingPermissions):
            title = "The bot is missing some permissions!"
            desc = str(exc)
            perms = inter.channel.permissions_for(inter.me)
            if not perms.read_messages:
                desc += "\n\nAre you sure the bot has access to this channel?"
        elif isinstance(exc, commands.CheckFailure):
            desc = str(exc)
        elif isinstance(exc, commands.CommandInvokeError) and isinstance(
            exc.original, ServiceError
        ):
            desc = str(exc.original)
        elif type(exc) is commands.CommandError:
            desc = str(exc)
        else:
            is_ok = False

        embed = disnake.Embed(color=disnake.Color.red())
        embed.set_author(name=title, icon_url=self.bot.user.display_avatar)
        embed.description = desc
        kwargs = dict(content=None, components=self.bot._error_actionrow, embed=embed)


        try:
            message = await inter.original_response()
        except disnake.NotFound:
            message = None

        if message:
            await message.edit(**kwargs)
        else:
            await inter.followup.send(ephemeral=False, **kwargs)

        if not is_ok:
            raise exc


def setup(bot):
    bot.add_cog(ErrorHandler(bot))
