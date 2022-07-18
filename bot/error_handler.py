import disnake
from disnake.ext import commands


class ErrorHandler(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_slash_command_error(self, inter: disnake.AppCmdInter, exc):
        '''Handle command errors.'''

        if not inter.response.is_done():
            await inter.response.defer(ephemeral=True)

        embed = disnake.Embed(color=disnake.Color.red())

        embed.set_author(name='Oops!', icon_url=self.bot.user.display_avatar)

        kwargs = dict(content=None, embed=embed)

        if isinstance(exc, commands.UserInputError):
            embed.description = str(exc)
        elif type(exc) is commands.CommandError:
            embed.description = str(exc)
        else:
            embed.description = 'Some undefined error occurred, sorry about that!'

        try:
            message = await inter.original_message()
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
