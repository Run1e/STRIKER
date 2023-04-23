import disnake
from disnake.ext import commands

from .errors import BotAccountRequired


class GlobalCheck(commands.Cog, slash_command_attrs={"dm_permission": False}):
    """Global checks for monty."""

    def __init__(self, bot) -> None:
        self.bot = bot

    def bot_slash_command_check(self, inter: disnake.AppCmdInter) -> bool:
        """
        Require all commands in guilds have the bot scope.

        This essentially prevents commands from running when the Bot is not in a guild.

        However, this does allow slash commands in DMs as those are now controlled via
        the dm_permisions attribute on each app command.
        """

        if inter.guild or not inter.guild_id:
            return True

        invite = self.bot.craft_guild_invite_link(inter.guild)
        if inter.permissions.manage_guild:
            msg = (
                "The bot is missing the the necessary bot scope.\n\n"
                f"Please kick the bot, then invite the full bot by [clicking here](<{invite}>)."
            )
        else:
            msg = (
                "The bot is missing the the necessary bot scope.\n\n"
                f"Please ask a server manager to kick the bot, and then invite the bot **using this link**:\n{invite}"
            )
        raise BotAccountRequired(msg)


def setup(bot) -> None:
    """Add the global checks to the bot."""
    bot.add_cog(GlobalCheck(bot))
