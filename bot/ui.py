import asyncio
from collections import Counter
from functools import partial
from typing import List

import disnake
from tabulate import tabulate

from domain.domain import Death, Job, Player, User

from .area import places
from .sharecode import is_valid_sharecode
from .config import CT_COIN, T_COIN


class AbortButton(disnake.ui.Button):
    def __init__(self, *, callback, style=disnake.ButtonStyle.danger, label="Abort", row=0):
        super().__init__(style=style, label=label, row=row)
        self._callback = callback

    async def callback(self, inter: disnake.MessageInteraction):
        asyncio.create_task(self._callback(inter))


class PlayerButton(disnake.ui.Button):
    def __init__(self, callback, player, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._callback = callback
        self.player = player

    async def callback(self, inter: disnake.MessageInteraction):
        self.view.stop()
        asyncio.create_task(self._callback(inter, self.player))


class PlayerView(disnake.ui.View):
    def __init__(
        self,
        *,
        job: Job,
        player_callback,
        abort_callback,
        timeout_callback,
        timeout=180.0,
    ):
        super().__init__(timeout=timeout)
        self.job = job
        self.player_callback = player_callback
        self.abort_callback = abort_callback
        self.on_timeout = timeout_callback

        # team one starts as T, team two starts as CT
        team_one, team_two = job.demo.teams

        for row, players in enumerate([team_two, team_one]):
            label = f"Team {row + 1}"
            self.add_item(
                disnake.ui.Button(
                    style=disnake.ButtonStyle.secondary,
                    label=label,
                    disabled=True,
                    row=row * 2,
                )
            )

            for player in players:
                self.add_item(
                    PlayerButton(
                        callback=player_callback,
                        player=player,
                        label=player.name,
                        style=disnake.ButtonStyle.primary,
                        row=(row * 2) + 1,
                    )
                )

        self.add_item(AbortButton(callback=self.abort_callback))


class RoundButton(disnake.ui.Button):
    def __init__(self, callback, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._callback = callback

    async def callback(self, inter: disnake.MessageInteraction):
        self.view.stop()
        asyncio.create_task(self._callback(inter, int(self.label)))


class RoundView(disnake.ui.View):
    def __init__(
        self,
        *,
        round_callback,
        reselect_callback,
        abort_callback,
        timeout_callback,
        job: Job,
        embed_factory,
        player: Player,
        timeout=180.0,
    ):
        super().__init__(timeout=timeout)

        self.round_callback = round_callback
        self.reselect_callback = reselect_callback
        self.abort_callback = abort_callback
        self.on_timeout = timeout_callback
        self.timeout_callback = timeout_callback
        self.job = job
        self.embed_factory = embed_factory
        self.demo = job.demo
        self.player = player
        self.player_team = self.demo.get_player_team(player)
        self.kills: List[Death] = self.demo.get_player_kills(player)
        self.round_buttons = list()

        self.first_half.emoji = T_COIN if self.player_team == 0 else CT_COIN
        self.second_half.emoji = CT_COIN if self.player_team == 0 else T_COIN

        self.highlights = {
            True: self.create_table(True),
            False: self.create_table(False),
        }

        for round_id in range(1, self.demo.halftime + 1):
            button = RoundButton(
                callback=round_callback,
                style=disnake.ButtonStyle.primary,
                label="placeholder",
                row=((round_id - 1) // 5) + 1,
            )

            self.round_buttons.append(button)
            self.add_item(button)

    @disnake.ui.button(row=0)
    async def first_half(self, button: disnake.Button, inter: disnake.MessageInteraction):
        embed = await self.set_half(True)

        await inter.response.edit_message(
            content=None,
            embed=embed,
            view=self,
        )

    @disnake.ui.button(row=0)
    async def second_half(self, button: disnake.Button, inter: disnake.MessageInteraction):
        embed = await self.set_half(False)

        await inter.response.edit_message(
            content=None,
            embed=embed,
            view=self,
        )

    @disnake.ui.button(style=disnake.ButtonStyle.secondary, label="Select another player", row=0)
    async def reselect(self, button: disnake.Button, inter: disnake.MessageInteraction):
        self.stop()
        asyncio.create_task(self.reselect_callback(inter))

    @disnake.ui.button(style=disnake.ButtonStyle.danger, label="Abort", row=0)
    async def abort(self, button: disnake.Button, inter: disnake.MessageInteraction):
        self.stop()
        asyncio.create_task(self.abort_callback(inter))

    def round_range(self, first_half):
        halftime = self.demo.halftime
        return range(
            1 if first_half else halftime + 1,
            (halftime if first_half else halftime * 2) + 1,
        )

    def create_table(self, first_half):
        demo = self.job.demo
        round_range = self.round_range(first_half)
        map_area = places.get(demo.map, None)
        data = []

        for round_id in round_range:
            kills = self.kills.get(round_id, None)
            if kills is not None:
                data.append(demo.kills_info(round_id, kills, map_area))

        if not data:
            return "This player got zero kills this half."
        else:
            return tabulate(
                tabular_data=data,
                colalign=("left", "left", "left"),
                tablefmt="plain",
            )

    async def set_half(self, first_half):
        enabled_style = disnake.ButtonStyle.success
        disabled_style = disnake.ButtonStyle.primary

        self.first_half.disabled = first_half
        self.second_half.disabled = not first_half
        self.first_half.style = enabled_style if first_half else disabled_style
        self.second_half.style = disabled_style if first_half else enabled_style

        round_range = self.round_range(first_half)
        max_rounds = self.demo.round_count

        for round_id, button in zip(round_range, self.round_buttons):
            button.label = str(round_id)

            if round_id > max_rounds:
                button.disabled = True
                button.style = disnake.ButtonStyle.secondary
            else:
                button.disabled = round_id not in self.kills
                button.style = disnake.ButtonStyle.primary

        embed = self.embed_factory()

        table = self.highlights[first_half]
        half_one = "T" if self.player_team == 0 else "CT"
        half_two = "CT" if self.player_team == 0 else "T"
        team = half_one if first_half else half_two

        # embed.title = 'Select a round to render'

        embed.description = (
            f"Table of {self.player.name}'s frags on the {team} side.\n"
            f"```{table}```\n"
            "Click a round number below to record a highlight.\n"
            "Click the 'CT' or 'T' coins to show frags from the other half."
        )

        return embed


class ConfigButton(disnake.ui.Button):
    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key

    def set_state(self, state):
        if state:
            self.style = disnake.ButtonStyle.primary
        else:
            self.style = disnake.ButtonStyle.secondary


class CrosshairModal(disnake.ui.Modal):
    def __init__(self, *, on_submit, title: str, timeout: float = 600) -> None:
        components = [
            disnake.ui.TextInput(
                label="Crosshair sharecode",
                custom_id="crosshairinput",
                placeholder="Paste sharecode here",
                style=disnake.TextInputStyle.short,
                min_length=34,
                max_length=34,
            )
        ]

        self.callback = on_submit

        super().__init__(title=title, components=components, timeout=timeout)


class ConfigView(disnake.ui.View):
    def __init__(
        self,
        inter: disnake.AppCmdInter,
        user: User,
        store_callback,
        abort_callback,
        timeout=900.0,
    ):
        super().__init__(timeout=timeout)

        self.inter = inter
        self.user = user
        self.store_callback = store_callback
        self.abort_callback = abort_callback

        row_calc = lambda i: (i // 4)

        for index, (k, v) in enumerate(user.all_recording_settings().items()):
            row = row_calc(index)
            button = ConfigButton(
                key=k,
                label=self.name_mapper(k),
                row=row,
            )
            button.callback = partial(self.button_click, button)
            button.set_state(v)
            self.add_item(button)

        button = disnake.ui.Button(
            style=disnake.ButtonStyle.secondary, label="Change crosshair", row=row + 1
        )

        button.callback = self.send_crosshair_modal
        self.add_item(button)

        button = disnake.ui.Button(
            style=disnake.ButtonStyle.secondary, label="Reset crosshair", row=row + 1
        )

        button.callback = self.reset_crosshair
        self.add_item(button)

        button = disnake.ui.Button(
            style=disnake.ButtonStyle.primary, label="Save", row=row + 2
        )

        button.callback = self.save
        self.add_item(button)

        button = disnake.ui.Button(
            style=disnake.ButtonStyle.red, label="Abort", row=row + 2
        )

        button.callback = self.abort
        self.add_item(button)

    def name_mapper(self, k):
        return dict(
            fragmovie="Clean HUD",
            color_filter="Vibrancy filter",
            righthand="cl_righthand",
            sixteen_nine="16:9",
        ).get(k, k)

    def embed(self):
        e = disnake.Embed(
            color=disnake.Color.orange(),
        )

        e.set_author(
            name="STRIKER Patreon Configurator", icon_url=self.inter.bot.user.display_avatar
        )

        cc = self.user.crosshair_code
        e.description = f"Thank you for your support.\n\nCrosshair: {cc if cc is not None else 'Default'}"

        e.add_field(
            name="Clean HUD", value="Hide HUD except for killfeed and crosshair", inline=False
        )
        e.add_field(name="Vibrancy filter", value="Enable the video filter", inline=False)
        e.add_field(name="cl_righthand", value="Enable for right handed gun wielding", inline=False)
        e.add_field(name="16:9", value="Record at a 16:9 aspect ratio", inline=False)

        return e

    async def button_click(self, button: disnake.Button, inter: disnake.MessageInteraction):
        value = self.user.get(button.key)
        self.user.set(button.key, not value)
        button.set_state(not value)
        await inter.response.edit_message(view=self)

    async def send_crosshair_modal(self, inter: disnake.MessageInteraction):
        await inter.response.send_modal(
            modal=CrosshairModal(
                on_submit=self.save_crosshair, title="Set crosshair", timeout=500.0,
            )
        )

    async def save_crosshair(self, inter: disnake.ModalInteraction):
        sharecode = inter.text_values["crosshairinput"]

        if not is_valid_sharecode(sharecode):
            await inter.response.send_message(content="That does not appear to be a valid crosshair sharecode", delete_after=6.0)
            return

        self.user.crosshair_code = sharecode
        await inter.response.edit_message(embed=self.embed())

    async def reset_crosshair(self, inter: disnake.ModalInteraction):
        self.user.crosshair_code = None
        await inter.response.edit_message(embed=self.embed())

    async def save(self, inter: disnake.MessageInteraction):
        self.stop()
        asyncio.create_task(self.store_callback(inter, self.user))

    async def abort(self, inter: disnake.MessageInteraction):
        self.stop()
        asyncio.create_task(self.abort_callback(inter))

    async def on_timeout(self):
        e = disnake.Embed(color=disnake.Color.red())
        e.set_author(name="STRIKER", icon_url=self.inter.bot.user.display_avatar)
        e.description = "Configurator timed out"

        message = await self.inter.original_message()
        await message.edit(content=None, embed=e, view=None)
