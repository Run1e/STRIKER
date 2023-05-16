import asyncio
from collections import Counter
from functools import partial
from typing import List

import disnake
from tabulate import tabulate

from domain.demo_events import Death, Match, MatchHalf, Player

from .area import places
from .config import CT_COIN, T_COIN
from .sharecode import is_valid_sharecode


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
        match: Match,
        player_callback,
        abort_callback,
        timeout_callback,
        timeout=180.0,
    ):
        super().__init__(timeout=timeout)
        self.match = match
        self.player_callback = player_callback
        self.abort_callback = abort_callback
        self.on_timeout = timeout_callback

        half = self.match.halves[0]
        team_one, team_two = half.teams

        # team one starts as T, team two starts as CT
        team_one, team_two = half.teams[2], half.teams[3]

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

        self.add_item(AbortButton(callback=self.abort_callback, row=4))


class HalfButton(disnake.ui.Button):
    def __init__(self, key, callback, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._key = key
        self._callback = callback

    async def callback(self, inter: disnake.MessageInteraction):
        asyncio.create_task(self._callback(inter, self._key))


class RoundButton(disnake.ui.Button):
    def __init__(self, callback, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._callback = callback

    async def callback(self, inter: disnake.MessageInteraction):
        self.view.stop()
        asyncio.create_task(self._callback(inter))


def coin_lookup(teamnum):
    return {
        2: T_COIN,
        3: CT_COIN,
    }.get(teamnum)


class RoundView(disnake.ui.View):
    def __init__(
        self,
        *,
        match: Match,
        player: Player,
        round_callback,
        reselect_callback,
        abort_callback,
        timeout_callback,
        embed_factory,
        timeout=180.0,
    ):
        super().__init__(timeout=timeout)

        self.match = match
        self.round_callback = round_callback
        self.reselect_callback = reselect_callback
        self.abort_callback = abort_callback
        self.on_timeout = timeout_callback
        self.timeout_callback = timeout_callback

        self.embed_factory = embed_factory
        self.player = player

        self.half_buttons = dict()
        self.round_buttons = list()

        for half_num, half in enumerate(self.match.halves):
            row = half_num // 5

            teamnum = half.get_player_teamnum(player)
            emoji = coin_lookup(teamnum)

            # faceit does some weird ass shit with their teamnums,
            # I genuinely don't know which team starts as CT/T and
            # when it switches. I'll have to investigate
            if match.origin == "faceit":
                emoji = None

            button = HalfButton(
                key=half_num,
                label=half.name or "UNK",
                callback=self.half_callback,
                row=row,
                emoji=emoji,
            )

            self.half_buttons[half_num] = button
            self.add_item(button)

        # round buttons are gonna have to start at this + 1
        self.min_row = row

        row = 1 if len(match.halves) > 5 else 4

        reselect_button = disnake.ui.Button(
            style=disnake.ButtonStyle.secondary,
            label="Select another player",
            row=row,
        )
        reselect_button.callback = self.reselect_click

        abort_button = disnake.ui.Button(
            style=disnake.ButtonStyle.danger,
            label="Abort",
            row=row,
        )
        abort_button.callback = self.abort_click

        self.add_item(reselect_button)
        self.add_item(abort_button)

    async def reselect_click(self, inter: disnake.MessageInteraction):
        self.stop()
        asyncio.create_task(self.reselect_callback(inter))

    async def abort_click(self, inter: disnake.MessageInteraction):
        self.stop()
        asyncio.create_task(self.abort_callback(inter))

    async def half_callback(self, inter: disnake.MessageInteraction, n):
        embed = self.set_half(n)
        await inter.response.edit_message(content=None, embed=embed, view=self)

    def set_half(self, n):
        # remove previous round buttons
        for button in self.round_buttons:
            self.remove_item(button)
        self.round_buttons.clear()

        for half_num, half_button in self.half_buttons.items():
            half_button.disabled = half_num == n

        half = self.match.halves[n]
        all_kills = half.get_player_kills(self.player)

        for idx, (round_num, kills) in enumerate(all_kills.items()):
            button = RoundButton(
                partial(self.round_callback, half=n, round_id=round_num),
                style=disnake.ButtonStyle.primary,
                label=round_num,
                row=(idx // 5) + self.min_row + 1,
                disabled=not kills,
            )

            self.round_buttons.append(button)
            self.add_item(button)

        e: disnake.Embed = self.embed_factory()

        table = self.gen_table(half)
        e.description = (
            f"Table of {self.player.name}'s frags.\n"
            f"```{table}```\n"
            "Click a round number below to record a highlight.\n"
            "Click the top row of buttons to change half."
        )

        return e

    def round_range(self, first_half):
        halftime = self.match.halftime
        return range(
            1 if first_half else halftime + 1,
            (halftime if first_half else halftime * 2) + 1,
        )

    def gen_table(self, half: MatchHalf):
        map_area = places.get(self.match.map, None)
        data = []

        all_kills = half.get_player_kills(self.player)
        for round_num, kills in all_kills.items():
            if kills:
                data.append(half.kills_info(round_num, kills, map_area))

        if not data:
            return "This player got zero kills this half."
        else:
            return tabulate(
                tabular_data=data,
                colalign=("left", "left", "left"),
                tablefmt="plain",
            )

    # async def set_half(self, first_half):
    #     enabled_style = disnake.ButtonStyle.success
    #     disabled_style = disnake.ButtonStyle.primary

    #     self.first_half.disabled = first_half
    #     self.second_half.disabled = not first_half
    #     self.first_half.style = enabled_style if first_half else disabled_style
    #     self.second_half.style = disabled_style if first_half else enabled_style

    #     round_range = self.round_range(first_half)
    #     max_rounds = self.match.round_count

    #     for round_id, button in zip(round_range, self.round_buttons):
    #         button.label = str(round_id)

    #         if round_id > max_rounds:
    #             button.disabled = True
    #             button.style = disnake.ButtonStyle.secondary
    #         else:
    #             button.disabled = round_id not in self.kills
    #             button.style = disnake.ButtonStyle.primary

    #     embed = self.embed_factory()

    #     table = self.highlights[first_half]
    #     half_one = "T" if self.player_team == 0 else "CT"
    #     half_two = "CT" if self.player_team == 0 else "T"
    #     team = half_one if first_half else half_two

    #     # embed.title = 'Select a round to render'

    #     embed.description = (
    #         f"Table of {self.player.name}'s frags on the {team} side.\n"
    #         f"```{table}```\n"
    #         "Click a round number below to record a highlight.\n"
    #         "Click the 'CT' or 'T' coins to show frags from the other half."
    #     )

    #     return embed


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
        user_settings: dict,
        store_callback,
        abort_callback,
        timeout=900.0,
    ):
        super().__init__(timeout=timeout)

        self.inter = inter
        self.user_settings = user_settings
        self.store_callback = store_callback
        self.abort_callback = abort_callback

        self.updates = dict()

        row_calc = lambda i: (i // 4)

        for index, (k, v) in enumerate(self.user_settings.items()):
            row = row_calc(index)
            button = ConfigButton(
                key=k,
                label=self.name_mapper(k),
                row=row,
            )
            button.callback = self.callback_mapper(k, button)

            if isinstance(v, bool):
                button.set_state(v)
            else:
                button.style = disnake.ButtonStyle.secondary

            self.add_item(button)

        # row calc here needs to change if we add too many more settings...
        button = disnake.ui.Button(
            style=disnake.ButtonStyle.secondary, label="Reset crosshair", row=row
        )

        button.callback = self.reset_crosshair
        self.add_item(button)

        button = disnake.ui.Button(style=disnake.ButtonStyle.green, label="Save", row=row + 1)

        button.callback = self.save
        self.add_item(button)

        button = disnake.ui.Button(style=disnake.ButtonStyle.red, label="Abort", row=row + 1)

        button.callback = self.abort
        self.add_item(button)

    def get_value(self, k):
        return self.updates.get(k, self.user_settings[k])

    def callback_mapper(self, k: str, button: disnake.ui.Button):
        # don't like this
        if k == "crosshair_code":
            return self.send_crosshair_modal
        return partial(self.button_click, button)

    def name_mapper(self, k):
        return dict(
            fragmovie="Clean HUD",
            color_filter="Vibrancy filter",
            righthand="cl_righthand",
            use_demo_crosshair="Use demo crosshair",
            crosshair_code="Change crosshair",
            hq="High quality",
        ).get(k, k)

    def embed(self):
        e = disnake.Embed(
            color=disnake.Color.orange(),
        )

        e.set_author(
            name="STRIKER Patreon Configurator", icon_url=self.inter.bot.user.display_avatar
        )

        if self.get_value("use_demo_crosshair"):
            if self.get_value("crosshair_code"):
                crosshair_text = "using player crosshair from demo (not crosshair sharecode)"
            else:
                crosshair_text = "using player crosshair from demo"
        elif self.get_value("crosshair_code") is not None:
            crosshair_text = self.get_value("crosshair_code")
        else:
            crosshair_text = "default"

        e.description = f"Thank you for your support.\n\nCrosshair: {crosshair_text}"

        e.add_field(name="High quality", value="Record at 1600x900", inline=False)

        e.add_field(
            name="Clean HUD", value="Hide HUD except for killfeed and crosshair", inline=False
        )

        e.add_field(name="Vibrancy filter", value="Enable the video filter", inline=False)

        e.add_field(name="cl_righthand", value="Enable for right handed gun wielding", inline=False)

        e.add_field(
            name="Use demo crosshair",
            value="Use the crosshair of the player being recorded",
            inline=False,
        )

        e.add_field(
            name="Change crosshair",
            value="Change the default crosshair using a crosshair sharecode",
            inline=False,
        )

        return e

    async def button_click(self, button: disnake.Button, inter: disnake.MessageInteraction):
        key = button.key
        value = self.get_value(key)
        self.updates[key] = not value
        button.set_state(not value)
        await inter.response.edit_message(embed=self.embed(), view=self)

    async def send_crosshair_modal(self, inter: disnake.MessageInteraction):
        await inter.response.send_modal(
            modal=CrosshairModal(
                on_submit=self.save_crosshair,
                title="Paste crosshair sharecode",
                timeout=500.0,  # idiotic timeout
            )
        )

    async def save_crosshair(self, inter: disnake.ModalInteraction):
        sharecode = inter.text_values["crosshairinput"]

        if not is_valid_sharecode(sharecode):
            await inter.response.send_message(
                content="That does not appear to be a valid crosshair sharecode", delete_after=6.0
            )
            return

        self.updates["crosshair_code"] = sharecode
        await inter.response.edit_message(embed=self.embed())

    async def reset_crosshair(self, inter: disnake.ModalInteraction):
        self.updates["crosshair_code"] = None
        await inter.response.edit_message(embed=self.embed())

    async def save(self, inter: disnake.MessageInteraction):
        self.stop()
        asyncio.create_task(self.store_callback(inter, self.updates))

    async def abort(self, inter: disnake.MessageInteraction):
        self.stop()
        asyncio.create_task(self.abort_callback(inter))

    async def on_timeout(self):
        e = disnake.Embed(color=disnake.Color.red())
        e.set_author(
            name="STRIKER Patreon Configurator", icon_url=self.inter.bot.user.display_avatar
        )
        e.description = "Timed out."

        message = await self.inter.original_message()
        await message.edit(content=None, embed=e, view=None)
