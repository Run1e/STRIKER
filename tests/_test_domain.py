from json import loads

import pytest
from domain.domain import Demo, DemoState, Job, JobState

from tests.data import demo_data
from tests.testutils import *


def test_demo_has_nothing():
    demo = new_demo(DemoState.MATCH, queued=False, sharecode="asd", has_matchinfo=False)

    assert not demo.has_matchinfo()
    assert not demo.has_data()
    assert not demo.is_up_to_date()
    assert not demo.is_ready()


def test_demo_has_matchinfo():
    demo = new_demo(DemoState.MATCH, queued=False, sharecode="asd", has_matchinfo=True)

    assert demo.has_matchinfo()
    assert not demo.has_data()
    assert not demo.is_up_to_date()
    assert not demo.is_ready()


def test_demo_has_data():
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=False,
        sharecode="asd",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    assert demo.has_matchinfo()
    assert demo.has_data()
    assert demo.is_up_to_date()
    assert demo.is_ready()


def test_demo_is_out_of_date():
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=False,
        sharecode="asd",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    demo.version -= 1

    assert demo.has_matchinfo()
    assert demo.has_data()
    assert not demo.is_up_to_date()
    assert not demo.is_ready()


def test_demo_misc():
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=False,
        sharecode="asd",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    demo.parse()

    assert demo.score == [9, 4]
    assert demo.score_string == "9-4"

    assert demo.protocol == 4
    assert demo.map == "de_dust2"
    assert demo.halftime == demo.max_rounds // 2
    assert demo.max_rounds == 16


def test_demo_players():
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=False,
        sharecode="asd",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    demo.parse()

    assert len(demo.teams[0]) == 5
    assert len(demo.teams[1]) == 5

    for team_id, team in enumerate(demo.teams):
        for player in team:
            assert demo.get_player_team(player) == team_id

    player = demo.teams[0][0]
    assert player is demo.get_player_by_id(player.userid)
    assert player is demo.get_player_by_xuid(player.xuid)


def test_demo_kills():
    demo = new_demo(
        state=DemoState.SUCCESS,
        queued=False,
        sharecode="asd",
        has_matchinfo=True,
        data=loads(demo_data[0]),
    )

    demo.parse()

    player = demo.get_player_by_id(6)

    kills = demo.get_player_kills(player)
    round_kills = kills[10]

    assert len(round_kills) == 2
    assert demo.death_is_tk(round_kills[1])
    assert not demo.death_is_tk(round_kills[0])

    kill = round_kills[0]
    assert kill.attacker is player
