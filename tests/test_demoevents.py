from json import loads
from domain.match import Match
from .testutils import valve, faceit

import pytest


def test_init(valve):
    m = Match(loads(valve))
    m.parse()


def test_valve(valve):
    m = Match(loads(valve))
    m.parse()
    print("asd")


def test_faceit(faceit):
    m = Match(loads(faceit))
    m.parse()
    print("asd")
