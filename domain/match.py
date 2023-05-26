from collections import Counter, defaultdict, namedtuple
from copy import deepcopy
from typing import List
import logging

log = logging.getLogger(__name__)

Player = namedtuple("Player", "xuid name userid")
Death = namedtuple("Death", "tick victim attacker pos weapon")


class MatchHalf:
    def __init__(self, rnd) -> None:
        self.rnd = rnd
        self.teams = defaultdict(set)
        self.rounds = defaultdict(list)
        self.name = ""

    def __iter__(self):
        yield from self.rounds

    def set_round(self, rnd):
        self.rnd = rnd
        # if rnd in self.rounds:
        #     self.rounds.pop(rnd, None)

    @classmethod
    def from_preceding(cls, preceding):
        self = cls(preceding.rnd)
        for team_num, team_set in preceding.teams.items():
            self.teams[team_num] = team_set.copy()

        return self

    def get_player_kills(self, player: Player):
        return {
            round_id: [d for d in deaths if d.attacker is player]
            for round_id, deaths in self.rounds.items()
        }

    def get_player_kills_round(self, player: Player, rnd: int):
        deaths = self.rounds.get(rnd)
        if deaths is None:
            raise ValueError("Round %s not in this half", rnd)

        return [d for d in deaths if d.attacker is player]

    def get_player_teamnum(self, player: Player):
        for teamnum, players in self.teams.items():
            if player in players:
                return teamnum
        return None

    def death_is_tk(self, death: Death) -> bool:
        attacker_team = self.get_player_teamnum(death.attacker)
        victim_team = self.get_player_teamnum(death.victim)
        return attacker_team == victim_team

    def add_death(self, death: Death):
        self.rounds[self.rnd].append(death)

    def add_player(self, player: Player, teamnum: str):
        for team_num, players in self.teams.items():
            if team_num != teamnum and player in players:
                # log.info("Removing %s from %s", player.userid, team_num)
                players.remove(player)

        # log.info("Adding %s to %s", player.userid, teamnum)
        self.teams[teamnum].add(player)

    def next_round(self):
        self.rnd += 1

    # TODO: presentation stuff, does not belong here

    @staticmethod
    def weapon_by_order(kills, n=2):
        c = Counter([k.weapon for k in kills])
        return ", ".join(weap for weap, _ in c.most_common(n))

    @staticmethod
    def area_by_order(kills, map_area, n=2):
        if map_area is None:
            return "?"

        areas = Counter([map_area.get_vec_name(kill.pos) for kill in kills])
        return ", ".join(area for area, _ in areas.most_common(n))

    def kills_info(self, round_id, kills, map_area=None):
        k = 0
        tk = 0
        for kill in kills:
            if self.death_is_tk(kill):
                tk += 1
            else:
                k += 1

        info = [f"{k}k"]
        if tk:
            info.append(f"({tk}tk)")

        info.append(self.weapon_by_order(kills))

        return (
            f"R{round_id}",
            " ".join(info),
            self.area_by_order(kills, map_area),
        )


class Match:
    def __init__(self, data, origin=None, time=None) -> None:
        self.data = data

        self.halves: List[MatchHalf] = list()

        self.has_knife_round = False
        self.origin = origin
        self.time = time

        self._parsed = False
        self._id_mapper = dict()
        self._players = dict()

    @classmethod
    def from_demo(cls, demo):
        return cls(demo.data, demo.origin.name.lower(), demo.time)

    def get_player_by_id(self, _id) -> Player:
        return self._players.get(self._ground_userid(_id), None)

    def get_player_by_xuid(self, xuid) -> Player:
        for player in self._players.values():
            if player.xuid == xuid:
                return player
        return None

    def _add_half(self, half: MatchHalf):
        if not half.name:
            idx = len(self.halves) - (1 if self.has_knife_round else 0)
            if idx < 2:
                name = "REG"
            else:
                name = f"OT{idx - 1}"

            half.name = name

        self.halves.append(half)

    def _parse_events(self, events: List[dict]):
        last_round_of_half = False
        half = MatchHalf(1)

        for data in events:
            event = data.pop("event")

            if event == "round_announce_match_start":
                if half.rounds:  # knife round, most likely
                    half.name = "KNF"
                    self._add_half(half)
                    self.has_knife_round = True

                half = MatchHalf.from_preceding(half)
                half.set_round(1)

            elif event == "round_start":
                half.set_round(data["round"])

            elif event == "round_officially_ended":
                _rnd = half.rnd

                if _rnd <= self.max_rounds:
                    # regulation rules

                    # half border, store deaths into rounds
                    if last_round_of_half or _rnd == self.max_rounds:
                        self._add_half(half)
                        half = MatchHalf.from_preceding(half)
                        last_round_of_half = False
                else:
                    # overtime rules

                    if (_rnd - self.max_rounds) % 6 == 0:
                        self._add_half(half)
                        half = MatchHalf.from_preceding(half)
                        last_round_of_half = False

            elif event == "round_announce_last_round_half":
                last_round_of_half = True

            elif event == "player_team":
                player = self.get_player_by_id(data["userid"])
                if not player:
                    # log.info("Could not find player %s", data["userid"])
                    continue

                half.add_player(player, data["team"])

            elif event == "player_death":
                half.add_death(self._make_death(data))

        self._add_half(half)

    @property
    def time_str(self):
        return "Unknown" if self.time is None else self.time.strftime(f" %Y/%m/%d at %I:%M")

    @property
    def score_str(self):
        return "-".join(str(s) for s in self.score)

    def parse(self):
        if self._parsed:
            return

        data = deepcopy(self.data)

        # primarily userinfo stuff
        self._parse_stringtables(data["stringtables"])

        # probably useless at this stage
        self._parse_convars(data["convars"])

        # tickrate, map, etc
        self._parse_demoheader(data["demoheader"])

        # gameevents
        self._parse_events(data["events"])

        self.score = data["score"]

        self._parsed = True

    def _parse_convars(self, convars: dict):
        self.max_rounds = int(convars["mp_maxrounds"])

    def _parse_demoheader(self, header: dict):
        self.map = header["mapname"]
        self.tickrate = header["tickrate"]
        self.protocol = header["protocol"]

    def _parse_stringtables(self, tables: List[dict]):
        for table in tables:
            table_name = table.pop("table")
            if table_name == "userinfo":
                self._add_player(table)

    def _make_death(self, data):
        victim_id = data.pop("victim")
        attacker_id = data.pop("attacker")

        data["victim"] = self.get_player_by_id(victim_id)
        data["attacker"] = self.get_player_by_id(attacker_id)

        return Death(**data)

    def _ground_userid(self, _id):
        return self._id_mapper.get(_id, _id)

    def _add_player(self, data):
        xuid = data["xuid"]
        data["xuid"] = (xuid[1] << 32) + xuid[0]  # I truly hate javascript

        player = Player(**data)
        actual_player = self.get_player_by_xuid(player.xuid)

        if actual_player is None:
            self._players[player.userid] = player
        else:
            self._id_mapper[player.userid] = actual_player.userid

    # remaining stuff is presentation related
