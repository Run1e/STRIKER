from collections import Counter, defaultdict, namedtuple
from copy import deepcopy
from typing import List


Player = namedtuple("Player", "xuid name userid fakeplayer")
Death = namedtuple("Death", "tick victim attacker pos weapon")


class DemoEvents:
    def __init__(self, data) -> None:
        self.data = data
        self._parsed = False

    @classmethod
    def from_demo(cls, demo):
        self = cls(demo.data)

        self.origin = demo.origin
        self.time = demo.time

        return self

    def get_player_team(self, player: Player) -> int:
        for teamidx, players in enumerate(self.teams):
            if player in players:
                return teamidx
        return None

    def death_is_tk(self, death: Death) -> bool:
        attacker_team = self.get_player_team(death.attacker)
        victim_team = self.get_player_team(death.victim)
        return attacker_team == victim_team

    def get_player_by_id(self, _id) -> Player:
        return self._players.get(self._ground_userid(_id), None)

    def get_player_by_xuid(self, xuid) -> Player:
        for player in self._players.values():
            if player.xuid == xuid:
                return player
        return None

    @property
    def halftime(self) -> int:
        # in half two if round > .halftime
        return self.max_rounds // 2

    def get_player_kills_round(self, player: Player, round_id, kills=None):
        deaths = kills or self._rounds.get(round_id, None)

        if deaths is None:
            return None

        from_player = list()
        for death in deaths:
            if death.attacker.xuid == player.xuid:
                from_player.append(death)

        return from_player or None

    def get_player_kills(self, player: Player):
        kills = dict()  # round_id: List[Death]

        for round_id, deaths in self._rounds.items():
            from_player = self.get_player_kills_round(player, round_id, deaths)

            if from_player:
                kills[round_id] = from_player

        return kills

    @property
    def time_str(self):
        if self.time is None:
            return "Unknown"

        ordinal = {1: "st", 2: "nd", 3: "rd"}.get(self.time.day % 10, "th")
        return (
            self.time.strftime("%d").lstrip("0")
            + ordinal
            + self.time.strftime(" %b %Y at %I:%M")
        )

    @property
    def score_str(self):
        return "-".join(str(s) for s in self.score)

    def format(self):
        date = self.time_str
        score = self.score_str

        return f"{self.map} [{score}] - {date}"

    def parse(self):
        if self._parsed:
            return

        self._player_team = dict()
        self._players = dict()
        self._rounds = defaultdict(list)
        self._win_reasons = dict()
        self._id_mapper = dict()

        data = deepcopy(self.data)
        self._parse_stringtables(data["stringtables"])
        self._parse_convars(data["convars"])
        self._parse_demoheader(data["demoheader"])
        self._parse_events(data["events"])

        self.score = data["score"]

        # list(dict.fromkeys(iter)) forces an in ordered list with unique elements
        # the teams hold all the userids of players that played for a team
        # players can reconnect and they get a new id, hence duplicates
        # can occur, and they need to be made unique
        self.teams = [
            list(dict.fromkeys([self.get_player_by_id(_id) for _id in lst]))
            for lst in (data["teams"]["2"], data["teams"]["3"])
        ]

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

    def _parse_events(self, events: List[dict]):
        rnd = 0

        for data in events:
            event = data.pop("event")

            if event == "round_announce_match_start":
                rnd = 1

            if event == "round_officially_ended":
                rnd += 1

            elif event == "player_death":
                self._add_death(rnd, data)

        self.round_count = rnd

    def _team_idx_at_round(self, team_id, rnd):
        team_idx = team_id - 2
        return team_idx if rnd <= self.halftime else abs(team_idx - 1)

    def _ground_userid(self, id):
        return self._id_mapper.get(id, id)

    def _add_player(self, data):
        xuid = data["xuid"]
        data["xuid"] = (xuid[1] << 32) + xuid[0]  # I truly hate javascript

        player = Player(**data)
        actual_user = self.get_player_by_xuid(player.xuid)

        if actual_user is None:
            self._players[player.userid] = player
        else:
            self._id_mapper[player.userid] = actual_user.userid

    def _add_death(self, rnd, data):
        victim_id = data.pop("victim")
        attacker_id = data.pop("attacker")

        data["victim"] = self.get_player_by_id(victim_id)
        data["attacker"] = self.get_player_by_id(attacker_id)

        self._rounds[rnd].append(Death(**data))

    # remaining stuff is presentation related

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