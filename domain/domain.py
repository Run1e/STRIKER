import json
import pickle
from collections import Counter, defaultdict, namedtuple
from copy import deepcopy
from datetime import datetime, timezone
from enum import Enum, auto
from typing import List

import disnake
from disnake.ext import commands

from messages import dto, events
from shared.const import DEMOPARSE_VERSION

demoevents_cache = dict()


class JobState(Enum):
    WAITING = auto()  # associated demo is currently processing
    SELECTING = auto()  # job is or can have user selecting
    RECORDING = auto()  # job is on record queue
    UPLOADING = auto()  # job is on upload queue
    ABORTED = auto()  # job was aborted by the user, view timeout, or job limit
    FAILED = auto()  # job failed for some reason
    SUCCESS = auto()  # job completed successfully


class DemoGame(Enum):
    CSGO = auto()
    CS2 = auto()


class DemoOrigin(Enum):
    VALVE = auto()
    FACEIT = auto()
    UPLOAD = auto()


class DemoState(Enum):
    PROCESSING = auto()
    FAILED = auto()  # demo failed, not recordable
    READY = auto()  # demo successful, can be used by jobs
    DELETED = auto()  # demo unavailable, archived/delete probably


class RecordingType(Enum):
    PLAYER_ROUND = auto()


class Entity:
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} id={self.id}>"


class Demo(Entity):
    def __init__(
        self,
        game: DemoGame,
        origin: DemoOrigin,
        state: DemoState,
        identifier: str = None,
        sharecode: str = None,
        time: datetime = None,
        download_url: str = None,
        map: str = None,
        score: List[int] = None,
        downloaded_at: datetime = None,
        data_version: int = None,
        data: dict = None,
    ):
        self.game = game
        self.origin = origin
        self.state = state
        self.identifier = identifier
        self.sharecode = sharecode
        self.time = time
        self.download_url = download_url
        self.map = map
        self.score = score
        self.downloaded_at = downloaded_at
        self.data_version = data_version
        self.data = data

        self.events = []

    def has_download_url(self):
        return self.download_url is not None

    def has_data(self):
        return self.data is not None

    def is_up_to_date(self):
        # I don't like how this uses an external constant at all
        return self.data_version == DEMOPARSE_VERSION

    def is_ready(self):
        return self.has_data() and self.is_up_to_date() and self.state is DemoState.READY

    def failed(self, reason):
        self.state = DemoState.FAILED
        self.events.append(events.DemoFailure(self.id, reason))

    def processing(self):
        self.state = DemoState.PROCESSING
        self.events.append(events.DemoProcessing(self.id))

    def ready(self):
        self.state = DemoState.READY
        self.events.append(events.DemoReady(self.id))

    def set_demo_data(self, data, version):
        data = json.loads(data)

        self.data = data
        self.data_version = version
        self.downloaded_at = datetime.now(timezone.utc)

        demoheader = data["demoheader"]
        self.map = demoheader["mapname"]
        self.score = data["score"]

        self.ready()


class Recording(Entity):
    def __init__(self, recording_type: RecordingType, player_xuid: int, round_id: int = None):
        self.recording_type = recording_type
        self.player_xuid = player_xuid
        self.round_id = round_id

        self.events = []


# this job class is discord-specific
# which does mean some discord (frontend) specific things
# kind of flow into the domain, which is not the best,
# but it'll have to do. it's just the easiest way of doing this
class Job(Entity):
    demo: Demo
    recording: Recording

    def __init__(
        self,
        state: JobState,
        guild_id: int,
        channel_id: int,
        user_id: int,
        started_at: datetime,
        inter_payload: bytes,
        completed_at: datetime = None,
    ):
        self.state = state
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.user_id = user_id
        self.started_at = started_at
        self.inter_payload = inter_payload
        self.completed_at = completed_at

        self.events = []

    def set_demo(self, demo: Demo):
        self.demo = demo

        if demo.is_ready() and self.state is JobState.WAITING:
            self.demo_ready()

    def demo_ready(self):
        self.state = JobState.SELECTING
        demo_events = DemoEvents(self.demo.data)
        self.events.append(dto.JobSelectable(self.id, self.state, self.inter_payload, demo_events))

    def failed(self, reason: str):
        self.state = JobState.FAILED
        self.events.append(events.JobFailure(self.id, reason))

    def make_dto(self):
        self.events


class User(Entity):
    modifiable_fields = (
        "crosshair_code",
        "fragmovie",
        "color_filter",
        "righthand",
        "use_demo_crosshair",
    )

    def __init__(
        self,
        user_id: int,
        crosshair_code: str = None,
        fragmovie: bool = None,
        color_filter: bool = None,
        righthand: bool = None,
        use_demo_crosshair: bool = None,
    ) -> None:
        self.user_id = user_id
        self.crosshair_code = crosshair_code
        self.fragmovie = fragmovie
        self.color_filter = color_filter
        self.righthand = righthand
        self.use_demo_crosshair = use_demo_crosshair

    def get(self, key):
        return self.all_recording_settings(False).get(key)

    def set(self, key, value):
        if key in self.all_recording_settings(False):
            setattr(self, key, value)
        else:
            raise ValueError

    def all_recording_settings(self, only_toggleable=True):
        default = lambda v, d: v if v is not None else d

        d = dict(
            fragmovie=default(self.fragmovie, False),
            color_filter=default(self.color_filter, True),
            righthand=default(self.righthand, True),
            use_demo_crosshair=default(self.use_demo_crosshair, False),
        )

        if not only_toggleable:
            d["crosshair_code"] = default(self.crosshair_code, "CSGO-SG5dx-aAeRk-dnoAc-TwqMh-yTSFE")

        return d

    def update_recorder_settings(self):
        d = dict()

        for attr in self.modifiable_fields:
            val = getattr(self, attr)
            if val is not None:
                d[attr] = val

        return d


Player = namedtuple("Player", "xuid name userid fakeplayer")
Death = namedtuple("Death", "tick victim attacker pos weapon")


class DemoEvents(Entity):
    def __init__(self, data) -> None:
        self.data = data
        self._parsed = False

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
    def matchtime_string(self):
        ordinal = {1: "st", 2: "nd", 3: "rd"}.get(self.matchtime.day % 10, "th")
        return (
            self.matchtime.strftime("%d").lstrip("0")
            + ordinal
            + self.matchtime.strftime(" %b %Y at %I:%M")
        )

    @property
    def score_string(self):
        return "-".join(str(s) for s in self.score)

    def format(self):
        date = self.matchtime_string
        score = self.score_string

        return f"{self.map} [{score}] - {date}"

    def parse(self):
        if self._parsed:
            return

        self.score = [0, 0]
        self.teams = [[], []]
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


# @LRUCache(maxsize=256)
def demo_data_parse(data):
    demo_events = DemoEvents(data)
    demo_events.parse()
    return demo_events
