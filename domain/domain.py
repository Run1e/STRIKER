import pickle
from collections import defaultdict, namedtuple
from copy import deepcopy
from datetime import datetime
from enum import Enum, auto
from typing import List

import disnake
from disnake.ext import commands
from shared.const import DEMOPARSE_VERSION


class JobState(Enum):
    DEMO = auto()  # associated demo is currently processing
    SELECT = auto()  # job is or can have user selecting
    RECORD = auto()  # job is on record queue
    ABORTED = auto()  # job was aborted by the user or a view timeout
    FAILED = auto()  # job failed for some reason
    SUCCESS = auto()  # job completed successfully


class DemoState(Enum):
    MATCH = auto()  # demo is or should be on matchinfo queue
    PARSE = auto()  # demo is or should be on parse queue
    FAILED = auto()  # demo failed, not recordable
    SUCCESS = auto()  # demo successful, can be used by jobs


class Entity:
    def __repr__(self) -> str:
        return f'<{self.__class__.__name__} id={self.id}>'


Player = namedtuple('Player', 'xuid name userid fakeplayer')
Death = namedtuple('Death', 'tick victim attacker pos weapon')


class Demo(Entity):
    _parsed = False

    def __init__(
        self,
        state: DemoState,
        queued: bool,
        sharecode: str,
        matchid: int = None,
        matchtime: datetime = None,
        url: str = None,
        map: str = None,
        version: int = None,
        downloaded_at: datetime = None,
        score: List[int] = None,
        data: dict = None,
    ):
        self.state = state
        self.queued = queued
        self.sharecode = sharecode
        self.matchid = matchid
        self.matchtime = matchtime
        self.url = url
        self.map = map
        self.version = version
        self.downloaded_at = downloaded_at
        self.score = score
        self.data = data

    def has_matchinfo(self):
        return all((self.matchid, self.matchtime, self.url))

    def has_data(self):
        return self.data is not None

    def is_up_to_date(self):
        return self.version == DEMOPARSE_VERSION

    def can_record(self):
        return (
            self.has_matchinfo()
            and self.has_data()
            and self.is_up_to_date()
            and self.state is DemoState.SUCCESS
        )

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

    def get_player_kills(self, player: Player):
        kills = dict()  # round_id: List[Death]

        for round_id, deaths in self._rounds.items():
            from_player = list()
            for death in deaths:
                if death.attacker.xuid == player.xuid:
                    from_player.append(death)

            if from_player:
                kills[round_id] = from_player

        return kills

    @property
    def matchtime_string(self):
        return self.matchtime.strftime('%Y-%m-%d')

    @property
    def score_string(self):
        return '-'.join(str(s) for s in self.score)

    def format(self):
        date = self.matchtime_string
        score = self.score_string

        return f'{date} {self.map} {score}'

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
        self._parse_stringtables(data['stringtables'])
        self._parse_convars(data['convars'])
        self._parse_demoheader(data['demoheader'])
        self._parse_events(data['events'])

        self.score = data['score']
        self.teams = [
            [self.get_player_by_id(_id) for _id in lst]
            for teamnum, lst in data['teams'].items()
        ]

        self._parsed = True

    def _parse_convars(self, convars: dict):
        self.max_rounds = int(convars['mp_maxrounds'])

    def _parse_demoheader(self, header: dict):
        self.map = header['mapname']
        self.tickrate = header['tickrate']
        self.protocol = header['protocol']

    def _parse_stringtables(self, tables: List[dict]):
        for table in tables:
            table_name = table.pop('table')
            if table_name == 'userinfo':
                self._add_player(table)

    def _parse_events(self, events: List[dict]):
        rnd = 0

        for data in events:
            event = data.pop('event')

            if event == 'round_announce_match_start':
                rnd = 1

            if event == 'round_officially_ended':
                rnd += 1

            elif event == 'player_death':
                self._add_death(rnd, data)

        self.round_count = rnd

    def _team_idx_at_round(self, team_id, rnd):
        team_idx = team_id - 2
        return team_idx if rnd <= self.halftime else abs(team_idx - 1)

    def _ground_userid(self, id):
        return self._id_mapper.get(id, id)

    def _add_player(self, data):
        xuid = data['xuid']
        data['xuid'] = (xuid[1] << 32) + xuid[0]  # I truly hate javascript

        player = Player(**data)
        actual_user = self.get_player_by_xuid(player.xuid)

        if actual_user is None:
            self._players[player.userid] = player
        else:
            self._id_mapper[player.userid] = actual_user.userid

    def _add_death(self, rnd, data):
        data['victim'] = self.get_player_by_id(data.pop('victim'))
        data['attacker'] = self.get_player_by_id(data.pop('attacker'))
        self._rounds[rnd].append(Death(**data))


# this job class is discord-specific
# which does mean some discord (frontend) specific things
# kind of flow into the domain, which is not the best,
# but it'll have to do. it's just the easiest way of doing this
class Job(Entity):
    demo: Demo

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

    def get_inter(self, bot: commands.InteractionBot) -> disnake.AppCommandInteraction:
        if self.inter_payload is None:
            raise ValueError(
                'Attempted to restore job interaction without stored payload'
            )

        return disnake.ApplicationCommandInteraction(
            data=pickle.loads(self.inter_payload), state=bot._connection
        )

    def embed(self, bot: commands.InteractionBot) -> disnake.Embed:
        color = {
            JobState.DEMO: disnake.Color.yellow(),
            JobState.SELECT: disnake.Color.blurple(),
            JobState.RECORD: disnake.Color.yellow(),
            JobState.SUCCESS: disnake.Color.green(),
            JobState.FAILED: disnake.Color.red(),
            JobState.ABORTED: disnake.Color.red(),
        }.get(self.state, disnake.Color.blurple())

        title = {
            JobState.DEMO: 'Demo queued',
            JobState.SELECT: 'Select what you want to record',
            JobState.RECORD: 'Recording queued',
            JobState.SUCCESS: 'Recording job complete!',
            JobState.FAILED: 'Oops!',
            JobState.ABORTED: 'Job aborted',
        }.get(self.state, None)

        e = disnake.Embed(color=color)
        e.set_author(name=title, icon_url=bot.user.display_avatar)
        e.set_footer(text=f'ID: {self.id}')

        return e
