var fs = require('fs');
var demofile = require('demofile');

var r = { convars: new Map(), stringtables: new Array(), events: new Array() };
const demoFile = new demofile.DemoFile();

var matchStarted = false;
var inWarmup = true;
var seen = new Set();
var seenTeam = new Set();
var clearSeenTeam = false;

function addEvent(data) {
  // console.log(data);
  r['events'].push(data);
}

function addConVar(k, v) {
  r['convars'][k] = v;
}

function addStringTable(data) {
  r['stringtables'].push(data);
}

demoFile.on('start', e => {
  r['demoheader'] = {
    mapname: demoFile.header.mapName,
    tickrate: demoFile.tickRate,
    protocol: demoFile.header.protocol,
  }
});

demoFile.gameEvents.on('player_team', e => {
  if (seenTeam.has(e.userid)) return;
  const player = demoFile.entities.getByUserId(e.userid);
  if (!player) return;
  if (player.isFakePlayer) return;
  if (e.disconnect || e.isbot) return;

  seenTeam.add(e.userid);

  addEvent({
    "event": "player_team",
    "userid": e.userid,
    "team": e.team,
    // "oldteam": e.oldteam,
  });
});

demoFile.conVars.on('change', e => {
  if (e.name == 'mp_maxrounds') {
    addConVar(e.name, e.value);
  }
});

demoFile.stringTables.on('update', e => {
  var tableName = e.table.name;

  if (tableName == 'userinfo' && e.userData != null) {
    if (e.userData.fakePlayer) return;
    if (seen.has(e.userData.userId)) return;
    seen.add(e.userData.userId)

    var xuid = e.userData.xuid;
    addStringTable({
      table: tableName,
      xuid: [xuid.low, xuid.high],
      name: e.userData.name,
      userid: e.userData.userId,
    });
  }
});

demoFile.gameEvents.on('round_announce_last_round_half', e => {
  addEvent({
    event: 'round_announce_last_round_half',
  });
  clearSeenTeam = true;
});

demoFile.gameEvents.on('round_announce_match_point', e => {
  addEvent({
    event: 'round_announce_match_point',
  });
});

demoFile.gameEvents.on('round_announce_match_start', e => {
  addEvent({
    event: 'round_announce_match_start',
  });

  inWarmup = false;
  matchStarted = true;
});

demoFile.gameEvents.on('round_announce_warmup', e => {
  addEvent({
    event: 'round_announce_warmup',
  });

  inWarmup = true;
});

demoFile.gameEvents.on('round_end', e => {
  addEvent({
    event: 'round_end',
  });
});

demoFile.gameEvents.on('round_officially_ended', e => {
  addEvent({
    event: 'round_officially_ended',
  });

  if (clearSeenTeam) {
    seenTeam.clear();
    clearSeenTeam = false;
  }
});

demoFile.gameEvents.on('round_start', e => {
  addEvent({
    event: 'round_start',
    round: demoFile.gameRules.roundsPlayed + 1,
  });

  [demoFile.teams[2], demoFile.teams[3]].forEach(team => {
    team.members.forEach(player => {
      var userid = player.userInfo.userId;
      if (!seenTeam.has(userid)) {
        addEvent({
          "event": "player_team",
          "userid": userid,
          "team": team.teamNumber,
        });
        seenTeam.add(userid);
      }
    });
  });
});

demoFile.gameEvents.on("player_death", e => {
  if (!matchStarted || inWarmup) return;

  if (e.attacker == e.userid) return;
  if (e.attacker == 0) return;

  const attacker = demoFile.entities.getByUserId(e.attacker);
  var pos = attacker.position;

  addEvent({
    event: 'player_death',
    tick: demoFile.currentTick,
    attacker: e.attacker,
    victim: e.userid,
    weapon: e.weapon,
    pos: Object.values(pos).map(k => ~~k),
  });
});

demoFile.on('end', e => {
  var teamOne = demoFile.teams[2];
  var teamTwo = demoFile.teams[3];
  r['score'] = [teamOne.score, teamTwo.score];
  console.log(JSON.stringify(r, null, space = 0));
})


var demo = process.argv[2];
demoFile.parseStream(fs.createReadStream(demo));
