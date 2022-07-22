var fs = require('fs');
var demofile = require('demofile');

var r = { convars: new Map(), stringtables: new Array(), events: new Array() };
var teams = new Map();
const demoFile = new demofile.DemoFile();

var warmupOver = false;

function addEvent(data) {
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
  // no need to update which team players are on after warmup
  // this is because of how we convert later userids to the original once
  // in the Demo domain entity
  if (warmupOver) return;

  const player = demoFile.entities.getByUserId(e.userid);
  if (player.isFakePlayer) return;

  var team = e.team;
  if (!teams.has(team)) {
    teams.set(team, new Set());
  }

  teams.get(team).add(e.userid);
});

demoFile.gameEvents.on('player_spawn', e => {
  // as this also does team userid list updates, same as above
  // applied here
  if (warmupOver) return;

  const player = demoFile.entities.getByUserId(e.userid);
  if (player.isFakePlayer) return;

  var team = e.teamnum;
  if (!teams.has(team)) {
    teams.set(team, new Set());
  }

  teams.get(team).add(e.userid);
});

demoFile.conVars.on('change', e => {
  if (e.name == 'mp_maxrounds') {
    addConVar(e.name, e.value);
  }
});

demoFile.stringTables.on('update', e => {
  var tableName = e.table.name;
  if (tableName == 'userinfo' && e.userData != null) {
    var xuid = e.userData.xuid;
    addStringTable({
      table: tableName,
      xuid: [xuid.low, xuid.high],
      name: e.userData.name,
      userid: e.userData.userId,
      fakeplayer: e.userData.fakePlayer,
    });
  }
});

demoFile.gameEvents.on('round_announce_match_start', e => {
  addEvent({
    event: 'round_announce_match_start',
  });

  warmupOver = true;
});

demoFile.gameEvents.on('round_officially_ended', e => {
  addEvent({
    event: 'round_officially_ended',
  });
});

demoFile.gameEvents.on("player_death", e => {
  if (!warmupOver) return;

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
  r['teams'] = new Map();
  teams.forEach((v, k) => {
    if (k == 2 || k == 3) {
      r['teams'][k] = Array.from(v).sort((a, b) => a - b);
    }
  });
  console.log(JSON.stringify(r, null, space = 0));
})


var demo = process.argv[2];
demoFile.parseStream(fs.createReadStream(demo));
