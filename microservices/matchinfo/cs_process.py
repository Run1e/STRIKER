import logging
import gevent
from csgo.client import CSGOClient
from steam.client import EResult, SteamClient

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class SentrySteamClient(SteamClient):
    def get_sentry(self, username):
        return self.sentry

    def store_sentry(self, username, sentry):
        self.sentry = sentry
        self.pipe.send(dict(event="store_sentry", username=username, sentry=sentry))


def cs_process(pipe, event, username, password, sentry):
    client = SentrySteamClient()

    client.sentry = sentry
    client.pipe = pipe

    client.verbose_debug = False
    clog = client._LOG
    clog.setLevel(logging.DEBUG)

    cs = CSGOClient(client)

    def connect():
        resp = client.login(username=username, password=password)

        if resp != EResult.OK:
            raise ValueError(f"Failed logging in: {repr(resp)}")

        cs.launch()
        cs.wait_event("ready")

    connect()

    client.on("disconnected", connect)

    # tell parent process it can start listening to queue
    event.set()

    def recv_match_info(message):
        # no match
        try:
            match = message.matches[0]
        except IndexError:
            raise ValueError("No matches in matchinfo reply")

        matchid = match.matchid
        matchtime = match.matchtime
        demo_url = match.roundstatsall[-1].map

        log.info(f"Received {matchid}")
        pipe.send(dict(event="matchinfo", matchid=matchid, matchtime=matchtime, url=demo_url))

    cs.on("full_match_info", recv_match_info)

    while True:
        if not pipe.poll():
            gevent.idle()
        else:
            decoded = pipe.recv()
            log.info(f"Requesting {decoded['matchid']}")
            cs.request_full_match_info(**decoded)
