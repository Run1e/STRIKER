import logging

import config


def cs_process(p, e):
    import gevent
    from csgo.client import CSGOClient
    from steam.client import EResult, SteamClient

    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)

    client = SteamClient()
    client.set_credential_location("cred")

    client.verbose_debug = False
    clog = client._LOG
    clog.setLevel(logging.DEBUG)

    cs = CSGOClient(client)

    def connect():
        resp = client.login(username=config.STEAM_USER, password=config.STEAM_PASS)

        if resp != EResult.OK:
            raise ValueError(f"Failed logging in: {repr(resp)}")

        cs.launch()
        cs.wait_event("ready")

    connect()

    client.on("disconnected", connect)

    # tell parent process it can start listening to queue
    e.set()

    def recv_match_info(message):
        # no match
        try:
            match = message.matches[0]
        except IndexError:
            raise ValueError("No matches in matchinfo reply")

        matchid = match.matchid
        matchtime = match.matchtime
        demo_url = match.roundstatsall[-1].map

        log.info("Received %s", matchid)
        p.send(dict(matchid=matchid, matchtime=matchtime, url=demo_url))

    cs.on("full_match_info", recv_match_info)

    while True:
        if not p.poll():
            gevent.idle()
        else:
            decoded = p.recv()

            log.info("Requesting %s", decoded["matchid"])
            cs.request_full_match_info(**decoded)
