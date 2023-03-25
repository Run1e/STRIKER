import asyncio
import logging
import re

import config
from ipc import CSGO
from recorder import make_csgo

log = logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

map_re = r"^.*\(fs\) ((ar|cs|de).*)\.bsp$"
INGAME = r"ChangeGameUIState: CSGO_GAME_UI_STATE_INGAME -> CSGO_GAME_UI_STATE_INGAME"
NEEDS_ANALYZE = "The nav mesh needs a full"


async def main():
    csgo = make_csgo()

    await csgo.connect()
    result = await csgo.run("maps *")
    await csgo.run("sv_cheats 1")

    for line in result:
        match = re.match(map_re, line)
        if not match:
            continue

        _map = match.group(1)

        await csgo.run(f"map {_map}")
        await csgo.wait_for(check=lambda line: line == INGAME)

        header, *names = await csgo.run("nav_place_list")

        if header == "Map uses 0 place names:":
            log.info("%s uses 0 place names, skipping", _map)

        else:
            result = await csgo.run("nav_save")
            if NEEDS_ANALYZE in " ".join(result):
                log.info(f"{_map} needs analyze first...")
                log.info(result)
                await csgo.run("nav_generate")
                await csgo.wait_for(check=lambda line: line == INGAME)
                await csgo.run("nav_analyze")
                result = await csgo.run("nav_save")
            log.info(result)

        await asyncio.sleep(2.0)
        await csgo.run("disconnect")
        await asyncio.sleep(2.0)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
