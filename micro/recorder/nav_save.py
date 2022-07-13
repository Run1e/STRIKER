import asyncio
import logging
import re

import config
from ipc import CSGO

log = logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

r = r'^.*\(fs\) ((ar|cs|de).*)\.bsp$'
INGAME = r'ChangeGameUIState: CSGO_GAME_UI_STATE_INGAME -> CSGO_GAME_UI_STATE_INGAME'
NEEDS_ANALYZE = 'The nav mesh needs a full'


async def main():
    csgo = CSGO(
        hlae_exe=config.HLAE_EXE,
        csgo_exe=config.CSGO_BIN,
        mmcfg_dir=config.MMCFG_FOLDER,
        width=1280,
        height=720,
    )

    await csgo.connect()
    maps_result = await csgo.run('maps *')
    await csgo.run('sv_cheats 1')

    maps = []

    for line in maps_result.splitlines():
        mat = re.match(r, line)
        if mat:
            maps.append(mat.group(1))

    for map in maps:
        await csgo.run(f'map {map}')
        await csgo.readuntil(INGAME)
        place_names = await csgo.run('nav_place_list')
        if place_names.startswith('Map uses 0 place names'):
            log.info('%s uses 0 place names, skipping', map)
            continue
        result = await csgo.run('nav_save')
        # if NEEDS_ANALYZE in result:
        #     log.info(f'{map} needs analyze first...')
        #     await csgo.run('nav_generate')
        #     await csgo.readuntil(INGAME)
        #     await csgo.run('nav_analyze')
        #     result = await csgo.run('nav_save')
        log.info(result)

        # await asyncio.sleep(2.0)
        # await csgo.run('disconnect')
        await asyncio.sleep(2.0)

    csgo.kill()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()
