import os
from glob import glob
from subprocess import run

from micro.recorder.config import CSGO_FOLDER


def main():
    map_folder = CSGO_FOLDER + '/maps'

    map_files = glob(map_folder + r'/*.bsp')
    nav_files = list()

    for map_path in map_files:
        map_name = os.path.split(map_path)[1].split('.')[0]
        nav_file = map_folder + f'/{map_name}.nav'

        if not os.path.isfile(nav_file):
            print('Map', map_name, 'does not have nav file!')
            continue

        nav_files.append(nav_file)

    run(['go', 'run', 'navparse.go', '../bot/nav', *nav_files])


if __name__ == '__main__':
    main()
