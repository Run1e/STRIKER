import logging
from time import sleep

from shared.log import logging_config

from . import config
from .sandboxie import Sandboxie

logging_config()

log = logging.getLogger(__name__)


def setup_box(sb: Sandboxie, box: str):
    if config.VENV is not None:
        sb.run('powershell', f'{config.VENV}/Scripts/Activate.ps1', box=box)

    sb.run(
        config.STEAM_BIN,
        '-nocache',
        '-nofriendsui',
        '-silent',
        '-login',
        config.STEAM_USER,
        config.STEAM_PASS,
        box=box,
    )
    sleep(20)
    sb.run(f'{config.VENV}/Scripts/python.exe', config.RECORDER, box=box)


def main():
    try:
        sb = Sandboxie(config.START_BIN)
        sb.cleanup(config.BOXES)

        for box in config.BOXES:
            setup_box(sb, box)

        while True:
            sleep(1)
    except KeyboardInterrupt:
        sb.cleanup(boxes=config.BOXES)
