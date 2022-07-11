import logging
import os
import subprocess
from glob import glob
from shutil import rmtree

import aiormq
import aiormq.types
from shared.log import logging_config
from shared.message import MessageError, MessageWrapper

from . import config
from .craft_vdm import TICK_PADDING, craft_vdm
from .ipc import CSGO

logging_config(config.DEBUG)
log = logging.getLogger(__name__)


async def on_message(message: aiormq.channel.DeliveredMessage):
    wrap = MessageWrapper(
        message=message,
        default_error='An error occurred while recording.',
        ack_on_failure=True,
    )

    async with wrap as ctx:
        data = ctx.data

        job_id = data['job_id']
        matchid = data['matchid']
        demo = rf'{config.DEMO_FOLDER}\{matchid}.dem'
        start = data['start_tick']
        end = data['end_tick']
        xuid = data['xuid']
        output = rf'{config.VIDEO_DIR}\{job_id}.mp4'
        capture_dir = config.TEMP_FOLDER
        fps = data['fps']
        resolution = data['resolution']
        video_bitrate = data['video_bitrate']
        audio_bitrate = data['audio_bitrate']
        skips = data['skips']
        video_filters = config.VIDEO_FILTERS if data['color_correction'] else None

        if not os.path.isfile(demo):
            raise ValueError(f'Demo {demo} does not exist.')

        if end < start:
            raise ValueError('FY FAEN RUNAR DU E IDIOT')

        log.info(
            f'Recording player {xuid} from tick {start} to {end} with skips {skips}'
        )

        vdm_script = craft_vdm(
            start_tick=start,
            end_tick=end,
            skips=skips,
            xuid=xuid,
            fps=fps,
            bitrate=video_bitrate,
            capture_dir=capture_dir,
            video_filters=video_filters,
        )

        # change res
        await csgo.set_resolution(*resolution)

        # make sure deathmsg doesn't fill up and clear lock spec
        await csgo.run(f'mirv_deathmsg lifetime 0')
        take_folder = await csgo.playdemo(
            demo=demo,
            vdm=vdm_script,
            return_take=True,
            unblock_at='RECORDING FINISHED',
            start_at=start - TICK_PADDING,
        )

        # mux audio
        wav = glob(take_folder + r'\*.wav')[0]
        subprocess.run(
            [
                config.FFMPEG_BIN,
                '-i',
                take_folder + r'\normal\video.mp4',
                '-i',
                wav,
                '-c:v',
                'copy',
                '-c:a',
                'aac',
                '-b:a',
                f'{audio_bitrate}k',
                '-y',
                output,
            ],
            capture_output=True,
        )

        rmtree(take_folder)

        await ctx.success()


async def main():
    global csgo

    logging.getLogger('aiormq').setLevel(logging.INFO)

    log.info('Starting up...')

    csgo = CSGO(
        hlae_exe=config.HLAE_EXE,
        csgo_exe=config.CSGO_BIN,
        mmcfg_dir=config.MMCFG_FOLDER,
        width=720,
        height=480,
    )

    await csgo.connect()

    mq = await aiormq.connect(config.RABBITMQ_HOST)
    chan = await mq.channel()

    await chan.basic_qos(prefetch_count=1)
    await chan.basic_consume(
        queue=config.RECORDER_QUEUE, consumer_callback=on_message, no_ack=False
    )

    # await chan.basic_recover

    log.info('Ready to record!')
