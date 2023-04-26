import xml.etree.ElementTree as ET
from collections import defaultdict


class CommandSystemBuilder:
    def __init__(self):
        self._tick = 0
        self._commands = defaultdict(list)

    def tick(self, tick):
        self._tick = tick

    def delta(self, delta):
        self._tick += int(delta)

    def run(self, *commands):
        self._commands[self._tick].extend(commands)

    def skip(self, tick):
        self._commands[self._tick].append(f"demo_gototick {tick}")

    def save(self, file):
        root = ET.Element("commandSystem")
        commands = ET.SubElement(root, "commands")

        for tick, command_list in sorted(self._commands.items()):
            for command in command_list:
                ET.SubElement(commands, "c", attrib=dict(tick=str(tick))).text = command

        ET.ElementTree(root).write(file)


def make_script(
    tickrate: int,
    start_tick: int,
    end_tick: int,
    skips: list,
    xuid: int,
    fps: int,
    bitrate: int,
    capture_dir: str,
    video_filters: str,
    unblock_string: str,
    fragmovie: bool,
    righthand: bool,
    crosshair_code: str,
) -> CommandSystemBuilder:
    c = CommandSystemBuilder()

    padding = 4 * tickrate
    c.tick(start_tick - padding)

    # exec movie config and block death messages
    c.delta(tickrate)
    c.run(f"spec_lock_to_accountid {xuid}", f"mirv_deathmsg highLightId x{xuid}", "exec recorder")

    c.delta(tickrate * 0.5)

    # set the capture dir
    c.run(f'mirv_streams record name "{capture_dir}"')

    # https://write.corbpie.com/ffmpeg-preset-comparison-x264-2019-encode-speed-and-file-size/
    ffmpeg_opt = [
        "-c:v libx264",
        "-b:v " + str(bitrate),
        "-pix_fmt yuv420p",
        "-preset superfast",
    ]

    if video_filters is not None:
        ffmpeg_opt.append(f'-vf "{video_filters}"')

    ffmpeg_opt.append("-y")
    ffmpeg_opt.append(r'"{AFX_STREAM_PATH}\video.mp4"')

    # set the ffmpeg options
    c.run(
        'mirv_streams settings edit ff options "{opt}"'.format(
            opt=" ".join(ffmpeg_opt).replace('"', "{QUOTE}")
        )
    )

    # spec the correct player, clear death message blocks and highlight the correct players' death messages
    c.delta(tickrate * 0.5)
    # ; demo_timescale 0.5')
    c.run(f"spec_lock_to_accountid {xuid}", "spec_mode 4", "mirv_deathmsg lifetime 999")

    c.delta(tickrate * 0.25)
    c.run(
        f"cl_righthand {1 if righthand else 0}",
        f"cl_draw_only_deathnotices {1 if fragmovie else 0}",
        f"apply_crosshair_code {crosshair_code}",
    )

    # record!
    c.tick(start_tick)
    c.run(
        f"spec_lock_to_accountid {xuid}",
        f"host_framerate {fps}",
        "host_timescale 0",
        "mirv_snd_timescale 1",
        "volume 0.5",
        "mirv_streams record start",
    )

    for start, end in skips:
        c.tick(start)
        c.skip(end)

    # stop recording!
    c.tick(end_tick)
    c.run(f"mirv_streams record end", "host_framerate 0", f"echo {unblock_string}")

    # quit
    c.delta(tickrate * 0.5)
    c.run("disconnect")

    return c
