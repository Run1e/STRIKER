from vdm import Script
import config

TICK_PADDING = 384


def craft_vdm(
    start_tick,
    end_tick,
    skips,
    xuid,
    fps,
    bitrate,
    capture_dir,
    video_filters,
    unblock_string,
):
    s = Script()

    s.tick(start_tick - TICK_PADDING)

    # exec movie config and block death messages
    s.delta(64)
    s.PlayCommands(
        f"spec_lock_to_accountid {xuid}; mirv_deathmsg highLightId x{xuid}; exec recorder"
    )

    s.delta(32)

    # set the capture dir
    s.PlayCommands(f'mirv_streams record name "{capture_dir}"')

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
    s.PlayCommands(
        'mirv_streams settings edit ff options "{opt}"'.format(
            opt=" ".join(ffmpeg_opt).replace('"', "{QUOTE}")
        )
    )

    # spec the correct player, clear death message blocks and highlight the correct players' death messages
    s.delta(32)
    # ; demo_timescale 0.5')
    s.PlayCommands(f"spec_lock_to_accountid {xuid}; spec_mode 4; mirv_deathmsg lifetime 999")

    # record!
    s.tick(start_tick)
    s.PlayCommands(
        f"spec_lock_to_accountid {xuid}; host_framerate {fps}; host_timescale 0; mirv_snd_timescale 1; volume 0.5; mirv_streams record start"
    )

    for (start, end) in skips:
        # s.tick(start - 64)
        # s.ScreenFadeStart(duration="1.000", holdtime="1.000")
        s.tick(start)
        s.SkipAhead(end)

    # stop recording!
    s.tick(end_tick)
    s.PlayCommands(f"mirv_streams record end; host_framerate 0; echo {unblock_string}")

    # quit
    s.delta(16)
    s.PlayCommands("disconnect")

    return s
