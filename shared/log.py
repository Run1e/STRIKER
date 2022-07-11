import logging

import coloredlogs


def logging_config(debug: bool = False):
    coloredlogs.install(
        level=logging.DEBUG if debug else logging.INFO,
        fmt='{asctime} [{levelname}] {name}: {message}',
        style='{',
        level_styles=dict(
            spam=dict(color=8, faint=True),
            debug=dict(color=8, faint=True),
            verbose=dict(color=12),
            info=dict(color=15),
            notice=dict(color=15),
            warning=dict(bold=True, color=13),
            success=dict(color=15),
            error=dict(color=9, bold=True),
            critical=dict(bold=True, color=9),
        ),
    )


"""
    spam=dict(color='green', faint=True),
    debug=dict(color='green'),
    verbose=dict(color='blue'),
    info=dict(),
    notice=dict(color='magenta'),
    warning=dict(color='yellow'),
    success=dict(color='green', bold=True),
    error=dict(color='red'),
    critical=dict(color='red', bold=True),
"""
