from more_itertools import windowed

# how much time (at minimum) before and after each kill
DEAD_TIME = 2

ADD_INTRO = 4
ADD_OUTRO = 3

ADD_BEFORE_KILL = 2.5
ADD_AFTER_KILL = 1.5

MAX_INTERLEAVE = 1


def single_highlight(tick_rate, kills):
    start_tick = kills[0].tick - int(tick_rate * ADD_INTRO)
    end_tick = kills[-1].tick + int(tick_rate * ADD_OUTRO)

    ticks = end_tick - start_tick

    skips = list()

    if len(kills) > 1:
        for k1, k2 in windowed(kills, n=2, step=1):
            end = k1.tick + int(tick_rate * ADD_AFTER_KILL)
            start = k2.tick - int(tick_rate * ADD_BEFORE_KILL)

            if start - end > tick_rate * (
                ADD_BEFORE_KILL + ADD_AFTER_KILL + MAX_INTERLEAVE
            ):
                skips.append((end, start))
                ticks -= start - end

    return start_tick, end_tick, skips, ticks / tick_rate
