from collections import defaultdict

from vdf import dumps


class Script:
    def __init__(self):
        self.data = dict(demoactions=dict())

        self._tick = 0
        self._key = 1
        self._counter = defaultdict(int)

    def dumps(self):
        return dumps(self.data, pretty=True)

    def add(self, factory, **kwargs):
        self._counter[factory] += 1

        self.data["demoactions"][self._key] = dict(
            factory=factory,
            name=kwargs.get("name", f"{factory}{self._counter[factory]}"),
            starttick=self._tick,
            **kwargs,
        )

        self._key += 1

    def tick(self, tick):
        self._tick = tick

    def delta(self, delta):
        self._tick += delta

    def SkipAhead(self, skiptotick):
        self.add("SkipAhead", skiptotick=skiptotick)
        self._tick = skiptotick

    def PlayCommands(self, command):
        self.add("PlayCommands", commands=command)

    def StopPlayback(self):
        self.add("StopPlayback")

    def ScreenFadeStart(
        self, duration=1.0, holdtime=0.0, r=0, g=0, b=0, a=255, FFADE_OUT=1, FFADE_IN=1
    ):
        self.add(
            "ScreenFadeStart",
            duration=duration,
            holdtime=holdtime,
            FFADE_IN=FFADE_IN,
            FFADE_OUT=FFADE_OUT,
            r=r,
            g=g,
            b=b,
            a=a,
        )
        """
	{
		factory "ScreenFadeStart"
		name "ScreenFadeStart2"
		starttick "0"
		duration "1.000"
		holdtime "1.000"
		FFADE_IN "1"
		FFADE_OUT "1"
		r "0"
		g "0"
		b "0"
		a "255"
	}"3"
		"""
