import json
import os
from math import sqrt
from string import ascii_uppercase

CAPITALIZERS = ascii_uppercase + ''.join(str(i) for i in range(10))

REPLACEMENTS = (('Topof', 'Top of'), ('Backof', 'Back of'))


def dist(a, b):
    x = b[0] - a[0]
    y = b[1] - a[1]
    z = b[2] - a[2]
    return sqrt(x * x + y * y + z * z)


class MapAreas:
    def __init__(self, map, data):
        self.map = map

        self._areas = dict()
        self._places = list(data.keys())

        for idx, areas in enumerate(data.values()):
            for area in areas:
                area = tuple(area)
                self._areas[area] = idx

    def prettify_name(self, name):
        p = name[0]
        s = ''

        for c in name[1:]:
            c_is_upper = c in CAPITALIZERS
            p_is_upper = p in CAPITALIZERS

            if p_is_upper and not c_is_upper:
                s += ' '

            s += p
            p = c

        if c in CAPITALIZERS:
            s += ' '
        s += c

        for old, new in REPLACEMENTS:
            s = s.replace(old, new)

        return s.strip()

    def get_place(self, place_id):
        try:
            place_name = self._places[place_id]
        except ValueError:
            return 'Unknown'

        return self.prettify_name(place_name)

    def get_vec_name(self, vec):
        return self.get_place(self.get_vec_id(vec))

    def get_vec_id(self, vec):
        best_dist = None
        winning_place_id = None

        for area_vec, place_id in self._areas.items():
            d = dist(vec, area_vec)

            if best_dist is None or d < best_dist:
                best_dist = d
                winning_place_id = place_id

        return winning_place_id


places = {}

for file in os.listdir('bot/navparse/nav'):
    map_name = file.split('.')[0]
    with open(f'bot/navparse/nav/{file}', 'r') as f:
        data = json.loads(f.read())
        if not data:
            continue

        places[map_name] = MapAreas(map_name, data)
