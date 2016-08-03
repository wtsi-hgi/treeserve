from collections import defaultdict
from copy import deepcopy
from typing import Any


COST_PER_TIB_YEAR = 150
SECONDS_PER_YEAR = 60 * 60 * 24 * 365
ONE_TIB = 1024 ** 4
COMBINED_COST = COST_PER_TIB_YEAR / (ONE_TIB * SECONDS_PER_YEAR)


class Mapping(dict):
    def combine_with(self, other):
        for k, v in other.items():
            if k in self:
                self[k] += v
            else:
                self[k] = v

    def subtract(self, other):
        to_remove = []
        for k, v in self.items():
            if k in other:
                v -= (other[k])
                if v == 0:
                    to_remove.append(k)
        # Can't remove items from dictionary whilst iterating over it.
        for k in to_remove:
            del self[k]

    def add(self, attribute: str, group: str, user: str, category: str, value: Any):
        self[(attribute, group, user, category)] = value

    def add_multiple(self, attribute: str, group: str, user: str, category: str, value: Any):
        for g in ("*", group):
            for u in ("*", user):
                self.add(attribute, g, u, category, value)

    def to_json(self):
        json = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))  # ew
        for key, value in self.items():
            data_type = key[0]
            group = key[1]
            user = key[2]
            category = key[3]
            # Need to convert numbers to strings - why? Who knows?
            if data_type.endswith("time"):
                value *= COMBINED_COST
            json[data_type][group][user][category] = str(value)
        return json
