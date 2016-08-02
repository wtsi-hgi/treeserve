from collections import defaultdict
from typing import Any, List


class Mapping(dict):
    def combine_with(self, other):
        for k, v in other.items():
            if k in self:
                self[k] += v
            else:
                self[k] = v

    def __sub__(self, other):
        to_remove = []
        for k, v in self.items():
            if k in other:
                v -= other[k]
                if v == 0:
                    to_remove.append(k)
        # Can't remove items from dictionary whilst iterating over it.
        for k in to_remove:
            del self[k]
        print('type of self after removes:\n', type(self))

    def add(self, attribute: str, group: str, user: str, category: str, value: Any):
        self[(attribute, group, user, category)] = value

    def add_multiple(self, attribute: str, group: str, user: str, category: str, value: Any):
        for g in ("*", group):
            for u in ("*", user):
                self.add(attribute, g, u, category, value)

    def to_json(self):
        json = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict)))  # ew
        for key, value in self.items():
            data_type = key[0]
            group = key[1]
            user = key[2]
            category = key[3]
            json[data_type][group][user][category] = value
        return json
