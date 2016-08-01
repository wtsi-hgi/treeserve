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
        for i, (key, value) in enumerate(self.items()):
            if key in other:
                value -= other[key]
                if value == 0:
                    to_remove.append(key)
        # Can't remove items from dictionary whilst iterating over it.
        for key in to_remove:
            del self[key]

    def add(self, attribute: str, group: str, user: str, category: str, value: Any):
        self[(attribute, group, user, category)] = value

    def add_multiple(self, attribute: str, group: str, user: str, category: str, value: Any):
        for g in ("*", group):
            for u in ("*", user):
                self.add(attribute, g, u, category, value)

    def to_json(self):
        json = {}
        for key, value in self.items():
            data_type = key[0]
            group = key[1]
            user = key[2]
            category = key[3]
            json[data_type][group][user][category] = value
        return json
