from abc import abstractmethod
from collections import defaultdict
from typing import Any, Dict
import struct


COST_PER_TIB_YEAR = 150
SECONDS_PER_YEAR = 60 * 60 * 24 * 365
ONE_TIB = 1024 ** 4
COMBINED_COST = COST_PER_TIB_YEAR / (ONE_TIB * SECONDS_PER_YEAR)


class Mapping(dict):
    """
    A custom subclass of `dict` that can be added to and subtracted from other `Mapping`s.
    """

    def __missing__(self, key):
        return 0

    def update(self, other: "Mapping"):
        """
        Combine the given `Mapping` with self and store the result in self.

        :param other:
        :return:
        """
        if self:
            for key, count in other.items():
                self[key] += count
        else:
            super().update(other)

    def subtract(self, other: "Mapping"):
        """
        Subtract the given `Mapping` from self and store the result in self.

        If there would be values equal to 0 in the new `Mapping`, do not store them.

        :param other:
        :return:
        """
        to_remove = []
        for k, v in self.items():
            if k in other:
                v -= (other[k])
                self[k] = v
                if v == 0:
                    to_remove.append(k)
        # Can't remove items from dictionary whilst iterating over it.
        for k in to_remove:
            del self[k]

    def set(self, attribute: str, group: str, user: str, category: str, value: Any):
        """
        Set the value for the criteria given.

        :param attribute:
        :param group:
        :param user:
        :param category:
        :param value:
        :return:
        """
        # Although semantically correct, using += is significantly slower; looping over ("*", user)
        # and ("*", group) is also slower.
        self[attribute, "*", "*", category] = value
        self[attribute, "*", user, category] = value
        self[attribute, group, "*", category] = value
        self[attribute, group, user, category] = value

    def format(self) -> Dict:
        """
        Format self for output via the API.

        :return:
        """
        rtn = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))  # ew
        for key, value in self.items():
            data_type = key[0]
            group = key[1]
            user = key[2]
            category = key[3]
            if data_type.endswith("time"):
                value *= COMBINED_COST
            # Need to convert numbers to strings - why? Who knows?
            rtn[data_type][group][user][category] = str(round(value, 3))
        return rtn


class SerializableMapping(Mapping):
    @abstractmethod
    def serialize(self) -> bytes:
        """
        Serialize self for storage (e.g. in a database).

        :return:
        """
        pass

    @classmethod
    @abstractmethod
    def deserialize(cls, serialized: bytes) -> "SerializableMapping":
        """
        Deserialize a previously serialized `SerializableMapping`.

        :param serialized:
        :return:
        """
        pass


class JSONSerializableMapping(SerializableMapping):
    def serialize(self) -> Dict:
        rtn = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))  # ew
        for key, value in self.items():
            data_type = key[0]
            group = key[1]
            user = key[2]
            category = key[3]
            rtn[data_type][group][user][category] = value
        return rtn

    @classmethod
    def deserialize(cls, serialized: Dict) -> "JSONSerializableMapping":
        rtn = cls()
        for data_type in serialized:
            for group in serialized[data_type]:
                for user in serialized[data_type][group]:
                    for category, value in serialized[data_type][group][user].items():
                        rtn[data_type, group, user, category] = value
        return rtn
