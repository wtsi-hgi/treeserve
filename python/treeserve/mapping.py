from abc import abstractmethod
from collections import defaultdict
from typing import Any, Dict, Set
import struct


SECONDS_PER_YEAR = 60 * 60 * 24 * 365
ONE_TIB = 1024 ** 4

class Mapping(dict):
    """
    A custom subclass of `dict` that can be added to and subtracted from other `Mapping`s.
    """
    cost_per_tib_year = 150
    combined_cost = cost_per_tib_year / (ONE_TIB * SECONDS_PER_YEAR)

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

        NB: this method is currently unused, but will be potentially useful for incremental
        updates.

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

    @classmethod
    def recalc_cost(cls, new_cost_per_tib_year: int):
        cls.cost_per_tib_year = new_cost_per_tib_year
        cls.combined_cost = new_cost_per_tib_year / (ONE_TIB * SECONDS_PER_YEAR)


    def format(self, whitelist: Set[str]) -> Dict:
        """
        Format self for output via the API.

        :return:
        """
        rtn = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))  # ew
        for (data_type, group, user, category), value in self.items():
            if not whitelist or category in whitelist:
                if data_type.endswith("time"):
                    value *= self.combined_cost
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


class DictSerializableMapping(SerializableMapping):
    def serialize(self) -> Dict:
        rtn = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))  # ew
        for (data_type, group, user, category), value in self.items():
            rtn[data_type][group][user][category] = value
        return rtn

    @classmethod
    def deserialize(cls, serialized: Dict) -> "DictSerializableMapping":
        rtn = cls()
        for data_type in serialized:
            for group in serialized[data_type]:
                for user in serialized[data_type][group]:
                    for category, value in serialized[data_type][group][user].items():
                        rtn[data_type, group, user, category] = value
        return rtn


class StructSerializableMapping(SerializableMapping):
    """
    Format:

        num_keys: unsigned int "I" (4 bytes)
        for key in range(num_keys):
            for key_index in range(4):
                len_key_part: unsigned short "H" (2 bytes)
                key_part: char[len_key_part] "s" (`len_key_part` bytes)
            value: unsigned long long "QQ" (16 bytes)
    """
    def serialize(self, buf: memoryview) -> int:
        no_keys = len(self)
        offset = 0
        struct.pack_into(">I", buf, offset, no_keys)
        offset += 4
        for key, value in self.items():
            for i in key:
                offset = self.pack_var_str(i, buf, offset)
            try:
                struct.pack_into(">QQ", buf, offset, value >> 64, value & (2 ** 64 - 1))
            except struct.error:
                print(value)
                raise
            offset += 16
        return offset

    def calc_length(self) -> int:
        """
        Calculate the length in bytes of the serialized mapping.

        :return:
        """
        total = 4  # unsigned int (4 bytes) for the number of keys
        for key in self:
            #
            # plus two unsigned long longs (16 bytes) for the value
            total += sum(self.calc_var_str(x) for x in key) + 16
        return total

    @classmethod
    def pack_var_str(cls, string: str, buf: memoryview, offset: int=0) -> int:
        # 2 bytes for the length of the string, then the string itself.
        assert len(string) < 2 ** 16, "String cannot be over {} characters long".format(2 ** 16)
        struct.pack_into(">H{}s".format(len(string)), buf, offset, len(string), string.encode())
        offset += cls.calc_var_str(string)
        return offset

    @classmethod
    def calc_var_str(cls, string: str) -> int:
        return 2 + len(string)

    @classmethod
    def unpack_var_str(cls, serialized: memoryview, offset: int=0) -> (str, int):
        length = struct.unpack_from(">H", serialized, offset)[0]
        offset += 2
        string = struct.unpack_from(">{}s".format(length), serialized, offset)[0]
        return string.decode(), offset + length  # result, next offset

    @classmethod
    def deserialize(cls, serialized: memoryview) -> ("StructSerializableMapping", int):
        rtn = cls()
        offset = 0
        num_keys = struct.unpack_from(">I", serialized)[0]
        offset += 4  # Length of "I" (num_keys) in bytes
        for key_index in range(num_keys):
            key = []
            for i in range(4):
                string, offset = cls.unpack_var_str(serialized, offset)
                key.append(string)
            a, b = struct.unpack_from(">QQ", serialized, offset)
            value = (a << 64) + b
            offset += 16
            rtn[tuple(key)] = value
        return rtn, offset
