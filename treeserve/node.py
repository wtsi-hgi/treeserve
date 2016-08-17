from abc import abstractmethod
import json
import pickle
import struct
from typing import Set

from treeserve.mapping import Mapping, DictSerializableMapping, StructSerializableMapping


class Node:
    """
    A container for information about a file.
    """

    def __init__(self, is_directory: bool, path: str):
        self._is_directory = is_directory
        self._path = path
        self._child_names = set()  # type: Set[str]
        self._mapping = Mapping()

    def __repr__(self):
        return "<Node object at {}>".format(repr(self._path))

    def __eq__(self, other: "Node"):
        for attr in ("child_names", "is_directory", "mapping", "path"):
            try:
                self_attr, other_attr = [getattr(obj, attr) for obj in (self, other)]
            except AttributeError:
                return False
            else:
                if self_attr != other_attr:
                    return False
        return True

    @property
    def child_names(self) -> Set[str]:
        """
        Return the names of the children of self.

        :return:
        """
        return self._child_names

    @property
    def is_directory(self) -> bool:
        """
        Return whether self is a directory.

        :return:
        """
        return self._is_directory

    @property
    def name(self) -> str:
        """
        Return the name (final path component) of self.

        :return:
        """
        return self._path.split("/")[-1]

    @property
    def mapping(self) -> Mapping:
        return self._mapping

    @property
    def path(self) -> str:
        """
        Return the absolute path of self.

        :return:
        """
        return self._path

    def get_child_path(self, child_name) -> str:
        return self._path + "/" + child_name

    def update(self, mapping: Mapping):
        """
        Combine the given `Mapping` with self's `Mapping`.

        :param mapping:
        :return:
        """
        self._mapping.update(mapping)

    def add_child(self, node: "Node"):
        """
        Add a child `Node` to self.

        This method is idempotent -- if it is called multiple times with the same `Node`, the same
        child will *not* be added multiple times.

        :param node:
        :return:
        """
        self._child_names.add(node.name)

    def remove_child(self, node: "Node"):
        """
        Remove a child `Node` from self.

        :param node:
        :return:
        """
        self._child_names.remove(node.name)


class SerializableNode(Node):
    @abstractmethod
    def serialize(self) -> bytes:
        """
        Serialize self for storage (e.g. in a database).

        :return:
        """
        pass

    @classmethod
    @abstractmethod
    def deserialize(cls, path: str, serialized: bytes) -> "SerializableNode":
        """
        Deserialize a previously serialized `SerializableNode`.

        :param serialized:
        :return:
        """
        pass


class JSONSerializableNode(SerializableNode):
    def __init__(self, is_directory: bool, path: str):
        super().__init__(is_directory, path)
        self._mapping = DictSerializableMapping()

    @classmethod
    def uses_buffers(cls): return False

    def serialize(self) -> bytes:
        rtn = {
            "children": list(self._child_names),
            "is_directory": self._is_directory,
            "mapping": self._mapping.serialize()
        }
        return json.dumps(rtn).encode()

    @classmethod
    def deserialize(cls, path: str, serialized: bytes) -> "JSONSerializableNode":
        serialized = json.loads(serialized.decode())
        is_directory = serialized["is_directory"]
        rtn = cls(is_directory, path)
        rtn.update(DictSerializableMapping.deserialize(serialized["mapping"]))
        for child_name in serialized["children"]:
            rtn._child_names.add(child_name)
        return rtn


class PickleSerializableNode(SerializableNode):
    def __init__(self, is_directory: bool, path: str):
        super().__init__(is_directory, path)
        self._mapping = DictSerializableMapping()

    @classmethod
    def uses_buffers(cls): return False

    def serialize(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, path: str, serialized: bytes) -> "PickleSerializableNode":
        return pickle.loads(serialized)


class StructSerializableNode(SerializableNode):
    """
    Format:
        no_children: unsigned short "H" (2 bytes)
        for child in children:
            len_child_name: unsigned short "H" (2 bytes)
            child_name: char[len_child_name] "s" (`len_child_name` bytes)
        is_directory: bool "?" (1 byte)
        mapping: struct{mapping}
    """

    def __init__(self, is_directory: bool, path: str):
        super().__init__(is_directory, path)
        self._mapping = StructSerializableMapping()

    @classmethod
    def uses_buffers(cls): return True

    def serialize(self) -> memoryview:
        # children: VarList[VarStr]
        # is_directory: boolean
        # mapping: struct
        #print("SERIALIZE", self.path)
        size = self.calc_length()
        byte_array = bytearray(size)
        buf = memoryview(byte_array)
        offset = 0
        struct.pack_into(">H", buf, offset, len(self.child_names))
        offset += 2
        for child in self.child_names:
            offset = self.pack_var_str(child, buf, offset)
        struct.pack_into(">?", buf, offset, self.is_directory)
        offset += 1
        offset += self.mapping.serialize(buf[offset:])
        assert offset == size
        #print(byte_array)
        return buf

    def pack_var_str(self, string: str, buf: memoryview, offset: int=0) -> int:
        # 2 bytes for the length of the string, then the string itself.
        assert len(string) < 2 ** 16, "String cannot be over {} characters long".format(2 ** 16)
        struct.pack_into(">H{}s".format(len(string)), buf, offset, len(string), string.encode())
        offset += self.calc_var_str(string)
        return offset

    @classmethod
    def calc_var_str(cls, string: str) -> int:
        return 2 + len(string)

    @classmethod
    def unpack_var_str(cls, serialized: memoryview, offset: int=0) -> (str, int):
        # 2 bytes for the length of the string, then the string itself.
        length = struct.unpack_from(">H", serialized, offset)[0]
        string = struct.unpack_from(">{}s".format(length), serialized, offset + 2)[0]
        return string.decode(), offset + 2 + length  # result, next offset

    def calc_length(self):
        total = 2
        total += sum(self.calc_var_str(child) for child in self.child_names)
        total += 1
        total += self.mapping.calc_length()
        return total

    @classmethod
    def deserialize(cls, path: str, serialized: memoryview) -> "StructSerializableNode":
        #print("DESERIALIZE", path)
        offset = 0
        no_children = struct.unpack_from(">H", serialized, offset)[0]
        offset += 2
        child_names = []
        for i in range(no_children):
            string, offset = cls.unpack_var_str(serialized, offset)
            child_names.append(string)
        is_directory = struct.unpack_from(">?", serialized, offset)[0]
        offset += 1
        mapping, len_mapping = StructSerializableMapping.deserialize(serialized[offset:])
        offset += len_mapping
        rtn = cls(is_directory, path)
        rtn._child_names = set(child_names)
        rtn.update(mapping)
        assert offset == len(serialized), (offset, len(serialized))
        return rtn
