from abc import abstractmethod
import json
from typing import Set

from treeserve.mapping import Mapping, JSONSerializableMapping


class Node:
    """
    A container for information about a file.
    """

    _node_count = 0

    def __init__(self, is_directory: bool, path: str):
        Node._node_count += 1
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

    @classmethod
    def get_node_count(cls) -> int:
        """
        Return the number of `Node`s that have been created.

        Note that this count is not decremented when a `Node` is deleted.

        :return:
        """
        return cls._node_count

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
    def deserialize(cls, serialized: bytes) -> "SerializableNode":
        """
        Deserialize a previously serialized `SerializableNode`.

        :param serialized:
        :return:
        """
        pass


class JSONSerializableNode(SerializableNode):
    def __init__(self, is_directory: bool, path: str):
        super().__init__(is_directory, path)
        self._mapping = JSONSerializableMapping()

    def serialize(self) -> bytes:
        rtn = {
            "path": self.path,
            "children": list(self._child_names),
            "is_directory": self._is_directory,
            "mapping": self._mapping.serialize()
        }
        return json.dumps(rtn).encode()

    @classmethod
    def deserialize(cls, serialized: bytes) -> "JSONSerializableNode":
        serialized = json.loads(serialized.decode())
        is_directory = serialized["is_directory"]
        rtn = cls(is_directory, serialized["path"])
        rtn.update(JSONSerializableMapping.deserialize(serialized["mapping"]))
        for child_name in serialized["children"]:
            rtn.add_child(child_name)
        Node._node_count -= 1  # Don't count accesses as 'creating a new node' - subject to change.
        return rtn
