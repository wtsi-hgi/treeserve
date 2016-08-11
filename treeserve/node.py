from abc import abstractmethod
from collections import deque
import json
from typing import Dict, Optional

from treeserve.mapping import Mapping, JSONSerializableMapping


class Node:
    """
    A container for information about a file.
    """

    _node_count = 0

    def __init__(self, name: str, is_directory: bool, path: str):
        self._name = name
        self._is_directory = is_directory
        self._path = path
        Node._node_count += 1
        self.children = {}  # type: Dict[str, Node]
        self._mapping = Mapping()

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
        return self._name

    @property
    def path(self):
        return self._path

    @classmethod
    def get_node_count(cls) -> int:
        """
        Return the number of `Node`s that have been created.

        Note that this count is not decremented when a `Node` is deleted, as it may be used as a
        unique identifier in the future.

        :return:
        """
        return cls._node_count

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
        self.children[node.name] = node

    def remove_child(self, node: "Node"):
        """
        Remove a child `Node` from self.

        :param node:
        :return:
        """
        del self.children[node.name]

    def get_child(self, name: str) -> Optional["Node"]:
        """
        Return a child `Node` with the given name, or `None` if no such child exists.

        Implementation detail of `InMemoryTree`.

        :param name:
        :return:
        """
        return self.children.get(name, None)

    def finalize(self) -> Mapping:
        """
        Prepare self for output via the API.

        This consists of the following:

        - creating a ``*.*`` `Node` under self
        - populating the ``*.*`` `Node` with data from self [1]_
        - finalizing children
        - adding the data from self's files to the ``*.*`` `Node`.
        - deleting the files under self [2]_
        - adding the data from the directories under self to self

        .. [1] The reason for this is that typically a directory will take up some space on disk,
           but its data fields are full of data about its child directories; the best place for
           information about the directory itself is therefore with the information about the files
           the directory contains.

        .. [2] This is done to avoid the files turning up in the output from the API.

        :return:
        """
        star_child = None
        if self._mapping and self._is_directory:
            # If this node was listed in the mpistat file (list space directory occupies as
            # belonging to files inside directory):
            star_child = Node("*.*", is_directory=False, path=self.path + "/*.*")
            self.add_child(star_child)
            star_child.update(self._mapping)
        file_children = []
        child_mappings = []
        if self.children:
            for child in self.children.values():
                if child is star_child: continue
                child_mappings.append(child.finalize())
                if not child.is_directory:
                    file_children.append(child)
        if file_children:
            # If the directory has file (or link) children:
            for child in file_children:
                star_child.update(child._mapping)  # Add the remaining data from child to *.*.
                self.remove_child(child)  # Delete the node, since it shouldn't end up in the JSON.
        for mapping in child_mappings:
            self.update(mapping)
        return self._mapping

    def format(self, depth: int) -> Dict:
        """
        Format self for output via the API.

        :param depth:
        :return:
        """
        child_dirs = []
        rtn = {
            "name": self.name,
            "path": self.path,
            "data": self._mapping.format()
        }
        if depth > 0 and self.children:
            for child in self.children.values():
                child_dirs.append(child.format(depth - 1))
            rtn["child_dirs"] = child_dirs
        return rtn


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
    def __init__(self, name: str, is_directory: bool, path: str):
        super().__init__(name, is_directory, path)
        self._mapping = JSONSerializableMapping()

    def serialize(self) -> bytes:
        rtn = {
            "path": self.path,
            "children": list(self.children.keys()),
            "is_directory": self._is_directory,
            "mapping": self._mapping.serialize()
        }
        return json.dumps(rtn).encode()

    @classmethod
    def deserialize(cls, serialized: bytes) -> "JSONSerializableNode":
        serialized = json.loads(serialized.decode())
        split_path = serialized["path"].split("/")
        name = split_path[-1]
        is_directory = serialized["is_directory"]
        rtn = cls(name, is_directory, serialized["path"])
        rtn.update(JSONSerializableMapping.deserialize(serialized["mapping"]))
        return rtn
