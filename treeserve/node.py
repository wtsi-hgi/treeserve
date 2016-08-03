from copy import copy, deepcopy
from collections import deque

from treeserve.mapping import Mapping


class Node:
    _node_count = 0

    def __init__(self, name: str, parent=None):
        self._name = name
        self._parent = parent
        Node._node_count += 1
        if self._parent is not None:
            self._depth = self._parent.depth + 1
            self._parent.add_child(self)
        else:
            self._depth = 0
        self._children = {}
        self._mapping = Mapping()

    @property
    def depth(self) -> int:
        return self._depth

    @property
    def name(self) -> str:
        return self._name

    @property
    def parent(self) -> "Node":
        return self._parent

    @property
    def path(self) -> str:
        fragments = deque()
        current = self
        while current is not None:
            fragments.appendleft(current.name)
            current = current.parent
        return "/" + "/".join(fragments)

    @classmethod
    def get_node_count(cls) -> int:
        return cls._node_count

    def update(self, mapping: Mapping):
        self._mapping.update(mapping)

    def add_child(self, node: "Node"):
        self._children[node.name] = node

    def get_child(self, name: str) -> "Node":
        return self._children.get(name, None)

    def finalize(self):
        mapping_copy = copy(self._mapping)
        if self._children:
            for child in self._children.values():
                child.finalize()
                mapping_copy.subtract(child._mapping)
        if mapping_copy:
            # If not all data in self._mapping was due to child directories:
            child = Node("*.*", parent=self)
            child.update(mapping_copy)  # Add the remaining data to child

    def to_json(self, depth: int):
        child_dirs = []
        json = {
            "name": self.name,
            "path": self.path,
            "data": self._mapping.to_json()
        }
        if depth > 0 and self._children:
            for name, child in self._children.items():
                child_dirs.append(child.to_json(depth-1))
            json["child_dirs"] = child_dirs
        return json
