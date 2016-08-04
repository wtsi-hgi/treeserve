from copy import copy, deepcopy
from collections import deque

from treeserve.mapping import Mapping


class Node:
    _node_count = 0

    def __init__(self, name: str, is_directory: bool, parent=None):
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
        self._is_directory = is_directory

    @property
    def depth(self) -> int:
        return self._depth

    @property
    def is_directory(self):
        return self._is_directory

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

    def remove_child(self, node: "Node"):
        del self._children[node.name]

    def get_child(self, name: str) -> "Node":
        return self._children.get(name, None)

    def finalize(self):
        star_child = None
        if self._mapping and self._is_directory:
            # If this node was listed in the mpistat file (list space directory occupies as belonging to files inside directory):
            star_child = Node("*.*", is_directory=False, parent=self)
            star_child.update(self._mapping)
        not_directory_children = []
        child_mappings = []
        if self._children:
            for child in self._children.values():
                if child is star_child: continue
                child_mappings.append(child.finalize())
                if not child.is_directory:
                    not_directory_children.append(child)
        if not_directory_children:
            # If the directory has non-directory children:
            for child in not_directory_children:
                star_child.update(child._mapping)  # Add the remaining data to child.
                self.remove_child(child)  # Delete the node, since it shouldn't end up in the JSON.
        for mapping in child_mappings:
            self.update(mapping)
        return self._mapping

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
