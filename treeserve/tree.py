import sys

from mapping import Mapping
from node import Node


class Tree:
    def __init__(self):
        self._root = None

    def add_node(self, path: str, mapping: Mapping):

        split_path = path.strip("/").split("/")
        if self._root is None:
            self._root = Node(split_path[0])
        current = self._root
        for fragment in split_path[1:]:
            # Every step down in the tree gets the new node's values added to it. Refactor this to
            # postpone this step until tree finalization, with a bottom-up/'bubbling' approach.
            current.combine(mapping)
            temp = current.get_child(fragment)
            if temp is None:
                current = Node(fragment, current)
            else:
                current = temp
        current.combine(mapping)

    def get_node_at(self, path: str):
        split_path = path.strip("/").split("/")
        current = self._root
        for fragment in split_path[1:]:
            current = current.get_child(fragment)
            if current is None:
                # If there is no node with the given path:
                return None
        return current

    def finalize(self):
        if self._root:
            self._root.finalize()

    def to_json(self, *, path: str=None, depth: int=sys.maxsize):
        node = self.get_node_at(path) if path is not None else self._root

        if depth == 0:
            depth = 1
        if node is None:
            return {}
        else:
            return node.to_json(depth)
