import lmdb
from typing import Dict

from treeserve.mapping import Mapping
from treeserve.node import Node


class Tree:
    def __init__(self):
        self._root = None

    def add_node(self, path: str, is_directory: bool, mapping: Mapping) -> Node:
        split_path = path.strip("/").split("/")
        if self._root is None:
            self._root = Node(split_path[0], is_directory=True)
        current_node = self._root
        for fragment in split_path[1:]:
            # Start from the node after root - this has the side-effect of ignoring the first part
            # of the path totally (e.g. /foo/scratch115 will work just fine).
            child_node = current_node.get_child(fragment)
            if child_node is None:
                current_node = Node(fragment, is_directory, parent=current_node)
            else:
                current_node = child_node
        current_node.update(mapping)
        return current_node

    def get_node_at(self, path: str) -> Node:
        split_path = path.strip("/").split("/")
        current_node = self._root
        for fragment in split_path[1:]:
            current_node = current_node.get_child(fragment)
            if current_node is None:
                # If there is no node with the given path:
                return None
        return current_node

    def finalize(self):
        if self._root:
            self._root.finalize()

    def format(self, path: str, depth: int) -> Dict:
        node = self.get_node_at(path) if path is not None else self._root

        if node is None:
            return {}
        else:
            return node.format(depth + 1)


class LMDBTree(Tree):
    """
    A container for `Node`s that stores nodes in LMDB when not being used.
    """

    def __init__(self, lmdb_dir):
        self._env = lmdb.open(lmdb_dir)
        super().__init__()
