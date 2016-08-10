from abc import ABCMeta, abstractmethod
import lmdb
from typing import Dict, Optional

from treeserve.mapping import Mapping
from treeserve.node import Node, JSONSerializableNode


class Tree(metaclass=ABCMeta):
    """
    A container for `Node`s.
    """

    @abstractmethod
    def add_node(self, path: str, is_directory: bool, mapping: Mapping):
        """
        Turn the components of a node into a `Node` and add it to self.

        If `Node`'s parents don't exist, create them too (but don't give them a `Mapping` --
        they should hopefully be present later in the input file, which will cause them to gain a
        `Mapping`).

        :param path:
        :param is_directory:
        :param mapping:
        :return:
        """
        pass

    @abstractmethod
    def get_node(self, path: str) -> Node:
        """
        Return a `Node` from self based on a path.

        :param path:
        :return:
        """
        pass

    @abstractmethod
    def finalize(self):
        """
        Prepare self for output via the API.

        This consists of the following:

        - creating a ``*.*`` `Node` under each directory that was in the input file
        - populating each ``*.*`` `Node` with data from its parent directory [1]_
        - finalizing children
        - adding the data from each `Node`'s files to its ``*.*`` `Node`.
        - deleting the files under each directory [2]_
        - adding the data from the directories in each directory to the directory

        .. [1] The reason for this is that typically a directory will take up some space on disk,
           but its data fields are full of data about its child directories; the best place for
           information about the directory itself is therefore with the information about the files
           the directory contains.

        .. [2] This is done to avoid the files turning up in the output from the API.

        :return:
        """
        pass

    @abstractmethod
    def format(self, path: str, depth: int) -> Dict:
        """
        Format self (or a subtree of self) for output via the API.

        :param path:
        :param depth:
        :return:
        """
        pass


class InMemoryTree(Tree):
    """
    A container for `Node`s that keeps all `Node`s in memory.
    """

    def __init__(self):
        self._root = None  # type: Optional[Node]

    def add_node(self, path: str, is_directory: bool, mapping: Mapping):
        split_path = path.strip("/").split("/")
        if self._root is None:
            self._root = Node(split_path[0], is_directory=True)
        current_node = self._root
        # The first path component should always be the name of the root node.
        assert split_path[0] == self._root.name, (split_path[0], self._root.name)
        for fragment in split_path[1:]:
            # Start from the node after root - this has the side-effect of ignoring the first part
            # of the path totally (e.g. /foo/scratch115 will work just fine).
            child_node = current_node.get_child(fragment)
            if child_node is None:
                current_node = Node(fragment, is_directory, parent=current_node)
            else:
                current_node = child_node
        current_node.update(mapping)

    def get_node(self, path: str) -> Node:
        split_path = path.strip("/").split("/")
        current_node = self._root
        if not split_path[0]:
            return current_node
        # The first path component should always be the name of the root node.
        assert split_path[0] == self._root.name, (split_path[0], self._root.name)
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
        node = self.get_node(path)

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

    def add_node(self, path: str, is_directory: bool, mapping: Mapping):
        split_path = path.split("/")
        name = split_path[-1]
        node = JSONSerializableNode(name, is_directory)
        node.update(mapping)
        with self._env.begin(write=True) as txn:
            txn.put(path.encode(), node.serialize())

    def get_node(self, path: str) -> Node:
        with self._env.begin() as txn:
            node = txn.get(path.encode())
        return node

    def finalize(self):
        pass

    def format(self, path: str, depth: int) -> Dict:
        pass
