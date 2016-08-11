from abc import ABCMeta, abstractmethod
import lmdb
from typing import Dict, List, Optional

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
    def get_node(self, path: str) -> Optional[Node]:
        """
        Return a `Node` from self based on a path, or `None` if the node does not exist.

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
        # This maps node paths to node objects, much like a database would.
        # An example:
        # {
        #     "/root": ...,
        #     "/root/somefile.txt": ...,
        #     "/root/somedirectory/*.*": ...
        # }
        # tl;dr: leading slash included, trailing slash not included.
        self._nodes = {}  # type: Dict[str, Node]
        self._root_path = None

    def add_node(self, path: str, is_directory: bool, mapping: Mapping=None):
        split_path = path.strip("/").split("/")
        if self._root_path is None:
            # Special-case root node creation, since it doesn't have a parent.
            self._root_path = "/" + split_path[0]
            self._register_node(Node(is_directory=True, path=self._root_path))
        # The first path component should always be the name of the root node.
        assert split_path[0] == self._root_path.lstrip("/"), (split_path[0], self._root_path)
        path_stack = ["", split_path[0]]  # The empty element at the start causes "/".join to insert a slash at the start of the string.
        current_node = self.get_node(self._root_path)
        for fragment in split_path[1:-1]:
            # Create parent directories
            path_stack.append(fragment)
            child_node = self.get_node("/".join(path_stack))
            if child_node is None:
                tmp = Node(True, path="/".join(path_stack))
                self._register_node(tmp)
                current_node.add_child(tmp)
                current_node = tmp
            else:
                current_node = child_node
        # Nasty hack to work around the fact that the root node is special.
        if path != self._root_path:
            # Make the node we were actually asked to make, add it to the tree and give it data.
            child = Node(is_directory, path)
            current_node.add_child(child)
            if mapping is not None:
                child.update(mapping)
            self._register_node(child)
        else:
            # We were asked to make the root node, which already exists due to special case; we just need to give it data.
            if mapping is not None:
                current_node.update(mapping)

    def get_node(self, path: str) -> Optional[Node]:
        if not path.strip("/"):
            return self.get_node(self._root_path)
        return self._nodes.get(path.rstrip("/"))  # Remove a trailing slash, but not a leading one

    def _register_node(self, node: Node):
        self._nodes[node.path] = node

    def finalize(self):
        if self._nodes and self._root_path:
            self._finalize_node(self.get_node(self._root_path))

    def _add_child(self, node: Node, child: Node):
        node.add_child(child)

    def _remove_child(self, node: Node, child: Node):
        node.remove_child(child)
        del self._nodes[child.path]

    def _finalize_node(self, node: Node) -> Mapping:
        file_children = []  # type: List[Node]
        child_mappings = []  # type: List[Mapping]
        for child_name in node.child_names:
            child_path = node.get_child_path(child_name)
            child = self.get_node(child_path)
            child_mappings.append(self._finalize_node(child))
            if not child.is_directory:
                file_children.append(child)
        if (node._mapping and node.is_directory) or file_children:
            # If this node was listed in the mpistat file (list space directory occupies as
            # belonging to files inside directory):
            star = Node(is_directory=False, path=node.path + "/*.*")
            self._add_child(node, star)
            star.update(node._mapping)
            self._register_node(star)
        for child in file_children:
            # Add data from child files to *.* and delete the files' nodes, since they shouldn't
            # appear in the JSON outputted by the API.
            star.update(child._mapping)
            self._remove_child(node, child)
        for mapping in child_mappings:
            # This must be postponed until after ``star.update(node._mapping)`` has been called.
            node.update(mapping)
        return node._mapping

    def format(self, path: str="/", depth: int=0) -> Dict:
        node = self.get_node(path)
        if node is None:
            return {}
        else:
            return self._format_node(node, depth + 1)

    def _format_node(self, node: Node, depth: int) -> Dict:
        """
        Format self for output via the API.

        :param depth:
        :return:
        """
        child_dirs = []
        rtn = {
            "name": node.name,
            "path": node.path,
            "data": node._mapping.format()
        }
        if depth > 0 and node.child_names:
            for child_name in node.child_names:
                child_path = node.get_child_path(child_name)
                child = self.get_node(child_path)
                child_dirs.append(self._format_node(child, depth - 1))
            rtn["child_dirs"] = child_dirs
        return rtn


class LMDBTree(Tree):
    """
    A container for `Node`s that stores nodes in LMDB when not being used.
    """

    def __init__(self, lmdb_dir):
        self._env = lmdb.open(lmdb_dir)

    def add_node(self, path: str, is_directory: bool, mapping: Mapping):
        node = JSONSerializableNode(is_directory, path)
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
