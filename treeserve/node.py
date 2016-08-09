from collections import deque
from typing import Dict, Optional

from treeserve.mapping import Mapping


class Node:
    """
    A container for information about a file.
    """

    _node_count = 0

    def __init__(self, name: str, is_directory: bool, parent: "Node"=None):
        self._name = name
        self._parent = parent
        Node._node_count += 1
        if self._parent is not None:
            self._parent.add_child(self)
        self._children = {}  # type: Dict[str, Node]
        self._mapping = Mapping()
        self._is_directory = is_directory

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
    def parent(self) -> "Node":
        """
        Return the parent `Node` of self.

        :return:
        """
        return self._parent

    @property
    def path(self) -> str:
        """
        Return the absolute path of self.

        This is constructed each time it is requested by walking the tree upwards; the full path of
        each node is not stored, in order to save space.

        :return:
        """
        fragments = deque()
        current = self
        while current is not None:
            fragments.appendleft(current.name)
            current = current.parent
        return "/" + "/".join(fragments)

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
        self._children[node.name] = node

    def remove_child(self, node: "Node"):
        """
        Remove a child `Node` from self.

        :param node:
        :return:
        """
        del self._children[node.name]

    def get_child(self, name: str) -> Optional["Node"]:
        """
        Return a child `Node` with the given name, or `None` if no such child exists.

        :param name:
        :return:
        """
        return self._children.get(name, None)

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
        if depth > 0 and self._children:
            for name, child in self._children.items():
                child_dirs.append(child.format(depth - 1))
            rtn["child_dirs"] = child_dirs
        return rtn
