from collections.abc import Sized
from time import strftime
from typing import Dict, List, Optional

from treeserve.mapping import Mapping
from treeserve.node import Node, JSONSerializableNode
from treeserve.node_store import NodeStore


class Tree(Sized):
    """
    A container for `Node`s.
    """

    def __init__(self, node_store: NodeStore):
        self._node_store = node_store
        self._root_path = None
        self._Node = node_store.node_type

    def __len__(self):
        return len(self._node_store)

    def add_node(self, path: str, is_directory: bool, mapping: Mapping=None):
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
        split_path = path.strip("/").split("/")
        if self._root_path is None:
            # Special-case root node creation, since it doesn't have a parent.
            self._root_path = "/" + split_path[0]
            self._commit_node(self._Node(is_directory=True, path=self._root_path))
            self._node_store._root_path = self._root_path
        # The first path component should always be the name of the root node.
        assert split_path[0] == self._root_path.lstrip("/"), (split_path[0], self._root_path)
        current_node = None
        path_stack = ["", split_path[0]]  # The empty element at the start causes "/".join to insert a slash at the start of the string.
        for fragment in split_path[1:]:
            # Create parent directories
            path_stack.append(fragment)
            current_path = "/".join(path_stack)
            if current_path not in self._node_store:
                # Child node doesn't exist, so create it.
                if current_node:
                    old_node = current_node
                else:
                    old_node = self.get_node("/".join(path_stack[:-1]))
                current_node = self._Node(True, path="/".join(path_stack))
                self._add_child(old_node, current_node)
                self._commit_node(old_node)
        if current_node is None:
            # Should only happen for root node, since it has no parent.
            current_node = self.get_node(path)
        assert current_node.path == path
        # Nasty hack to work around the fact that the root node is special - by the time we get
        # here for the root node, we've already created it.
        if path != self._root_path:
            # Make the node we were actually asked to make, add it to the tree and give it data.
            child = self._Node(is_directory, path)
            if mapping is not None:
                child.update(mapping)
            self._commit_node(child)
        else:
            # We were asked to make the root node, which already exists due to special case; we just need to give it data.
            if mapping is not None:
                current_node.update(mapping)
                self._commit_node(current_node)

    def get_node(self, path: str) -> Optional[Node]:
        """
        Return a `Node` from self based on a path, or `None` if the node does not exist.

        :param path:
        :return:
        """
        if not path.strip("/"):
            return self.get_node(self._root_path)
        return self._node_store.get(path.rstrip("/"))  # Remove a trailing slash, but not a leading one

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
        if self._node_store and self._root_path:
            print(strftime("[%H:%M:%S]"), "Finalizing...")
            self._finalize_node(self.get_node(self._root_path))
            print(strftime("[%H:%M:%S]"), "Done finalizing")
        self._node_store.close()
        print(strftime("[%H:%M:%S]"), "Closed NodeStore")

    def _finalize_node(self, node: Node) -> Mapping:
        file_children = []  # type: List[Node]
        child_mappings = []  # type: List[Mapping]
        for child_name in node.child_names:
            # For each child:
            #   finalize child
            #   update self with child's mapping (postponed until *.* is updated from self)
            #   if child is a file:
            #     update *.* with child's mapping (postponed until *.* is created)
            child_path = node.get_child_path(child_name)
            child = self.get_node(child_path)
            if not child.is_directory:
                file_children.append(child)
            child_mappings.append(self._finalize_node(child))
        if (node.mapping and node.is_directory) or file_children:
            # If this node is:
            #   - listed in mpistat
            #   - a directory
            # or:
            #   - has children that are files (and therefore is implicitly a directory)
            # then create a *.* node that is a child of this node, and has a mapping that is the
            # same as this node's mapping
            star = self._Node(is_directory=False, path=node.path + "/*.*")
            self._add_child(node, star)
            star.update(node.mapping)
            for child in file_children:
                # Add the mappings of this node's file children to this node's *.* (postponed from
                # earlier), and remove this node's file children to stop them showing up in the
                # JSON.
                star.update(child.mapping)
                self._remove_child(node, child)
            self._commit_node(star)
        for child_mapping in child_mappings:
            # Update this node with the mappings from all children
            # This must be postponed until after ``star.update(node.mapping)`` has been called.
            node.update(child_mapping)
        self._commit_node(node)
        return node.mapping

    def _commit_node(self, node: Node):
        self._node_store[node.path] = node

    def _add_child(self, node: Node, child: Node):
        node.add_child(child)

    def _remove_child(self, node: Node, child: Node):
        node.remove_child(child)
        del self._node_store[child.path]

    def format(self, path: str="/", depth: int=0) -> Dict:
        """
        Format self (or a subtree of self) for output via the API.

        :param path:
        :param depth:
        :return:
        """
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
            "data": node.mapping.format()
        }
        if depth > 0 and node.child_names:
            for child_name in node.child_names:
                child_path = node.get_child_path(child_name)
                child = self.get_node(child_path)
                child_dirs.append(self._format_node(child, depth - 1))
            rtn["child_dirs"] = child_dirs
        return rtn
