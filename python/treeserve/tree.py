from collections.abc import Sized
import logging
from typing import Dict, List, Optional, Set

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
        self.logger = logging.getLogger(__name__)

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
            self.logger.debug("Special-casing the root node")
            # Special-case root node creation, since it doesn't have a parent. It also won't
            # necessarily appear in mpistats, and needs to be accessible from the API as "/".
            self._root_path = "/" + split_path[0]
            self._commit_node(self._Node(is_directory=True, path=self._root_path))
            self._node_store._root_path = self._root_path
            assert self.get_node(self._root_path) is not None
        # The first path component should always be the name of the root node.
        assert split_path[0] == self._root_path.lstrip("/"), (split_path[0], self._root_path)
        current_node = None
        path_stack = [""]  # The empty element at the start causes "/".join to insert a slash at the start of the string.
        self.logger.debug("Starting walk...")
        for fragment in split_path[:-1]:
            # Create *parent* nodes of the node given to us
            path_stack.append(fragment)
            current_path = "/".join(path_stack)
            self.logger.debug("Walked to %r", current_path)
            if current_path in self._node_store:
                # This should always happen at least once, since the initial path fragment should
                # always be the same (e.g. "/root").
                current_node = self.get_node("/".join(path_stack))
                self.logger.debug("Got existing node %s", current_node)
            else:
                assert current_node is not None
                # Current node doesn't exist, so create it.
                self.logger.debug("Inferred existence of node at %r", current_path)
                # Store the parent of the soon-to-be-current_node
                parent_node = current_node
                current_node = self._Node(True, path="/".join(path_stack))
                self._add_child(parent_node, current_node)
                self._commit_node(parent_node)
                self._commit_node(current_node)
            assert current_path in self._node_store
            assert current_node.path == current_path
        if current_node is None:
            # Should only happen for root node, - it has only one path fragment (split_path[:-1] is []).
            current_node = self.get_node(self._root_path)
            self.logger.debug("Got root node %s at path %r", current_node, self._root_path)
        assert current_node is not None
        # Nasty hack to work around the fact that the root node is special - by the time we get
        # here for the root node, we've already created it.
        if path != self._root_path:
            # Make the node we were actually asked to make, add it to the tree and give it data.
            self.logger.debug("Creating node at %s...", path)
            child = self._Node(is_directory, path)
            if mapping is not None:
                self.logger.debug("Updating node %s with mapping...", child)
                child.update(mapping)
            self._add_child(current_node, child)
            self._commit_node(child)
            self._commit_node(current_node)
            self.logger.debug("Created final node %s", child)
        else:
            self.logger.debug("Updating the root node %s with mapping...", current_node)
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
        - adding the data from the directories in each directory to that directory

        .. [1] The reason for this is that typically a directory will take up some space on disk,
           but its data fields are full of data about its child directories; the best place for
           information about the directory itself is therefore with the information about the files
           the directory contains.

        .. [2] This is done to avoid the files turning up in the output from the API.

        :return:
        """
        if self._node_store and self._root_path:
            self.logger.info("Finalizing...")
            self._finalize_node(self.get_node(self._root_path))
            self.logger.info("Done finalizing")
        else:
            self.logger.error("Not finalizing!")
            self.logger.debug("bool(self._node_store) is %s", bool(self._node_store))
            self.logger.debug("bool(self._root_path) is %s", bool(self._root_path))
        self._node_store.close()
        self.logger.info("Closed node store")

    def _finalize_node(self, node: Node) -> Mapping:
        """
        Recursively prepare nodes for output via the API.

        See documentation for Tree.finalize() for details.

        :param node:
        :return:
        """
        self.logger.debug("Finalizing node %s", node)
        file_children = []  # type: List[Node]
        child_mappings = []  # type: List[Mapping]
        for child_name in node.child_names:
            # For each child:
            #   finalize child
            #   if child is a file:
            #     update *.* with child's mapping (postponed until *.* is created)
            #   update self with child's mapping (postponed until *.* is updated from self)
            child_path = node.get_child_path(child_name)
            child = self.get_node(child_path)
            if not child.is_directory:
                self.logger.debug("Child node %s is a file", child)
                file_children.append(child)
            self.logger.debug("Postponing node update using node %s", child)
            child_mappings.append(self._finalize_node(child))
        if (node.mapping and node.is_directory) or file_children:
            # If this node is:
            #   - listed in mpistat, and
            #   - a directory
            # or:
            #   - has children that are files (and therefore is implicitly a directory)
            #     (NB: don't create *.* if the node only has directory children, as they should not
            #     be used to calculate the metrics for *.*)
            # then create a *.* node that is a child of this node, and has a mapping that is the
            # same as this node's mapping + file children's mappings
            self.logger.debug("Creating *.* node for node %s", node)
            star = self._Node(is_directory=False, path=node.path + "/*.*")
            self._add_child(node, star)
            star.update(node.mapping)
            for child in file_children:
                # Add the mappings of this node's file children to this node's *.* (postponed from
                # earlier), and remove this node's file children to stop them showing up in the
                # JSON.
                self.logger.debug("Updating *.* node of node %s with mapping from node %s", node, child)
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
        """
        Ensure that the given node is in the NodeStore.

        :param node:
        :return:
        """
        self._node_store[node.path] = node

    def _add_child(self, node: Node, child: Node):
        """
        Add a child to a node.

        :param node:
        :param child:
        :return:
        """
        node.add_child(child)

    def _remove_child(self, node: Node, child: Node):
        """
        Remove a child from a node.

        Raises KeyError if the child is not a child of the given node.

        :param node:
        :param child:
        :return:
        """
        node.remove_child(child)
        del self._node_store[child.path]

    def format(self, path: str="/", depth: int=0, whitelist: Set[str]=set()) -> Dict:
        """
        Format self (or a subtree of self) for output via the API.

        :param path:
        :param depth:
        :param whitelist:
        :return:
        """
        node = self.get_node(path)
        if node is None:
            return {}
        else:
            return self._format_node(node, depth + 1, whitelist)

    def _format_node(self, node: Node, depth: int, whitelist: Set[str]) -> Dict:
        """
        Format self for output via the API.

        :param depth:
        :return:
        """
        child_dirs = []
        rtn = {
            "name": node.name,
            "path": node.path,
            "data": node.mapping.format(whitelist)
        }
        if depth > 0 and node.child_names:
            for child_name in node.child_names:
                child_path = node.get_child_path(child_name)
                child = self.get_node(child_path)
                child_dirs.append(self._format_node(child, depth - 1, whitelist))
            rtn["child_dirs"] = child_dirs
        return rtn
