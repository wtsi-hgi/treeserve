from typing import Dict, List

from treeserve.mapping import Mapping
from treeserve.node import Node


class Tree:
    def __init__(self):
        self.alive_nodes = []  # type: List[Node]

    def add_node(self, path: str, is_directory: bool, mapping: Mapping, txn) -> Node:
        split_path = path.strip("/").split("/")  # e.g. "/lustre/scratch115/teams" -> ["lustre", "scratch115", "teams"]

        if self.alive_nodes and self.alive_nodes[-1].node_id == 137597 or Node.get_node_count() == 137597:
            print(str(self.alive_nodes[-1].node_id).encode())

        for i, path in enumerate(split_path):
            if len(self.alive_nodes) <= i:
                # If not all of this node's parents exist:
                if i == len(split_path) - 1:
                    # If we're at the end of the path:
                    # Add child
                    self.alive_nodes.append(Node(path, is_directory=is_directory, parent=self.alive_nodes[-1]))
                else:
                    # Not yet at the end of the path
                    # Add parent
                    parent = None
                    if self.alive_nodes:
                        # Not root, has a parent
                        parent = self.alive_nodes[-1]
                    self.alive_nodes.append(Node(path, is_directory=True, parent=parent))
                    self.write_node(self.alive_nodes[-1], txn)
            elif self.alive_nodes[i].name != path:
                # If we've walked back up the tree (current directory has changed upwards):
                self.alive_nodes = self.alive_nodes[:i]  # Trim excess nodes
                if i == len(split_path) - 1:
                    # If we're at the end of the path:
                    # Add child
                    self.alive_nodes.append(Node(path, is_directory=is_directory, parent=self.alive_nodes[-1]))
                else:
                    # Not yet at the end of the path
                    # Add parent
                    self.alive_nodes.append(Node(path, is_directory=True, parent=self.alive_nodes[-1]))
                    self.write_node(self.alive_nodes[-1], txn)
            if len(self.alive_nodes) > len(split_path) == i + 1:
                # Why does this happen? (only happens twice) - possibly when you move from a child directory to the parent directory
                self.alive_nodes.pop()

        # Check if alive nodes and path are in sync
        assert self.alive_nodes[-1].is_directory == is_directory
        if [node.name for node in self.alive_nodes] != split_path:
            print(self.alive_nodes)
            print(split_path)
            assert False, "Alive nodes aren't the same as the path"

        self.alive_nodes[-1].update(mapping)

        # Alive nodes and path are in sync at this point
        if is_directory:
            # Update directory *.* with self
            self.alive_nodes[-1].update_star(mapping)
            # Put the directory and its star node in the db
            self.write_node(self.alive_nodes[-1], txn)
            self.write_node(self.alive_nodes[-1]._star_node, txn)
            # Assume /lustre never turns up in mpistats...
            self.alive_nodes[-2].update(mapping)
        else:
            # Update parent's *.* entry
            self.alive_nodes[-2].update_star(mapping)
            del self.alive_nodes[-2]._children[self.alive_nodes[-1].name]

    def add_lustre(self, txn):
        # Lustre isn't in mpistats. Hacky workaround
        txn.put(b'0', self.alive_nodes[0].pack())

    def write_node(self, node: Node, txn) -> bool:
        return txn.put(str(node.node_id).encode(), node.pack())

    def get_node_at(self, path: str) -> Node:
        split_path = path.strip("/").split("/")
        current_node = self.alive_nodes[0]
        for fragment in split_path[1:]:
            current_node = current_node.get_child(fragment)
            if current_node is None:
                # If there is no node with the given path:
                return None
        return current_node

    def to_json(self, *, path: str, depth: int, txn) -> Dict:
        node = self.get_node_at(path)

        if node is None:
            return {}
        else:
            return node.to_json(depth+1, txn)
