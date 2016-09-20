import unittest

from treeserve.node import Node, JSONSerializableNode
from treeserve.node_store import InMemoryNodeStore
from treeserve.tree import Tree


class TestTree(unittest.TestCase):
    def setUp(self):
        self.tree = Tree(InMemoryNodeStore(JSONSerializableNode))

    def test_add_node(self):
        self.tree.add_node("/root", True)
        self.assertIsNotNone(self.tree.get_node("/root").path)
        self.tree.add_node("/root/a/b/c", True)
        self.assertIsNotNone(self.tree.get_node("/root/a/b/c"))
        self.tree.add_node("/root/foo/bar/baz.txt", False)
        self.assertIsNotNone(self.tree.get_node("/root/foo/bar/baz.txt"))


if __name__ == '__main__':
    unittest.main()
