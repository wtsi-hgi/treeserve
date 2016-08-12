import unittest

from treeserve.node import JSONSerializableNode
from treeserve.node_store import InMemoryNodeStore, LMDBNodeStore


class TestInMemoryNodeStore(unittest.TestCase):
    def setUp(self):
        self.node_store = InMemoryNodeStore()
        self.node = JSONSerializableNode(True, "/root")
        self.node_store["/root"] = self.node

    def test_get_set(self):
        new_node = self.node_store["/root"]
        self.assertEqual(self.node, new_node)

    def test_delete(self):
        del self.node_store["/root"]
        self.assertIsNone(self.node_store.get("/root"))

    def test_get(self):
        node = self.node_store["/root"]
        self.assertEqual(self.node, node)
        none = self.node_store.get("/does/not/exist")
        self.assertIsNone(none)


class TestLMDBNodeStore(unittest.TestCase):
    def setUp(self):
        self.node_store = LMDBNodeStore("/tmp/lmdb", JSONSerializableNode)
        self.node = JSONSerializableNode(True, "/root")
        self.node_store["/root"] = self.node

    def test_getitem(self):
        node = self.node_store["/root"]
        self.assertEqual(self.node, node)

    def test_delete(self):
        del self.node_store["/root"]
        self.assertIsNone(self.node_store.get("/root"))

    def test_get(self):
        node = self.node_store["/root"]
        self.assertEqual(self.node, node)
        none = self.node_store.get("/does/not/exist")
        self.assertIsNone(none)


if __name__ == '__main__':
    unittest.main()
