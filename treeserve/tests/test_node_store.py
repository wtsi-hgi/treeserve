import unittest
import shutil

from treeserve.node import JSONSerializableNode
from treeserve.node_store import InMemoryNodeStore, LMDBNodeStore


class TestInMemoryNodeStore(unittest.TestCase):
    def setUp(self):
        self.node_store = InMemoryNodeStore(JSONSerializableNode)
        self.node = JSONSerializableNode(True, "/root")
        self.child = JSONSerializableNode(False, "/root/child")
        self.node.add_child(self.child)
        self.node_store["/root"] = self.node
        self.node_store["/root/child"] = self.child

    def test_set(self):
        new_child = JSONSerializableNode(False, "/root/child_2")
        self.node_store["/root/child_2"] = new_child
        self.assertIn("/root/child_2", self.node_store)

    def test_delete(self):
        del self.node_store["/root"]
        self.assertIsNone(self.node_store.get("/root"))

    def test_get(self):
        node = self.node_store["/root"]
        self.assertEqual(self.node, node)
        self.assertIsNot(self.node, node)
        none = self.node_store.get("/does/not/exist")
        self.assertIsNone(none)

    def test_contains(self):
        self.assertIn("/root", self.node_store)
        self.assertNotIn("/does/not/exist", self.node_store)


class TestLMDBNodeStore(unittest.TestCase):
    def setUp(self):
        self.lmdb_directory = "/tmp/lmdb"
        self.node_store = LMDBNodeStore(JSONSerializableNode, self.lmdb_directory)
        self.node = JSONSerializableNode(True, "/root")
        self.node_store["/root"] = self.node

    def tearDown(self):
        shutil.rmtree(self.lmdb_directory)

    def test_getitem(self):
        node = self.node_store["/root"]
        self.assertEqual(self.node, node)
        with self.assertRaises(KeyError):
            none = self.node_store["/does/not/exist"]

    def test_delete(self):
        del self.node_store["/root"]
        self.assertIsNone(self.node_store.get("/root"))
        with self.assertRaises(KeyError):
            none = self.node_store["/root"]

    def test_get(self):
        node = self.node_store.get("/root")
        self.assertEqual(self.node, node)
        none = self.node_store.get("/does/not/exist")
        self.assertIsNone(none)

    def test_contains(self):
        self.assertIn("/root", self.node_store)
        self.assertNotIn("/does/not/exist", self.node_store)

if __name__ == '__main__':
    unittest.main()
