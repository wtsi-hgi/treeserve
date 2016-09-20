import shutil
import unittest
from abc import ABCMeta
from tempfile import mkdtemp

from treeserve.node import JSONSerializableNode
from treeserve.node_store import LMDBNodeStore, NodeStore, InMemoryNodeStore


class _TestNodeStore(unittest.TestCase, metaclass=ABCMeta):
    """
    Tests for `NodeStore`.
    """
    def __init__(self, node_store: NodeStore, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.node_store = node_store
        self.node = JSONSerializableNode(False, "/1/2")

    def test_get_when_not_exists(self):
        self.assertRaises(KeyError, self.node_store.__getitem__, "/other")

    def test_set(self):
        self.node_store[self.node.path] = self.node
        self.assertEqual(self.node, self.node_store[self.node.path])

    def test_contains_when_not_present(self):
        self.assertNotIn("/other", self.node_store)

    def test_contains_when_present(self):
        self.node_store[self.node.path] = self.node
        self.assertIn(self.node.path, self.node_store)

    def test_delete(self):
        self.node_store[self.node.path] = self.node
        assert self.node.path in self.node_store
        del self.node_store[self.node.path]
        self.assertNotIn(self.node.path, self.node_store)


class TestInMemoryNodeStore(_TestNodeStore):
    """
    Tests for `InMemoryNodeStore`.
    """
    def __init__(self, *args, **kwargs):
        node_store = InMemoryNodeStore(JSONSerializableNode)
        super().__init__(node_store, *args, **kwargs)


class TestLMDBNodeStore(_TestNodeStore):
    """
    Tests for `LMDBNodeStore`.
    """
    _DATABASE_SIZE = 1024 * 1024 * 10

    def __init__(self, *args, **kwargs):
        self._lmdb_directory = mkdtemp()
        node_store = LMDBNodeStore(
            JSONSerializableNode, self._lmdb_directory, max_size=TestLMDBNodeStore._DATABASE_SIZE)
        super().__init__(node_store, *args, **kwargs)

    def tearDown(self):
        shutil.rmtree(self._lmdb_directory)


# Stop unittest from running the base class as a test
del _TestNodeStore


if __name__ == "__main__":
    unittest.main()
