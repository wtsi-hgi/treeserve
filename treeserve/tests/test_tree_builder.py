import unittest
import json
import shutil

from treeserve.node_store import InMemoryNodeStore, LMDBNodeStore
from treeserve.node import JSONSerializableNode
from treeserve.tree_builder import TreeBuilder
from treeserve.tree import Tree


class TestTreeBuilder(unittest.TestCase):
    def setUp(self):
        self.lmdb_directory = "/tmp/test_tree_builder_lmdb"
        node = JSONSerializableNode
        # node_store = InMemoryNodeStore()
        node_store = LMDBNodeStore(self.lmdb_directory, node)
        tree = Tree(node_store)
        self.tree_builder = TreeBuilder(tree)

    def tearDown(self):
        shutil.rmtree(self.lmdb_directory, ignore_errors=True)

    def test_correct_output(self):
        tree = self.tree_builder.from_lstat(["../../samples/test_minimal.dat.gz"], now=1470299913)
        out = tree.format(depth=0, path="/")

        with open("test_minimal.json") as file:
            correct = json.load(file)
        self.maxDiff = None
        self.assertEqual(correct, json.loads(json.dumps(out)))

    def test_real_data(self):
        self.tree_builder.from_lstat(["../../samples/sampledata.dat.gz"], now=1470299913).format()


if __name__ == '__main__':
    unittest.main()
