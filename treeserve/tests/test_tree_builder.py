import unittest
from nose_parameterized import parameterized
import json
import shutil

from treeserve.node_store import InMemoryNodeStore, LMDBNodeStore
from treeserve.node import JSONSerializableNode, PickleSerializableNode, StructSerializableNode
from treeserve.tree_builder import TreeBuilder
from treeserve.tree import Tree


class TestTreeBuilder(unittest.TestCase):
    lmdb_directory = "/tmp/test_tree_builder_lmdb"

    def tearDown(self):
        shutil.rmtree(TestTreeBuilder.lmdb_directory, ignore_errors=True)

    @parameterized.expand([
        (InMemoryNodeStore, [PickleSerializableNode]),
        (InMemoryNodeStore, [JSONSerializableNode]),
        (LMDBNodeStore, [PickleSerializableNode, lmdb_directory]),
        (LMDBNodeStore, [JSONSerializableNode, lmdb_directory]),
        (LMDBNodeStore, [PickleSerializableNode, lmdb_directory, 10]),
        (LMDBNodeStore, [JSONSerializableNode, lmdb_directory, 10]),
    ])
    def test_correct_output(self, node_store_type, builder):
        node_store = node_store_type(*builder)
        print(node_store)
        tree = Tree(node_store)
        tree_builder = TreeBuilder(tree)
        tree = tree_builder.from_lstat(["../../samples/test_minimal.dat.gz"], now=1470299913)
        out = tree.format(depth=0, path="/")

        with open("test_minimal.json") as file:
            correct = json.load(file)
        self.maxDiff = None
        self.assertEqual(correct, json.loads(json.dumps(out)))

    def test_real_data(self):
        node_store = LMDBNodeStore(PickleSerializableNode, TestTreeBuilder.lmdb_directory)
        tree = Tree(node_store)
        tree_builder = TreeBuilder(tree)
        tree_builder.from_lstat(["../../samples/sampledata.dat.gz"], now=1470299913).format()


if __name__ == '__main__':
    unittest.main()
