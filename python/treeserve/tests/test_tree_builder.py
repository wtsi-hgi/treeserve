import unittest
from nose_parameterized import parameterized
import json
import logging
import shutil
from sys import stdout

from treeserve.node_store import InMemoryNodeStore, LMDBNodeStore, InMemoryLMDBNodeStore
from treeserve.node import JSONSerializableNode, PickleSerializableNode, StructSerializableNode
from treeserve.tree_builder import TreeBuilder
from treeserve.tree import Tree


class TestTreeBuilder(unittest.TestCase):
    lmdb_directory = "/tmp/test_tree_builder_lmdb"

    @classmethod
    def setUpClass(cls):
        logger = logging.getLogger("treeserve")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(stdout)
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(fmt="%(levelname)-8s | %(asctime)s | %(name)s: %(message)s",
                                      datefmt="%H:%M:%S")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    def setUp(self):
        shutil.rmtree(TestTreeBuilder.lmdb_directory, ignore_errors=True)

    def tearDown(self):
        shutil.rmtree(TestTreeBuilder.lmdb_directory, ignore_errors=True)

    @parameterized.expand([
        (InMemoryNodeStore, [PickleSerializableNode]),
        (InMemoryNodeStore, [JSONSerializableNode]),
        (InMemoryNodeStore, [StructSerializableNode]),
        (LMDBNodeStore, [PickleSerializableNode, lmdb_directory]),
        (LMDBNodeStore, [JSONSerializableNode, lmdb_directory]),
        (LMDBNodeStore, [StructSerializableNode, lmdb_directory])
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

    @parameterized.expand([
        (InMemoryNodeStore, [PickleSerializableNode]),
        (InMemoryLMDBNodeStore, [PickleSerializableNode, lmdb_directory]),
        (LMDBNodeStore, [PickleSerializableNode, lmdb_directory])
    ])
    def test_real_data(self, node_store_type, builder):
        logging.getLogger("treeserve").setLevel(logging.INFO)
        node_store = node_store_type(*builder)
        tree = Tree(node_store)
        tree_builder = TreeBuilder(tree)
        tree_builder.from_lstat(["../../samples/sampledata.dat.gz"], now=1470299913).format()


if __name__ == '__main__':
    unittest.main()
