import unittest
import json

from treeserve.tree_builder import TreeBuilder
from treeserve.tree import InMemoryTree


class TestTreeBuilder(unittest.TestCase):
    def setUp(self):
        self.tree_builder = TreeBuilder(InMemoryTree())

    def test_from_lstat(self):
        tree = self.tree_builder.from_lstat(["../../samples/test_minimal.dat.gz"], now=1470299913)
        out = tree.format(depth=0, path="/")

        with open("test_minimal.json") as file:
            correct = json.load(file)

        self.assertEqual(correct, out)


if __name__ == '__main__':
    unittest.main()
