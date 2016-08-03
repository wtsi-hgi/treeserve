import unittest

from treeserve.tree_builder import TreeBuilder


class TestTreeBuilder(unittest.TestCase):
    def setUp(self):
        self.tree_builder = TreeBuilder()

    def test_from_lstat(self):
        tree = self.tree_builder.from_lstat(["../../samples/sampledata.dat.gz"])
        print(tree)
        print(tree.to_json(depth=0, path="/"))


if __name__ == '__main__':
    unittest.main()
