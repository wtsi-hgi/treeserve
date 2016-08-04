import unittest
import json

from treeserve.tree_builder import TreeBuilder


class TestTreeBuilder(unittest.TestCase):
    def setUp(self):
        self.tree_builder = TreeBuilder()

    def test_from_lstat(self):
        tree = self.tree_builder.from_lstat(["../../samples/minimaldata.dat.gz"])
        out = tree.to_json(depth=0, path="/")
        print(json.dumps(out))



if __name__ == '__main__':
    unittest.main()
