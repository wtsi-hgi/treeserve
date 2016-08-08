import unittest
import json
import lmdb

from treeserve.tree_builder import TreeBuilder


class TestTreeBuilder(unittest.TestCase):
    def setUp(self):
        self.tree_builder = TreeBuilder()
        self.env = lmdb.open(".test_lmdb")

    def test_from_lstat(self):
        tree = self.tree_builder.from_lstat(["../../samples/sampledata_sorted.dat.gz"], now=1470299913)
        with self.env.begin(write=True) as txn:
            out = tree.to_json(depth=0, path="/", txn=txn)

        with open("test_minimal.json") as file:
            correct = json.load(file)
        self.maxDiff = None
        print(out)
        self.assertEqual(correct, json.loads(json.dumps(out)))


if __name__ == '__main__':
           unittest.main()
