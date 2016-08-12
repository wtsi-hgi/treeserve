import unittest

from treeserve.node import Node, JSONSerializableNode


class TestJSONSerializableNode(unittest.TestCase):
    def setUp(self):
        self.node = JSONSerializableNode(True, "/root")

    def test_serialize_deserialize(self):
        self.assertEqual(self.node, JSONSerializableNode.deserialize(self.node.serialize()))


if __name__ == '__main__':
    unittest.main()
