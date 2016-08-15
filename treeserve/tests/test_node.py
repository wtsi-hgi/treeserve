import unittest

from treeserve.node import Node, JSONSerializableNode


class TestJSONSerializableNode(unittest.TestCase):
    def setUp(self):
        self.node = JSONSerializableNode(True, "/root")
        self.node.mapping.set("size", "group", "user", "file", 42)
        self.node.mapping.set("size", "group", "user_2", "file_2", 7)
        self.child = JSONSerializableNode(False, "/root/test_file")
        self.node.add_child(self.child)
        self.child.mapping.set("size", "group", "user", "file_3", 66)

    def test_serialize_deserialize(self):
        copied_node = JSONSerializableNode.deserialize(self.node.serialize())
        self.assertEqual(self.node, copied_node)
        self.assertEqual(self.child, JSONSerializableNode.deserialize(self.child.serialize()))
        self.assertEqual(self.node.child_names, copied_node.child_names)
        self.assertEqual(self.node.is_directory, copied_node.is_directory)
        self.assertEqual(self.node.name, copied_node.name)
        self.assertEqual(self.node.mapping, copied_node.mapping)
        self.assertEqual(self.node.path, copied_node.path)


if __name__ == '__main__':
    unittest.main()
