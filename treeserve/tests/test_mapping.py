import unittest

from treeserve.mapping import Mapping, DictSerializableMapping


class TestMapping(unittest.TestCase):
    def setUp(self):
        self.mapping = Mapping()
        self.mapping.set("size", "group", "user", "file", 42)
        self.mapping.set("count", "group", "user2", "directory", 10)
        self.other_mapping = Mapping()
        self.other_mapping.set("size", "group", "user", "file", 21)

    def test_update(self):
        correct = Mapping()
        correct.set("size", "group", "user", "file", 63)
        correct.set("count", "group", "user2", "directory", 10)
        self.mapping.update(self.other_mapping)
        self.assertEqual(correct, self.mapping)

    def test_subtract(self):
        correct = Mapping()
        correct.set("size", "group", "user", "file", 21)
        correct.set("count", "group", "user2", "directory", 10)
        self.mapping.subtract(self.other_mapping)
        self.assertEqual(correct, self.mapping)
        self.mapping.subtract(correct)
        self.assertEqual(Mapping(), self.mapping)

    def test_format(self):
        correct = {
            "size": {
                "group": {
                    "user": {
                        "file": "42"
                    },
                    "*": {
                        "file": "42"
                    }
                },
                "*": {
                    "user": {
                        "file": "42"
                    },
                    "*": {
                        "file": "42"
                    }
                }
            },
            "count": {
                "group": {
                    "user2": {
                        "directory": "10"
                    },
                    "*": {
                        "directory": "10"
                    }
                },
                "*": {
                    "user2": {
                        "directory": "10"
                    },
                    "*": {
                        "directory": "10"
                    }
                }
            }
        }
        self.assertEqual(correct, self.mapping.format(set()))


class TestDictSerializableMapping(unittest.TestCase):
    def setUp(self):
        self.mapping = DictSerializableMapping()
        self.mapping.set("size", "group", "user", "file", 42)
        self.mapping.set("size", "group", "user_2", "file", 7)

    def test_serialize_deserialize(self):
        self.assertEqual(self.mapping, DictSerializableMapping.deserialize(self.mapping.serialize()))


if __name__ == '__main__':
    unittest.main()
