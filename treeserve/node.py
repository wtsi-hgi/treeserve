from copy import deepcopy

from treeserve.mapping import Mapping


class Node:
    _node_count = 0

    def __init__(self, name: str, parent=None):
        self._name = name
        self._parent = parent
        self._node_count += 1
        if self._parent is not None:
            self._depth = self._parent.depth + 1
            self._parent.add_child(self)
        else:
            self._depth = 0
        self._children = {}
        self._mapping = Mapping()

    @property
    def depth(self):
        return self._depth

    @property
    def name(self):
        return self._name

    @property
    def parent(self):
        return self._parent

    @property
    def path(self):
        fragments = []
        current = self
        while current is not None:
            fragments.append(current.name)
            current = current.parent
        return "/" + "/".join(fragments)

    @classmethod
    def get_node_count(cls):
        return cls._node_count

    def combine(self, mapping: Mapping):
        self._mapping.combine_with(mapping)

    def add_child(self, node):
        self._children[node.name] = node

    def get_child(self, name: str):
        return self._children.get(name, None)

    def finalize(self):
        # cloned_data = self._mapping.copy()
        cloned_data = deepcopy(self._mapping)
        assert cloned_data is not None
        print('cloned data:\n', cloned_data)
        if self._children:
            assert cloned_data is not None, "aaargh"
            print('children', self._children)
            for child in self._children.values():
                print('type of cloned_data before finalizing:\n', cloned_data)
                child.finalize()
                print('type of cloned_data:\n', type(cloned_data))
                print('actual cloned data:\n', cloned_data)
                print('type of child._mapping:\n', type(child._mapping))
                cloned_data -= child._mapping
        if cloned_data:
            # If not all data in self._mapping was due to child directories:
            child = Node("*.*", parent=self)
            child.combine(cloned_data)  # Add the remaining data to child
            self.add_child(child)

    def to_json(self, depth: int):
        child_dirs = []
        json = {
            "name": self.name,
            "path": self.path,
            "data": self._mapping.to_json()
        }
        if depth > 0 and self._children:
            for name, child in self._children.items():
                child_dirs.append(child.to_json(depth-1))
            json["child_dirs"] = child_dirs
        return json
