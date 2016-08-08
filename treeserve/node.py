from collections import deque
import json
import struct
from typing import Optional, Dict

from treeserve.mapping import Mapping


class Node:
    _node_count = 0

    def __init__(self, name: str, is_directory: bool, parent: "Node"=None):
        self._name = name
        self._node_id = Node._node_count
        Node._node_count += 1
        self._parent = parent
        if self._parent is not None:
            self._parent.add_child(self)
        self._children = {}  # type: Dict[str, int]
        self._star_node = None
        self._mapping = Mapping()
        self._is_directory = is_directory
        if self._node_id == 0:
            assert self._name == "lustre", repr(self._name) # We can assume the root node will always have id 0
        # if self.node_id in (57911,):
        #     print("BAD", self.path, self._children, self.parent, self.parent.node_id, self.is_directory)
        # if self.parent and self.parent._node_id == 57911:
        #     print("BADPARENT!", self.path, self._children, self.parent, self.parent.node_id, self._is_directory)

    def __repr__(self):
        return self.name

    @property
    def is_directory(self) -> bool:
        return self._is_directory

    @property
    def name(self) -> str:
        return self._name

    @property
    def parent(self) -> "Node":
        return self._parent

    @property
    def path(self) -> str:
        fragments = deque()
        current = self
        while current is not None:
            fragments.appendleft(current.name)
            current = current.parent
        return "/" + "/".join(fragments)

    @property
    def node_id(self):
        return self._node_id

    @classmethod
    def get_node_count(cls) -> int:
        return cls._node_count

    def update(self, mapping: Mapping):
        self._mapping.update(mapping)

    def add_child(self, node: "Node"):
        self._children[node.name] = node.node_id

    def remove_child(self, node: "Node"):
        del self._children[node.name]

    def get_child(self, name: str) -> int:
        # print(name, self._children)
        return self._children.get(name, None)

    @classmethod
    def from_id(cls, node_id: int, txn) -> "Node":
        rtn = txn.get(str(node_id).encode())
        assert rtn is not None, "Node (%s) is not in database"%node_id
        return cls.unpack(rtn, txn)

    def update_star(self, mapping: Mapping) -> "Node":
        if "*.*" not in self._children:
            self._star_node = Node("*.*", is_directory=False, parent=self)
        self._star_node.update(mapping)

    def to_json(self, depth: int, txn) -> dict:
        print("node.to_json() self", self)
        print("node.to_json() self._mapping", self._mapping)
        print("node.to_json() children", self._children)
        child_dirs = []
        json = {
            "name": self.name,
            "path": self.path,
            "data": self._mapping.to_json()
        }
        if depth > 0 and self._children:
            for name, child_id in self._children.items():
                child_dirs.append(self.from_id(child_id, txn).to_json(depth - 1, txn))
            json["child_dirs"] = child_dirs
        return json

    def pack_struct(self) -> bytes:
        if self._parent:
            parent_id = self._parent.node_id
        else:
            parent_id = 0
            print(self._name)
        name = self._name
        len_name = len(name)
        packed_map = self._mapping.pack()
        struct.pack(">LH%is"%(len_name), parent_id, len_name, name)

    def pack_json(self) -> bytes:
        if self._parent:
            parent_id = self._parent.node_id
        else:
            parent_id = None
        assert self.is_directory or self.name == "*.*"
        packed_json = {"child_ids": self._children,
                       "parent_id": parent_id,
                       "is_directory": self._is_directory,
                       "name": self._name,
                       "mapping": self._mapping.pack_json()
        }
        return json.dumps(packed_json).encode()

    @classmethod
    def from_json(cls, packed_json: bytes, txn) -> Optional["Node"]:
        print("node.from_json() packed", packed_json)
        assert packed_json is not None
        partial = json.loads(packed_json.decode())
        print("node.from_json() partial", partial["name"], partial["parent_id"])
        parent = None
        if partial["parent_id"]:
            parent = cls.from_id(partial["parent_id"], txn)
        new_node = cls(partial["name"], partial["is_directory"], parent)
        new_node._mapping = Mapping.from_json(partial["mapping"])
        new_node._children = partial["child_ids"]
        return new_node

    def pack_none(self) -> bytes:
        return bytes()

    pack = pack_json
    unpack = from_json
