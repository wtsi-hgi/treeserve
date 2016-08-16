from abc import ABCMeta, abstractmethod
from collections import MutableMapping, Iterator
import lmdb
from typing import Optional

from treeserve.node import Node, SerializableNode, JSONSerializableNode


class NodeStore(MutableMapping, metaclass=ABCMeta):
    """
    A store for `Node`s.

    This maps node paths to node objects, much like a database would.
    An example:
    {
        "/root": ...,
        "/root/somefile.txt": ...,
        "/root/somedirectory/*.*": ...
    }
    tl;dr: leading slash included, trailing slash not included.
    """

    @abstractmethod
    def __getitem__(self, path):
        pass

    @abstractmethod
    def __setitem__(self, path, node):
        pass

    @abstractmethod
    def __delitem__(self, path):
        pass

    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def __len__(self):
        pass

    @abstractmethod
    def __contains__(self, path):
        pass


class InMemoryNodeStore(NodeStore):
    """
    A store for `Node`s that keeps nodes in memory.
    """

    def __init__(self):
        self._store = {}

    def __getitem__(self, path: str) -> Node:
        return JSONSerializableNode.deserialize(self._store[path.encode()])

    def __setitem__(self, path: str, node: Node):
        self._store[path.encode()] = node.serialize()

    def __delitem__(self, path: str):
        self._store.__delitem__(path.encode())

    def __iter__(self) -> Iterator:
        return self._store.__iter__()

    def __len__(self) -> int:
        return self._store.__len__()

    def __contains__(self, path):
        return path.encode() in self._store

    def close(self):
        pass


class LMDBNodeStore(NodeStore):
    """
    A store for `Node`s that keeps nodes in LMDB.
    """

    _sentinel = object()

    def __init__(self, lmdb_dir: str, node_type: type(SerializableNode)):
        self._env = lmdb.open(lmdb_dir, map_size=1024**3)
        self._node_type = node_type

    def __getitem__(self, path: str) -> Optional[SerializableNode]:
        with self._env.begin() as txn:
            serialized = txn.get(path.encode(), default=LMDBNodeStore._sentinel)
            if serialized is LMDBNodeStore._sentinel:
                raise KeyError(path)
            else:
                return self._node_type.deserialize(serialized)

    def __setitem__(self, path: str, node: SerializableNode):
        with self._env.begin(write=True) as txn:
            txn.put(path.encode(), node.serialize())

    def __delitem__(self, path: str):
        with self._env.begin(write=True) as txn:
            txn.delete(path.encode())

    def __iter__(self):
        raise NotImplementedError

    def __len__(self) -> int:
        entries = self._env.stat()["entries"]
        print("LMDB entries:", entries)
        return entries

    def __contains__(self, path):
        with self._env.begin() as txn:
            serialized = txn.get(path.encode(), default=LMDBNodeStore._sentinel)
            return serialized is LMDBNodeStore._sentinel

    def close(self):
        pass
        # self._txn = self._env.begin()  # Reopen read-only for output
