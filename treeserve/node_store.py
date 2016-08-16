from abc import ABCMeta, abstractmethod
from collections import MutableMapping
import lmdb
from typing import Optional, Iterator

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
    def __init__(self, node_type: type(Node)):
        self._node_type = node_type

    @property
    def node_type(self) -> type(Node):
        return self._node_type

    @abstractmethod
    def __getitem__(self, path: str) -> Node:
        pass

    @abstractmethod
    def __setitem__(self, path: str, node: Node):
        pass

    @abstractmethod
    def __delitem__(self, path: str):
        pass

    @abstractmethod
    def __iter__(self) -> Iterator:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    @abstractmethod
    def __contains__(self, path: str) -> bool:
        pass

    def __bool__(self) -> bool:
        return bool(len(self))

    @abstractmethod
    def close(self):
        pass


class InMemoryNodeStore(NodeStore):
    """
    A store for `Node`s that keeps nodes in memory.
    """

    def __init__(self, node_type: type(Node)):
        super().__init__(node_type)
        self._store = {}

    @property
    def node_type(self) -> type(Node):
        return self._node_type

    def __getitem__(self, path: str) -> Node:
        return self._node_type.deserialize(self._store[path.encode()])

    def __setitem__(self, path: str, node: Node):
        self._store[path.encode()] = node.serialize()

    def __delitem__(self, path: str):
        self._store.__delitem__(path.encode())

    def __iter__(self) -> Iterator:
        return self._store.__iter__()

    def __len__(self) -> int:
        return self._store.__len__()

    def __contains__(self, path: str) -> bool:
        return path.encode() in self._store

    def close(self):
        pass


class LMDBNodeStore(NodeStore):
    """
    A store for `Node`s that keeps nodes in LMDB.
    """

    _sentinel = object()

    def __init__(self, node_type: type(SerializableNode), lmdb_dir: str):
        super().__init__(node_type)
        self._env = lmdb.open(lmdb_dir, map_size=1024**3)
        self._txn = lmdb.Transaction(self._env, write=True, buffers=node_type.uses_buffers())

    def __getitem__(self, path: str) -> Optional[SerializableNode]:
        serialized = self._txn.get(path.encode(), default=LMDBNodeStore._sentinel)
        if serialized is LMDBNodeStore._sentinel:
            raise KeyError(path)
        else:
            return self._node_type.deserialize(path, serialized)

    def __setitem__(self, path: str, node: SerializableNode):
        self._txn.put(path.encode(), node.serialize())

    def __delitem__(self, path: str):
        self._txn.delete(path.encode())

    def __iter__(self):
        raise NotImplementedError

    def __len__(self) -> int:
        # self._txn is not yet committed, so self._env.stat() will return different (old) data.
        entries = self._txn.stat()["entries"]
        print("LMDB entries:", entries)
        return entries

    def __contains__(self, path: str) -> bool:
        serialized = self._txn.get(path.encode(), default=LMDBNodeStore._sentinel)
        return serialized is LMDBNodeStore._sentinel

    def close(self):
        self._txn.commit()
        self._txn = lmdb.Transaction(self._env)  # Reopen transaction read-only for output.
