from abc import ABCMeta, abstractmethod
from collections.abc import MutableMapping, Container
from collections import OrderedDict
import lmdb
from time import strftime
from typing import Optional, Iterator, Tuple, Any, List
from sys import platform

from treeserve.node import Node, SerializableNode, JSONSerializableNode


class NodeStore(MutableMapping, Container, metaclass=ABCMeta):
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

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._node_type.__name__)

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

    @property
    @abstractmethod
    def _root_path(self):
        pass

    @_root_path.setter
    @abstractmethod
    def _root_path(self, value):
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
        return self._store[path]

    def __setitem__(self, path: str, node: Node):
        self._store[path] = node

    def __delitem__(self, path: str):
        self._store.__delitem__(path)

    def __iter__(self) -> Iterator:
        return self._store.__iter__()

    def __len__(self) -> int:
        return self._store.__len__()

    def __contains__(self, path: str) -> bool:
        return path in self._store

    def close(self):
        pass

    @property
    def _root_path(self):
        raise NotImplementedError

    @_root_path.setter
    def _root_path(self, value):
        pass


class InMemoryLMDBNodeStore(InMemoryNodeStore):
    """
    A store for `Node`s that keeps nodes in memory.
    """
    max_txn_size = 1000000

    def __init__(self, node_type: type(SerializableNode), lmdb_dir: str):
        super().__init__(node_type)
        self.lmdb_dir = lmdb_dir

    def close(self):
        not_macos = platform != "darwin"  # OS X doesn't support sparse files, so these just break things24 ** 3, writemap=not_macos,
        self._env = lmdb.open(self.lmdb_dir, map_size=50 * 1024 ** 3, writemap=not_macos,
                              map_async=not_macos)
        for i, (path, node) in enumerate(self._store.items()):
            if i % self.max_txn_size == 0:
                print("Starting/Committing")
                try:
                    self._txn.commit()
                except AttributeError as e:
                    print(e)
                self._txn = lmdb.Transaction(self._env, write=True, buffers=self._node_type.uses_buffers())
            if i%10000==0:
                print(i)
            self._txn.put(path.encode(), node.serialize())
        self._txn.put(b'_root_path', b'/lustre')
        print("Finished dumping to DB")
        self._txn.commit()
        del self._store


class LMDBNodeStore(NodeStore):
    """
    A store for `Node`s that keeps nodes in LMDB.
    """

    _sentinel = object()
    # LMDB apparently has a maximum transaction size - it's unclear what it is, so we assume that
    # committing every million operations will work.
    max_txn_size = 1000000

    def __init__(self, node_type: type(SerializableNode), lmdb_dir: str, cache_size: int=4):
        super().__init__(node_type)
        self.lmdb_dir = lmdb_dir
        # writemap=True and map_async=True increase speed slightly
        not_macos = platform != "darwin"  # OS X doesn't support sparse files, so these just break things
        self._env = lmdb.open(self.lmdb_dir, map_size=50*1024**3, writemap=not_macos, map_async=not_macos)
        self._txn = lmdb.Transaction(self._env, write=True, buffers=node_type.uses_buffers())
        self._last_get = (None, None)  # type: Tuple[str, Node]
        self._set_cache = FIFOCache(cache_size)
        self.current_txn_size = 0

    def __repr__(self):
        return "{}({}, {}, {})".format(self.__class__.__name__, self._node_type.__name__, repr(self.lmdb_dir), self._set_cache.max_size)

    def __getitem__(self, path: str) -> Optional[SerializableNode]:
        if path == self._last_get[0]:
            return self._last_get[1]
        if path in self._set_cache:
            return self._set_cache[path]
        serialized = self._txn.get(path.encode(), default=LMDBNodeStore._sentinel)
        if serialized is LMDBNodeStore._sentinel:
            raise KeyError(path)
        else:
            rtn = self._node_type.deserialize(path, serialized)
            self._last_get = (path, rtn)
            return rtn

    def __setitem__(self, path: str, node: SerializableNode):
        if path == self._last_get[0]:
            self._last_get = path, node
        for path, node in self._set_cache.add(path, node):
            self._txn_inc_commit()
            self._txn.put(path.encode(), node.serialize())

    def __delitem__(self, path: str):
        self._txn_inc_commit()
        self._txn.delete(path.encode())
        if path in self._set_cache:
            del self._set_cache[path]

    def __iter__(self):
        raise NotImplementedError

    def __len__(self) -> int:
        # self._txn is not yet committed, so self._env.stat() will return different (old) data.
        entries = self._txn.stat()["entries"] + len(self._set_cache)
        return entries

    def __contains__(self, path: str) -> bool:
        if path in self._set_cache:
            return True
        serialized = self._txn.get(path.encode(), default=LMDBNodeStore._sentinel)
        return serialized is not LMDBNodeStore._sentinel

    def _txn_inc_commit(self):
        self.current_txn_size += 1
        if self.current_txn_size >= LMDBNodeStore.max_txn_size:
            print(strftime("[%H:%M:%S]"), "Committing current transaction")
            self._commit(write=True)
            self.current_txn_size = 0

    def _commit(self, write: bool):
        for path, node in self._set_cache.items():
            self._txn.put(path.encode(), node.serialize())
        self._txn.commit()
        self._txn = lmdb.Transaction(self._env, write=write, buffers=self.node_type.uses_buffers())

    def close(self):
        self._commit(write=False)
        self._set_cache.clear()

    @property
    def _root_path(self):
        return bytes(self._txn.get(b'_root_path')).decode()

    @_root_path.setter
    def _root_path(self, value):
        self._txn.put(b'_root_path', value.encode())


class FIFOCache(OrderedDict):
    """
    A cache of configurable size where the least-recently accessed items are removed.
    """

    def __init__(self, max_size: int):
        self.max_size = max_size
        super().__init__()

    def __setitem__(self, key, value):
        if key in self:
            del self[key]
        super().__setitem__(key, value)

    def add(self, key, value) -> List[Tuple[Any, Any]]:
        self[key] = value
        rtn = []
        while len(self) > self.max_size:
            rtn.append(self.popitem(last=False))
        return rtn
