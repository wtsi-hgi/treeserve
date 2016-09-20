from abc import ABCMeta
from collections.abc import MutableMapping, Container
from typing import Dict, Optional, Iterator

import lmdb

from treeserve.node import Node, SerializableNode


class NodeStore(MutableMapping, Container, metaclass=ABCMeta):
    """
    A store for `Node`s.
    """
    def __init__(self, node_type: type):
        self.node_type = node_type

    def __bool__(self) -> bool:
        # TODO: Not sure why this function exists?
        return bool(len(self))

    def close(self):
        """
        Closes this node store.
        """

    def _root_path(self) -> str:
        """
        All NodeStores must be capable of storing and retrieving the path of the root node. This is
        so that the Tree that owns the NodeStore always has an entry-point into the tree (once a
        reference to the root node is obtained, other nodes can be discovered by walking the tree).
        :return:
        """
        # FIXME: Hardcoded to "/" because that is, after all, the root node
        return "/"

    # TODO: Given that the node's path is stored in the node, it would be useful to just `add` a node!


class InMemoryNodeStore(NodeStore):
    """
    A store for `Node`s that keeps nodes in memory.
    """
    def __init__(self, node_type: type):
        super().__init__(node_type)
        self._store = dict()    # type: Dict[str, Node]

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


class LMDBNodeStore(NodeStore):
    """
    A store for `Node`s that keeps them in LMDB.
    """
    def __init__(self, node_type: type, directory: str, max_size=1024 ** 4):
        """
        Constructor.
        :param node_type: the type of the node in the store
        :param directory: the directory where the LMDB database is
        :param max_size: the maximum size that the LMDB database can grow to
        """
        super().__init__(node_type)
        # TODO: It's quite horrible how the node deserialiser is extracted like this
        self._node_deserialiser = node_type.deserialize
        self._database = lmdb.open(directory, writemap=True, map_size=max_size)

    def __getitem__(self, path: str) -> Optional[SerializableNode]:
        with self._database.begin() as transaction:
            serialised_node = transaction.get(path.encode())
            if serialised_node is None:
                raise KeyError("Node with path `%s` is not in this node store" % path)
        node = self._node_deserialiser(path, serialised_node)
        return node

    def __setitem__(self, path: str, node: SerializableNode):
        # TODO: It's quite horrible how the node serialises itself...
        serialised_node = node.serialize()
        with self._database.begin(write=True) as transaction:
            return transaction.put(path.encode(), serialised_node)

    def __delitem__(self, path: str):
        with self._database.begin(write=True) as transaction:
            transaction.delete(path.encode())

    def __iter__(self):
        # TODO: Assuming that this is not currently used!
        raise NotImplementedError()

    def __len__(self) -> int:
        with self._database.begin() as transaction:
            return transaction.stat()["entries"]

    def __contains__(self, path: str) -> bool:
        with self._database.begin(buffers=True) as transaction:
            cursor = transaction.cursor()
            return cursor.set_key(path.encode())

    def close(self):
        self._database.close()
