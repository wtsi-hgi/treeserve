from base64 import b64decode
import csv
from grp import getgrgid
import gzip
import json
import lmdb
from pwd import getpwuid
from time import strftime, time
from typing import Dict, List

from treeserve.mapping import Mapping
from treeserve.node import Node
from treeserve.tree import Tree


class TreeBuilder:
    file_type_checks = {
        "cram": lambda s: s.endswith(".cram"),
        "bam": lambda s: s.endswith(".bam"),
        "index": lambda s: s.endswith((".crai", ".bai", ".sai", ".fai", ".csi")),
        "compressed": lambda s: s.endswith((".bzip2", ".gz", ".tgz", ".zip", ".xz", ".bgz", ".bcf")),
        "uncompressed": lambda s: s.endswith((".sam", ".fasta", ".fastq", ".fa", ".fq", ".vcf", ".csv", ".tsv", ".txt", ".text", "README", ".o", ".e", ".oe", ".dat")),
        "checkpoint": lambda s: s.endswith("jobstate.context"),
        "temporary": lambda s: ("tmp" in s) or ("temp" in s)
    }

    file_types = {
        "d": "directory",
        "f": "file",
        "l": "link"
    }

    def __init__(self, lmdb_path: str=".lmdb"):
        self._env = lmdb.open(lmdb_path, map_size=2*1024**3, writemap=True)
        self._tree = Tree()
        self._uid_map = {}  # type: Dict[int, str]
        self._gid_map = {}  # type: Dict[int, str]

    def from_lstat(self, files: List[str], now=int(time())) -> Tree:
        start = time()
        linecount = 0
        with self._env.begin(write=True) as txn:
            for filename in files:
                with gzip.open(filename, mode="rt") as file:
                    reader = csv.reader(file, delimiter="\t")
                    for row in reader:
                        if not row: continue

                        if linecount % 10000 == 0:
                            print(strftime("[%H:%M:%S]"),
                                  "Processed", linecount, "lines,",
                                  "created", Node.get_node_count(), "nodes")

                        linecount += 1

                        path = b64decode(row[0]).decode()

                        size = int(row[1])
                        uid = int(row[2])
                        gid = int(row[3])
                        access_time = int(row[4])
                        modification_time = int(row[5])
                        creation_time = int(row[6])
                        file_type = row[7]

                        user = self.uid_lookup(uid)
                        group = self.gid_lookup(gid)

                        categories = [name for name, func in self.file_type_checks.items()
                                      if func(path)] or ["other"]
                        categories.append("*")
                        categories.append(self.file_types.get(file_type, "type_" + file_type))

                        mapping = Mapping()

                        for category in categories:
                            # Inode counts
                            mapping.add_multiple("count", group, user, category, 1)
                            # Size
                            mapping.add_multiple("size", group, user, category, size)

                            # Access time
                            atime = size * (now - access_time)
                            mapping.add_multiple("atime", group, user, category, atime)
                            # Modification time
                            mtime = size * (now - modification_time)
                            mapping.add_multiple("mtime", group, user, category, mtime)
                            # Creation time
                            ctime = size * (now - creation_time)
                            mapping.add_multiple("ctime", group, user, category, ctime)

                        # if file_type == "d":
                        #     self._tree.add_node(path, mapping)
                        # elif file_type in "fl":
                        #     dirname = os.path.dirname(path)
                        #     self._tree.add_node(dirname, mapping)
                        if file_type in "dlf":
                            self._tree.add_node(path, file_type in "d", mapping, txn)
            self._tree.add_lustre(txn)

        print(strftime("[%H:%M:%S]"), "Finalizing tree after", time() - start, "seconds")
        #self._tree.finalize()
        print(strftime("[%H:%M:%S]"), "Built tree in", time() - start, "seconds")
        print(strftime("[%H:%M:%S]"), Node.get_node_count(), "nodes created")
        return self._tree

    def from_db(self, now=int(time())):
        with self._env.begin() as txn:
            #Get /lustre
            print(txn.get(b'0'))

    def uid_lookup(self, uid: int) -> str:
        try:
            user = self._uid_map[uid]
        except KeyError:
            try:
                user = getpwuid(uid)[0]
            except KeyError:
                user = uid
            self._uid_map[uid] = user
        return str(user)

    def gid_lookup(self, gid: int) -> str:
        try:
            group = self._gid_map[gid]
        except KeyError:
            try:
                group = getgrgid(gid)[0]
            except KeyError:
                group = gid
            self._gid_map[gid] = group
        return str(group)

if __name__ == "__main__":
    tree_builder = TreeBuilder()
    tree = tree_builder.from_lstat(["samples/largesampledata_sorted.dat.gz"])
    #tree = tree_builder.from_db()
    # with tree_builder._env.begin() as txn:
    #     print(json.dumps(tree.to_json(depth=3, path="/", txn=txn)))
    with tree_builder._env.begin() as txn:
        print(json.dumps(Node.from_id(1, txn).to_json(1, txn)))
