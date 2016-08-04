from base64 import b64decode
import csv
from grp import getgrgid
import gzip
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
        "checkpoint": lambda s: s.endswith(("jobstate.context")),
        "temporary": lambda s: ("tmp" in s) or ("temp" in s)
    }

    file_types = {
        "d": "directory",
        "f": "file",
        "l": "link"
    }

    def __init__(self):
        self._tree = Tree()
        self._uid_map = {}  # type: Dict[int, str]
        self._gid_map = {}  # type: Dict[int, str]

    def from_lstat(self, files: List[str], now=int(time())) -> Tree:
        start = time()
        linecount = 0
        for filename in files:
            with gzip.open(filename, mode="rt") as file:
                reader = csv.reader(file, delimiter="\t")
                for row in reader:
                    if not row: continue

                    linecount += 1

                    if linecount % 10000 == 0:
                        print(strftime("[%H:%M:%S]"),
                              "Processed", linecount, "lines,",
                              "created", Node.get_node_count(), "nodes")

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
                        atime_cost = size * (now - access_time)
                        mapping.add_multiple("atime", group, user, category, atime_cost)
                        # Modification time
                        mtime_cost = size * (now - modification_time)
                        mapping.add_multiple("mtime", group, user, category, mtime_cost)
                        # Creation time
                        ctime_cost = size * (now - creation_time)
                        mapping.add_multiple("ctime", group, user, category, ctime_cost)

                    # if file_type == "d":
                    #     self._tree.add_node(path, mapping)
                    # elif file_type in "fl":
                    #     dirname = os.path.dirname(path)
                    #     self._tree.add_node(dirname, mapping)
                    if file_type in "dlf":
                        self._tree.add_node(path, file_type in "d", mapping)

        print(strftime("[%H:%M:%S]"), "Finalizing tree after", time() - start, "seconds")
        self._tree.finalize()
        print(strftime("[%H:%M:%S]"), "Built tree in", time() - start, "seconds")
        print(strftime("[%H:%M:%S]"), Node.get_node_count(), "nodes created")
        return self._tree

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
    tree = tree_builder.from_lstat(["samples/sampledata.dat.gz"])
    print(tree.to_json(depth=0, path="/"))
