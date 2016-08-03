from base64 import b64decode
from grp import getgrgid
import gzip
import os
from pwd import getpwuid
from re import compile, IGNORECASE
from time import strftime, time
from typing import Dict, List

from treeserve.mapping import Mapping
from treeserve.node import Node
from treeserve.tree import Tree


class TreeBuilder:
    path_property_regexes = {
        "cram": compile(r".*[.]cram$", IGNORECASE),
        "bam": compile(r".*[.]bam$", IGNORECASE),
        "index": compile(r".*[.](crai|bai|sai|fai|csi)$", IGNORECASE),
        "compressed": compile(r".*[.](bzip2|gz|tgz|zip|xz|bgz|bcf)$", IGNORECASE),
        "uncompressed": compile(r".*([.]sam|[.]fasta|[.]fastq|[.]fa|[.]fq|[.]vcf|[.]csv|[.]tsv|[.]txt|[.]text|README|[.]o|[.]e|[.]oe|[.]dat)$", IGNORECASE),
        "checkpoint": compile(r".*jobstate[.]context$", IGNORECASE),
        "temporary": compile(r".*(tmp|TMP|temp|TEMP).*", IGNORECASE)
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

    def from_lstat(self, files: List[str]) -> Tree:
        now = int(time())  # Current time in seconds since epoch

        linecount = 0
        for filename in files:
            with gzip.open(filename, mode="rt") as file:
                for line in file:
                    linecount += 1

                    if linecount % 10000 == 0:
                        print(strftime("[%H:%M:%S]"),
                              "Processed", linecount, "lines,",
                              "created", Node.get_node_count(), "nodes")

                    tokens = line.split("\t")

                    path = b64decode(tokens[0]).decode()

                    size = int(tokens[1])
                    uid = int(tokens[2])
                    gid = int(tokens[3])
                    access_time = int(tokens[4])
                    modification_time = int(tokens[5])
                    creation_time = int(tokens[6])
                    file_type = tokens[7]

                    user = self.uid_lookup(uid)
                    group = self.gid_lookup(gid)

                    categories = [name for name, regex in self.path_property_regexes.items()
                                  if regex.match(path) is not None] or ["other"]
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

                    if file_type == "d":
                        self._tree.add_node(path, mapping)
                    elif file_type in "fl":
                        dirname = os.path.dirname(path)
                        self._tree.add_node(dirname, mapping)

        print(strftime("[%H:%M:%S]"), "Finalizing tree after", time() - now, "seconds")
        self._tree.finalize()
        print(strftime("[%H:%M:%S]"), "Built tree in", time() - now, "seconds")
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
