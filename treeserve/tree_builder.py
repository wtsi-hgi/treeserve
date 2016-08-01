from base64 import b64decode
from grp import getgrgid
import gzip
from pwd import getpwuid
from re import compile, IGNORECASE
from time import time
from typing import Dict, List

from mapping import Mapping
from node import Node
from tree import Tree


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

    def __init__(self):
        self._tree = Tree()
        self._uid_map = {}  # type: Dict[int, str]
        self._gid_map = {}  # type: Dict[int, str]

    def from_lstat(self, files: List[str]):
        now = time()  # Current time in seconds since epoch
        seconds_in_year = 60 * 60 * 24 * 365
        cost_per_tib_year = 150  # Cost to store 1 TiB for 1 year in pounds
        one_tib = 1024 ** 4

        linecount = 0
        for filename in files:
            with gzip.open(filename, mode="rt") as file:
                for line in file:
                    if linecount % 10000 == 0:
                        print("processed", linecount, "lines,",
                              "created", Node.node_count, "nodes")

                    mapping = Mapping()

                    tokens = line.split("\t")
                    path = b64decode(tokens[0]).decode()

                    size = int(tokens[1])
                    size_tib = size / one_tib

                    uid = int(tokens[2])
                    user = self.uid_lookup(uid)

                    gid = int(tokens[3])
                    group = self.gid_lookup(gid)

                    atime = int(tokens[4])
                    atime_years = (now - atime) / seconds_in_year

                    mtime = int(tokens[5])
                    mtime_years = (now - mtime) / seconds_in_year

                    ctime = int(tokens[6])
                    ctime_years = (now - ctime) / seconds_in_year

                    file_type = tokens[7]

                    categories = []

                    for name, regex in self.path_property_regexes:
                        if regex.match(path) is not None:
                            categories.append(name)

                    if not categories:
                        categories.append("other")

                    categories.append("*")

                    if file_type == "d":
                        categories.append("directory")
                    elif file_type == "f":
                        categories.append("file")
                    elif file_type == "l":
                        categories.append("link")
                    else:
                        categories.append("type_" + file_type)

                    for category in categories:
                        # Inode counts
                        mapping.add_multiple("count", group, user, category, 1)
                        # Size
                        mapping.add_multiple("size", group, user, category, size)
                        # Access time
                        atime_cost = cost_per_tib_year * size_tib * atime_years
                        mapping.add_multiple("atime", group, user, category, atime_cost)
                        # Modification time
                        mtime_cost = cost_per_tib_year * size_tib * mtime_years
                        mapping.add_multiple("mtime", group, user, category, mtime_cost)
                        # Creation time
                        ctime_cost = cost_per_tib_year * size_tib * ctime_years
                        mapping.add_multiple("ctime", group, user, category, ctime_cost)

                    if file_type == "d":
                        self._tree.add_node(path, mapping)
                    elif file_type == "f" or file_type == "l":
                        split = path.split("/")
                        path = "/".join(split[:-1])
                        self._tree.add_node(path, mapping)

                #

    def uid_lookup(self, uid: int):
        if uid in self._uid_map:
            return self._uid_map[uid]
        else:
            try:
                user = getpwuid(uid)[0]
            except KeyError:
                user = uid
            else:
                self._uid_map[uid] = user
        return str(user)

    def gid_lookup(self, gid: int):
        if gid in self._gid_map:
            return self._gid_map[gid]
        else:
            try:
                group = getgrgid(gid)[0]
            except KeyError:
                group = gid
            else:
                self._gid_map[gid] = group
        return str(group)
