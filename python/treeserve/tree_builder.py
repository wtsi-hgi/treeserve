from base64 import b64decode
import csv
from grp import getgrgid
import gzip
import logging
from numbers import Number
from pwd import getpwuid
from time import strftime, time
from typing import Dict, List

from treeserve.mapping import Mapping
from treeserve.tree import Tree


class TreeBuilder:
    """
    A class that can construct trees from data about files on a filesystem.
    """

    file_category_checks = {
        "cram": lambda s: s.endswith(".cram"),
        "bam": lambda s: s.endswith(".bam"),
        "index": lambda s: s.endswith((".crai", ".bai", ".sai", ".fai", ".csi")),
        "compressed": lambda s: s.endswith((".bzip2", ".gz", ".tgz", ".zip", ".xz", ".bgz", ".bcf")),
        "uncompressed": lambda s: s.endswith((".sam", ".fasta", ".fastq", ".fa", ".fq", ".vcf", ".csv", ".tsv", ".txt", ".text", "README", ".o", ".e", ".oe", ".dat")),
        "checkpoint": lambda s: s.endswith("jobstate.context"),
        "temporary": lambda s: "tmp" in s or "temp" in s
    }

    file_types = {
        "d": "directory",
        "f": "file",
        "l": "link"
    }

    def __init__(self, tree: Tree):
        self._tree = tree
        self._uid_map = {}  # type: Dict[int, str]
        self._gid_map = {}  # type: Dict[int, str]
        self.logger = logging.getLogger(__name__)

    def from_lstat(self, files: List[str], now: Number=None) -> Tree:
        """
        Construct a `Tree` from files outputted by mpistat.

        Each file should be a TSV file, with the following columns:

        - path (base64 encoded)
        - size (bytes)
        - owner (UID)
        - group (GID)
        - atime (seconds since epoch)
        - mtime (seconds since epoch)
        - ctime (seconds since epoch)
        - object type
        - inode number
        - number of hardlinks
        - device ID

        :param files: a list of filenames to construct a tree from.
        :param now: a time in seconds since the epoch to use to calculate the cost of files.
        :return: a `Tree`
        """

        if now is None:
            now = int(time())
        start = time()
        linecount = 0

        for filename in files:
            self.logger.info("Processing file %s...", repr(filename))
            with gzip.open(filename, mode="rt") as file:
                reader = csv.reader(file, delimiter="\t")
                for row in reader:
                    if not row: continue

                    linecount += 1

                    if linecount % 10000 == 0:
                        self.logger.info("Processed %s lines, created %s nodes", linecount, len(self._tree))

                    path = b64decode(row[0]).decode()  # type: str

                    size = int(row[1])
                    uid = int(row[2])
                    gid = int(row[3])
                    access_time = int(row[4])
                    modification_time = int(row[5])
                    change_time = int(row[6])
                    file_type = row[7]  # type: str

                    user = self.uid_lookup(uid)
                    group = self.gid_lookup(gid)

                    categories = [name for name, func in self.file_category_checks.items()
                                  if func(path.lower())] or ["other"]
                    categories.append("*")
                    categories.append(self.file_types.get(file_type, "type_" + file_type))

                    mapping = Mapping()

                    for category in categories:
                        # Inode counts
                        mapping.set("count", group, user, category, 1)
                        # Size
                        mapping.set("size", group, user, category, size)

                        # Access time
                        atime_cost = size * (now - access_time)
                        mapping.set("atime", group, user, category, atime_cost)
                        # Modification time
                        mtime_cost = size * (now - modification_time)
                        mapping.set("mtime", group, user, category, mtime_cost)
                        # Change time
                        ctime_cost = size * (now - change_time)
                        mapping.set("ctime", group, user, category, ctime_cost)

                    if file_type in self.file_types:
                        self._tree.add_node(path, file_type == "d", mapping)

        self.logger.info("Finalizing tree after %s seconds", time() - start)
        exit()
        self._tree.finalize()
        self.logger.info("Built tree in %s seconds", time() - start)
        self.logger.info("%s nodes in database", len(self._tree))
        return self._tree

    def uid_lookup(self, uid: int) -> str:
        """
        Look up a username from a UID, or return the UID if the username is not found.

        :param uid:
        :return:
        """
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
        """
        Look up a group name from a GID, or return the GID if the group name is not found.

        :param gid:
        :return:
        """
        try:
            group = self._gid_map[gid]
        except KeyError:
            try:
                group = getgrgid(gid)[0]
            except KeyError:
                group = gid
            self._gid_map[gid] = group
        return str(group)
