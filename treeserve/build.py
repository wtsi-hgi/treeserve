#!/usr/bin/env python3

import argparse
import gzip

import lmdb

try:
    from treeserve.models import Node
except:
    from models import Node

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description="Generate tree data structure in LMDB database from mpistat output.")
    parser.add_argument("-i", "--infile", help="Path to input file.", required=True,
                        metavar="infile")
    parser.add_argument("-l", "--lmdb_dir", help="Directory in which LMDB database is created.",
                        required=True, metavar="lmdb_dir")
    args = parser.parse_args()

    lmdb_env = lmdb.open(args.lmdb_dir, map_size=50 * 1024 ** 3)  # 50 GiB

    count = 0
    with gzip.open(args.infile, "rt") as infile:
        for line in infile:
            with lmdb_env.begin(buffers=True, write=True) as txn:
                count += 1
                if count % 10000 == 0:
                    print(count)
                if line == "":
                    break
                tokens = line[:-1].split("\t")  # Remove trailing newline
                Node(tokens, txn)
    with lmdb_env.begin(buffers=True, write=True) as txn:
        pass  # finalize tree
