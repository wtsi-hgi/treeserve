#!/usr/bin/env python3

import gzip
import base64
import pwd
import grp
import pickle
from typing import List

import mpistat_config

# build a tree representation of mpistat data in an lmdb database
# we can only store key->value pairs in the database
# the key will be the path of the inode
# value will be a dict. initially will only have three items - the total size
# below the particular inode, its type and a list of pointers to child
# directories


# class to represent lstat items for a given path
class Lstat:

    _username_cache = dict()
    _group_cache = dict()

    def __init__(self, line, txn):
        bits = line.split('\t')
        path = base64.b64decode(bits[0])
        value = dict()
        value['size'] = int(bits[1])
        value['user'] = self._get_uname(int(bits[2]))
        value['grp'] = self._get_grp((bits[3]))
        value['atime'] = int(bits[4])
        value['mtime'] = int(bits[5])
        value['ctime'] = bits[6]
        value['mode'] = bits[7]
        if value['mode'] == 'd':
            value['children'] = list()  # type: List[str]
        value['ino'] = bits[8]
        value['hardlinks'] = bits[9]
        value['dev'] = bits[10]

        # put the value into the db
        txn.put(path, pickle.dumps(value))

        # if the node is a directory, get the pointer to the data
        # so that we can add it to the child lists for upstream nodes
        # pointer = txn.get(path)

        # now want to recurse up the path tree
        # if the node doesn't exist it needs to be created
        # presumably that directory should be in the input file somewhere, so check for
        # an existing stub entry before creating each node
        # if stub exists, add the existing value for `children` instead of replacing it
        # if this node is a directory, the parent node needs an item added to
        # its children list
        split = path.split('/')
        for i in range(len(split)-1, 1, -1):
            # stop at 1 because the root path is `/lustre`
            newpath = '/' + '/'.join(split[1:i])
            parent = txn.get(newpath)
            if parent is None:
                txn.put(newpath, )

    # get username from uid - cache the answer
    @staticmethod
    def _get_uname(uid):
        """
        Get a username from a user ID and cache the result.
        :param uid: a user ID
        :return: the human-readable username
        """
        username = str(uid)
        if uid in Lstat._username_cache:
            username = Lstat._username_cache[uid]
        else:
            try:
                username = pwd.getpwuid(uid)[0]
            except KeyError:
                pass
            else:
                Lstat._username_cache[uid] = username
        return username

    # get group from gid - cache the answer
    @staticmethod
    def _get_grp(gid):
        """
        Get a group name from a group ID and cache the result.
        :param gid: a group ID
        :return: the human-readable group name
        """
        group = str(gid)
        if gid in Lstat._group_cache:
            group = Lstat._group_cache[gid]
        else:
            try:
                group = grp.getgrgid(gid)[0]
            except KeyError:
                pass
            else:
                Lstat._group_cache[gid] = group
        return group


# main program
if __name__ == '__main__':
    # loop over the mpistat lines and create entries
    # in the database for each line
    count = 0
    with gzip.open(mpistat_config.args['infile'], 'rt') as f:
        with mpistat_config.args['lmdb_env'].begin(buffers=True, write=True) as txn:
            for line in f:
                count += 1
                if count % 10000 == 0:
                    print(count)
                if line == '':
                    break
                Lstat(line[:-1], txn)
