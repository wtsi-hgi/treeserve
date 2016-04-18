import gzip
import argparse
import base64
import pwd
import grp
import lmdb

# build a tree representation of mpistat data in an lmdb database
# we can only store key->value pairs in the database
# the key will be the path of the inode
# value will be a dict. initially will only have three items - the total size
# below the particular inode, its type and a list of pointers to child
# directories


# parse the command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description='''
Generate tree data structure in lmdb database from mpistat output.''')
    parser.add_argument('-i', '--infile', help='Path to input file',
                        required=True, metavar='infile')
    parser.add_argument('-l', '--lmdb_dir',
                        help='Directory in which lmdb db is created',
                        required=True, metavar='lmdb_dir')
    return vars(parser.parse_args())


# class to represent lstat items for a given path
class Lstat:

    _username_cache = dict()
    _group_cache = dict()

    def __init__(self, line):
        bits = line.split('\t')
        path = base64.b64decode(bits[0])  # a bytes object not a string
        size = int(bits[1])
        # user = int(self._get_uname(bits[2]))
        # grp = int(self._get_grp(bits[3]))
        # atime = bits[4]
        # mtime = bits[5]
        # ctime = bits[6]
        mode = bits[7]
        # ino = bits[8]
        # nlink = bits[9]
        # dev = bits[10]

        # get snappy compression of the path to use as the lmdb key
        # create the 'value' dict
        value = dict()
        value['size'] = size
        value['mode'] = mode[7]
        if mode == 'd':
            value['children'] = list()

        # put the value into the db
        global lmdb_env
        with lmdb_env.begin(write=True, buffers=True) as txn:
            txn.put(path, value)

            # if the node is a directory, get the pointer to the data
            # so that we can add it to the child lists for upstream nodes
            pointer = txn.get(path)

        # now want to recurse up the path tree
        # if the node doesn't exist it needs to be created
        # if the inode is a directory, the parent node needs an item added to
        # its children list

    # get username from uid - cache the answer
    def _get_uname(self, uid):
        username = str(uid)
        if uid in Lstat._username_cache:
            username = Lstat._username_cache[uid]
        else:
            try:
                username = pwd.getpwuid(uid)[0]
                Lstat._username_cache[uid] = username
            except:
                pass
        return username

    # get group from gid - cache the answer
    def _get_grp(self, gid):
        group = str(gid)
        if gid in Lstat._group_cache:
            group = Lstat._group_cache[gid]
        else:
            try:
                group = grp.getgrgid(gid)[0]
                Lstat._group_cache[gid] = group
            except:
                pass
        return group


# main program
if __name__ == '__main__':

    # parse command line arguments
    args = parse_args()

    # create the lmdb database
    ldmb_env = lmdb.open(args['lmdb_db'],
                         map_size=50*1024*1024*1024,
                         writemap=True)

    # loop over the mpistat lines ans create entries
    # in the database for each line
    with gzip.open(args['infile'], 'rb') as f:
        while True:
            line = f.readline().decode('utf-8')
            if line == '':
                break
            l = Lstat(line)
            print(l.path, l.size, l.user, l.grp, l.atime)
