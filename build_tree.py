import gzip
import base64
import pwd
import grp
import pickle
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
        mode = bits[7]
        if mode == 'd':
            value['children'] = list()
        value['mode'] = mode
        value['ino'] = bits[8]
        value['nlink'] = bits[9]
        value['dev'] = bits[10]

        # put the value into the db
        txn.put(path, pickle.dumps(value))

        # if the node is a directory, get the pointer to the data
        # so that we can add it to the child lists for upstream nodes
        # pointer = txn.get(path)

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

    # loop over the mpistat lines and create entries
    # in the database for each line
    count = 0
    with gzip.open(mpistat_config.args['infile'], 'rb') as f:
        with mpistat_config.args['lmdb_env'].begin(buffers=True, write=True)\
                    as txn:
            while True:
                count += 1
                if count % 100000 == 0:
                    print(count)
                line = f.readline().decode('utf-8')
                if line == '':
                    break
                Lstat(line[:-1], txn)
