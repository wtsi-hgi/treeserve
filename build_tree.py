import gzip
import argparse
import base64
import pwd
import grp


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
        self.path = base64.b64decode(bits[0]).decode('utf-8')
        self.size = bits[1]
        self.user = int(self._get_uname(bits[2]))
        self.grp = int(self._get_grp(bits[3]))
        self.atime = bits[4]
        self.mtime = bits[5]
        self.ctime = bits[6]
        self.mode = bits[7]
        self.ino = bits[8]
        self.nlink = bits[9]
        self.dev = bits[10]

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
    args = parse_args()
    with gzip.open(args['infile'], 'rb') as f:
        while True:
            line = f.readline().decode('utf-8')
            if line == '':
                break
            l = Lstat(line)
            print(l.path, l.size, l.user, l.grp, l.atime)
