import argparse
import lmdb


def parse_args():
    parser = argparse.ArgumentParser(description='''
Generate tree data structure in lmdb database from mpistat output.''')
    parser.add_argument('-i', '--infile', help='Path to input file',
                        required=True, metavar='infile')
    parser.add_argument('-l', '--lmdb_dir',
                        help='Directory in which lmdb db is created',
                        required=True, metavar='lmdb_dir')
    return vars(parser.parse_args())


# create lmdb environment
def get_lmdb_env():
    global args
    return lmdb.open(args['lmdb_dir'],
                     map_size=50*1024*1024*1024,
                     writemap=True)
args = parse_args()
args['lmdb_env'] = get_lmdb_env()
