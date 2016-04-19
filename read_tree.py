import lmdb
import argparse
import pickle


def parse_args():
    parser = argparse.ArgumentParser(description='''
Construct JSON from tree data structure in lmdb database.''')
    parser.add_argument('-l', '--lmdb_dir',
                        help='Directory in which lmdb db is created',
                        required=True, metavar='lmdb_dir')
    return vars(parser.parse_args())


if __name__ == '__main__':
    args = parse_args()
    env = lmdb.open(args['lmdb_dir'])
    with env.begin() as txn:
        cursor = txn.cursor()
        while cursor.next():
            k, v = cursor.item()
            value = pickle.loads(v)
            print(value)
