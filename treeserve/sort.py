import gzip
import os
from base64 import b64decode
from typing import Iterator

_PATH_ROW_INDEX = 0


def get_traversal_order(data_path: str) -> Iterator:
    """
    TODO
    :param data_path:
    :return:
    """
    path_position_tuples = []

    with gzip.open(data_path, mode="rb") as file:
        position = 0
        while True:
            line = file.readline()
            if len(line) == 0:
                break
            line = line.decode("utf8")
            path = b64decode(line.split("\t")[0])
            path_position_tuples.append((path, position))
            position = file.tell()

            if len(path_position_tuples) % 10000 == 0:
                print("Read: %d" % len(path_position_tuples))

    path_position_tuples = sorted(path_position_tuples, key=lambda x: x[0])
    return reversed(path_position_tuples)


if __name__ == "__main__":
    data_path = os.path.join(os.path.dirname(__file__), "../samples/test_minimal.dat.gz")
    # data_path = os.path.join(os.path.dirname(__file__), "../samples/sampledata.dat.gz")
    ordered_path_position_tuples = get_traversal_order(data_path)

    with gzip.open(data_path, mode="rb") as file:
        counter = 0
        for path_position_tuple in ordered_path_position_tuples:
            position = path_position_tuple[1]
            file.seek(position)
            line = file.readline()

            counter += 1
            print("Seeked: %d" % counter)
