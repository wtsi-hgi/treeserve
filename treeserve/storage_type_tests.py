import json
import pickle
import struct
from ast import literal_eval
import time

def test_dump_json(data):
    return json.dumps(list(data.values()))
def test_load_json(data):
    return json.loads(data)

def test_dump_pickle(data):
    return pickle.dumps(data)
def test_load_pickle(data):
    return pickle.loads(data)

def test_dump_literal_eval(data):
    return repr(data)
def test_load_literal_eval(data):
    return literal_eval(data)

def test_dump_struct(data):
    return struct.pack(">H3sHH", data["a"], bytes(data["12"], encoding='utf8'), data["list"][0], data["list"][1])
def test_load_struct(data):
    return struct.unpack(">H3sHH", data)

tests = {test_dump_json: test_load_json,
         test_dump_literal_eval: test_load_literal_eval,
         test_dump_pickle: test_load_pickle,
         test_dump_struct: test_load_struct}

test_data = {"a":123,
             "12":"456",
             "list": [324,64]}

for test_dump, test_load in tests.items():
    print(test_dump.__name__)
    start = time.time()
    for i in range(100000):
        test_load(test_dump(test_data))
    print(len(test_dump(test_data)), test_dump(test_data))
    print(time.time() - start)
