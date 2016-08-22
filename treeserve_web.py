"""Web interface for treeserve written in Python 3"""
from flask import Flask, request, jsonify
import glob
import sys, argparse

from treeserve.node import PickleSerializableNode
from treeserve.node_store import LMDBNodeStore
from treeserve.tree_builder import TreeBuilder
from treeserve.tree import Tree


def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='''Enable debug mode? (development only)''')
    parser.add_argument('-d', '--debug', dest='debug', action='store_const',
                   const=True, default=False)
    args = parser.parse_args(args)
    return args

app = Flask(__name__)

@app.route("/api")
def api_call():
    path = request.args.get("path", "/")
    depth = int(request.args.get("depth", "0"))
    output_dict = tree.format(path=path, depth=depth)
    return jsonify(output_dict)

@app.route("/dummy_api")
def dummy_api():
    import random, json
    sample_list = glob.glob("json/*.json")
    json_file = open(random.choice(sample_list))
    rtn = json_file.read()
    json_file.close()
    return jsonify(json.loads(rtn))


def create_tree(test_mode=False):
    global tree
    sample_list = [filename for filename in glob.glob("samples/*.dat.gz") if (("test_" not in filename)^test_mode)]
    sample_list = ["samples/sampledata.dat.gz"]
    if test_mode:
        sample_list = ["../../samples/sampledata.dat.gz"]
    print("Using samples:", sample_list)
    tree = Tree(LMDBNodeStore(PickleSerializableNode, "/tmp/web_lmdb"))
    if len(tree):
        tree._root_path = tree._node_store._root_path
    else:
        tree_builder = TreeBuilder(tree)
        tree = tree_builder.from_lstat(sample_list)
    print("Created tree.")

if __name__ == '__main__':
    args = parse_args()
    app.debug = args.debug
    if app.debug:
        create_tree = app.before_first_request(create_tree)
    else:
        create_tree()

    app.run("0.0.0.0", port=8080)
