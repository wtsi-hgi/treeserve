"""Web interface for treeserve written in Python 3"""
import argparse
import glob
import logging
import sys
import time

from flask import Flask, request, jsonify, render_template

from treeserve.mapping import Mapping
from treeserve.node import PickleSerializableNode
from treeserve.node_store import LMDBNodeStore
from treeserve.tree import Tree
from treeserve.tree_builder import TreeBuilder


def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description="Python Treeserve")
    parser.add_argument('-i', '--input', dest="input_file", default="samples/sampledata.dat.gz",
                        help="Path to input file")
    parser.add_argument('-c', '--cache', dest="cache_dir", default="/tmp/lmdb_web",
                        help="Path for lmdb cache directory (doesn't need to exist)")
    parser.add_argument('-a', '--address', dest="ip", default="0.0.0.0",
                        help="Ip address to bind to")
    parser.add_argument('-p', '--port', dest='port', type=int,
                        default=8000,
                        help="Port to run on")
    parser.add_argument('-d', '--debug', dest='debug', action='store_const',
                        const=True, default=False,
                        help="Enable web debug mode? (development only, replaces 500 errors with interactive prompt)")
    parser.add_argument('-l', '--log', dest="log_level", default="INFO",
                        help="One of 'debug', 'info', 'warning', 'error' or 'critical'. Default 'info'")
    parser.add_argument('-t', '--time', dest='now', type=int,
                        default=time.time(),
                        help="Current Unix time to use when calculating storage costs. Defaults to current time")
    parser.add_argument('--cost_tib_year', dest='cost_tib_year', type=int,
                        default=150,
                        help="Cost to store 1 TiB for a year")
    args = parser.parse_args(args)
    return args


app = Flask(__name__, static_url_path='/')


@app.route("/api")
def api_call():
    path = request.args.get("path", "/")
    depth = int(request.args.get("depth", "0"))
    whitelist = set(request.args.get("categories", "").split(","))
    if whitelist == {''}:
        whitelist = set()
    output_dict = tree.format(path=path, depth=depth, whitelist=whitelist)
    return jsonify(output_dict)


@app.route("/")
def index():
    return render_template("index.html")


def create_tree(test_mode=False, now=None, input_file=None):
    global tree
    sample_list = [filename for filename in glob.glob("samples/*.dat.gz") if (("test_" not in filename)^test_mode)]
    sample_list = [input_file]
    if test_mode:
        sample_list = ["../../samples/sampledata.dat.gz"]
    print("Using samples:", sample_list)
    tree = Tree(LMDBNodeStore(PickleSerializableNode, args.cache_dir))
    if len(tree):
        tree._root_path = tree._node_store._root_path
    else:
        tree_builder = TreeBuilder(tree)
        tree = tree_builder.from_lstat(sample_list, now=now)
    print("Created tree.")


if __name__ == '__main__':
    args = parse_args()
    app.debug = args.debug

    # Set up logging
    logger = logging.getLogger("treeserve")
    logger.setLevel(getattr(logging, args.log_level.upper()))
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(fmt="%(levelname)-8s | %(asctime)s | %(name)s: %(message)s",
                                  datefmt="%H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    Mapping.recalc_cost(args.cost_tib_year)

    if app.debug:
        app.before_first_request(lambda: create_tree(now=args.now, input_file=args.input_file))
    else:
        create_tree(now=args.now, input_file=args.input_file)

    app.run(args.ip, port=args.port)

