"""Web interface for treeserve written in Python 3"""
from flask import Flask, request, jsonify
import glob
import sys, argparse

from treeserve.tree_builder import TreeBuilder


def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='''Enable debug mode? (development only)''')
    parser.add_argument('-d', '--debug', dest='debug', action='store_const',
                   const=True, default=False)
    args = parser.parse_args(args)
    return args

app = Flask(__name__)

@app.route("/api")
def api_call():
    path, depth, errors = get_path_depth(request.args)
    if errors:
        error_dict = {"errors": errors}
        return jsonify(error_dict)

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

def get_path_depth(args):
    """Given the args from a request, return the path and depth parameters.
       If they aren't present or invalid, return errors in json format"""
    path=depth=None #By default there is no path or depth
                    #Overwritten if no errors
    errors = []
    if "depth" in args:
        depth = args["depth"]
        try:
            depth = int(depth, 10) # Only accept depth in base 10
        except ValueError:
            depth = 0
            errors.append("'depth' not integer")
    else:
        errors.append("no 'depth'")
    if "path" in args:
        path = args["path"]
    else:
        errors.append("no 'path'")
    return path, depth, errors

def create_tree(test_mode=False):
    global tree
    sample_list = [filename for filename in glob.glob("samples/*.dat.gz") if (("test_" not in filename)^test_mode)]
    print("Using samples:", sample_list)
    tree_builder = TreeBuilder()
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
