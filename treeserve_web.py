"""Web interface for treeserve written in Python 3"""
from flask import Flask, request, jsonify

from treeserve.tree_builder import TreeBuilder

import glob

app = Flask(__name__)

@app.route("/api")
def api_call():
    path, depth, errors = get_path_depth(request.args)
    if errors:
        error_dict = {"errors": errors}
        return jsonify(error_dict)

    sample_list = glob.glob("samples/*.dat.gz")
    print("Using samples:", sample_list)
    tree_builder = TreeBuilder()
    tree = tree_builder.from_lstat(sample_list)
    print("Created tree.")
    output_dict = tree.to_json(path=path, depth=depth)
    return jsonify(output_dict)

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
            errors.append("'depth' wasn't an integer")
    else:
        errors.append("Didn't recieve integer parameter 'depth'")
    if "path" in args:
        path = args["path"]
    else:
        errors.append("Didn't recieve parameter 'path'")
    return path, depth, errors

if __name__ == '__main__':
    app.debug = True
    app.run("0.0.0.0", port=80)
