from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/api")
def api_call():
    errors = []
    if "depth" in request.args:
        depth = request.args["depth"]
        try:
            depth = int(depth, 10)
        except ValueError:
            depth = 0
            errors.append("'depth' wasn't an integer")
    else:
        errors.append("Didn't recieve integer parameter 'depth'")
    if "path" in request.args:
        path = request.args["path"]
    else:
        errors.append("Didn't recieve parameter 'path'")
    if errors:
        error_dict = {"errors": errors}
        return jsonify(error_dict)
        
    print("API called, path =", path, "depth =",depth)
    output_dict = {}
    return jsonify(output_dict)


if __name__ == '__main__':
    app.debug = True
    app.run("0.0.0.0", port=80)
