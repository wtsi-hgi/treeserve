from flask import Flask, request, redirect, render_template, send_from_directory

import json

app = Flask(__name__)

@app.route("/api")
def api_call():
    depth = int(request.args.get("depth", "0"))
    path = request.args["path"]
    print("API called, path =", path, "depth =",depth)
    output_dict = {}
    return json.dumps(output_dict)


if __name__ == '__main__':
    app.debug = True
    app.run("0.0.0.0", port=80)
