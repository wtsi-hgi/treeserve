from flask import Flask, request, redirect, render_template, send_from_directory

app = Flask(__name__)
app.debug = True

@app.route("/api")
def api_call():
    depth = int(request.args.get("depth", "0"))
    path = request.args.get("path", "/")
    return "API, depth=%i, path=%s"%(depth, path)


app.run("0.0.0.0", port=80)
