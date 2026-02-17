from flask import jsonify

def register_routes(app):

    @app.route("/health")
    def health():
        return jsonify({"status": "Running"})
