from __future__ import annotations

from flask import Flask, jsonify, request


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/health")
    def health():
        return jsonify(
            {
                "status": "ok",
                "service": "team-helper",
                "mode": "prototype",
            }
        )

    @app.post("/bpmis/create-jira")
    def create_jira():
        payload = request.get_json(silent=True) or {}
        return (
            jsonify(
                {
                    "status": "not_implemented",
                    "message": "Team helper BPMIS bridge is scaffolded but not implemented yet.",
                    "received_keys": sorted(payload.keys()),
                }
            ),
            501,
        )

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8787)
