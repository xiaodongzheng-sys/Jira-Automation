from __future__ import annotations

from flask import Flask, jsonify, request

from bpmis_jira_tool.bpmis import BPMISPageApiClient
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import BPMISError
from bpmis_jira_tool.models import ProjectMatch


def create_app() -> Flask:
    app = Flask(__name__)

    @app.after_request
    def add_cors_headers(response):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        return response

    @app.route("/health", methods=["GET", "OPTIONS"])
    def health():
        if request.method == "OPTIONS":
            return ("", 204)
        return jsonify(
            {
                "status": "ok",
                "service": "team-helper",
                "mode": "prototype",
            }
        )

    @app.route("/bpmis/create-jira", methods=["POST", "OPTIONS"])
    def create_jira():
        if request.method == "OPTIONS":
            return ("", 204)

        payload = request.get_json(silent=True) or {}
        issue_id = str(payload.get("issue_id") or "").strip()
        fields = payload.get("fields") or {}
        if not issue_id:
            return jsonify({"status": "error", "message": "Missing issue_id"}), 400
        if not isinstance(fields, dict):
            return jsonify({"status": "error", "message": "fields must be an object"}), 400

        settings = Settings.from_env()
        client = BPMISPageApiClient(settings)
        project = ProjectMatch(project_id=issue_id, raw={"issueId": issue_id})

        try:
            ticket = client.create_jira_ticket(project, {str(key): str(value) for key, value in fields.items()})
        except BPMISError as error:
            return jsonify({"status": "error", "message": str(error)}), 400

        return jsonify(
            {
                "status": "created",
                "ticket_key": ticket.ticket_key,
                "ticket_link": ticket.ticket_link,
            }
        )

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8787)
