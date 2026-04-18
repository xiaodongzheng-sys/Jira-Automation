from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from bpmis_jira_tool.bpmis import BPMISError, BPMISNotConfiguredError
from bpmis_jira_tool.config import Settings


DEBUG_PAYLOAD_PATH = Path(__file__).resolve().parent.parent / "tmp" / "last_bpmis_api_result.json"

REQUIRED_REQUEST_FIELDS = ("access_token", "issue_id", "market", "summary")
OPTIONAL_REQUEST_FIELD_MAPPINGS = {
    "description": "Description",
    "prd_links": "PRD Link/s",
    "td_links": "TD Link/s",
    "fix_version": "Fix Version",
    "component": "Component",
    "priority": "Priority",
    "assignee": "Assignee",
    "reporter": "Reporter",
    "product_manager": "Product Manager",
    "dev_pic": "Dev PIC",
    "qa_pic": "QA PIC",
    "biz_pic": "Biz PIC",
    "need_uat": "Need UAT",
    "involved_tracks": "Involved Tracks",
}


def _clean_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _debug_path_str() -> str | None:
    return str(DEBUG_PAYLOAD_PATH) if DEBUG_PAYLOAD_PATH.exists() else None


def load_request_from_stdin(stdin_text: str | None = None) -> dict[str, Any]:
    raw_text = stdin_text if stdin_text is not None else sys.stdin.read()
    payload_text = raw_text.strip()
    if not payload_text:
        raise ValueError("Expected a JSON object on stdin.")

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as error:
        raise ValueError("Input must be valid JSON.") from error

    if not isinstance(payload, dict):
        raise ValueError("Input JSON must be an object.")
    return payload


def build_fields_from_request(request_data: dict[str, Any]) -> tuple[dict[str, str], str]:
    missing = [name for name in REQUIRED_REQUEST_FIELDS if not _clean_string(request_data.get(name))]
    if missing:
        quoted = ", ".join(missing)
        raise ValueError(f"Missing required request field(s): {quoted}.")

    resolved_task_type = _clean_string(request_data.get("task_type")) or "Feature"
    fields = {
        "Market": _clean_string(request_data["market"]),
        "Summary": _clean_string(request_data["summary"]),
        "Task Type": resolved_task_type,
    }

    for source_name, target_name in OPTIONAL_REQUEST_FIELD_MAPPINGS.items():
        value = _clean_string(request_data.get(source_name))
        if value:
            fields[target_name] = value

    return fields, resolved_task_type


def create_jira_from_request(
    request_data: dict[str, Any],
    *,
    settings: Settings | None = None,
    client_factory=None,
) -> dict[str, Any]:
    settings = settings or Settings.from_env()
    access_token = _clean_string(request_data.get("access_token"))
    issue_id = _clean_string(request_data.get("issue_id"))
    resolved_task_type = _clean_string(request_data.get("task_type")) or "Feature"

    if client_factory is None:
        from bpmis_jira_tool.service import build_bpmis_client

        client_factory = build_bpmis_client

    try:
        fields, resolved_task_type = build_fields_from_request(request_data)
        client = client_factory(settings, access_token)
        project = client.find_project(issue_id)
        created_ticket = client.create_jira_ticket(project, fields)
        return {
            "success": True,
            "message": "Created Jira ticket successfully.",
            "ticket_key": created_ticket.ticket_key,
            "ticket_link": created_ticket.ticket_link,
            "issue_id": issue_id,
            "resolved_task_type": resolved_task_type,
            "debug_payload_path": _debug_path_str(),
        }
    except (ValueError, BPMISError, BPMISNotConfiguredError) as error:
        return {
            "success": False,
            "message": str(error),
            "ticket_key": None,
            "ticket_link": None,
            "issue_id": issue_id,
            "resolved_task_type": resolved_task_type,
            "debug_payload_path": _debug_path_str(),
        }


def main(stdin_text: str | None = None) -> int:
    try:
        request_data = load_request_from_stdin(stdin_text)
        response = create_jira_from_request(request_data)
        exit_code = 0 if response["success"] else 1
    except ValueError as error:
        response = {
            "success": False,
            "message": str(error),
            "ticket_key": None,
            "ticket_link": None,
            "issue_id": "",
            "resolved_task_type": "Feature",
            "debug_payload_path": _debug_path_str(),
        }
        exit_code = 1

    sys.stdout.write(json.dumps(response, ensure_ascii=False))
    sys.stdout.write("\n")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
