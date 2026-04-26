from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from bpmis_jira_tool.bpmis import BPMISClient
from bpmis_jira_tool.errors import BPMISError, ToolError
from bpmis_jira_tool.models import ProjectMatch, RunResult
from bpmis_jira_tool.user_config import WebConfigStore


SYNCABLE_PROJECT_FIELDS = ("bpmis_id", "project_name", "brd_link", "market")


class BPMISProjectStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_db()

    def upsert_project(
        self,
        *,
        user_key: str,
        bpmis_id: str,
        project_name: str,
        brd_link: str,
        market: str,
    ) -> str:
        owner = self._require_user_key(user_key)
        issue_id = self._require_bpmis_id(bpmis_id)
        with sqlite3.connect(self.db_path) as connection:
            row = connection.execute(
                "SELECT deleted_at FROM bpmis_projects WHERE user_key = ? AND bpmis_id = ?",
                (owner, issue_id),
            ).fetchone()
            if row and row[0]:
                return "deleted"
            connection.execute(
                """
                INSERT INTO bpmis_projects (
                    user_key, bpmis_id, project_name, brd_link, market, synced_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(user_key, bpmis_id) DO UPDATE SET
                    project_name = excluded.project_name,
                    brd_link = excluded.brd_link,
                    market = excluded.market,
                    synced_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE bpmis_projects.deleted_at IS NULL
                """,
                (
                    owner,
                    issue_id,
                    str(project_name or "").strip(),
                    str(brd_link or "").strip(),
                    str(market or "").strip(),
                ),
            )
            connection.commit()
        return "updated" if row else "created"

    def list_projects(self, *, user_key: str) -> list[dict[str, Any]]:
        owner = self._require_user_key(user_key)
        with sqlite3.connect(self.db_path) as connection:
            connection.row_factory = sqlite3.Row
            project_rows = connection.execute(
                """
                SELECT user_key, bpmis_id, project_name, brd_link, market, synced_at, created_at, updated_at
                FROM bpmis_projects
                WHERE user_key = ? AND deleted_at IS NULL
                ORDER BY updated_at DESC, bpmis_id DESC
                """,
                (owner,),
            ).fetchall()
            ticket_rows = connection.execute(
                """
                SELECT id, user_key, bpmis_id, component, market, system, jira_title, prd_link,
                       description, fix_version_name, fix_version_id, ticket_key, ticket_link,
                       status, message, raw_response_json, created_at
                FROM bpmis_project_jira_tickets
                WHERE user_key = ?
                ORDER BY id ASC
                """,
                (owner,),
            ).fetchall()

        tickets_by_project: dict[str, list[dict[str, Any]]] = {}
        for row in ticket_rows:
            ticket = self._row_to_dict(row)
            ticket["raw_response"] = self._loads_json(ticket.pop("raw_response_json", ""))
            tickets_by_project.setdefault(str(ticket.get("bpmis_id") or ""), []).append(ticket)

        projects = []
        for row in project_rows:
            project = self._row_to_dict(row)
            project["jira_tickets"] = tickets_by_project.get(str(project.get("bpmis_id") or ""), [])
            projects.append(project)
        return projects

    def get_project(self, *, user_key: str, bpmis_id: str) -> dict[str, Any] | None:
        owner = self._require_user_key(user_key)
        issue_id = self._require_bpmis_id(bpmis_id)
        for project in self.list_projects(user_key=owner):
            if str(project.get("bpmis_id") or "") == issue_id:
                return project
        return None

    def soft_delete_project(self, *, user_key: str, bpmis_id: str) -> bool:
        owner = self._require_user_key(user_key)
        issue_id = self._require_bpmis_id(bpmis_id)
        with sqlite3.connect(self.db_path) as connection:
            cursor = connection.execute(
                """
                UPDATE bpmis_projects
                SET deleted_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE user_key = ? AND bpmis_id = ? AND deleted_at IS NULL
                """,
                (owner, issue_id),
            )
            connection.commit()
        return cursor.rowcount > 0

    def add_jira_ticket(
        self,
        *,
        user_key: str,
        bpmis_id: str,
        component: str,
        market: str,
        system: str,
        jira_title: str,
        prd_link: str,
        description: str,
        fix_version_name: str,
        fix_version_id: str = "",
        ticket_key: str = "",
        ticket_link: str = "",
        status: str = "created",
        message: str = "",
        raw_response: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        owner = self._require_user_key(user_key)
        issue_id = self._require_bpmis_id(bpmis_id)
        with sqlite3.connect(self.db_path) as connection:
            connection.row_factory = sqlite3.Row
            cursor = connection.execute(
                """
                INSERT INTO bpmis_project_jira_tickets (
                    user_key, bpmis_id, component, market, system, jira_title, prd_link,
                    description, fix_version_name, fix_version_id, ticket_key, ticket_link,
                    status, message, raw_response_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    owner,
                    issue_id,
                    str(component or "").strip(),
                    str(market or "").strip(),
                    str(system or "").strip(),
                    str(jira_title or "").strip(),
                    str(prd_link or "").strip(),
                    str(description or "").strip(),
                    str(fix_version_name or "").strip(),
                    str(fix_version_id or "").strip(),
                    str(ticket_key or "").strip(),
                    str(ticket_link or "").strip(),
                    str(status or "").strip(),
                    str(message or "").strip(),
                    json.dumps(raw_response or {}, ensure_ascii=False),
                ),
            )
            row = connection.execute(
                """
                SELECT id, user_key, bpmis_id, component, market, system, jira_title, prd_link,
                       description, fix_version_name, fix_version_id, ticket_key, ticket_link,
                       status, message, raw_response_json, created_at
                FROM bpmis_project_jira_tickets
                WHERE id = ?
                """,
                (cursor.lastrowid,),
            ).fetchone()
            connection.commit()
        ticket = self._row_to_dict(row)
        ticket["raw_response"] = self._loads_json(ticket.pop("raw_response_json", ""))
        return ticket

    def _ensure_db(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS bpmis_projects (
                    user_key TEXT NOT NULL,
                    bpmis_id TEXT NOT NULL,
                    project_name TEXT NOT NULL DEFAULT '',
                    brd_link TEXT NOT NULL DEFAULT '',
                    market TEXT NOT NULL DEFAULT '',
                    deleted_at TEXT,
                    synced_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_key, bpmis_id)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS bpmis_project_jira_tickets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_key TEXT NOT NULL,
                    bpmis_id TEXT NOT NULL,
                    component TEXT NOT NULL DEFAULT '',
                    market TEXT NOT NULL DEFAULT '',
                    system TEXT NOT NULL DEFAULT '',
                    jira_title TEXT NOT NULL DEFAULT '',
                    prd_link TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    fix_version_name TEXT NOT NULL DEFAULT '',
                    fix_version_id TEXT NOT NULL DEFAULT '',
                    ticket_key TEXT NOT NULL DEFAULT '',
                    ticket_link TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    message TEXT NOT NULL DEFAULT '',
                    raw_response_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            connection.commit()

    @staticmethod
    def _require_user_key(value: str) -> str:
        user_key = str(value or "").strip()
        if not user_key:
            raise ToolError("User identity is required.")
        return user_key

    @staticmethod
    def _require_bpmis_id(value: str) -> str:
        bpmis_id = str(value or "").strip()
        if not bpmis_id:
            raise ToolError("BPMIS ID is required.")
        return bpmis_id

    @staticmethod
    def _row_to_dict(row) -> dict[str, Any]:
        return {key: row[key] for key in row.keys()} if row is not None else {}

    @staticmethod
    def _loads_json(value: str) -> dict[str, Any]:
        try:
            payload = json.loads(value or "{}")
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}


class PortalProjectSyncService:
    def __init__(self, store: BPMISProjectStore, bpmis_client: BPMISClient):
        self.store = store
        self.bpmis_client = bpmis_client

    def sync_projects(self, *, user_key: str, pm_email: str, progress_callback=None) -> list[RunResult]:
        if not str(pm_email or "").strip():
            raise ToolError("PM email is required before syncing BPMIS projects.")

        self._emit_progress(progress_callback, "fetching", "Fetching BPMIS Biz Projects.", 0, 0)
        projects = self.bpmis_client.list_biz_projects_for_pm_email(str(pm_email or "").strip())
        issue_ids = [str(project.get("issue_id") or "").strip() for project in projects if str(project.get("issue_id") or "").strip()]
        brd_links_by_issue_id = self.bpmis_client.get_brd_doc_links_for_projects(issue_ids) if issue_ids else {}

        total = len(projects)
        results: list[RunResult] = []
        for index, project in enumerate(projects, start=1):
            issue_id = str(project.get("issue_id") or "").strip()
            project_name = str(project.get("project_name") or "").strip()
            market = str(project.get("market") or "").strip()
            self._emit_progress(progress_callback, "syncing", f"Saving BPMIS Issue ID {issue_id}.", index, total)
            if not issue_id:
                results.append(RunResult(row_number=0, issue_id="", status="error", message="BPMIS returned a project without Issue ID."))
                continue
            brd_link = "\n".join(link.strip() for link in brd_links_by_issue_id.get(issue_id, []) if str(link or "").strip())
            status = self.store.upsert_project(
                user_key=user_key,
                bpmis_id=issue_id,
                project_name=project_name,
                brd_link=brd_link,
                market=market,
            )
            if status == "deleted":
                results.append(
                    RunResult(
                        row_number=0,
                        issue_id=issue_id,
                        status="skipped",
                        message="Skipped because this BPMIS project was deleted in the portal.",
                        project_label=project_name or issue_id,
                        matched_project_id=market or None,
                    )
                )
                continue
            results.append(
                RunResult(
                    row_number=0,
                    issue_id=issue_id,
                    status="created" if status == "created" else "updated",
                    message="Saved BPMIS project in the portal." if status == "created" else "Updated BPMIS project in the portal.",
                    project_label=project_name or issue_id,
                    matched_project_id=market or None,
                )
            )
        self._emit_progress(progress_callback, "completed", "BPMIS sync finished.", total, total)
        return results

    @staticmethod
    def _emit_progress(progress_callback, stage: str, message: str, current: int, total: int) -> None:
        if progress_callback is not None:
            progress_callback(stage, message, current, total)


class PortalJiraCreationService:
    def __init__(
        self,
        *,
        store: BPMISProjectStore,
        bpmis_client: BPMISClient,
        config_store: WebConfigStore,
        config_data: dict[str, Any],
    ):
        self.store = store
        self.bpmis_client = bpmis_client
        self.config_store = config_store
        self.config_data = config_data

    def jira_options(self, *, user_key: str, bpmis_id: str) -> dict[str, Any]:
        project = self.store.get_project(user_key=user_key, bpmis_id=bpmis_id)
        if project is None:
            raise ToolError("BPMIS project was not found.")
        route_rules = self.config_store._parse_component_route_rules(str(self.config_data.get("component_route_rules_text", "")))
        default_rules = self.config_store._parse_component_default_rules(str(self.config_data.get("component_default_rules_text", "")))
        if not route_rules:
            raise ToolError("System + Market to Component routing is required before creating Jira.")
        defaults_by_component = {rule["component"].strip().lower(): rule for rule in default_rules}
        components: dict[str, dict[str, Any]] = {}
        for rule in route_rules:
            component = rule["component"].strip()
            market = rule["market"].strip()
            system = rule["system"].strip()
            if not component or not market:
                continue
            item = components.setdefault(
                component,
                {
                    "component": component,
                    "markets": [],
                    "defaults": defaults_by_component.get(component.lower(), {}),
                },
            )
            item["markets"].append({"market": market, "system": system})

        return {
            "project": project,
            "components": sorted(components.values(), key=lambda item: item["component"].lower()),
        }

    def create_tickets(
        self,
        *,
        user_key: str,
        bpmis_id: str,
        items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        project = self.store.get_project(user_key=user_key, bpmis_id=bpmis_id)
        if project is None:
            raise ToolError("BPMIS project was not found.")
        if not items:
            raise ToolError("At least one Jira item is required.")

        options = self.jira_options(user_key=user_key, bpmis_id=bpmis_id)
        route_lookup: dict[tuple[str, str], str] = {}
        defaults_by_component: dict[str, dict[str, str]] = {}
        for component in options["components"]:
            component_name = str(component.get("component") or "").strip()
            defaults_by_component[component_name.lower()] = component.get("defaults") or {}
            for market_item in component.get("markets") or []:
                route_lookup[(component_name.lower(), str(market_item.get("market") or "").strip().lower())] = str(
                    market_item.get("system") or ""
                ).strip()

        results: list[dict[str, Any]] = []
        for item in items:
            component = str(item.get("component") or "").strip()
            market = str(item.get("market") or "").strip()
            system = route_lookup.get((component.lower(), market.lower()), "")
            if not component or not market or not system:
                results.append(
                    {
                        "status": "error",
                        "component": component,
                        "market": market,
                        "message": "Component and Market do not match saved routing.",
                    }
                )
                continue

            defaults = defaults_by_component.get(component.lower(), {})
            jira_title = str(item.get("jira_title") or "").strip()
            if not jira_title:
                jira_title = self.default_jira_title(project_name=str(project.get("project_name") or ""), system=system)
            fields = {
                "Task Type": str(self.config_data.get("task_type_value") or "Feature").strip() or "Feature",
                "Market": market,
                "System": system,
                "Summary": jira_title,
                "Component": component,
                "Assignee": str(defaults.get("assignee") or "").strip(),
                "Dev PIC": str(defaults.get("dev_pic") or "").strip(),
                "QA PIC": str(defaults.get("qa_pic") or "").strip(),
                "Fix Version": str(item.get("fix_version") or defaults.get("fix_version") or "").strip(),
                "Priority": str(self.config_data.get("priority_value") or "").strip(),
                "Product Manager": str(self.config_data.get("product_manager_value") or "").strip(),
                "Reporter": str(self.config_data.get("reporter_value") or "").strip(),
                "Biz PIC": str(self.config_data.get("biz_pic_value") or "").strip(),
                "Need UAT": self._need_uat_for_market(market),
                "PRD Link/s": str(item.get("prd_link") or "").strip(),
                "Description": str(item.get("description") or "").strip(),
            }
            fields = {key: value for key, value in fields.items() if value}

            try:
                ticket = self.bpmis_client.create_jira_ticket(
                    ProjectMatch(project_id=str(project.get("bpmis_id") or "")),
                    fields,
                    preformatted_summary=True,
                )
                stored = self.store.add_jira_ticket(
                    user_key=user_key,
                    bpmis_id=str(project.get("bpmis_id") or ""),
                    component=component,
                    market=market,
                    system=system,
                    jira_title=jira_title,
                    prd_link=fields.get("PRD Link/s", ""),
                    description=fields.get("Description", ""),
                    fix_version_name=fields.get("Fix Version", ""),
                    ticket_key=ticket.ticket_key or "",
                    ticket_link=ticket.ticket_link or ticket.ticket_key or "",
                    status="created",
                    message="Created Jira ticket successfully.",
                    raw_response=ticket.raw,
                )
                results.append({"status": "created", "ticket": stored, "component": component, "market": market})
            except BPMISError as error:
                results.append({"status": "error", "component": component, "market": market, "message": str(error)})
        return results

    def _need_uat_for_market(self, market: str) -> str:
        need_uat = self.config_data.get("need_uat_by_market", {})
        if not isinstance(need_uat, dict):
            return ""
        return str(need_uat.get(market, "") or "").strip()

    @staticmethod
    def default_jira_title(*, project_name: str, system: str) -> str:
        clean_project_name = str(project_name or "").strip()
        clean_system = str(system or "").strip()
        return f"[Feature][{clean_system}]{clean_project_name}" if clean_system else f"[Feature]{clean_project_name}"
