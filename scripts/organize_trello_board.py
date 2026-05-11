#!/usr/bin/env python3
"""Reorganize the personal Trello task board into workflow lists."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from bpmis_jira_tool.seatalk_daily_email import _trello_domain_labels
from bpmis_jira_tool.trello_daily_summary import (
    DEFAULT_DAILY_LIST_NAME,
    TRELLO_DOMAIN_LABELS,
    TRELLO_WORKFLOW_LIST_BACKLOG,
    TRELLO_WORKFLOW_LIST_DONE,
    TRELLO_WORKFLOW_LIST_FOLLOW_UP,
    TRELLO_WORKFLOW_LIST_INBOX,
    TRELLO_WORKFLOW_LIST_PERSONAL,
    TRELLO_WORKFLOW_LIST_THIS_WEEK,
    TRELLO_WORKFLOW_LIST_TODAY,
    TRELLO_WORKFLOW_LIST_WATCH,
    TrelloDailySummaryClient,
)


WORKFLOW_LISTS = [
    TRELLO_WORKFLOW_LIST_INBOX,
    TRELLO_WORKFLOW_LIST_TODAY,
    TRELLO_WORKFLOW_LIST_THIS_WEEK,
    TRELLO_WORKFLOW_LIST_FOLLOW_UP,
    TRELLO_WORKFLOW_LIST_WATCH,
    TRELLO_WORKFLOW_LIST_BACKLOG,
    TRELLO_WORKFLOW_LIST_PERSONAL,
    TRELLO_WORKFLOW_LIST_DONE,
]
DOMAIN_LIST_LABELS = {
    "ai": ("AI",),
    "grc / it pmo (rene)": ("GRC",),
    "anti-fraud - id / overall": ("AF-ID",),
    "anti fraud - sg": ("AF-SG",),
    "anti fraud - ph": ("AF-PH",),
    "credit risk & data": ("Credit Risk",),
}
PERSONAL_LISTS = {"home & financial", "others"}
LEGACY_EMPTY_LISTS = set(DOMAIN_LIST_LABELS) | PERSONAL_LISTS | {DEFAULT_DAILY_LIST_NAME.lower()}
PLACEHOLDER_ARCHIVE_NAMES = {"[direct] review rollout", "[direct] review rollout note"}
SENSITIVE_MARKERS = ("password", "token", "secret", "vpn")


def _normalize(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


WORKFLOW_LIST_BY_NORMALIZED_NAME = {_normalize(name): name for name in WORKFLOW_LISTS}


def _card_id(card: dict[str, Any]) -> str:
    return str(card.get("id") or "").strip()


def _card_name(card: dict[str, Any]) -> str:
    return str(card.get("name") or "").strip()


def _existing_card_label_names(card: dict[str, Any]) -> set[str]:
    names = set()
    for label in card.get("labels") or []:
        if isinstance(label, dict):
            name = str(label.get("name") or "").strip()
            if name:
                names.add(name)
    return names


def _target_list_for_card(card: dict[str, Any], current_list_name: str) -> str:
    name = _card_name(card)
    normalized_name = _normalize(name)
    current = _normalize(current_list_name)
    if normalized_name in PLACEHOLDER_ARCHIVE_NAMES:
        return "__archive__"
    if current in PERSONAL_LISTS:
        return TRELLO_WORKFLOW_LIST_PERSONAL
    if current == TRELLO_WORKFLOW_LIST_DONE.lower():
        return TRELLO_WORKFLOW_LIST_DONE
    if current == TRELLO_WORKFLOW_LIST_TODAY.lower():
        return TRELLO_WORKFLOW_LIST_TODAY
    if normalized_name.startswith("[follow-up]"):
        return TRELLO_WORKFLOW_LIST_FOLLOW_UP
    if normalized_name.startswith("[watch]"):
        return TRELLO_WORKFLOW_LIST_WATCH
    if normalized_name.startswith("[direct]"):
        return TRELLO_WORKFLOW_LIST_INBOX if not card.get("due") else TRELLO_WORKFLOW_LIST_THIS_WEEK
    if current == TRELLO_WORKFLOW_LIST_THIS_WEEK.lower():
        return TRELLO_WORKFLOW_LIST_THIS_WEEK
    if current in WORKFLOW_LIST_BY_NORMALIZED_NAME:
        return WORKFLOW_LIST_BY_NORMALIZED_NAME[current]
    if current in DOMAIN_LIST_LABELS or current == DEFAULT_DAILY_LIST_NAME.lower():
        return TRELLO_WORKFLOW_LIST_BACKLOG
    return TRELLO_WORKFLOW_LIST_BACKLOG


def _domain_labels_for_card(card: dict[str, Any], current_list_name: str) -> tuple[str, ...]:
    current = _normalize(current_list_name)
    name = _card_name(card)
    if current in PERSONAL_LISTS:
        labels = ["Personal"]
        if any(marker in name.lower() for marker in SENSITIVE_MARKERS):
            labels.append("Sensitive")
        return tuple(labels)
    if current in DOMAIN_LIST_LABELS:
        return DOMAIN_LIST_LABELS[current]
    if current in WORKFLOW_LIST_BY_NORMALIZED_NAME:
        return ()
    inferred = list(_trello_domain_labels("", name))
    return tuple(inferred)


def plan_board_reorganization(
    *,
    lists: list[dict[str, Any]],
    cards: list[dict[str, Any]],
    labels: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    open_lists = [item for item in lists if isinstance(item, dict) and not item.get("closed")]
    lists_by_norm = {_normalize(item.get("name")): item for item in open_lists}
    list_names_by_id = {str(item.get("id") or ""): str(item.get("name") or "") for item in open_lists}
    existing_label_names = {str(label.get("name") or "").strip() for label in labels if isinstance(label, dict) and str(label.get("name") or "").strip()}
    existing_label_names_lower = {name.lower() for name in existing_label_names}

    actions: dict[str, list[dict[str, Any]]] = {
        "create_lists": [],
        "rename_lists": [],
        "create_labels": [],
        "move_cards": [],
        "add_labels": [],
        "archive_cards": [],
    }

    for list_name in WORKFLOW_LISTS:
        existing = lists_by_norm.get(_normalize(list_name))
        if not existing:
            actions["create_lists"].append({"name": list_name})
        elif str(existing.get("name") or "") != list_name:
            actions["rename_lists"].append({"id": existing.get("id"), "from": existing.get("name"), "to": list_name})

    needed_labels = set(TRELLO_DOMAIN_LABELS)
    for card in cards:
        current_list_name = list_names_by_id.get(str(card.get("idList") or ""))
        if not current_list_name:
            continue
        if _target_list_for_card(card, current_list_name) == "__archive__":
            actions["archive_cards"].append({"id": _card_id(card), "name": _card_name(card)})
            continue
        target = _target_list_for_card(card, current_list_name)
        if _normalize(current_list_name) != _normalize(target):
            actions["move_cards"].append({"id": _card_id(card), "name": _card_name(card), "from": current_list_name, "to": target})
        existing_names = _existing_card_label_names(card)
        for label_name in _domain_labels_for_card(card, current_list_name):
            needed_labels.add(label_name)
            if label_name not in existing_names:
                actions["add_labels"].append({"id": _card_id(card), "name": _card_name(card), "label": label_name})

    for label_name in sorted(needed_labels):
        if label_name.lower() not in existing_label_names_lower:
            actions["create_labels"].append({"name": label_name, "color": TRELLO_DOMAIN_LABELS.get(label_name, "blue")})
    return actions


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def _print_plan(actions: dict[str, list[dict[str, Any]]]) -> None:
    for group in ("create_lists", "rename_lists", "create_labels", "archive_cards", "move_cards", "add_labels"):
        items = actions.get(group) or []
        print(f"{group}: {len(items)}")
        for item in items:
            print(f"  - {json.dumps(item, ensure_ascii=False, sort_keys=True)}")


def _apply_plan(client: TrelloDailySummaryClient, actions: dict[str, list[dict[str, Any]]]) -> None:
    for item in actions["create_lists"]:
        client.get_or_create_list_id(str(item["name"]))
    for item in actions["rename_lists"]:
        client.rename_list(list_id=str(item["id"]), name=str(item["to"]))
    label_ids = {label.get("name"): client.get_or_create_label_id(str(label.get("name")), color=str(label.get("color") or "blue")) for label in actions["create_labels"]}
    for label in client.board_labels():
        name = str(label.get("name") or "").strip()
        if name:
            label_ids.setdefault(name, str(label.get("id") or "").strip())
    target_list_ids = {list_name: client.get_or_create_list_id(list_name) for list_name in WORKFLOW_LISTS}
    for item in actions["archive_cards"]:
        client.archive_card(card_id=str(item["id"]))
    for item in actions["move_cards"]:
        client.move_card(card_id=str(item["id"]), list_id=target_list_ids[str(item["to"])])
    for item in actions["add_labels"]:
        label_id = label_ids.get(str(item["label"]))
        if label_id:
            client.add_label_to_card(card_id=str(item["id"]), label_id=label_id)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print planned Trello changes without applying them.")
    parser.add_argument("--apply", action="store_true", help="Apply the Trello reorganization.")
    parser.add_argument("--env-file", default=".env", help="Optional env file with Trello API settings.")
    args = parser.parse_args(argv)
    if bool(args.dry_run) == bool(args.apply):
        parser.error("Choose exactly one of --dry-run or --apply.")

    _load_env_file(Path(args.env_file))
    client = TrelloDailySummaryClient.from_env()
    actions = plan_board_reorganization(lists=client.board_lists(), cards=client.list_board_cards(), labels=client.board_labels())
    _print_plan(actions)
    if args.apply:
        _apply_plan(client, actions)
        print("Applied Trello board reorganization.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
