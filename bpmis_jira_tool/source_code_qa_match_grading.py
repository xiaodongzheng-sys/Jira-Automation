from __future__ import annotations

import re
from typing import Any


def match_is_definition_only(match: dict[str, Any], focus_terms: list[str]) -> bool:
    del focus_terms
    path = str(match.get("path") or "").lower()
    snippet = str(match.get("snippet") or "").lower()
    if any(term in path for term in ("enum", "constant", "constants")):
        return True
    if re.search(r"\b(enum|interface)\s+\w+", snippet):
        return True
    if re.search(r"\b(class|public|private|protected)\s+\w+", snippet) and not any(marker in snippet for marker in ("select ", "insert ", "update ", "delete ", "repository", "mapper", "client", "controller")):
        return True
    return False


def evidence_role(path: str, snippet: str, reason: str) -> str:
    lowered_path = str(path or "").lower()
    lowered = f"{snippet or ''} {reason or ''}".lower()
    if any(marker in lowered_path for marker in ("mapper", "repository", "dao")) or re.search(r"\bselect\b.+\bfrom\b", lowered):
        return "data_source"
    if any(marker in lowered_path for marker in ("client", "controller", "api")) or any(marker in lowered for marker in ("requestmapping", "postmapping", "getmapping", "resttemplate", "webclient", "feign")):
        return "api"
    if "config" in lowered_path or "properties" in lowered_path or "apollo" in lowered:
        return "config"
    if "test" in lowered_path:
        return "test"
    if any(marker in lowered for marker in ("enum", "constant")):
        return "definition"
    return "logic"


def match_answer_grade(match: dict[str, Any], *, intent_label: str = "general") -> bool:
    path = str(match.get("path") or "").lower()
    snippet = str(match.get("snippet") or "")
    reason = str(match.get("reason") or "")
    role = evidence_role(path, snippet, reason)
    if intent_label == "data_source":
        return role in {"data_source", "api", "config"}
    return role not in {"definition", "test"}
