"""Source Code QA Effort Assessment prompt, cache, evidence, and job helpers."""
from __future__ import annotations

from functools import lru_cache


def _bind_web_globals(functions: list[object], global_context: dict[str, object]) -> None:
    for function in functions:
        target = getattr(function, "__wrapped__", function)
        globals_dict = getattr(target, "__globals__", None)
        if globals_dict is not None:
            globals_dict.update(global_context)


def _source_code_qa_effort_assessment_language(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"en", "english"}:
        return "en"
    return "zh"


def _source_code_qa_effort_sentences(text: str) -> list[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    parts = re.split(r"(?<=[。！？!?])\s+|\n+", raw)
    return [part.strip() for part in parts if part.strip()]


def _source_code_qa_effort_matches(text: str, patterns: list[str]) -> bool:
    lowered = str(text or "").lower()
    return any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in patterns)


def _source_code_qa_effort_unique(items: list[Any]) -> list[Any]:
    seen: set[str] = set()
    output: list[Any] = []
    for item in items:
        if item is None:
            continue
        key = json.dumps(item, ensure_ascii=False, sort_keys=True) if isinstance(item, dict) else str(item)
        if key in seen:
            continue
        seen.add(key)
        output.append(item)
    return output


def _source_code_qa_load_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


@lru_cache(maxsize=1)
def _load_source_code_qa_effort_dictionaries() -> dict[str, Any]:
    return _source_code_qa_load_json_file(SOURCE_CODE_QA_EFFORT_DICTIONARY_PATH)


@lru_cache(maxsize=1)
def _load_source_code_qa_domain_profile_config() -> dict[str, Any]:
    return _source_code_qa_load_json_file(SOURCE_CODE_QA_DOMAIN_PROFILES_PATH)


@lru_cache(maxsize=1)
def _load_source_code_qa_domain_knowledge_config() -> dict[str, Any]:
    return _source_code_qa_load_json_file(SOURCE_CODE_QA_DOMAIN_KNOWLEDGE_PATH)


def _source_code_qa_effort_domain_entries(pm_team: str) -> list[dict[str, Any]]:
    dictionaries = _load_source_code_qa_effort_dictionaries()
    domain = ((dictionaries.get("domains") or {}).get(str(pm_team or "").upper()) or {})
    entries = domain.get("entries") if isinstance(domain, dict) else []
    entries = entries if isinstance(entries, list) else []
    return [entry for entry in entries if isinstance(entry, dict)]


def _source_code_qa_effort_country_hint(requirement: str, fallback: str) -> str:
    text = f" {str(requirement or '').lower()} "
    if re.search(r"(?<![a-z0-9])sg(?![a-z0-9])", text) or "singapore" in text:
        return "SG"
    if re.search(r"(?<![a-z0-9])id(?![a-z0-9])", text) or "indonesia" in text:
        return "ID"
    if re.search(r"(?<![a-z0-9])ph(?![a-z0-9])", text) or "philippines" in text:
        return "PH"
    return str(fallback or "").strip() or "All"


def _source_code_qa_effort_term_matches(text: str, term: str) -> bool:
    normalized_text = str(text or "").lower()
    normalized_term = str(term or "").strip().lower()
    if not normalized_term:
        return False
    if len(normalized_term) <= 3 and re.fullmatch(r"[a-z0-9]+", normalized_term):
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(normalized_term)}(?![a-z0-9])", normalized_text))
    return normalized_term in normalized_text


def _source_code_qa_effort_scope_terms_by_team() -> dict[str, list[str]]:
    teams = set(SOURCE_CODE_QA_EFFORT_SCOPE_ALIASES)
    dictionaries = _load_source_code_qa_effort_dictionaries()
    teams.update(str(team or "").upper() for team in ((dictionaries.get("domains") or {}).keys()))
    knowledge = _load_source_code_qa_domain_knowledge_config()
    teams.update(str(team or "").upper() for team in (((knowledge.get("domains") or {}) if isinstance(knowledge, dict) else {}).keys()))
    terms_by_team: dict[str, list[str]] = {}
    for team in sorted(team for team in teams if team):
        terms: list[str] = list(SOURCE_CODE_QA_EFFORT_SCOPE_ALIASES.get(team, []))
        for entry in _source_code_qa_effort_domain_entries(team):
            terms.append(str(entry.get("id") or ""))
            for key in ("business_aliases", "technical_terms", "product_terms", "limit_terms", "evidence_hints"):
                terms.extend(str(item) for item in (entry.get(key) or []) if item)
        domain = ((knowledge.get("domains") or {}).get(team) or {}) if isinstance(knowledge, dict) else {}
        if isinstance(domain, dict):
            for module in domain.get("module_map") or []:
                if not isinstance(module, dict):
                    continue
                terms.append(str(module.get("name") or ""))
                for key in ("aliases", "repo_hints", "code_hints", "business_flows"):
                    terms.extend(str(item) for item in (module.get(key) or []) if item)
            for item in domain.get("terminology") or []:
                if not isinstance(item, dict):
                    continue
                terms.append(str(item.get("term") or ""))
                terms.extend(str(value) for value in (item.get("aliases") or []) if value)
                terms.extend(str(value) for value in (item.get("code_terms") or []) if value)
            retrieval_terms = domain.get("retrieval_terms") if isinstance(domain.get("retrieval_terms"), dict) else {}
            for values in retrieval_terms.values():
                terms.extend(str(item) for item in (values or []) if item)
        filtered = []
        for term in _source_code_qa_effort_unique(terms):
            normalized = str(term or "").strip()
            lowered = normalized.lower()
            if len(lowered) < 3 or lowered in SOURCE_CODE_QA_EFFORT_SCOPE_COMMON_TERMS:
                continue
            filtered.append(normalized)
        terms_by_team[team] = filtered[:120]
    return terms_by_team


def _source_code_qa_effort_scope_guard(
    *,
    pm_team: str,
    country: str,
    requirement: str,
) -> dict[str, Any]:
    selected_team = str(pm_team or "").strip().upper()
    terms_by_team = _source_code_qa_effort_scope_terms_by_team()
    scores: dict[str, int] = {}
    matched_terms: dict[str, list[str]] = {}
    for team, terms in terms_by_team.items():
        score = 0
        hits: list[str] = []
        for term in terms:
            if not _source_code_qa_effort_term_matches(requirement, term):
                continue
            lowered = str(term).lower()
            explicit = lowered in {item.lower() for item in SOURCE_CODE_QA_EFFORT_SCOPE_ALIASES.get(team, [])}
            score += 6 if explicit else 2
            hits.append(str(term))
        if selected_team == team and selected_team and _source_code_qa_effort_term_matches(requirement, selected_team):
            score += 8
            hits.append(selected_team)
        scores[team] = score
        matched_terms[team] = _source_code_qa_effort_unique(hits)[:12]
    best_team = max(scores, key=lambda key: scores.get(key, 0), default=selected_team)
    selected_score = scores.get(selected_team, 0)
    best_score = scores.get(best_team, 0)
    mismatch = (
        bool(selected_team)
        and bool(best_team)
        and best_team != selected_team
        and best_score >= 10
        and best_score >= selected_score + 6
        and best_score >= selected_score * 2 + 4
    )
    return {
        "status": "mismatch" if mismatch else "ok",
        "selected_pm_team": selected_team,
        "selected_country": str(country or "").strip() or "All",
        "suggested_pm_team": best_team if mismatch else selected_team,
        "suggested_country": _source_code_qa_effort_country_hint(requirement, country) if mismatch else str(country or "").strip() or "All",
        "scores": scores,
        "matched_terms": matched_terms,
        "reason": (
            f"Requirement terms match {best_team} more strongly than selected {selected_team}."
            if mismatch
            else "Selected scope is compatible with requirement terms."
        ),
    }


def _source_code_qa_effort_scope_mismatch_result(
    *,
    pm_team: str,
    country: str,
    language: str,
    requirement: str,
    llm_provider: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
    scope_guard: dict[str, Any],
) -> dict[str, Any]:
    suggested_team = str(scope_guard.get("suggested_pm_team") or "").strip() or "the correct PM team"
    suggested_country = str(scope_guard.get("suggested_country") or "").strip() or "the correct country"
    matched = ", ".join(str(item) for item in (scope_guard.get("matched_terms") or {}).get(suggested_team, [])[:8])
    if language == "zh":
        answer = "\n".join(
            [
                "Scope Mismatch / 选择范围不匹配",
                f"当前选择的是 {pm_team}:{country}，但需求文本更像 {suggested_team}:{suggested_country} 的改动。",
                f"命中的范围信号: {matched or '未记录'}。",
                f"请切换 PM Team 到 {suggested_team}、Country 到 {suggested_country} 后重新运行 Effort Assessment。",
                "这次不会基于当前 repo 生成 BE/FE 人天估算，避免把错误代码库里的能力硬套到需求上。",
            ]
        )
    else:
        answer = "\n".join(
            [
                "Scope Mismatch",
                f"The selected scope is {pm_team}:{country}, but the requirement matches {suggested_team}:{suggested_country} more strongly.",
                f"Matched scope signals: {matched or 'not recorded'}.",
                f"Switch PM Team to {suggested_team} and Country to {suggested_country}, then run Effort Assessment again.",
                "No BE/FE estimate was generated from the selected repository scope to avoid misleading output.",
            ]
        )
    missing_evidence = [
        f"Selected repository scope {pm_team}:{country} does not match requirement signals for {suggested_team}:{suggested_country}."
    ]
    return {
        "status": "scope_mismatch",
        "summary": "Selected repository scope does not match the requirement.",
        "llm_answer": answer,
        "llm_provider": llm_provider or "default",
        "llm_model": "",
        "trace_id": f"effort-scope-{hashlib.sha1(str(requirement or '').encode('utf-8')).hexdigest()[:12]}",
        "matches": [],
        "citations": [],
        "missing_evidence": missing_evidence,
        "assessment_confidence": "scope_mismatch",
        "effort_evidence_status": "scope_mismatch",
        "effort_scope_guard": scope_guard,
        "effort_evidence_matrix": {"version": 1, "groups": [], "quality": {"confirmed_group_count": 0, "inferred_group_count": 0, "missing_group_count": 0, "status": "scope_mismatch"}},
        "effort_evidence_matrix_quality": {"confirmed_group_count": 0, "inferred_group_count": 0, "missing_group_count": 0, "status": "scope_mismatch"},
        "effort_generic_output_guard": {"status": "blocked", "issues": ["scope_mismatch"], "confirmed_or_inferred_group_count": 0},
        "effort_timing": {"cache_hit": False, "scope_guard": scope_guard},
        "structured_assessment": {
            "version": 2,
            "language": language,
            "confidence": "scope_mismatch",
            "business_understanding": business_plan,
            "code_change_points": [],
            "be_estimate": [],
            "fe_estimate": [],
            "confirmed_evidence": [],
            "inferred_impact": [],
            "missing_evidence": missing_evidence,
            "questions": [f"Should this request be assessed under {suggested_team}:{suggested_country}?"],
        },
        "assessment": {
            "type": "effort_assessment",
            "pm_team": pm_team,
            "country": country,
            "language": language,
            "requirement": requirement,
            "business_plan": business_plan,
            "technical_candidates": technical_candidates,
            "estimation_rubric": estimation_rubric,
            "structured_assessment": {},
            "confidence": "scope_mismatch",
            "missing_evidence": missing_evidence,
            "evidence_status": "scope_mismatch",
            "scope_guard": scope_guard,
        },
    }


def _source_code_qa_effort_seed_terms(pm_team: str) -> list[str]:
    team = str(pm_team or "").upper()
    terms: list[str] = []
    profiles = _load_source_code_qa_domain_profile_config()
    profile = profiles.get(team) if isinstance(profiles, dict) else {}
    if isinstance(profile, dict):
        for key in ("data_carriers", "source_terms", "api_terms", "config_terms", "logic_terms", "field_population_terms"):
            terms.extend(str(item) for item in (profile.get(key) or []) if item)
    knowledge = _load_source_code_qa_domain_knowledge_config()
    domain = ((knowledge.get("domains") or {}).get(team) or {}) if isinstance(knowledge, dict) else {}
    if isinstance(domain, dict):
        for module in domain.get("module_map") or []:
            if not isinstance(module, dict):
                continue
            terms.append(str(module.get("name") or ""))
            terms.extend(str(item) for item in (module.get("aliases") or []) if item)
            terms.extend(str(item) for item in (module.get("code_hints") or []) if item)
        for term in domain.get("terminology") or []:
            if not isinstance(term, dict):
                continue
            terms.append(str(term.get("term") or ""))
            terms.extend(str(item) for item in (term.get("aliases") or []) if item)
            terms.extend(str(item) for item in (term.get("code_terms") or []) if item)
        retrieval_terms = domain.get("retrieval_terms") if isinstance(domain.get("retrieval_terms"), dict) else {}
        for values in retrieval_terms.values():
            terms.extend(str(item) for item in (values or []) if item)
    return [str(item) for item in _source_code_qa_effort_unique([item for item in terms if str(item or "").strip()])]


def _source_code_qa_effort_entry_applies(entry: dict[str, Any], *, country: str, requirement: str) -> bool:
    countries = [str(item).upper() for item in (entry.get("country_terms") or []) if item]
    if countries and str(country or "").upper() not in countries:
        return False
    aliases = [str(item) for item in (entry.get("business_aliases") or []) if str(item or "").strip()]
    technical_terms = [str(item) for item in (entry.get("technical_terms") or []) if str(item or "").strip()]
    haystack = str(requirement or "").lower()
    for value in [*aliases, *technical_terms]:
        normalized = value.lower().strip()
        if not normalized:
            continue
        if normalized in haystack:
            return True
    return False


def _source_code_qa_effort_group_typed_candidates(entries: list[dict[str, Any]], *, seed_terms: list[str]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {
        "backend_service": [],
        "frontend_surface": [],
        "table_or_config": [],
        "workflow_rule": [],
        "downstream_reporting": [],
    }
    for entry in entries:
        terms = [str(item) for item in (entry.get("technical_terms") or []) if item]
        for surface in entry.get("surfaces") or []:
            surface_key = str(surface or "").strip()
            if surface_key in grouped:
                grouped[surface_key].extend(terms)
    if seed_terms:
        grouped["backend_service"].extend(seed_terms[:30])
    return {key: _source_code_qa_effort_unique(values)[:60] for key, values in grouped.items()}


def _build_source_code_qa_effort_business_plan(
    *,
    pm_team: str,
    country: str,
    language: str,
    requirement: str,
) -> dict[str, Any]:
    raw_requirement = str(requirement or "").strip()
    sentences = _source_code_qa_effort_sentences(raw_requirement)
    option_matches = list(
        re.finditer(
            r"(方案\s*[一二12]|option\s*[12])\s*[:：]?\s*(.*?)(?=(?:方案\s*[一二12]|option\s*[12])\s*[:：]|$)",
            raw_requirement,
            flags=re.IGNORECASE | re.DOTALL,
        )
    )
    options = []
    for index, match in enumerate(option_matches, start=1):
        body = re.sub(r"\s+", " ", match.group(2).strip())
        if body:
            options.append({"id": f"option_{index}", "label": match.group(1).strip(), "summary": body[:1200]})
    has_explicit_options = bool(options)
    if not options:
        options.append({"id": "option_1", "label": "single proposed change", "summary": raw_requirement[:1200]})

    user_segments = []
    if _source_code_qa_effort_matches(raw_requirement, [r"高收入", r"annual\s*income", r"income\s*[><=]", r"120k"]):
        user_segments.append("high income customers")
    if _source_code_qa_effort_matches(raw_requirement, [r"好用户", r"good\s+customer", r"premium"]):
        user_segments.append("qualified or good customers")

    products = []
    if _source_code_qa_effort_matches(raw_requirement, [r"信用卡", r"credit\s*card"]):
        products.append("credit card")
    if _source_code_qa_effort_matches(raw_requirement, [r"现金分期", r"cash\s*installment", r"cash\s*instalment"]):
        products.append("cash installment")
    if _source_code_qa_effort_matches(raw_requirement, [r"cashline", r"cash\s*line"]):
        products.append("cashline")

    limit_types = []
    if _source_code_qa_effort_matches(raw_requirement, [r"额度", r"limit"]):
        limit_types.append("limit amount")
    if _source_code_qa_effort_matches(raw_requirement, [r"信用卡.*额度", r"credit\s*card.*limit"]):
        limit_types.append("credit card limit")
    if _source_code_qa_effort_matches(raw_requirement, [r"现金分期.*(专项)?额度", r"cash\s*installment.*limit"]):
        limit_types.append("cash installment dedicated limit")
    if _source_code_qa_effort_matches(raw_requirement, [r"103", r"104", r"sub\s*product", r"子产品"]):
        limit_types.append("sub-product limit 103/104")

    flow_changes = []
    if _source_code_qa_effort_matches(raw_requirement, [r"申请", r"apply", r"application"]):
        flow_changes.append("application flow")
    if _source_code_qa_effort_matches(raw_requirement, [r"报送", r"submission", r"reporting"]):
        flow_changes.append("reporting or downstream submission")
    if _source_code_qa_effort_matches(raw_requirement, [r"用户教育", r"感知", r"引导", r"education", r"guide"]):
        flow_changes.append("user education or guidance")

    decision_points = [
        sentence[:800]
        for sentence in sentences
        if _source_code_qa_effort_matches(sentence, [r"核心问题", r"是否", r"是不是", r"可以讨论", r"确认", r"how", r"whether"])
    ]
    goals = []
    if _source_code_qa_effort_matches(raw_requirement, [r"策略区分", r"区分", r"separate", r"differentiat"]):
        goals.append("differentiate limit strategy by customer/product context")
    if _source_code_qa_effort_matches(raw_requirement, [r"感知", r"转化", r"教育", r"conversion"]):
        goals.append("make the limit or product path understandable to customers")
    if not goals:
        goals.append("assess technical impact for the requested business change")

    return {
        "raw_requirement": raw_requirement,
        "pm_team": pm_team,
        "country": country,
        "language": language,
        "business_goals": _source_code_qa_effort_unique(goals),
        "options": options,
        "has_explicit_options": has_explicit_options,
        "user_segments": _source_code_qa_effort_unique(user_segments),
        "products": _source_code_qa_effort_unique(products),
        "limit_types": _source_code_qa_effort_unique(limit_types),
        "flow_changes": _source_code_qa_effort_unique(flow_changes),
        "decision_points": _source_code_qa_effort_unique(decision_points)[:8],
    }


def _build_source_code_qa_effort_technical_candidates(
    *,
    pm_team: str,
    country: str,
    business_plan: dict[str, Any],
    requirement: str,
) -> dict[str, Any]:
    raw_requirement = str(requirement or "")
    seed_terms = _source_code_qa_effort_seed_terms(pm_team)
    domain_entries = _source_code_qa_effort_domain_entries(pm_team)
    matched_entries = [
        entry for entry in domain_entries
        if _source_code_qa_effort_entry_applies(entry, country=country, requirement=raw_requirement)
    ]
    terms = [
        "limit",
        "credit limit",
        "product limit",
        "sub product limit",
        "API",
        "config",
        "strategy",
        "workflow",
        "front end screen",
        "application flow",
    ]
    backend_surfaces = ["API validation", "service strategy", "workflow decision rule", "config lookup"]
    frontend_surfaces = ["limit display", "application entry", "customer guidance copy"]
    configs_or_tables = ["limitAmount", "productCode", "productType", "subProductCode"]
    product_terms = []
    limit_terms = []
    evidence_hints = []
    domain_notes = []
    for entry in matched_entries:
        terms.extend(str(item) for item in (entry.get("technical_terms") or []) if item)
        product_terms.extend(str(item) for item in (entry.get("product_terms") or []) if item)
        limit_terms.extend(str(item) for item in (entry.get("limit_terms") or []) if item)
        evidence_hints.extend(str(item) for item in (entry.get("evidence_hints") or []) if item)
        for surface in entry.get("surfaces") or []:
            surface_value = str(surface or "")
            if surface_value == "backend_service":
                backend_surfaces.append(str(entry.get("id") or "backend service impact"))
            elif surface_value == "frontend_surface":
                frontend_surfaces.append(str(entry.get("id") or "frontend impact"))
            elif surface_value in {"table_or_config", "downstream_reporting"}:
                configs_or_tables.extend(str(item) for item in (entry.get("technical_terms") or []) if item)
    terms.extend(seed_terms[:80])

    if str(pm_team or "").upper() == "CRMS":
        backend_surfaces.extend(
            [
                "CRMS underwriting and eligibility decision",
                "borrower/product/sub-product limit calculation",
                "cash installment limit strategy",
                "credit card daily consumption limit strategy",
                "cashline application or redirect flow",
                "downstream reporting payload",
            ]
        )
        frontend_surfaces.extend(
            [
                "credit card and cash installment limit display",
                "cashline application entry",
                "limit explanation and customer education",
            ]
        )
        domain_notes.append("CRMS dictionary v1: income-based credit, cash installment, cashline, and product/sub-product limits.")

    for key in ("products", "limit_types", "flow_changes"):
        for value in business_plan.get(key) or []:
            terms.append(str(value))
    if _source_code_qa_effort_matches(raw_requirement, [r"报送", r"submission", r"reporting"]):
        terms.extend(["reporting", "submission", "report payload"])
    if _source_code_qa_effort_matches(raw_requirement, [r"前端", r"展示", r"入口", r"引导", r"screen", r"display", r"entry"]):
        terms.extend(["screen", "display", "entry point", "guide copy"])

    return {
        "pm_team": pm_team,
        "country": country,
        "search_terms": _source_code_qa_effort_unique(terms)[:80],
        "backend_surfaces": _source_code_qa_effort_unique(backend_surfaces)[:40],
        "frontend_surfaces": _source_code_qa_effort_unique(frontend_surfaces)[:30],
        "configs_or_tables": _source_code_qa_effort_unique(configs_or_tables)[:50],
        "product_terms": _source_code_qa_effort_unique(product_terms)[:40],
        "limit_terms": _source_code_qa_effort_unique(limit_terms)[:40],
        "evidence_hints": _source_code_qa_effort_unique(evidence_hints)[:40],
        "matched_dictionary_entries": [str(entry.get("id") or "") for entry in matched_entries if entry.get("id")],
        "typed_candidates": _source_code_qa_effort_group_typed_candidates(matched_entries, seed_terms=seed_terms),
        "domain_notes": domain_notes,
    }


def _build_source_code_qa_effort_estimation_rubric(
    *,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
) -> dict[str, Any]:
    text = " ".join(
        [
            " ".join(str(item) for item in business_plan.get("products") or []),
            " ".join(str(item) for item in business_plan.get("limit_types") or []),
            " ".join(str(item) for item in business_plan.get("flow_changes") or []),
            " ".join(str(item) for item in technical_candidates.get("backend_surfaces") or []),
            " ".join(str(item) for item in technical_candidates.get("frontend_surfaces") or []),
        ]
    )
    high_complexity = _source_code_qa_effort_matches(
        text,
        [r"underwriting", r"borrower", r"sub[-\s]?product", r"reporting", r"submission", r"授信", r"额度模型"],
    )
    medium_complexity = _source_code_qa_effort_matches(text, [r"api", r"service", r"strategy", r"workflow", r"limit"])
    frontend_required = bool(technical_candidates.get("frontend_surfaces"))
    option_estimates = []
    for index, option in enumerate(business_plan.get("options") or [], start=1):
        option_text = str(option.get("summary") or "")
        option_high = high_complexity or _source_code_qa_effort_matches(option_text, [r"cashline", r"独立", r"报送", r"模型", r"多产品"])
        option_medium = medium_complexity or _source_code_qa_effort_matches(option_text, [r"额度", r"limit", r"策略", r"rule"])
        be_range = "8-15 PD" if option_high else ("3-6 PD" if option_medium else "1-3 PD")
        fe_range = "3-6 PD" if _source_code_qa_effort_matches(option_text, [r"用户教育", r"感知", r"入口", r"展示", r"guide", r"display"]) else ("1-3 PD" if frontend_required else "0-1 PD")
        option_estimates.append(
            {
                "id": option.get("id") or f"option_{index}",
                "label": option.get("label") or f"Option {index}",
                "be_person_days": be_range,
                "fe_person_days": fe_range,
                "basis": "planning-grade estimate before Dev final sizing",
            }
        )
    return {
        "rules": [
            "Config or rule parameter only: low complexity.",
            "BE API plus service, strategy, or limit-flow change: medium complexity.",
            "Underwriting engine, limit model, reporting, or multi-product limit linkage: high complexity.",
            "FE display, guidance, application entry, and customer education are estimated separately.",
            "Final answer must separate confirmed evidence, inferred impact, and missing evidence.",
        ],
        "complexity_drivers": {
            "backend": "high" if high_complexity else ("medium" if medium_complexity else "low"),
            "frontend": "medium" if frontend_required else "low",
        },
        "option_estimates": option_estimates,
    }


def _source_code_qa_effort_json_block(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)[:12000]


def _build_source_code_qa_effort_assessment_prompt(
    *,
    pm_team: str,
    country: str,
    language: str,
    requirement: str,
    llm_provider: str,
    runtime_evidence: list[dict[str, Any]],
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
) -> str:
    team_label = str((TEAM_PROFILE_DEFAULTS.get(pm_team) or {}).get("label") or pm_team or "Selected PM Team").strip()
    output_language = "Chinese" if language == "zh" else "English"
    runtime_items = [
        item for item in runtime_evidence
        if isinstance(item, dict)
    ]
    runtime_summary = ", ".join(
        sorted(
            {
                f"{item.get('pm_team') or pm_team}:{item.get('country') or country}:{item.get('source_type') or 'runtime'}"
                for item in runtime_items
            }
        )
    ) or "none"
    raw_requirement = str(requirement or "").strip()[:8000]
    has_explicit_options = bool(business_plan.get("has_explicit_options"))
    technical_change_section = "方案 1/2 代码改动点" if has_explicit_options else "代码改动点"
    option_rule = (
        "- The requirement contains explicit alternatives; keep the original option labels and compare each option separately."
        if has_explicit_options
        else "- The requirement does not contain explicit Option 1/Option 2 alternatives; do not invent option labels. Use 'proposed change' instead."
    )
    return "\n".join(
        [
            "You are performing a Source Code Q&A Effort Assessment for a new business requirement.",
            "",
            "Context:",
            f"- PM Team: {pm_team} ({team_label})",
            f"- Country: {country}",
            f"- Answer language: {output_language}",
            f"- Selected model provider: {llm_provider or 'default'}",
            f"- Runtime evidence available: {len(runtime_items)} item(s): {runtime_summary}",
            "",
            "Original business requirement, verbatim:",
            raw_requirement,
            "",
            "Business plan extracted from the requirement:",
            _source_code_qa_effort_json_block(business_plan),
            "",
            "Technical candidates for repository evidence search:",
            _source_code_qa_effort_json_block(technical_candidates),
            "",
            "Estimation rubric:",
            _source_code_qa_effort_json_block(estimation_rubric),
            "",
            "Optimized assessment task:",
            "- Use the business plan and technical candidates as the focused search map. Do not rely only on the original business wording.",
            "- Map the requirement to likely impacted repositories, modules, files, APIs, tables, configs, scheduled jobs, front-end screens and components, and tests.",
            "- Use current source-code evidence internally as the basis for implementation impact and person-day estimates.",
            "- If exact table or path lookup misses, record it as a warning and continue with focused technical-candidate search.",
            "- Use runtime evidence only as supporting context.",
            "- Translate technical findings into business-readable change points. Do not expose evidence, citations, or file-path proof lists in the visible final answer.",
            "- Estimate BE and FE work as ranges in person-days. Use 0 person-days if no FE or BE change is found, but explain why.",
            "- Include QA/test and integration impact in the relevant BE/FE estimate notes instead of creating a third estimate bucket.",
            "",
            "Required output sections:",
            "1. 业务理解 / Business Understanding",
            f"2. {technical_change_section} / Code Change Points",
            "3. BE 人天 / BE Person-days",
            "4. FE 人天 / FE Person-days",
            "5. QA / Integration Impact",
            "6. Assumptions / Risks",
            "7. Confirmation Questions",
            "",
            "Output rules:",
            f"- Write the final answer in {output_language}.",
            option_rule,
            "- Keep the answer concise but specific enough for PM and engineering planning.",
            "- Do not include visible sections named Evidence, Source / Runtime Evidence, Confirmed / Inferred / Missing Evidence, or Missing Evidence.",
            "- Do not include source citations, S-id references such as [S1], file-path proof lists, or runtime-evidence filenames in the final answer.",
            "- Code change points must be understandable to business users: describe behavior, process, rule, UI, API, data, integration, and testing changes before technical names.",
            "- Person-day estimates must be ranges such as 1-2 PD or 3-5 PD, with one sentence explaining the driver for each range.",
            "- If source evidence is weak, still estimate with low confidence and state the planning assumption without adding an evidence section.",
        ]
    )


def _source_code_qa_effort_compact_terms(technical_candidates: dict[str, Any], requirement: str) -> list[str]:
    terms: list[str] = []
    for key in ("search_terms", "configs_or_tables", "product_terms", "limit_terms", "evidence_hints"):
        terms.extend(str(item) for item in (technical_candidates.get(key) or []) if str(item or "").strip())
    typed_candidates = technical_candidates.get("typed_candidates") if isinstance(technical_candidates.get("typed_candidates"), dict) else {}
    for values in typed_candidates.values():
        terms.extend(str(item) for item in (values or []) if str(item or "").strip())
    terms.extend(IDENTIFIER_PATTERN.findall(str(requirement or "")))
    return [str(item) for item in _source_code_qa_effort_unique(terms) if str(item or "").strip()][:36]


def _build_source_code_qa_effort_evidence_query(
    *,
    requirement: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
) -> str:
    goals = ", ".join(str(item) for item in (business_plan.get("business_goals") or [])[:4])
    products = ", ".join(str(item) for item in (business_plan.get("products") or [])[:6])
    limit_types = ", ".join(str(item) for item in (business_plan.get("limit_types") or [])[:6])
    flow_changes = ", ".join(str(item) for item in (business_plan.get("flow_changes") or [])[:6])
    terms = ", ".join(_source_code_qa_effort_compact_terms(technical_candidates, requirement)[:28])
    dictionary_entries = ", ".join(str(item) for item in (technical_candidates.get("matched_dictionary_entries") or [])[:10])
    return "\n".join(
        [
            "Effort assessment evidence lookup. Find current source-code evidence for implementation impact.",
            f"Requirement summary: {str(requirement or '').strip()[:1200]}",
            f"Business goals: {goals or 'planning-grade technical impact assessment'}",
            f"Products: {products or 'n/a'}",
            f"Limit/flow terms: {', '.join(item for item in (limit_types, flow_changes) if item) or 'n/a'}",
            f"Technical search terms: {terms or 'n/a'}",
            f"Dictionary hits: {dictionary_entries or 'none'}",
            "Focus on impacted APIs, services, strategies, configs/tables, frontend screens, tests, and downstream/reporting paths.",
        ]
    )


def _source_code_qa_effort_evidence_digest(evidence_result: dict[str, Any]) -> dict[str, Any]:
    matches = evidence_result.get("matches") if isinstance(evidence_result.get("matches"), list) else []
    return {
        "index_freshness": evidence_result.get("index_freshness") or {},
        "matches": [
            {
                "repo": match.get("repo"),
                "path": match.get("path"),
                "line_start": match.get("line_start"),
                "line_end": match.get("line_end"),
                "retrieval": match.get("retrieval"),
                "trace_stage": match.get("trace_stage"),
            }
            for match in matches[:16]
            if isinstance(match, dict)
        ],
    }


def _source_code_qa_effort_runtime_digest(runtime_evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    digest: list[dict[str, Any]] = []
    for item in runtime_evidence:
        if not isinstance(item, dict):
            continue
        digest.append(
            {
                "source_type": item.get("source_type") or "",
                "filename": item.get("filename") or "",
                "sha256": hashlib.sha256(str(item.get("text") or "").encode("utf-8")).hexdigest()[:16],
            }
        )
    return digest


def _source_code_qa_effort_cache_key(
    *,
    pm_team: str,
    country: str,
    language: str,
    requirement: str,
    llm_provider: str,
    evidence_query: str,
    evidence_result: dict[str, Any],
    evidence_matrix: dict[str, Any],
    runtime_evidence: list[dict[str, Any]],
) -> str:
    dictionaries = _load_source_code_qa_effort_dictionaries()
    payload = {
        "version": 4,
        "pm_team": pm_team,
        "country": country,
        "language": language,
        "requirement_sha256": hashlib.sha256(str(requirement or "").encode("utf-8")).hexdigest(),
        "llm_provider": llm_provider or "default",
        "evidence_query_sha256": hashlib.sha256(str(evidence_query or "").encode("utf-8")).hexdigest(),
        "effort_dictionary_version": dictionaries.get("version"),
        "effort_dictionary_updated_at": dictionaries.get("updated_at"),
        "runtime_evidence": _source_code_qa_effort_runtime_digest(runtime_evidence),
        "evidence": _source_code_qa_effort_evidence_digest(evidence_result),
        "evidence_matrix": {
            "version": evidence_matrix.get("version") if isinstance(evidence_matrix, dict) else 0,
            "groups": [
                {
                    "key": group.get("key"),
                    "status": group.get("status"),
                    "terms": group.get("terms") or [],
                    "match_count": len(group.get("matches") or []),
                }
                for group in ((evidence_matrix or {}).get("groups") or [])
                if isinstance(group, dict)
            ],
        },
    }
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _source_code_qa_effort_cache_root(settings: Settings) -> Path:
    return _team_portal_data_root(settings) / "source_code_qa" / "effort_assessment_cache"


def _load_source_code_qa_effort_cached_result(settings: Settings, cache_key: str) -> dict[str, Any] | None:
    try:
        path = _source_code_qa_effort_cache_root(settings) / f"{cache_key}.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    result = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(result, dict):
        return None
    result = dict(result)
    result["effort_cache_hit"] = True
    result["effort_cache_key"] = cache_key
    if isinstance(result.get("llm_route"), dict):
        result["llm_route"] = {**result["llm_route"], "task": "effort_assessment", "effort_cache_hit": True}
    return result


def _store_source_code_qa_effort_cached_result(settings: Settings, cache_key: str, result: dict[str, Any]) -> None:
    try:
        cache_root = _source_code_qa_effort_cache_root(settings)
        cache_root.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 4,
            "cache_key": cache_key,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "result": result,
        }
        (cache_root / f"{cache_key}.json").write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    except (OSError, TypeError):
        current_app.logger.warning("Could not store Source Code Q&A effort assessment cache.", exc_info=True)


def _source_code_qa_effort_compact_evidence(result: dict[str, Any]) -> dict[str, Any]:
    matches = result.get("matches") if isinstance(result.get("matches"), list) else []
    citations = result.get("citations") if isinstance(result.get("citations"), list) else []
    evidence_outline = result.get("evidence_outline") if isinstance(result.get("evidence_outline"), dict) else {}
    return {
        "status": result.get("status") or "",
        "summary": result.get("summary") or "",
        "answer_quality": result.get("answer_quality") or {},
        "evidence_outline": evidence_outline,
        "citations": citations[:12],
        "matches": [
            {
                "repo": match.get("repo"),
                "path": match.get("path"),
                "line_start": match.get("line_start"),
                "line_end": match.get("line_end"),
                "retrieval": match.get("retrieval"),
                "trace_stage": match.get("trace_stage"),
                "reason": match.get("reason"),
                "snippet": str(match.get("snippet") or "")[:900],
            }
            for match in matches[:12]
            if isinstance(match, dict)
        ],
    }


def _source_code_qa_effort_matrix_terms(
    *,
    key: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
) -> list[str]:
    typed_candidates = technical_candidates.get("typed_candidates") if isinstance(technical_candidates.get("typed_candidates"), dict) else {}
    if key == "business_rule":
        values = (
            list(business_plan.get("business_goals") or [])
            + list(business_plan.get("products") or [])
            + list(business_plan.get("limit_types") or [])
            + list(business_plan.get("decision_points") or [])
            + list(technical_candidates.get("product_terms") or [])
            + list(technical_candidates.get("limit_terms") or [])
        )
    elif key == "workflow_api":
        values = (
            list(business_plan.get("flow_changes") or [])
            + list(technical_candidates.get("backend_surfaces") or [])
            + list(typed_candidates.get("backend_service") or [])
            + list(typed_candidates.get("api") or [])
            + list(typed_candidates.get("workflow") or [])
        )
    elif key == "config_table":
        values = (
            list(technical_candidates.get("configs_or_tables") or [])
            + list(typed_candidates.get("configuration") or [])
            + list(typed_candidates.get("table") or [])
        )
    elif key == "frontend_surface":
        values = list(technical_candidates.get("frontend_surfaces") or []) + list(typed_candidates.get("frontend_surface") or [])
    elif key == "downstream_reporting":
        values = (
            list(typed_candidates.get("downstream_reporting") or [])
            + list(typed_candidates.get("integration") or [])
            + list(typed_candidates.get("downstream") or [])
        )
    else:
        values = list(typed_candidates.get("test") or []) + ["test", "qa", "regression", "integration"]
    return _source_code_qa_effort_unique(str(item) for item in values if str(item or "").strip())[:12]


def _source_code_qa_effort_match_text(match: dict[str, Any]) -> str:
    return " ".join(
        str(match.get(field) or "")
        for field in ("repo", "path", "reason", "snippet", "retrieval", "trace_stage")
    ).lower()


def _source_code_qa_effort_matrix_quality(matrix: dict[str, Any]) -> dict[str, Any]:
    groups = matrix.get("groups") if isinstance(matrix, dict) else []
    if not isinstance(groups, list):
        groups = []
    counts = {"confirmed": 0, "inferred": 0, "missing": 0}
    for group in groups:
        if not isinstance(group, dict):
            continue
        status = str(group.get("status") or "missing")
        if status in counts:
            counts[status] += 1
    return {
        "confirmed_group_count": counts["confirmed"],
        "inferred_group_count": counts["inferred"],
        "missing_group_count": counts["missing"],
        "status": "confirmed" if counts["confirmed"] >= 3 and counts["missing"] == 0 else ("partial" if counts["confirmed"] else "planning_assumption"),
    }


def _build_source_code_qa_effort_evidence_matrix(
    *,
    evidence_result: dict[str, Any],
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
) -> dict[str, Any]:
    matches = evidence_result.get("matches") if isinstance(evidence_result.get("matches"), list) else []
    group_defs = [
        ("business_rule", "Business rule / decision logic", ("rule", "decision", "limit", "amount", "income", "product", "strategy")),
        ("workflow_api", "Workflow / API / service path", ("api", "service", "controller", "workflow", "approval", "review", "appeal", "suspension")),
        ("config_table", "Config / table / parameter", ("config", "table", "mapper", "sql", "apollo", "properties", "param", "dictionary")),
        ("frontend_surface", "Frontend screen / operation path", ("frontend", "screen", "page", "component", "vue", "react", "template", "webform")),
        ("downstream_reporting", "Downstream / reporting / integration", ("report", "submission", "downstream", "dwh", "cbs", "integration", "mq", "sync")),
        ("tests", "Tests / QA regression", ("test", "spec", "qa", "regression", "integration")),
    ]
    groups: list[dict[str, Any]] = []
    for key, title, fallback_markers in group_defs:
        terms = _source_code_qa_effort_matrix_terms(
            key=key,
            business_plan=business_plan,
            technical_candidates=technical_candidates,
        )
        markers = [marker.lower() for marker in list(fallback_markers) + [term for term in terms if len(str(term)) >= 3]]
        group_matches: list[dict[str, Any]] = []
        for match in matches:
            if not isinstance(match, dict):
                continue
            match_text = _source_code_qa_effort_match_text(match)
            if any(str(marker).lower() in match_text for marker in markers):
                group_matches.append(
                    {
                        "repo": match.get("repo"),
                        "path": match.get("path"),
                        "line_start": match.get("line_start"),
                        "line_end": match.get("line_end"),
                        "reason": match.get("reason"),
                        "retrieval": match.get("retrieval"),
                    }
                )
        status = "confirmed" if group_matches else ("inferred" if terms else "missing")
        groups.append(
            {
                "key": key,
                "title": title,
                "status": status,
                "terms": terms,
                "matches": group_matches[:6],
                "planning_note": (
                    "Grounded by retrieved source-code references."
                    if status == "confirmed"
                    else (
                        "Candidate impact inferred from requirement and domain dictionary; visible answer must phrase this as a planning assumption."
                        if status == "inferred"
                        else "No source or candidate evidence found for this workstream."
                    )
                ),
            }
        )
    matrix = {
        "version": 1,
        "groups": groups,
    }
    matrix["quality"] = _source_code_qa_effort_matrix_quality(matrix)
    return matrix


def _source_code_qa_effort_generic_output_guard(answer: str, evidence_matrix: dict[str, Any]) -> dict[str, Any]:
    text = str(answer or "")
    generic_patterns = (
        "api validation, service strategy",
        "service strategy, workflow decision rule",
        "config lookup",
        "frontend_guidance",
        "test/regression suite",
    )
    issues = [pattern for pattern in generic_patterns if pattern.lower() in text.lower()]
    confirmed_or_inferred = [
        group for group in (evidence_matrix.get("groups") or [])
        if isinstance(group, dict) and group.get("status") in {"confirmed", "inferred"}
    ]
    if "code change" in text.lower() or "代码改动" in text:
        status = "ok" if not issues and confirmed_or_inferred else "warning"
    else:
        status = "warning"
        issues.append("missing_code_change_section")
    return {
        "status": status,
        "issues": _source_code_qa_effort_unique(issues),
        "confirmed_or_inferred_group_count": len(confirmed_or_inferred),
    }


def _build_source_code_qa_effort_compact_synthesis_prompt(
    *,
    pm_team: str,
    country: str,
    language: str,
    requirement: str,
    llm_provider: str,
    runtime_evidence: list[dict[str, Any]],
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
    evidence_result: dict[str, Any],
    evidence_matrix: dict[str, Any],
) -> str:
    output_language = "Chinese" if language == "zh" else "English"
    return "\n".join(
        [
            "You are performing a Source Code Q&A Effort Assessment. Use the compact evidence pack below; do not restart broad repository exploration unless required evidence is contradictory.",
            "",
            "Context:",
            f"- PM Team: {pm_team}",
            f"- Country: {country}",
            f"- Answer language: {output_language}",
            f"- Selected model provider: {llm_provider or 'default'}",
            f"- Runtime evidence available: {len(runtime_evidence)} item(s)",
            "",
            "Original business requirement, verbatim:",
            str(requirement or "").strip()[:4000],
            "",
            "Business plan:",
            _source_code_qa_effort_json_block(business_plan),
            "",
            "Technical candidates:",
            _source_code_qa_effort_json_block(technical_candidates),
            "",
            "Estimation rubric:",
            _source_code_qa_effort_json_block(estimation_rubric),
            "",
            "Compact source-code evidence pack from the indexed repositories:",
            _source_code_qa_effort_json_block(_source_code_qa_effort_compact_evidence(evidence_result)),
            "",
            "Internal evidence matrix for planning quality. Use it for grounding only; do not expose it in the final answer:",
            _source_code_qa_effort_json_block(evidence_matrix),
            "",
            "Instructions:",
            "- Produce the final effort assessment from this evidence pack, business plan, runtime evidence, and rubric.",
            "- Use source-code evidence internally to decide impact, but do not expose evidence, citations, S-id references, file paths, or proof lists in the visible final answer.",
            "- Explain detailed code change points in business-readable language: behavior/process/rule/UI/API/data/integration/testing impact first, technical names only when useful.",
            "- Every visible code change point must be grounded in a confirmed or inferred evidence-matrix workstream; if a workstream is only inferred, phrase it as a planning assumption.",
            "- Do not output generic keyword strings such as API validation, service strategy, config lookup, frontend_guidance, or test/regression suite as change points.",
            "- Missing source-code evidence is acceptable; continue with low confidence and state planning assumptions without creating a visible evidence section.",
            "- Do not invent Option 1/Option 2 labels unless the original requirement had explicit alternatives.",
            "- Estimate BE and FE as person-day ranges and break down the driver by rule/workflow, backend/API, config/data, frontend, and integration/QA where applicable.",
            "- Include QA/test and integration impact inside the relevant BE/FE notes.",
            "",
            "Required output sections:",
            "1. 业务理解 / Business Understanding",
            "2. 代码改动点 / Code Change Points",
            "3. BE 人天 / BE Person-days",
            "4. FE 人天 / FE Person-days",
            "5. QA / Integration Impact",
            "6. Assumptions / Risks",
            "7. Confirmation Questions",
            "",
            "Do not include visible sections named Evidence, Source / Runtime Evidence, Confirmed / Inferred / Missing Evidence, Missing Evidence, or source/runtime proof.",
            "",
            f"Write the final answer in {output_language}.",
        ]
    )


def _source_code_qa_effort_missing_evidence(
    *,
    result: dict[str, Any],
    technical_candidates: dict[str, Any],
) -> list[str]:
    missing: list[str] = []
    matches = result.get("matches") if isinstance(result, dict) else []
    if not isinstance(matches, list) or not matches:
        missing.append("No confirmed source-code references were found for the technical candidates.")
    exact_lookup = result.get("exact_lookup") if isinstance(result, dict) else None
    if isinstance(exact_lookup, dict) and exact_lookup.get("terms") and not exact_lookup.get("matched_terms"):
        missing.append("Exact table/path lookup did not match; assessment continued with focused candidate search.")
    if technical_candidates.get("backend_surfaces") and (not isinstance(matches, list) or not matches):
        missing.append("Backend impact surfaces need Dev confirmation against current repositories.")
    return _source_code_qa_effort_unique(missing)


def _source_code_qa_effort_confidence(result: dict[str, Any], missing_evidence: list[str]) -> str:
    matches = result.get("matches") if isinstance(result, dict) else []
    if str(result.get("status") or "").lower() == "no_match":
        return "low"
    if isinstance(matches, list) and len(matches) >= 3 and not missing_evidence:
        return "high"
    if isinstance(matches, list) and matches:
        return "medium"
    return "low"


def _source_code_qa_effort_code_change_points(
    *,
    language: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
) -> list[dict[str, str]]:
    is_zh = language == "zh"
    typed_candidates = technical_candidates.get("typed_candidates") if isinstance(technical_candidates.get("typed_candidates"), dict) else {}
    backend_terms = [str(item) for item in (technical_candidates.get("backend_surfaces") or [])[:6] if str(item or "").strip()]
    frontend_terms = [str(item) for item in (technical_candidates.get("frontend_surfaces") or [])[:6] if str(item or "").strip()]
    config_terms = [str(item) for item in (technical_candidates.get("configs_or_tables") or [])[:6] if str(item or "").strip()]
    reporting_terms = [str(item) for item in (typed_candidates.get("downstream_reporting") or [])[:6] if str(item or "").strip()]
    products = [str(item) for item in (business_plan.get("products") or [])[:4] if str(item or "").strip()]
    limit_types = [str(item) for item in (business_plan.get("limit_types") or [])[:4] if str(item or "").strip()]
    flow_changes = [str(item) for item in (business_plan.get("flow_changes") or [])[:4] if str(item or "").strip()]
    estimates = [item for item in (estimation_rubric.get("option_estimates") or []) if isinstance(item, dict)]

    def join_terms(items: list[str], fallback: str) -> str:
        return ", ".join(items) if items else fallback

    points: list[dict[str, str]] = []

    def add(area: str, change: str, technical_surface: str, impact: str) -> None:
        if not change.strip():
            return
        points.append(
            {
                "area": area,
                "change": change,
                "likely_technical_surface": technical_surface,
                "impact": impact,
            }
        )

    if flow_changes or products or limit_types:
        add(
            "业务规则 / Business Rules" if is_zh else "Business Rules",
            (
                f"把需求中的流程、产品和额度规则落到系统判断中，覆盖 {join_terms(flow_changes + products + limit_types, '当前业务规则')}。"
                if is_zh
                else f"Map the requested flow, product, and limit-rule changes into system decision logic for {join_terms(flow_changes + products + limit_types, 'the affected business rules')}."
            ),
            join_terms(backend_terms + config_terms, "service/config rule layer" if not is_zh else "服务/配置规则层"),
            "影响审批、授信、额度或流程判断口径。" if is_zh else "Affects approval, credit, limit, or workflow decisions.",
        )
    if backend_terms or typed_candidates.get("backend_service"):
        add(
            "后端服务 / Backend" if is_zh else "Backend",
            (
                f"调整后端接口、服务或策略逻辑，让新规则能在核心流程中被计算、校验和保存。"
                if is_zh
                else "Update backend APIs, services, or strategy logic so the new rule can be calculated, validated, and persisted in the core flow."
            ),
            join_terms(backend_terms or [str(item) for item in (typed_candidates.get("backend_service") or [])[:6]], "backend service/API layer"),
            "主要决定 BE 人天范围。" if is_zh else "This is the main driver for BE person-days.",
        )
    if config_terms or typed_candidates.get("configuration"):
        add(
            "配置与数据 / Config & Data" if is_zh else "Config & Data",
            (
                f"新增或调整配置、字典、表字段映射或参数，确保规则可配置且不同环境口径一致。"
                if is_zh
                else "Add or adjust configuration, dictionary, table-field mapping, or parameters so the rule is configurable and consistent across environments."
            ),
            join_terms(config_terms or [str(item) for item in (typed_candidates.get("configuration") or [])[:6]], "config/table mapping"),
            "需要迁移、参数发布或数据校验配合。" if is_zh else "May require migration, parameter rollout, or data validation.",
        )
    if frontend_terms or typed_candidates.get("frontend_surface"):
        add(
            "前端页面 / Frontend" if is_zh else "Frontend",
            (
                "调整页面入口、字段展示、提示文案或用户操作路径，让用户能理解并使用新的业务规则。"
                if is_zh
                else "Update screen entry points, field display, helper copy, or user flow so users can understand and use the new business rule."
            ),
            join_terms(frontend_terms or [str(item) for item in (typed_candidates.get("frontend_surface") or [])[:6]], "frontend screen/component"),
            "决定是否需要 FE 人天；无页面变化时可为 0-1 PD。" if is_zh else "Determines FE effort; can be 0-1 PD if no user-facing screen changes.",
        )
    if reporting_terms or typed_candidates.get("integration") or typed_candidates.get("downstream"):
        add(
            "下游与报送 / Integration" if is_zh else "Integration",
            (
                "检查并调整下游接口、报送字段或同步任务，避免新规则只在主流程生效但下游口径不一致。"
                if is_zh
                else "Check and adjust downstream APIs, reporting fields, or sync jobs so the new rule does not diverge between the main flow and downstream consumers."
            ),
            join_terms(reporting_terms, "downstream/reporting path" if not is_zh else "下游/报送链路"),
            "增加联调和回归测试成本。" if is_zh else "Adds integration and regression testing cost.",
        )
    add(
        "测试与验收 / QA" if is_zh else "QA",
        (
            "补充单元测试、接口测试和关键业务场景回归，覆盖正常、边界和回退场景。"
            if is_zh
            else "Add unit, API, and key business regression tests covering normal, boundary, and rollback scenarios."
        ),
        "test/regression suite",
        "测试工作包含在 BE/FE 估算说明中。" if is_zh else "Testing work is included in the BE/FE estimate notes.",
    )

    if estimates:
        estimate_summary = "; ".join(
            f"{item.get('label') or item.get('id') or 'option'}: BE {item.get('be_person_days') or 'n/a'}, FE {item.get('fe_person_days') or 'n/a'}"
            for item in estimates[:3]
        )
        for point in points:
            point.setdefault("estimate_hint", estimate_summary)
    return points[:6]


def _source_code_qa_effort_fallback_answer(
    *,
    language: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
    missing_evidence: list[str],
) -> str:
    options = estimation_rubric.get("option_estimates") or []
    has_explicit_options = bool(business_plan.get("has_explicit_options"))
    option_lines = "\n".join(
        f"- {item.get('label')}: BE {item.get('be_person_days')}, FE {item.get('fe_person_days')} ({item.get('basis')})"
        for item in options
        if isinstance(item, dict)
    )
    code_change_points = _source_code_qa_effort_code_change_points(
        language=language,
        business_plan=business_plan,
        technical_candidates=technical_candidates,
        estimation_rubric=estimation_rubric,
    )
    point_lines = "\n".join(
        f"- {item['area']}: {item['change']} ({item['impact']})"
        for item in code_change_points
    )
    if language == "zh":
        goals = ", ".join(str(item) for item in business_plan.get("business_goals") or [])
        technical_title = "方案 1/2 代码改动点" if has_explicit_options else "代码改动点"
        confirmation_questions = [
            "- 额度策略是否只改参数，还是需要新增产品/子产品额度模型?",
            "- 是否需要前端新增 cashline 申请入口或额度解释文案?",
        ]
        if has_explicit_options:
            confirmation_questions.insert(0, "- 方案 1 和方案 2 是否二选一，还是都需要落地?")
        return "\n".join(
            [
                "业务理解",
                f"- 目标: {goals or '评估业务需求对应的技术改造范围'}",
                "",
                technical_title,
                point_lines or "- 按当前需求描述，需要调整业务规则、后端流程、可能的前端展示和测试回归范围。",
                "",
                "BE 人天 / FE 人天",
                option_lines or "- 单方案: BE 3-6 PD, FE 1-3 PD，低置信度。",
                "",
                "QA / Integration Impact",
                "- 需要覆盖核心业务路径、边界条件、配置发布和下游联调回归。",
                "",
                "Assumptions / Risks",
                "- 这是 planning-grade 低置信度估算，不替代 Dev final sizing。",
                "- 如果涉及授信引擎、额度模型、报送或多产品额度联动，BE 复杂度应按高复杂度处理。",
                "",
                "Confirmation Questions",
                *confirmation_questions,
            ]
        )
    technical_title = "Option Code Change Points" if has_explicit_options else "Code Change Points"
    confirmation_questions = [
        "- Is the requested limit change a config-only rule update or a new limit model?",
        "- Does the change require FE display, application entry, or customer education copy?",
    ]
    if has_explicit_options:
        confirmation_questions.insert(0, "- Are the listed options alternatives, or should more than one be implemented?")
    return "\n".join(
        [
            "Business Understanding",
            f"- Goals: {', '.join(str(item) for item in business_plan.get('business_goals') or [])}",
            "",
            technical_title,
            point_lines or "- Adjust business rules, backend flow, possible frontend display, and regression testing scope based on the requirement.",
            "",
            "BE / FE Person-days",
            option_lines or "- Single option: BE 3-6 PD, FE 1-3 PD, low confidence.",
            "",
            "QA / Integration Impact",
            "- Cover core business paths, boundary conditions, configuration rollout, and downstream integration regression.",
            "",
            "Assumptions / Risks",
            "- This is a planning-grade low-confidence estimate and does not replace Dev final sizing.",
            "",
            "Confirmation Questions",
            *confirmation_questions,
        ]
    )


def _build_source_code_qa_effort_structured_assessment(
    *,
    result: dict[str, Any],
    language: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
    missing_evidence: list[str],
    confidence: str,
) -> dict[str, Any]:
    matches = result.get("matches") if isinstance(result.get("matches"), list) else []
    evidence_matrix = result.get("effort_evidence_matrix") if isinstance(result.get("effort_evidence_matrix"), dict) else {}
    evidence_groups = evidence_matrix.get("groups") if isinstance(evidence_matrix.get("groups"), list) else []
    confirmed_evidence = [
        {
            "repo": str(match.get("repo") or ""),
            "path": str(match.get("path") or ""),
            "line_start": match.get("line_start") or 0,
            "line_end": match.get("line_end") or 0,
        }
        for match in matches[:8]
        if isinstance(match, dict)
    ]
    typed_candidates = technical_candidates.get("typed_candidates") if isinstance(technical_candidates.get("typed_candidates"), dict) else {}
    inferred_impact = [
        {
            "surface": surface,
            "terms": [str(item) for item in (terms or [])[:12]],
        }
        for surface, terms in typed_candidates.items()
        if terms
    ]
    code_change_points = _source_code_qa_effort_code_change_points(
        language=language,
        business_plan=business_plan,
        technical_candidates=technical_candidates,
        estimation_rubric=estimation_rubric,
    )
    matrix_status_by_key = {
        str(group.get("key") or ""): str(group.get("status") or "missing")
        for group in evidence_groups
        if isinstance(group, dict)
    }
    workstream_by_area = (
        ("business_rule", ("business", "业务规则")),
        ("workflow_api", ("backend", "后端", "api", "服务")),
        ("config_table", ("config", "data", "配置", "数据")),
        ("frontend_surface", ("frontend", "前端", "页面")),
        ("downstream_reporting", ("integration", "下游", "报送", "联调")),
        ("tests", ("qa", "测试", "验收")),
    )
    for point in code_change_points:
        area_text = str(point.get("area") or "").lower()
        workstream_key = next(
            (
                key
                for key, markers in workstream_by_area
                if any(marker in area_text for marker in markers)
            ),
            "",
        )
        status = matrix_status_by_key.get(workstream_key, "inferred" if workstream_key else "missing")
        point["evidence_status"] = status
        point["workstream"] = workstream_key or "planning"
        if status == "inferred":
            point["planning_assumption"] = "Candidate impact inferred from requirement/domain dictionary; Dev confirmation required."
        elif status == "missing":
            point["planning_assumption"] = "No direct source evidence found; treat as planning assumption."
    return {
        "version": 2,
        "language": language,
        "confidence": confidence,
        "business_understanding": {
            "goals": business_plan.get("business_goals") or [],
            "user_segments": business_plan.get("user_segments") or [],
            "products": business_plan.get("products") or [],
            "limit_types": business_plan.get("limit_types") or [],
            "flow_changes": business_plan.get("flow_changes") or [],
            "decision_points": business_plan.get("decision_points") or [],
        },
        "option_impacts": [
            {
                "id": item.get("id") or "",
                "label": item.get("label") or "",
                "summary": item.get("summary") or "",
            }
            for item in (business_plan.get("options") or [])
            if isinstance(item, dict)
        ],
        "code_change_points": code_change_points,
        "be_estimate": [
            {
                "option_id": item.get("id") or "",
                "person_days": item.get("be_person_days") or "",
                "basis": item.get("basis") or "",
            }
            for item in (estimation_rubric.get("option_estimates") or [])
            if isinstance(item, dict)
        ],
        "fe_estimate": [
            {
                "option_id": item.get("id") or "",
                "person_days": item.get("fe_person_days") or "",
                "basis": item.get("basis") or "",
            }
            for item in (estimation_rubric.get("option_estimates") or [])
            if isinstance(item, dict)
        ],
        "confirmed_evidence": confirmed_evidence,
        "inferred_impact": inferred_impact,
        "missing_evidence": missing_evidence,
        "evidence_matrix_quality": evidence_matrix.get("quality") or _source_code_qa_effort_matrix_quality(evidence_matrix),
        "questions": (
            ["Are the listed options alternatives, or should more than one be implemented?"]
            if business_plan.get("has_explicit_options")
            else []
        ) + [
            "Is the requested limit change a config-only rule update or a new limit model?",
            "Does the change require FE display, application entry, or customer education copy?",
        ],
        "dictionary_entries": technical_candidates.get("matched_dictionary_entries") or [],
    }


def _source_code_qa_effort_sanitize_visible_answer(value: Any) -> str:
    text = str(value or "")
    if not text.strip():
        return ""
    allowed_heading_patterns = (
        "business understanding",
        "业务理解",
        "code change",
        "代码改动",
        "technical change",
        "技术改造",
        "be person",
        "be 人天",
        "fe person",
        "fe 人天",
        "qa",
        "integration",
        "assumptions",
        "risks",
        "assumptions / risks",
        "假设",
        "风险",
        "confirmation questions",
        "确认问题",
        "需要确认",
    )
    blocked_heading_patterns = (
        "confirmed / inferred / missing evidence",
        "source / runtime evidence",
        "source/runtime evidence",
        "missing evidence",
        "runtime evidence",
        "source evidence",
        "evidence",
        "证据",
    )

    def normalized_heading(line: str) -> str:
        value = re.sub(r"^[#*\s>\-]*", "", line.strip())
        value = re.sub(r"^\d+[\.)、]\s*", "", value)
        value = value.strip("*:： ").lower()
        return value

    output: list[str] = []
    skipping = False
    for line in text.splitlines():
        heading = normalized_heading(line)
        is_blocked = any(pattern in heading for pattern in blocked_heading_patterns)
        is_allowed = any(pattern in heading for pattern in allowed_heading_patterns)
        if is_blocked and not is_allowed:
            skipping = True
            continue
        if skipping and is_allowed:
            skipping = False
        if skipping:
            continue
        output.append(line)
    cleaned = "\n".join(output).strip()
    cleaned = re.sub(r"\s*\[S\d+\]", "", cleaned)
    cleaned = re.sub(
        r"(?:[\w.-]+/)+[\w.-]+\.(?:py|java|js|ts|tsx|vue|sql|xml|yaml|yml|properties|kt|go|rb|php|html|css|sh)(?::\d+(?:-\d+)?)?",
        "source module",
        cleaned,
        flags=re.IGNORECASE,
    )
    return cleaned


def _normalize_source_code_qa_effort_assessment_result(
    *,
    result: dict[str, Any],
    language: str,
    business_plan: dict[str, Any],
    technical_candidates: dict[str, Any],
    estimation_rubric: dict[str, Any],
    evidence_matrix: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized = dict(result or {})
    if evidence_matrix is not None:
        normalized["effort_evidence_matrix"] = evidence_matrix
    elif not isinstance(normalized.get("effort_evidence_matrix"), dict):
        normalized["effort_evidence_matrix"] = _build_source_code_qa_effort_evidence_matrix(
            evidence_result=normalized,
            business_plan=business_plan,
            technical_candidates=technical_candidates,
        )
    missing_evidence = _source_code_qa_effort_missing_evidence(
        result=normalized,
        technical_candidates=technical_candidates,
    )
    confidence = _source_code_qa_effort_confidence(normalized, missing_evidence)
    if str(normalized.get("status") or "").lower() == "no_match":
        normalized["status"] = "ok"
        normalized["effort_evidence_status"] = "warning"
        normalized["summary"] = "Effort assessment completed with low confidence because source-code evidence is missing."
        normalized["llm_answer"] = _source_code_qa_effort_fallback_answer(
            language=language,
            business_plan=business_plan,
            technical_candidates=technical_candidates,
            estimation_rubric=estimation_rubric,
            missing_evidence=missing_evidence,
        )
    else:
        normalized["effort_evidence_status"] = "warning" if missing_evidence else "confirmed"
        if not normalized.get("summary"):
            normalized["summary"] = "Effort assessment completed."
        normalized["llm_answer"] = _source_code_qa_effort_sanitize_visible_answer(normalized.get("llm_answer") or normalized.get("answer") or "")
    normalized["assessment_confidence"] = confidence
    normalized["missing_evidence"] = missing_evidence
    normalized["effort_evidence_matrix_quality"] = _source_code_qa_effort_matrix_quality(
        normalized.get("effort_evidence_matrix") if isinstance(normalized.get("effort_evidence_matrix"), dict) else {}
    )
    normalized["effort_generic_output_guard"] = _source_code_qa_effort_generic_output_guard(
        normalized.get("llm_answer") or "",
        normalized.get("effort_evidence_matrix") if isinstance(normalized.get("effort_evidence_matrix"), dict) else {},
    )
    normalized["structured_assessment"] = _build_source_code_qa_effort_structured_assessment(
        result=normalized,
        language=language,
        business_plan=business_plan,
        technical_candidates=technical_candidates,
        estimation_rubric=estimation_rubric,
        missing_evidence=missing_evidence,
        confidence=confidence,
    )
    return normalized


def _run_source_code_qa_effort_assessment_job(app: Flask, job_id: str, payload: dict[str, Any]) -> None:
    with app.app_context():
        job_store: JobStore = app.config["JOB_STORE"]

        def progress_callback(stage: str, message: str, current: int, total: int) -> None:
            job_store.update(
                job_id,
                state="running",
                stage=stage,
                message=message,
                current=current,
                total=total,
            )

        try:
            if not _source_code_qa_provider_available(payload.get("llm_provider")):
                raise ToolError("Selected Source Code Q&A model is unavailable.")
            settings: Settings = app.config["SETTINGS"]
            service = _build_source_code_qa_service(payload.get("llm_provider")).with_codex_timeout_seconds(
                settings.source_code_qa_effort_codex_timeout_seconds,
            )
            pm_team = str(payload.get("pm_team") or "")
            country = str(payload.get("country") or "")
            language = _source_code_qa_effort_assessment_language(payload.get("language"))
            requirement = str(payload.get("requirement") or "").strip()
            if not requirement:
                raise ToolError("Business requirement is empty.")
            progress_callback("assessment_prompt", "Building optimized effort assessment evidence query.", 0, 1)
            runtime_evidence = _resolve_source_code_qa_runtime_evidence(pm_team=pm_team, country=country)
            business_plan = _build_source_code_qa_effort_business_plan(
                pm_team=pm_team,
                country=country,
                language=language,
                requirement=requirement,
            )
            technical_candidates = _build_source_code_qa_effort_technical_candidates(
                pm_team=pm_team,
                country=country,
                business_plan=business_plan,
                requirement=requirement,
            )
            estimation_rubric = _build_source_code_qa_effort_estimation_rubric(
                business_plan=business_plan,
                technical_candidates=technical_candidates,
            )
            llm_provider = str(payload.get("llm_provider") or "")
            scope_guard = _source_code_qa_effort_scope_guard(pm_team=pm_team, country=country, requirement=requirement)
            if scope_guard.get("status") == "mismatch":
                result = _source_code_qa_effort_scope_mismatch_result(
                    pm_team=pm_team,
                    country=country,
                    language=language,
                    requirement=requirement,
                    llm_provider=llm_provider,
                    business_plan=business_plan,
                    technical_candidates=technical_candidates,
                    estimation_rubric=estimation_rubric,
                    scope_guard=scope_guard,
                )
                current_app.logger.warning(
                    "source_code_qa_effort_assessment_scope_mismatch %s",
                    json.dumps(
                        {
                            "event": "source_code_qa_effort_assessment_scope_mismatch",
                            "job_id": job_id,
                            "pm_team": pm_team,
                            "country": country,
                            "scope_guard": scope_guard,
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
                job_store.complete(
                    job_id,
                    results=[result],
                    notice={
                        "title": "Effort Assessment",
                        "tone": "warning",
                        "summary": result["summary"],
                        "details": [
                            f"Selected: {pm_team}:{country}",
                            f"Suggested: {scope_guard.get('suggested_pm_team')}:{scope_guard.get('suggested_country')}",
                            "Status: scope_mismatch",
                        ],
                    },
                )
                return
            effort_started = time.perf_counter()
            evidence_query = _build_source_code_qa_effort_evidence_query(
                requirement=requirement,
                business_plan=business_plan,
                technical_candidates=technical_candidates,
            )
            progress_callback("effort_evidence", "Searching focused source-code evidence for effort assessment.", 0, 1)
            evidence_started = time.perf_counter()
            evidence_result = service.query(
                pm_team=pm_team,
                country=country,
                question=evidence_query,
                limit=16,
                answer_mode="retrieval_only",
                llm_budget_mode="cheap",
                query_mode="deep",
                conversation_context=None,
                attachments=[],
                runtime_evidence=runtime_evidence,
                progress_callback=progress_callback,
            )
            evidence_latency_ms = int((time.perf_counter() - evidence_started) * 1000)
            evidence_matrix = _build_source_code_qa_effort_evidence_matrix(
                evidence_result=evidence_result,
                business_plan=business_plan,
                technical_candidates=technical_candidates,
            )
            evidence_matrix["scope_guard"] = scope_guard
            evidence_matrix_quality = _source_code_qa_effort_matrix_quality(evidence_matrix)
            cache_key = _source_code_qa_effort_cache_key(
                pm_team=pm_team,
                country=country,
                language=language,
                requirement=requirement,
                llm_provider=llm_provider,
                evidence_query=evidence_query,
                evidence_result=evidence_result,
                evidence_matrix=evidence_matrix,
                runtime_evidence=runtime_evidence,
            )
            result = _load_source_code_qa_effort_cached_result(settings, cache_key)
            if result is None:
                synthesis_prompt = _build_source_code_qa_effort_compact_synthesis_prompt(
                    pm_team=pm_team,
                    country=country,
                    language=language,
                    requirement=requirement,
                    llm_provider=llm_provider,
                    runtime_evidence=runtime_evidence,
                    business_plan=business_plan,
                    technical_candidates=technical_candidates,
                    estimation_rubric=estimation_rubric,
                    evidence_result=evidence_result,
                    evidence_matrix=evidence_matrix,
                )
                progress_callback("effort_synthesis", "Generating compact code-grounded effort assessment.", 0, 1)
                synthesis_started = time.perf_counter()
                result = service.query(
                    pm_team=pm_team,
                    country=country,
                    question=synthesis_prompt,
                    limit=16,
                    answer_mode="auto",
                    llm_budget_mode="auto",
                    query_mode="deep",
                    conversation_context=None,
                    attachments=[],
                    runtime_evidence=runtime_evidence,
                    progress_callback=progress_callback,
                    effort_assessment=True,
                )
                synthesis_latency_ms = int((time.perf_counter() - synthesis_started) * 1000)
                if isinstance(result.get("llm_route"), dict):
                    result["llm_route"] = {
                        **result["llm_route"],
                        "task": "effort_assessment",
                        "effort_cache_hit": False,
                        "effort_evidence_query_sha256": hashlib.sha256(evidence_query.encode("utf-8")).hexdigest()[:16],
                        "effort_evidence_matrix_quality": evidence_matrix_quality,
                    }
                result["effort_evidence_matrix"] = evidence_matrix
                result["effort_timing"] = {
                    "evidence_retrieval_ms": evidence_latency_ms,
                    "synthesis_ms": synthesis_latency_ms,
                    "repair_decision_ms": int(
                        ((result.get("llm_route") or {}).get("codex_repair_decision_ms") or 0)
                        if isinstance(result.get("llm_route"), dict)
                        else 0
                    ),
                    "total_ms": int((time.perf_counter() - effort_started) * 1000),
                    "cache_hit": False,
                    "evidence_matrix_quality": evidence_matrix_quality,
                }
                result["effort_cache_key"] = cache_key
                _store_source_code_qa_effort_cached_result(settings, cache_key, result)
            else:
                result["effort_evidence_matrix"] = evidence_matrix
                result["effort_timing"] = {
                    **(result.get("effort_timing") if isinstance(result.get("effort_timing"), dict) else {}),
                    "evidence_retrieval_ms": evidence_latency_ms,
                    "cache_hit": True,
                    "evidence_matrix_quality": evidence_matrix_quality,
                    "repair_decision_ms": int(
                        ((result.get("llm_route") or {}).get("codex_repair_decision_ms") or 0)
                        if isinstance(result.get("llm_route"), dict)
                        else 0
                    ),
                }
            result = _normalize_source_code_qa_effort_assessment_result(
                result=result,
                language=language,
                business_plan=business_plan,
                technical_candidates=technical_candidates,
                estimation_rubric=estimation_rubric,
                evidence_matrix=evidence_matrix,
            )
            if isinstance(result.get("effort_timing"), dict):
                result["effort_timing"] = {
                    **result["effort_timing"],
                    "generic_output_guard": result.get("effort_generic_output_guard") or {},
                }
            result["effort_evidence_query"] = evidence_query
            result["effort_evidence_result"] = _source_code_qa_effort_compact_evidence(evidence_result)
            result["assessment"] = {
                "type": "effort_assessment",
                "pm_team": pm_team,
                "country": country,
                "language": language,
                "requirement": requirement,
                "business_plan": business_plan,
                "technical_candidates": technical_candidates,
                "estimation_rubric": estimation_rubric,
                "structured_assessment": result.get("structured_assessment") or {},
                "confidence": result.get("assessment_confidence") or "low",
                "missing_evidence": result.get("missing_evidence") or [],
                "evidence_status": result.get("effort_evidence_status") or "warning",
            }
            current_app.logger.warning(
                "source_code_qa_effort_assessment_quality %s",
                json.dumps(
                    {
                        "event": "source_code_qa_effort_assessment_quality",
                        "job_id": job_id,
                        "trace_id": str(result.get("trace_id") or ""),
                        "evidence_matrix_quality": result.get("effort_evidence_matrix_quality") or {},
                        "generic_output_guard": result.get("effort_generic_output_guard") or {},
                        "cache_hit": bool((result.get("effort_timing") or {}).get("cache_hit")) if isinstance(result.get("effort_timing"), dict) else False,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
            result["runtime_evidence"] = _source_code_qa_public_runtime_evidence(runtime_evidence)
            status = str(result.get("status") or "ok")
            notice_tone = "warning" if result.get("effort_evidence_status") == "warning" else ("success" if status == "ok" else "warning")
            job_store.complete(
                job_id,
                results=[result],
                notice={
                    "title": "Effort Assessment",
                    "tone": notice_tone,
                    "summary": result.get("summary") or "Effort assessment completed.",
                    "details": [
                        f"Status: {status}",
                        f"Evidence: {result.get('effort_evidence_status') or 'n/a'}",
                        f"Trace: {result.get('trace_id') or 'n/a'}",
                    ],
                },
            )
        except ToolError as error:
            job_store.fail(job_id, str(error), **_classify_source_code_qa_job_error(str(error)))
        except Exception as error:  # pragma: no cover - defensive guard for background worker failures.
            app.logger.exception("Source code QA effort assessment job failed unexpectedly.")
            message = f"Unexpected error: {error}"
            job_store.fail(job_id, message, **_classify_source_code_qa_job_error(message))


def bind_source_code_qa_effort_helpers(global_context: dict[str, object]) -> None:
    helpers = [
        _source_code_qa_effort_assessment_language,
        _source_code_qa_effort_sentences,
        _source_code_qa_effort_matches,
        _source_code_qa_effort_unique,
        _source_code_qa_load_json_file,
        _load_source_code_qa_effort_dictionaries,
        _load_source_code_qa_domain_profile_config,
        _load_source_code_qa_domain_knowledge_config,
        _source_code_qa_effort_domain_entries,
        _source_code_qa_effort_country_hint,
        _source_code_qa_effort_term_matches,
        _source_code_qa_effort_scope_terms_by_team,
        _source_code_qa_effort_scope_guard,
        _source_code_qa_effort_scope_mismatch_result,
        _source_code_qa_effort_seed_terms,
        _source_code_qa_effort_entry_applies,
        _source_code_qa_effort_group_typed_candidates,
        _build_source_code_qa_effort_business_plan,
        _build_source_code_qa_effort_technical_candidates,
        _build_source_code_qa_effort_estimation_rubric,
        _source_code_qa_effort_json_block,
        _build_source_code_qa_effort_assessment_prompt,
        _source_code_qa_effort_compact_terms,
        _build_source_code_qa_effort_evidence_query,
        _source_code_qa_effort_evidence_digest,
        _source_code_qa_effort_runtime_digest,
        _source_code_qa_effort_cache_key,
        _source_code_qa_effort_cache_root,
        _load_source_code_qa_effort_cached_result,
        _store_source_code_qa_effort_cached_result,
        _source_code_qa_effort_compact_evidence,
        _source_code_qa_effort_matrix_terms,
        _source_code_qa_effort_match_text,
        _source_code_qa_effort_matrix_quality,
        _build_source_code_qa_effort_evidence_matrix,
        _source_code_qa_effort_generic_output_guard,
        _build_source_code_qa_effort_compact_synthesis_prompt,
        _source_code_qa_effort_missing_evidence,
        _source_code_qa_effort_confidence,
        _source_code_qa_effort_code_change_points,
        _source_code_qa_effort_fallback_answer,
        _build_source_code_qa_effort_structured_assessment,
        _source_code_qa_effort_sanitize_visible_answer,
        _normalize_source_code_qa_effort_assessment_result,
        _run_source_code_qa_effort_assessment_job,
    ]
    _bind_web_globals(helpers, global_context)
