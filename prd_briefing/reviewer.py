from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa import CodexCliBridgeSourceCodeQALLMProvider

from .confluence import ConfluenceConnector, IngestedConfluencePage
from .storage import BriefingStore


PRD_REVIEW_PROMPT_VERSION = "v4_prd_review_skill_section_guidance"
PRD_SUMMARY_PROMPT_VERSION = "v1_prd_summary_codex"
PRD_REVIEW_MAX_SOURCE_CHARS = 90_000
PRD_SECTION_LONG_CHAR_THRESHOLD = 8_000
PRD_BRIEFING_REVIEW_CACHE_KEY = "__prd_briefing_url_review__"
PRD_URL_SUMMARY_CACHE_KEY = "__prd_url_summary__"


@dataclass(frozen=True)
class PRDReviewRequest:
    owner_key: str
    jira_id: str
    jira_link: str
    prd_url: str
    force_refresh: bool = False


@dataclass(frozen=True)
class PRDBriefingReviewRequest:
    owner_key: str
    prd_url: str
    language: str = "zh"
    force_refresh: bool = False
    selected_section_indexes: list[int] | None = None


class PRDReviewService:
    def __init__(
        self,
        *,
        store: BriefingStore,
        confluence: ConfluenceConnector,
        settings: Settings,
        workspace_root: Path,
    ) -> None:
        self.store = store
        self.confluence = confluence
        self.settings = settings
        self.workspace_root = Path(workspace_root)

    def review(self, request: PRDReviewRequest) -> dict[str, Any]:
        normalized = self._normalize_request(request)
        page = self.confluence.ingest_page(normalized.prd_url, "prd-review")
        if not page.sections:
            raise ToolError("PRD page did not contain readable sections.")
        cached = None if normalized.force_refresh else self.store.get_prd_review_result(
            owner_key=normalized.owner_key,
            jira_id=normalized.jira_id,
            prd_url=page.source_url,
            prd_updated_at=page.updated_at,
            prompt_version=PRD_REVIEW_PROMPT_VERSION,
        )
        if cached and cached.get("status") == "completed" and cached.get("result_markdown"):
            return {"status": "ok", "cached": True, "review": cached, "prd": self._page_metadata(page)}

        prompt = build_prd_review_prompt(
            jira_id=normalized.jira_id,
            jira_link=normalized.jira_link,
            prd_url=page.source_url,
            page=page,
        )
        try:
            generated = generate_prd_review_with_codex(
                prompt=prompt,
                settings=self.settings,
                workspace_root=self.workspace_root,
            )
        except Exception as error:  # noqa: BLE001 - persist the failure for visible retry context.
            self.store.save_prd_review_result(
                owner_key=normalized.owner_key,
                jira_id=normalized.jira_id,
                jira_link=normalized.jira_link,
                prd_url=page.source_url,
                prd_updated_at=page.updated_at,
                prompt_version=PRD_REVIEW_PROMPT_VERSION,
                status="failed",
                error=str(error),
            )
            raise ToolError(str(error)) from error

        review = self.store.save_prd_review_result(
            owner_key=normalized.owner_key,
            jira_id=normalized.jira_id,
            jira_link=normalized.jira_link,
            prd_url=page.source_url,
            prd_updated_at=page.updated_at,
            prompt_version=PRD_REVIEW_PROMPT_VERSION,
            status="completed",
            result_markdown=generated["result_markdown"],
            model_id=generated["model_id"],
            trace=generated["trace"],
        )
        return {"status": "ok", "cached": False, "review": review, "prd": self._page_metadata(page)}

    def review_url(self, request: PRDBriefingReviewRequest) -> dict[str, Any]:
        normalized = self._normalize_briefing_review_request(request)
        page = self.confluence.ingest_page(normalized.prd_url, "prd-briefing-review")
        if not page.sections:
            raise ToolError("PRD page did not contain readable sections.")
        selected_page, selection = self._select_sections(page, normalized.selected_section_indexes)
        prompt_version = prd_briefing_review_prompt_version(normalized.language)
        cache_jira_id = _cache_key_for_section_selection(
            PRD_BRIEFING_REVIEW_CACHE_KEY,
            page=page,
            selected_section_indexes=selection["cache_section_indexes"],
        )
        cached = None if normalized.force_refresh else self.store.get_prd_review_result(
            owner_key=normalized.owner_key,
            jira_id=cache_jira_id,
            prd_url=page.source_url,
            prd_updated_at=page.updated_at,
            prompt_version=prompt_version,
        )
        if cached and cached.get("status") == "completed" and cached.get("result_markdown"):
            return {
                "status": "ok",
                "cached": True,
                "review": cached,
                "prd": self._page_metadata(page),
                "language": normalized.language,
                "coverage": selection["coverage"],
            }

        prompt = build_prd_review_prompt(
            jira_id="",
            jira_link="",
            prd_url=page.source_url,
            page=selected_page,
            language=normalized.language,
        )
        try:
            generated = generate_prd_review_with_codex(
                prompt=prompt,
                settings=self.settings,
                workspace_root=self.workspace_root,
                language=normalized.language,
                prompt_version=prompt_version,
            )
        except Exception as error:  # noqa: BLE001 - persist the failure for visible retry context.
            self.store.save_prd_review_result(
                owner_key=normalized.owner_key,
                jira_id=cache_jira_id,
                jira_link="",
                prd_url=page.source_url,
                prd_updated_at=page.updated_at,
                prompt_version=prompt_version,
                status="failed",
                error=str(error),
            )
            raise ToolError(str(error)) from error

        review = self.store.save_prd_review_result(
            owner_key=normalized.owner_key,
            jira_id=cache_jira_id,
            jira_link="",
            prd_url=page.source_url,
            prd_updated_at=page.updated_at,
            prompt_version=prompt_version,
            status="completed",
            result_markdown=generated["result_markdown"],
            model_id=generated["model_id"],
            trace=generated["trace"],
        )
        return {
            "status": "ok",
            "cached": False,
            "review": review,
            "prd": self._page_metadata(page),
            "language": normalized.language,
            "coverage": selection["coverage"],
        }

    def summarize(self, request: PRDReviewRequest) -> dict[str, Any]:
        normalized = self._normalize_request(request)
        page = self.confluence.ingest_page(normalized.prd_url, "prd-summary")
        if not page.sections:
            raise ToolError("PRD page did not contain readable sections.")
        cached = None if normalized.force_refresh else self.store.get_prd_review_result(
            owner_key=normalized.owner_key,
            jira_id=normalized.jira_id,
            prd_url=page.source_url,
            prd_updated_at=page.updated_at,
            prompt_version=PRD_SUMMARY_PROMPT_VERSION,
        )
        if cached and cached.get("status") == "completed" and cached.get("result_markdown"):
            return {"status": "ok", "cached": True, "summary": cached, "prd": self._page_metadata(page)}

        prompt = build_prd_summary_prompt(
            jira_id=normalized.jira_id,
            jira_link=normalized.jira_link,
            prd_url=page.source_url,
            page=page,
        )
        try:
            generated = generate_prd_summary_with_codex(
                prompt=prompt,
                settings=self.settings,
                workspace_root=self.workspace_root,
            )
        except Exception as error:  # noqa: BLE001 - persist the failure for visible retry context.
            self.store.save_prd_review_result(
                owner_key=normalized.owner_key,
                jira_id=normalized.jira_id,
                jira_link=normalized.jira_link,
                prd_url=page.source_url,
                prd_updated_at=page.updated_at,
                prompt_version=PRD_SUMMARY_PROMPT_VERSION,
                status="failed",
                error=str(error),
            )
            raise ToolError(str(error)) from error

        summary = self.store.save_prd_review_result(
            owner_key=normalized.owner_key,
            jira_id=normalized.jira_id,
            jira_link=normalized.jira_link,
            prd_url=page.source_url,
            prd_updated_at=page.updated_at,
            prompt_version=PRD_SUMMARY_PROMPT_VERSION,
            status="completed",
            result_markdown=generated["result_markdown"],
            model_id=generated["model_id"],
            trace=generated["trace"],
        )
        return {"status": "ok", "cached": False, "summary": summary, "prd": self._page_metadata(page)}

    def summarize_url(self, request: PRDBriefingReviewRequest) -> dict[str, Any]:
        normalized = self._normalize_briefing_review_request(request)
        page = self.confluence.ingest_page(normalized.prd_url, "prd-self-assessment-summary")
        if not page.sections:
            raise ToolError("PRD page did not contain readable sections.")
        prompt_version = prd_summary_prompt_version(normalized.language)
        cached = None if normalized.force_refresh else self.store.get_prd_review_result(
            owner_key=normalized.owner_key,
            jira_id=PRD_URL_SUMMARY_CACHE_KEY,
            prd_url=page.source_url,
            prd_updated_at=page.updated_at,
            prompt_version=prompt_version,
        )
        if cached and cached.get("status") == "completed" and cached.get("result_markdown"):
            return {"status": "ok", "cached": True, "summary": cached, "prd": self._page_metadata(page), "language": normalized.language}

        prompt = build_prd_summary_prompt(
            jira_id="",
            jira_link="",
            prd_url=page.source_url,
            page=page,
            language=normalized.language,
        )
        try:
            generated = generate_prd_summary_with_codex(
                prompt=prompt,
                settings=self.settings,
                workspace_root=self.workspace_root,
                language=normalized.language,
                prompt_version=prompt_version,
            )
        except Exception as error:  # noqa: BLE001 - persist the failure for visible retry context.
            self.store.save_prd_review_result(
                owner_key=normalized.owner_key,
                jira_id=PRD_URL_SUMMARY_CACHE_KEY,
                jira_link="",
                prd_url=page.source_url,
                prd_updated_at=page.updated_at,
                prompt_version=prompt_version,
                status="failed",
                error=str(error),
            )
            raise ToolError(str(error)) from error

        summary = self.store.save_prd_review_result(
            owner_key=normalized.owner_key,
            jira_id=PRD_URL_SUMMARY_CACHE_KEY,
            jira_link="",
            prd_url=page.source_url,
            prd_updated_at=page.updated_at,
            prompt_version=prompt_version,
            status="completed",
            result_markdown=generated["result_markdown"],
            model_id=generated["model_id"],
            trace=generated["trace"],
        )
        return {"status": "ok", "cached": False, "summary": summary, "prd": self._page_metadata(page), "language": normalized.language}

    def list_url_sections(self, request: PRDBriefingReviewRequest) -> dict[str, Any]:
        normalized = self._normalize_briefing_review_request(request)
        page = self.confluence.ingest_page(normalized.prd_url, "prd-self-assessment-sections")
        if not page.sections:
            raise ToolError("PRD page did not contain readable sections.")
        return {
            "status": "ok",
            "prd": self._page_metadata(page),
            "sections": [
                {
                    "index": index,
                    "title": section.section_path or section.title or f"Section {index}",
                    "char_count": len(str(section.content or "")),
                    "long": len(str(section.content or "")) >= PRD_SECTION_LONG_CHAR_THRESHOLD,
                }
                for index, section in enumerate(page.sections, start=1)
            ],
        }

    @staticmethod
    def _normalize_request(request: PRDReviewRequest) -> PRDReviewRequest:
        owner_key = str(request.owner_key or "").strip()
        jira_id = str(request.jira_id or "").strip()
        prd_url = str(request.prd_url or "").strip()
        jira_link = str(request.jira_link or "").strip()
        if not owner_key:
            raise ToolError("Owner identity is required.")
        if not jira_id:
            raise ToolError("Jira ID is required.")
        if not prd_url:
            raise ToolError("PRD link is required.")
        if not prd_url.lower().startswith(("http://", "https://")):
            raise ToolError("PRD link must be an HTTP or HTTPS URL.")
        return PRDReviewRequest(
            owner_key=owner_key,
            jira_id=jira_id,
            jira_link=jira_link,
            prd_url=prd_url,
            force_refresh=bool(request.force_refresh),
        )

    @staticmethod
    def _normalize_briefing_review_request(request: PRDBriefingReviewRequest) -> PRDBriefingReviewRequest:
        owner_key = str(request.owner_key or "").strip()
        prd_url = str(request.prd_url or "").strip()
        language = normalize_prd_review_language(request.language)
        selected_section_indexes = _normalize_selected_section_indexes(request.selected_section_indexes)
        if not owner_key:
            raise ToolError("Owner identity is required.")
        if not prd_url:
            raise ToolError("PRD link is required.")
        if not prd_url.lower().startswith(("http://", "https://")):
            raise ToolError("PRD link must be an HTTP or HTTPS URL.")
        return PRDBriefingReviewRequest(
            owner_key=owner_key,
            prd_url=prd_url,
            language=language,
            force_refresh=bool(request.force_refresh),
            selected_section_indexes=selected_section_indexes,
        )

    @staticmethod
    def _select_sections(page: IngestedConfluencePage, selected_section_indexes: list[int] | None) -> tuple[IngestedConfluencePage, dict[str, Any]]:
        total_sections = len(page.sections)
        all_indexes = list(range(1, total_sections + 1))
        if selected_section_indexes is None:
            effective_indexes = all_indexes
            cache_section_indexes = None
        else:
            if not selected_section_indexes:
                raise ToolError("Select at least one PRD section to review.")
            invalid = [index for index in selected_section_indexes if index < 1 or index > total_sections]
            if invalid:
                raise ToolError(f"Selected PRD section index is out of range: {invalid[0]}.")
            effective_indexes = selected_section_indexes
            cache_section_indexes = None if effective_indexes == all_indexes else effective_indexes

        selected_sections = [page.sections[index - 1] for index in effective_indexes]
        selected_page = IngestedConfluencePage(
            page_id=page.page_id,
            title=page.title,
            source_url=page.source_url,
            updated_at=page.updated_at,
            language=page.language,
            sections=selected_sections,
            version_number=page.version_number,
            media_dict=page.media_dict,
            presentation_source_text=page.presentation_source_text,
        )
        coverage = _build_section_coverage(
            total_sections=total_sections,
            selected_indexes=effective_indexes,
            selected_sections=selected_sections,
            selection_hash=_section_selection_hash(effective_indexes) if cache_section_indexes else "",
        )
        return selected_page, {"coverage": coverage, "cache_section_indexes": cache_section_indexes}

    @staticmethod
    def _page_metadata(page: IngestedConfluencePage) -> dict[str, str]:
        return {
            "title": page.title,
            "source_url": page.source_url,
            "updated_at": page.updated_at,
            "page_id": page.page_id,
        }


def _build_prd_source(page: IngestedConfluencePage) -> str:
    sections = []
    used_chars = 0
    for index, section in enumerate(page.sections, start=1):
        content = str(section.content or "").strip()
        if not content:
            continue
        block = f"## Section {index}: {section.section_path}\n{content}"
        remaining = PRD_REVIEW_MAX_SOURCE_CHARS - used_chars
        if remaining <= 0:
            break
        if len(block) > remaining:
            block = block[:remaining].rstrip() + "\n[Truncated because the PRD is long.]"
        sections.append(block)
        used_chars += len(block)
    source = "\n\n".join(sections).strip()
    if not source:
        raise ToolError("PRD page did not contain readable text.")
    return source


def _build_section_coverage(
    *,
    total_sections: int,
    selected_indexes: list[int],
    selected_sections: list[Any],
    selection_hash: str,
) -> dict[str, Any]:
    used_chars = 0
    assessed_indexes: list[int] = []
    omitted_titles: list[str] = []
    truncated = False
    for original_index, section in zip(selected_indexes, selected_sections):
        content = str(section.content or "").strip()
        if not content:
            continue
        title = section.section_path or section.title or f"Section {original_index}"
        block = f"## Section {original_index}: {title}\n{content}"
        remaining = PRD_REVIEW_MAX_SOURCE_CHARS - used_chars
        if remaining <= 0:
            truncated = True
            omitted_titles.append(title)
            continue
        assessed_indexes.append(original_index)
        if len(block) > remaining:
            truncated = True
        used_chars += min(len(block), max(remaining, 0))
    selected_titles = [
        section.section_path or section.title or f"Section {index}"
        for index, section in zip(selected_indexes, selected_sections)
    ]
    return {
        "status": "truncated" if truncated else "full",
        "sections_total": total_sections,
        "selected_sections_total": len(selected_indexes),
        "sections_assessed": len(assessed_indexes),
        "selected_section_indexes": selected_indexes,
        "assessed_section_indexes": assessed_indexes,
        "selected_section_titles": selected_titles,
        "selection_hash": selection_hash,
        "truncated": truncated,
        "omitted_sections": omitted_titles,
    }


def _normalize_selected_section_indexes(value: list[int] | None) -> list[int] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ToolError("selected_section_indexes must be a list of section indexes.")
    indexes: list[int] = []
    seen: set[int] = set()
    for raw_index in value:
        if isinstance(raw_index, bool):
            raise ToolError("selected_section_indexes must contain integer section indexes.")
        try:
            index = int(raw_index)
        except (TypeError, ValueError) as error:
            raise ToolError("selected_section_indexes must contain integer section indexes.") from error
        if index not in seen:
            indexes.append(index)
            seen.add(index)
    return indexes


def _section_selection_hash(selected_section_indexes: list[int]) -> str:
    payload = ",".join(str(index) for index in selected_section_indexes)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _cache_key_for_section_selection(prefix: str, *, page: IngestedConfluencePage, selected_section_indexes: list[int] | None) -> str:
    if not selected_section_indexes:
        return prefix
    section_fingerprint = "|".join(
        f"{index}:{page.sections[index - 1].section_path}:{hashlib.sha256(str(page.sections[index - 1].content or '').encode('utf-8')).hexdigest()[:12]}"
        for index in selected_section_indexes
    )
    selection_hash = hashlib.sha256(section_fingerprint.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}:sections:{selection_hash}"


def normalize_prd_review_language(language: str | None) -> str:
    normalized = str(language or "zh").strip().lower()
    return "en" if normalized in {"en", "english"} else "zh"


def prd_briefing_review_prompt_version(language: str | None) -> str:
    return f"{PRD_REVIEW_PROMPT_VERSION}_briefing_{normalize_prd_review_language(language)}"


def prd_summary_prompt_version(language: str | None) -> str:
    return f"{PRD_SUMMARY_PROMPT_VERSION}_{normalize_prd_review_language(language)}"


def build_prd_review_prompt(
    *,
    jira_id: str,
    jira_link: str,
    prd_url: str,
    page: IngestedConfluencePage,
    language: str = "zh",
) -> str:
    source = _build_prd_source(page)
    if normalize_prd_review_language(language) == "en":
        return f"""# Role
You are a senior PRD delivery-readiness reviewer using the `prd-review` skill. Your job is to help the PM improve the PRD section by section so engineering, QA, release, operations, and support can execute without guessing.

# Review Boundary
- Review only delivery logic: business-flow closure, role actions, ownership, approvals, handoffs, status transitions, rule precedence, exception paths, reverse paths, operational fallback, and QA/dev acceptance clarity.
- Do not evaluate business value, ROI, KPI choice, API design, database design, code architecture, implementation approach, or engineering effort.
- Do not invent rules. If the selected PRD sections do not contain evidence for a claim, write `Source not found in selected sections`.

# Task
Assess the selected PRD sections for delivery readiness and provide concrete section-level improvement guidance. Every important blocker or gap must name the PRD section that should be changed and tell the PM exactly what to add or rewrite.

# Scoring Rubric
- **9 - 10 (Ready):** Main flow, exception paths, reverse paths, rule precedence, role actions, and fallback are clear enough for dev and QA.
- **7 - 8 (Needs Minor Clarification):** Happy path is clear, with only minor non-blocking clarifications.
- **4 - 6 (Needs Major Clarification):** Development, QA, release, operations, or support will likely be blocked by missing logic or unclear rules.
- **1 - 3 (Not Ready):** Core flow is broken, contradictory, or impossible to validate from the PRD.

# Required Review Checks
1. **Flow closure:** Can each user/system role move from trigger to terminal state without an undefined next step?
2. **Rule rigor:** Are preconditions, status changes, priorities, validation rules, and overlapping rules unambiguous?
3. **Role and ownership clarity:** Is it clear who can act, approve, reject, withdraw, edit, retry, or override at each step?
4. **Exception and reverse paths:** Are timeout, interruption, retry, rejection, rollback, cancellation, blacklist, permission failure, and manual fallback handled?
5. **Acceptance clarity:** Can QA/dev verify the expected behavior after the PRD is updated?

# Output Format
Return only concise Markdown in English. Every blocker or gap must include a section name.
---
### Delivery Logic Assessment
- **Final Score:** [X] / 10
- **Dev-Readiness Verdict:** [Ready / Needs Clarification / Not Ready]
- **Reason:** [One concise reason grounded in the PRD.]

### Critical Section Gaps
- **Section:** [section title, or Source not found in selected sections]
  - **Gap:** [What is missing, contradictory, or undefined.]
  - **Impact:** [Why this blocks development, QA, release, operations, or support.]

### Section-by-Section Improvement Suggestions
1. **Section: [section title]**
   - **Gap:** [What the current section does not define.]
   - **Why it matters:** [Delivery, QA, release, ops, or support impact.]
   - **Suggested PRD update:** [Specific rule, state, fallback, wording, or acceptance condition to add.]
   - **Acceptance check:** [What QA/dev should be able to verify after the PRD is updated.]
2. ...

### Missing Exception / Reverse Paths
- **Section:** [section title]
  - **Scenario:** [Concrete exception or reverse-path scenario.]
  - **Suggested PRD update:** [What the PM should add.]

### PM Clarification Checklist
- [Concrete yes/no or rule-choice question the PM should answer.]
---

# Review Context
- Jira ID: {jira_id or "-"}
- Jira Link: {jira_link or "-"}
- PRD Title: {page.title}
- PRD Link: {prd_url}
- PRD Updated At: {page.updated_at or "-"}

# PRD Content
{source}
"""
    return f"""# Role
你是一位资深 PRD 交付就绪度评审专家，使用 `prd-review` Skill 的方法论工作。你的目标不是评价产品想法，而是帮助 PM 逐个 Section 把 PRD 改到研发、QA、上线、运营和客服都能执行。

# 评审边界
- 只评审交付逻辑：业务流程闭环、角色动作、权限/归属、审批/交接、状态流转、规则优先级、异常路径、逆向路径、运营兜底、QA/研发验收清晰度。
- 不评价商业价值、ROI、指标选择、API 设计、数据库设计、代码架构、实现方案或研发工期。
- 不臆测 PRD 没写的规则。如果选中的 PRD Section 中没有依据，必须写 `Source not found in selected sections`。

# Task
请评估选中的 PRD Section 是否具备交付就绪度，并输出可直接帮助 PM 修改 PRD 的 Section 级建议。每个关键 blocker 或 gap 都必须指出应该修改哪个 PRD Section，并说明 PM 具体要补写或改写什么。

# 评分标准
- **9 - 10 分 (Ready)：** 主流程、异常、逆向、规则优先级、角色动作、运营兜底都足够清晰，研发和 QA 可以直接接手。
- **7 - 8 分 (Needs Minor Clarification)：** 主流程清楚，仅有少量不阻断交付的澄清项。
- **4 - 6 分 (Needs Major Clarification)：** 存在会阻塞研发、QA、上线、运营或客服承接的流程/规则缺口。
- **1 - 3 分 (Not Ready)：** 核心流程断裂、规则自相矛盾，或无法从 PRD 判断用户/系统下一步。

# 必查维度
1. **流程闭环：** 每个用户/系统角色是否能从触发条件走到终态，没有未定义的下一步？
2. **规则严密性：** 前置条件、状态变化、优先级、校验规则、多规则叠加是否清楚？
3. **角色与权限：** 谁能提交、审批、拒绝、撤回、编辑、重试、覆盖或人工介入是否明确？
4. **异常与逆向：** 超时、中断、重试、拒绝、回滚、取消、拉黑、权限失败、人工兜底是否有处理方式？
5. **验收清晰度：** PRD 补完后，QA/研发是否能验证行为正确？

# Output Format
请严格按以下结构输出精炼 Markdown。每个 blocker 或 gap 都必须包含 Section 名称。
---
### 交付逻辑评估
- **最终得分：** [X] / 10
- **Dev-Readiness Verdict：** [Ready / Needs Clarification / Not Ready]
- **判分理由：** [一句话说明，必须基于 PRD 内容。]

### 关键 Section 缺口
- **Section：** [section title，或 Source not found in selected sections]
  - **当前缺口：** [缺什么、哪里矛盾、哪里未定义。]
  - **交付影响：** [为什么会阻塞研发、QA、上线、运营或客服。]

### Section-by-Section 修改建议
1. **Section：[section title]**
   - **当前缺口：** [当前 Section 没有定义什么。]
   - **交付影响：** [对研发、QA、上线、运营或客服的影响。]
   - **建议补写：** [建议 PM 具体补哪条规则、哪个状态、哪个 fallback、哪句说明或哪个验收条件。]
   - **验收检查：** [补完后 QA/研发应该能验证什么。]
2. ...

### 缺失的异常 / 逆向路径
- **Section：** [section title]
  - **场景：** [具体异常或逆向流程场景。]
  - **建议补写：** [PM 应该补充的处理方式。]

### PM 待澄清清单
- [PM 需要回答的具体 yes/no 或规则选择问题。]
---

# Review Context
- Jira ID: {jira_id or "-"}
- Jira Link: {jira_link or "-"}
- PRD Title: {page.title}
- PRD Link: {prd_url}
- PRD Updated At: {page.updated_at or "-"}

# PRD Content
{source}
"""


def build_prd_summary_prompt(
    *,
    jira_id: str,
    jira_link: str,
    prd_url: str,
    page: IngestedConfluencePage,
    language: str = "zh",
) -> str:
    source = _build_prd_source(page)
    if normalize_prd_review_language(language) == "en":
        return f"""# Role
You are a senior product manager who turns PRDs into concise summaries that business, product, and engineering readers can use immediately.

# Task
Read the PRD content and produce a concise but complete English summary. Focus on:
1. Background and objective.
2. User/business flow.
3. Main functional scope.
4. Key rules, data definitions, and status transitions.
5. Launch dependencies and open questions.

# Output Format
Return only Markdown:
---
### PRD Summary
[3-5 sentence overview]

### Scope
- ...

### Key Logic
- ...

### Dependencies / Open Questions
- ...
---

# Context
- Jira ID: {jira_id or "-"}
- Jira Link: {jira_link or "-"}
- PRD Title: {page.title}
- PRD Link: {prd_url}
- PRD Updated At: {page.updated_at or "-"}

# PRD Content
{source}
"""
    return f"""# Role
你是一位资深产品经理，擅长把 PRD 快速整理成业务、产品、研发都能直接阅读的摘要。

# Task
请阅读 PRD 内容，输出一份简洁但完整的中文摘要。重点覆盖：
1. 背景和目标。
2. 用户/业务流程。
3. 主要功能范围。
4. 关键规则、数据口径、状态流转。
5. 上线/依赖/待确认事项。

# Output Format
请严格按 Markdown 输出：
---
### PRD Summary
[3-5 句话概括]

### Scope
- ...

### Key Logic
- ...

### Dependencies / Open Questions
- ...
---

# Context
- Jira ID: {jira_id or "-"}
- Jira Link: {jira_link or "-"}
- PRD Title: {page.title}
- PRD Link: {prd_url}
- PRD Updated At: {page.updated_at or "-"}

# PRD Content
{source}
"""


def generate_prd_review_with_codex(
    *,
    prompt: str,
    settings: Settings,
    workspace_root: Path,
    language: str = "zh",
    prompt_version: str = PRD_REVIEW_PROMPT_VERSION,
) -> dict[str, Any]:
    return _generate_with_codex(
        prompt=prompt,
        settings=settings,
        workspace_root=workspace_root,
        system_text=build_prd_review_system_text(language),
        prompt_mode=prompt_version,
    )


def build_prd_review_system_text(language: str | None = "zh") -> str:
    output_language = "English" if normalize_prd_review_language(language) == "en" else "Chinese"
    return (
        "You are using the prd-review skill to assess PRD delivery readiness. "
        "Review only delivery logic: flow closure, role actions, ownership, status transitions, rule rigor, "
        "exception and reverse paths, operational fallback, and QA/dev acceptance clarity. "
        "Do not evaluate business value, metrics, architecture, APIs, databases, code, implementation effort, or ROI. "
        "Every important blocker or gap must name the affected PRD section and include a concrete Suggested PRD update "
        "plus an Acceptance check. If evidence is not in the selected sections, say `Source not found in selected sections`. "
        f"Return only the requested Markdown review in {output_language}. Do not include tool logs."
    )


def generate_prd_summary_with_codex(
    *,
    prompt: str,
    settings: Settings,
    workspace_root: Path,
    language: str = "zh",
    prompt_version: str = PRD_SUMMARY_PROMPT_VERSION,
) -> dict[str, Any]:
    output_language = "English" if normalize_prd_review_language(language) == "en" else "Chinese"
    return _generate_with_codex(
        prompt=prompt,
        settings=settings,
        workspace_root=workspace_root,
        system_text=(
            "You are a senior product manager. "
            f"Return only the requested Markdown PRD summary in {output_language}. Do not include tool logs."
        ),
        prompt_mode=prompt_version,
    )


def _generate_with_codex(
    *,
    prompt: str,
    settings: Settings,
    workspace_root: Path,
    system_text: str,
    prompt_mode: str,
) -> dict[str, Any]:
    provider = CodexCliBridgeSourceCodeQALLMProvider(
        workspace_root=workspace_root,
        timeout_seconds=settings.source_code_qa_codex_timeout_seconds,
        concurrency_limit=settings.source_code_qa_codex_concurrency,
        session_mode="ephemeral",
        codex_binary=os.getenv("SOURCE_CODE_QA_CODEX_BINARY") or None,
    )
    result = provider.generate(
        payload={
            "systemInstruction": {"parts": [{"text": system_text}]},
            "contents": [{"parts": [{"text": prompt}]}],
            "codex_prompt_mode": prompt_mode,
        },
        primary_model=os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli"),
        fallback_model=os.getenv("SOURCE_CODE_QA_CODEX_MODEL", "codex-cli"),
    )
    return {
        "result_markdown": provider.extract_text(result.payload),
        "model_id": result.model,
        "trace": result.payload.get("codex_cli_trace") if isinstance(result.payload, dict) else {},
    }
