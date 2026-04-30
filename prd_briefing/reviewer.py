from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.errors import ToolError
from bpmis_jira_tool.source_code_qa import CodexCliBridgeSourceCodeQALLMProvider

from .confluence import ConfluenceConnector, IngestedConfluencePage
from .storage import BriefingStore


PRD_REVIEW_PROMPT_VERSION = "v3_strict_delivery_logic_review_codex"
PRD_SUMMARY_PROMPT_VERSION = "v1_prd_summary_codex"
PRD_REVIEW_MAX_SOURCE_CHARS = 90_000
PRD_BRIEFING_REVIEW_CACHE_KEY = "__prd_briefing_url_review__"


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
        prompt_version = prd_briefing_review_prompt_version(normalized.language)
        cached = None if normalized.force_refresh else self.store.get_prd_review_result(
            owner_key=normalized.owner_key,
            jira_id=PRD_BRIEFING_REVIEW_CACHE_KEY,
            prd_url=page.source_url,
            prd_updated_at=page.updated_at,
            prompt_version=prompt_version,
        )
        if cached and cached.get("status") == "completed" and cached.get("result_markdown"):
            return {"status": "ok", "cached": True, "review": cached, "prd": self._page_metadata(page), "language": normalized.language}

        prompt = build_prd_review_prompt(
            jira_id="",
            jira_link="",
            prd_url=page.source_url,
            page=page,
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
                jira_id=PRD_BRIEFING_REVIEW_CACHE_KEY,
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
            jira_id=PRD_BRIEFING_REVIEW_CACHE_KEY,
            jira_link="",
            prd_url=page.source_url,
            prd_updated_at=page.updated_at,
            prompt_version=prompt_version,
            status="completed",
            result_markdown=generated["result_markdown"],
            model_id=generated["model_id"],
            trace=generated["trace"],
        )
        return {"status": "ok", "cached": False, "review": review, "prd": self._page_metadata(page), "language": normalized.language}

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
        )

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


def normalize_prd_review_language(language: str | None) -> str:
    normalized = str(language or "zh").strip().lower()
    return "en" if normalized in {"en", "english"} else "zh"


def prd_briefing_review_prompt_version(language: str | None) -> str:
    return f"{PRD_REVIEW_PROMPT_VERSION}_briefing_{normalize_prd_review_language(language)}"


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
You are an extremely rigorous product delivery and execution-logic reviewer. Your standard is simple: a PRD is only dev-ready when the business flow is closed, the rules do not conflict, and the failure paths are explicit.

# Core Principles
1. **Do not question business value:** Assume the background, business goal, and success metrics are already justified. Do not ask why the feature should exist.
2. **Do not review technical implementation:** Assume engineering can implement correctly. Do not comment on code, database design, APIs, or architecture.
3. **Score honestly and create separation:** Do not average everything into 6 or 7. Use low scores for broken execution logic and reserve 9-10 for genuinely dev-ready documents.

# Task
Pressure-test this PRD only for business process and execution logic. Identify flow dead ends, missing exception paths, rule conflicts, and missing operational fallback.

# Scoring Rubric
- **9 - 10 (Ready for Dev):** The flow is closed end to end, key exception paths have fallback, and operational/customer-support intervention is clear.
- **7 - 8 (Minor Gaps):** The happy path is clear, with only 1-2 secondary edge cases missing.
- **4 - 6 (Major Blockers):** Main-flow blockers, unresolved rule conflicts, or weak exception handling would likely block delivery.
- **1 - 3 (Fundamentally Broken):** The core flow is incomplete or internally inconsistent.

# Review Dimensions
1. **Happy Path Closure:** Can the user/system move from start to finish without getting stuck?
2. **Exception and Reverse Paths:** Are timeout, interruption, retry, rejection, rollback, blacklisting, cancellation, and other edge cases handled?
3. **Rule Conflicts:** Are preconditions, priorities, statuses, and overlapping rules unambiguous?

# Output Format
Return only concise Markdown in English:
---
### Logic Rigor Assessment
- **Final Score: [X] / 10**
- **Reason:** [One sentence explaining the score.]

### Critical Flow Breaks and Conflicts
- [List blockers that can break the flow or make rules contradictory. Write "None" if there are none.]

### Missing Edge Cases
1. **[Scenario Name]:** [Concrete exception scenario]
   - **Issue:** [What the PRD does not define.]
   - **Recommendation:** [What should be clarified.]
2. ...

### Ops Fallback Checks
- [Operational or manual fallback items the PM should confirm.]
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
你是一位极其严谨、甚至有些毒舌的“产品交付与逻辑排雷专家”。你的信条是“完美的流程胜过一切”，对于千疮百孔的逻辑零容忍。

# Core Principles (绝对遵守的原则)
在进行 PRD 评审时，你必须严格遵守以下原则：
1. **不问商业价值：** 默认该需求的背景、业务目标已充分论证。绝对不要问“为什么要做”、“指标是什么”。
2. **不管技术细节：** 默认研发极其靠谱。绝对不要对代码怎么写、数据库怎么建、API 怎么定指手画脚。
3. **实事求是，拉开分差（核心要求）：** 你的打分必须真实反映 PRD 质量，**绝对禁止**一碗水端平均匀给 6 分或 7 分。遇到烂文档必须敢于打低分（1-4分），遇到逻辑极其严密的文档要吝啬地给出高分（9-10分）。

# Task
你的唯一任务是：对这篇 PRD 的“业务流程”和“执行逻辑”进行极其苛刻的压力测试，找出流程断点、规则冲突以及被遗漏的边缘场景。

# Scoring Rubric (严苛的评分标准)
请根据以下标准对 PRD 的“执行逻辑严密度”进行 1-10 分的评估：
- **9 - 10 分 (Ready for Dev)：** 逻辑天衣无缝，主流程闭环，所有边界异常（如断网、超时、拉黑）均有兜底，客服/运营介入机制清晰。
- **7 - 8 分 (Minor Gaps)：** 主流程顺畅，但缺失 1-2 个次要的异常分支或边缘场景，稍加补充即可。
- **4 - 6 分 (Major Blockers)：** 存在阻断主流程的逻辑死角，规则之间有明显冲突，或完全没有考虑逆向/异常情况。打回重写。
- **1 - 3 分 (Fundamentally Broken)：** 毫无逻辑，流程断裂，基本盘崩溃。

# Review Dimensions (核心排雷维度)
1. **主流程闭环 (Happy Path)：** 能否从起点走到终点？有无让用户/系统卡死的死胡同？
2. **异常分支与逆向 (Edge Cases)：** 第三方接口超时、用户强退、余额不足、中途被风控拉黑降级等情况是否有兜底和重试机制？（务必穷举 3-5 个异常场景）
3. **规则冲突 (Rule Conflicts)：** 各种前置条件是否严密？多重规则叠加时是否存在优先级冲突的灰色地带？

# Output Format
请严格按以下结构输出评审报告，语言要求精炼、直指问题：
---
### 📊 逻辑严密度评估
- **最终得分：[X] / 10**
- **判分理由：** [一句话解释为什么给这个分数。例如：“给了 4 分，因为虽然主流程通畅，但完全遗漏了活体检测失败后的降级与重试逻辑，会导致严重的用户卡死。”]

### 🚧 致命逻辑断点与冲突 (Critical Blockers)
- [列出可能导致流程彻底卡死、或规则自相矛盾的严重逻辑漏洞。若无则写“无”]

### 🚩 必须补齐的异常分支 (Missing Edge Cases)
1. **[场景名称]：** [描述具体异常场景，例如“征信局接口持续超时”]
   - **问题：** PRD 未定义此情况下的系统行为。
   - **建议：** 明确是引导重试、直接拒绝、还是转入人工审核队列。
2. ...

### 🛡 后勤与兜底确认 (Ops Check)
- [提醒 PM 确认的人工兜底方案，例如：“请确认若风控产生误杀，客服是否有解除限制的后台权限？”]
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
) -> str:
    source = _build_prd_source(page)
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
- Jira ID: {jira_id}
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
        "You are a rigorous and blunt product delivery and execution-logic reviewer. "
        "Review only business flow closure, exception paths, rule rigor, conflicts, and operational fallback. "
        "Score harshly and spread scores honestly: use 1-4 for broken documents, 4-6 for major blockers, "
        "7-8 only for minor gaps, and 9-10 only for dev-ready documents with complete edge-case fallback. "
        "Do not ask about business value, metrics, architecture, APIs, databases, or implementation details. "
        f"Return only the requested Markdown review in {output_language}. Do not include tool logs."
    )


def generate_prd_summary_with_codex(
    *,
    prompt: str,
    settings: Settings,
    workspace_root: Path,
) -> dict[str, Any]:
    return _generate_with_codex(
        prompt=prompt,
        settings=settings,
        workspace_root=workspace_root,
        system_text=(
            "You are a senior product manager. "
            "Return only the requested Markdown PRD summary. Do not include tool logs."
        ),
        prompt_mode=PRD_SUMMARY_PROMPT_VERSION,
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
