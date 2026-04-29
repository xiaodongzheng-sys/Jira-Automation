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


PRD_REVIEW_PROMPT_VERSION = "v2_delivery_logic_review_codex"
PRD_SUMMARY_PROMPT_VERSION = "v1_prd_summary_codex"
PRD_REVIEW_MAX_SOURCE_CHARS = 90_000


@dataclass(frozen=True)
class PRDReviewRequest:
    owner_key: str
    jira_id: str
    jira_link: str
    prd_url: str
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


def build_prd_review_prompt(
    *,
    jira_id: str,
    jira_link: str,
    prd_url: str,
    page: IngestedConfluencePage,
) -> str:
    source = _build_prd_source(page)
    return f"""# Role
你是一位极其严谨的“产品交付与逻辑排雷专家”。你的信条是“完美的流程胜过一切”。

# Core Principles (绝对遵守的原则)
在进行 PRD 评审时，你必须严格遵守以下“两不”原则：
1. **不问商业价值：** 默认该需求的背景、业务目标和数据指标已在上游被充分论证。**绝对不要**在评审中提出“为什么要做这个”、“核心指标是什么”这类问题。
2. **不管技术细节：** 默认研发团队具备极强的架构和编码能力。**绝对不要**对前后端如何交互、数据库表怎么建、API 字段怎么写指手画脚。

# Task
你的唯一任务是：对这篇 PRD 的“业务流程”和“执行逻辑”进行极其苛刻的压力测试，找出流程断点、规则冲突以及被遗漏的边缘场景。

# Review Dimensions (核心排雷维度)
请从以下3个核心维度，给 PRD 进行打分（1-10分），并给出具体评审意见。

1. **主流程的绝对闭环 (Happy Path Completeness):**
   - 流程是否能从起点顺畅走到终点？
   - 是否存在让用户/系统“卡住”无法进行下一步，也无法返回的逻辑死胡同？

2. **异常分支与逆向流程 (Unhappy Paths & Edge Cases) - [最核心任务]:**
   - **请务必穷举至少 3-5 个容易被忽视的异常场景。**
   - 例如：第三方接口（如征信局查询、活体检测）超时或无响应、用户中途强退杀后台、账户余额不足、状态突然变更（如操作中途被风控系统拉黑/降级）。
   - 针对这些异常，PRD 是否定义了明确的兜底交互或重试机制？

3. **规则冲突与严密性 (Rule Rigor & Conflicts):**
   - 各种前置条件和校验逻辑是否严密？
   - 业务规则之间是否存在互斥或未覆盖的灰色地带？（例如：白名单规则与特定风控拦截规则同时触发时，哪个优先级更高？）

# Output Format
请严格按以下结构输出评审报告，语言要求精炼、直指问题：
---
### 🛠 执行逻辑体检结论
- **结论：** [给出质量得分并一句话评价该 PRD 的逻辑严密度，例如：“8/10分，主流程清晰，但缺失关键的风控降级兜底方案”]

### 🚧 致命逻辑断点与冲突 (Critical Logic Blockers)
- [列出可能导致流程彻底卡死、或规则自相矛盾的严重逻辑漏洞，如果没有则写“无”]
- [详细说明为何会卡死]

### 🚩 必须补齐的异常分支 (Missing Edge Cases)
1. **[场景名称]：** [描述具体异常场景，例如“活体检测接口持续超时”]
   - **问题：** PRD 未定义此情况下的系统行为。
   - **建议补齐：** 明确是引导用户重试、直接拒绝、还是转入人工审核队列。
2. ...
3. ...

### 🛡 后勤与兜底确认 (Ops Check)
- [提醒 PM 确认的人工兜底方案，例如：“请确认如果用户被误杀拉黑，客服侧是否有解除黑名单的 SOP 和后台权限？”]
---

# Review Context
- Jira ID: {jira_id}
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
) -> dict[str, Any]:
    return _generate_with_codex(
        prompt=prompt,
        settings=settings,
        workspace_root=workspace_root,
        system_text=(
            "You are a rigorous product delivery and execution-logic reviewer. "
            "Review only business flow closure, exception paths, rule rigor, conflicts, and operational fallback. "
            "Do not ask about business value, metrics, architecture, APIs, databases, or implementation details. "
            "Return only the requested Markdown review. Do not include tool logs."
        ),
        prompt_mode=PRD_REVIEW_PROMPT_VERSION,
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
