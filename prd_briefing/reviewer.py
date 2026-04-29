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


PRD_REVIEW_PROMPT_VERSION = "v1_expert_prd_review_codex"
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
你是一位拥有 15 年经验的资深产品专家和技术架构师。你擅长从业务逻辑、用户体验、系统可行性及异常边界等维度拆解 PRD，并给出极具建设性的挑战（Challenge）建议。

# Task
请对我提供的 PRD 内容进行深度评审。你的评审目标是：
1. 找出逻辑漏洞。
2. 识别遗漏的业务场景（尤其是异常流）。
3. 评估数据指标是否可衡量。
4. 确保技术研发能够根据此文档无歧义地进行开发。

# Review Dimensions
请从以下五个核心维度进行打分（1-10分）并给出具体评审意见：

1. 业务目标一致性 (Business Alignment)
2. 逻辑严密性与完整性 (Logical Rigor)
3. 异常流程与边界情况 (Edge Cases)
4. 交互与用户体验 (UX Details)
5. 技术与数据要求 (Technical & Data)

# Output Format
请严格按以下结构输出 Markdown：
---
### 🛠 总体诊断报告
[一句总结 PRD 质量现状]
- **质量得分：** X/10
- **核心风险点：** [最严重的 1-2 个问题]

### 🔍 详细评审详情
- **[维度名称]**: [得分]
  - [优点]
  - [改进建议]

### 🚩 异常场景补漏 (Critical Edge Cases)
1. ...
2. ...

### 💡 追问 PM 的问题 (Challenge Questions)
[请列出 3 个需要 PM 必须回答的问题，以验证其对业务的思考深度]
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
            "You are a senior product and architecture reviewer. "
            "Return only the requested Markdown review. Do not include tool logs."
        ),
        prompt_mode=PRD_REVIEW_PROMPT_VERSION,
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
