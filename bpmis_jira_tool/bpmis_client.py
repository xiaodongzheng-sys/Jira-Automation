from __future__ import annotations

from bpmis_jira_tool.bpmis import BPMISClient, BPMISDirectApiClient
from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.local_agent_client import LocalAgentClient, RemoteBPMISClient


def build_bpmis_client(settings: Settings, access_token: str | None = None) -> BPMISClient:
    if _local_agent_bpmis_enabled(settings):
        return RemoteBPMISClient(
            LocalAgentClient(
                base_url=settings.local_agent_base_url or "",
                hmac_secret=settings.local_agent_hmac_secret or "",
                timeout_seconds=settings.local_agent_timeout_seconds,
                connect_timeout_seconds=settings.local_agent_connect_timeout_seconds,
            ),
            access_token=access_token,
        )
    return BPMISDirectApiClient(settings, access_token=access_token)


def _local_agent_bpmis_enabled(settings: Settings) -> bool:
    mode = (settings.bpmis_call_mode or "").strip().lower()
    return bool(
        mode in {"local_agent", "local-agent", "proxy", "remote"}
        and settings.local_agent_bpmis_enabled
        and settings.local_agent_base_url
        and settings.local_agent_hmac_secret
    )
