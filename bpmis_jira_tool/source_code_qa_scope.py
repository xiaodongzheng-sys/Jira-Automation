"""Scope and conversation compatibility for Source Code Q&A."""
ALL_ONLY_RISK_TEAMS = frozenset({"CRMS", "GRC"})
SOURCE_CONTEXT_VERSION = 1
RETIRED_UPLOAD_MESSAGE = "DB / Apollo Uploads has been removed."


def current_source_context(context):
    """Do not resume conversations built with the retired upload evidence."""
    if isinstance(context, dict) and context.get("source_context_version") == SOURCE_CONTEXT_VERSION:
        return context
    return None
