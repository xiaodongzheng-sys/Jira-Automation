class ToolError(Exception):
    """Base application error."""


class ConfigError(ToolError):
    """Raised when configuration is incomplete."""


class AuthenticationError(ToolError):
    """Raised when auth state is missing or invalid."""


class BPMISError(ToolError):
    """Raised when BPMIS interaction fails."""


class BPMISNotConfiguredError(BPMISError):
    """Raised when the BPMIS transport is not configured enough to run."""


class FieldResolutionError(ToolError):
    """Raised when Jira field mappings cannot be resolved."""

