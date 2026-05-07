from __future__ import annotations

import re
from typing import Any


STATIC_QA_RULES: tuple[dict[str, Any], ...] = (
    {
        "kind": "hardcoded_secret",
        "severity": "high",
        "score": 214,
        "pattern": re.compile(r"\b(password|passwd|secret|token|api[_-]?key|access[_-]?key)\b\s*[:=]\s*[\"'][^\"'${}]{4,}[\"']", re.IGNORECASE),
        "reason": "hardcoded credential-like value",
    },
    {
        "kind": "sql_string_concatenation",
        "severity": "high",
        "score": 208,
        "pattern": re.compile(r"\b(select|insert|update|delete)\b[^;\n]{0,180}(?:\+|\.format\s*\(|%s|\$\{)", re.IGNORECASE),
        "reason": "SQL appears to be assembled with string interpolation/concatenation",
    },
    {
        "kind": "command_execution",
        "severity": "high",
        "score": 204,
        "pattern": re.compile(r"\b(Runtime\.getRuntime\(\)\.exec|ProcessBuilder|subprocess\.(?:Popen|call|run)|os\.system)\s*\(", re.IGNORECASE),
        "reason": "command execution path needs input validation review",
    },
    {
        "kind": "unsafe_eval_exec",
        "severity": "high",
        "score": 204,
        "pattern": re.compile(r"\b(eval|exec)\s*\(", re.IGNORECASE),
        "reason": "dynamic code execution is risky",
    },
    {
        "kind": "unsafe_deserialization",
        "severity": "high",
        "score": 200,
        "pattern": re.compile(r"\b(ObjectInputStream|pickle\.loads|yaml\.load)\s*\(", re.IGNORECASE),
        "reason": "unsafe deserialization needs trust-boundary review",
    },
    {
        "kind": "broad_exception",
        "severity": "medium",
        "score": 176,
        "pattern": re.compile(r"\b(?:catch\s*\(\s*(?:Exception|Throwable)\b|except\s+(?:Exception|BaseException)\b)", re.IGNORECASE),
        "reason": "broad exception handling can hide specific failures",
    },
    {
        "kind": "swallowed_exception",
        "severity": "medium",
        "score": 174,
        "pattern": re.compile(r"\b(?:printStackTrace\s*\(|except\s+[^:]+:\s*pass\b|catch\s*\([^)]*\)\s*\{\s*\})", re.IGNORECASE),
        "reason": "exception appears logged weakly or swallowed",
    },
    {
        "kind": "debug_output",
        "severity": "low",
        "score": 140,
        "pattern": re.compile(r"\b(System\.out\.print(?:ln)?|console\.log|print)\s*\(", re.IGNORECASE),
        "reason": "debug output may leak operational details or noisy logs",
    },
    {
        "kind": "todo_fixme",
        "severity": "low",
        "score": 132,
        "pattern": re.compile(r"\b(TODO|FIXME|XXX)\b", re.IGNORECASE),
        "reason": "unfinished implementation marker",
    },
)
DEPENDENCY_QUESTION_TERMS = {
    "data", "source", "sources", "integration", "integrations", "upstream", "dependency",
    "dependencies", "call", "chain", "table", "tables", "api", "apis", "client", "clients",
    "service", "services", "fetch", "screening", "provider",
}
DEPENDENCY_PATH_HINTS = (
    "service", "client", "integration", "repository", "dao", "mapper", "adapter", "gateway", "provider", "strategy", "processor",
)
DEPENDENCY_SYMBOL_SUFFIXES = (
    "service", "client", "integration", "repository", "dao", "mapper", "adapter", "gateway", "provider", "facade", "strategy", "processor",
)
LOW_VALUE_CALL_SYMBOLS = {
    "add", "append", "build", "builder", "collect", "contains", "equals", "filter",
    "foreach", "get", "hashcode", "isempty", "list", "log", "map", "of", "orelse",
    "println", "put", "remove", "set", "size", "stream", "string", "tostring",
    "trim", "valueof",
}
LOW_VALUE_FOCUS_TERMS = {
    "term", "loan", "precheck", "pre", "check", "data", "source", "sources",
    "credit", "risk", "sg", "ph", "id", "used", "uses", "using",
}
DATA_SOURCE_HINTS = (
    "datasource", "data source", "source", "sources", "upstream", "table", "jdbc",
    "queryfor", "select", "repository", "mapper", "dao", "client",
    "integration", "provider", "gateway", "api", "userinfo", "customerinfo",
    " read from ", " write to ", " written to ", " comes from ",
    " loaded from ", " fetched from ", " persisted to ",
    "数据源", "来源", "上游", "表", "数据库", "查哪张表", "从哪里来", "哪里取数",
    "读取", "写入",
)
CONCRETE_SOURCE_HINTS = (
    "repository", "mapper", "dao", "jdbc", "queryfor", "select", " from ",
    "table", "client", "integration", "gateway", "api", "http", "endpoint",
    "feign", "resttemplate", "webclient",
)
ANSWER_CONCRETE_SOURCE_HINTS = (
    "repository", "mapper", "dao", "jdbc", "queryfor", "select", "table",
    "client", "integration", "gateway", "api", "http", "endpoint", "feign",
    "resttemplate", "webclient",
)
API_HINTS = ("api", "endpoint", "route", "controller", "requestmapping", "getmapping", "postmapping", "http", "url", "path")
API_HINTS = (*API_HINTS, "接口", "端点", "路由", "入口", "请求", "调用")
CONFIG_HINTS = ("config", "configuration", "property", "properties", "yaml", "yml", "env", "setting", "feature", "flag")
CONFIG_HINTS = (*CONFIG_HINTS, "配置", "开关", "参数", "环境变量", "属性", "在哪里配", "怎么配置")
MODULE_DEPENDENCY_HINTS = (
    "dependency", "dependencies", "depend on", "module", "maven", "gradle", "pom",
    "artifact", "artifactid", "groupid", "package.json", "npm", "yarn", "pnpm",
    "依赖", "模块", "哪个包", "哪个模块",
)
ERROR_HINTS = ("error", "exception", "failed", "failure", "stacktrace", "status", "code", "timeout", "报错", "异常", "失败", "超时", "错误")
RULE_HINTS = ("rule", "condition", "logic", "validate", "validation", "permission", "access", "approval", "eligible", "规则", "条件", "逻辑", "校验", "权限", "审批", "准入")
STATIC_QA_HINTS = (
    "static qa", "static analysis", "code quality", "code smell", "smell", "bug", "bugs",
    "risk", "risks", "security", "vulnerability", "vulnerabilities", "unsafe",
    "hardcoded", "secret", "password", "token", "sql injection", "injection",
    "empty catch", "swallow", "broad exception", "todo", "fixme",
    "静态", "代码质量", "风险", "安全", "漏洞", "硬编码", "密码", "令牌", "注入",
)
IMPACT_ANALYSIS_HINTS = (
    "impact", "impacted", "affect", "affected", "blast radius", "blast-radius",
    "change impact", "if change", "if changed", "who calls", "callers",
    "callees", "upstream", "downstream", "usage", "usages", "dependents",
    "depends on", "what breaks", "regression", "side effect", "side effects",
    "影响", "影响面", "改了会", "谁调用", "调用方", "被谁用", "上游", "下游",
    "依赖方", "会坏", "回归", "副作用",
)
TEST_COVERAGE_HINTS = (
    "test", "tests", "tested", "testing", "coverage", "covered", "unit test",
    "integration test", "spec", "specs", "junit", "pytest", "jest", "mocha",
    "assert", "mockito", "mock", "verify",
    "测试", "覆盖", "单测", "集成测试", "断言", "mock", "有没有测",
)
OPERATIONAL_BOUNDARY_HINTS = (
    "transaction", "transactional", "rollback", "commit", "cache", "cached",
    "cacheable", "cacheevict", "async", "asynchronous", "retry", "retryable",
    "circuit breaker", "circuitbreaker", "rate limit", "ratelimiter",
    "bulkhead", "timeout", "timelimiter", "lock", "schedulerlock",
    "preauthorize", "postauthorize", "authorization", "permission boundary",
    "事务", "回滚", "提交", "缓存", "异步", "重试", "熔断", "限流", "超时",
    "锁", "鉴权", "授权", "权限边界",
)
FIELD_POPULATION_HINTS = (
    "set", "get", "build", "populate", "provider", "factory", "converter",
    "assembler", "initiation", "underwritingbasicinfo", "customerinfo",
    "loaninfo", "creditriskinfo", "underwritinginitiationdto",
)
DATA_CARRIER_SUFFIXES = (
    "dto", "input", "context", "record", "result", "request", "response", "body",
    "do", "entity", "model", "profile", "info", "wrap",
)
QUALITY_GATE_TRACE_STAGE = "quality_gate"
TOOL_LOOP_TRACE_PREFIX = "tool_loop_"
ANSWER_POLICY_REGISTRY = {
    "data_source": {
        "label": "Data source evidence",
        "required_any": ["data_sources"],
        "supporting_any": ["field_population", "data_carriers", "downstream_components"],
        "missing": "concrete upstream source/table/API/repository evidence beyond DTO fields",
    },
    "api": {
        "label": "API surface evidence",
        "required_any": ["api_or_config", "entry_points"],
        "supporting_any": ["downstream_components"],
        "missing": "endpoint/client/API evidence",
    },
    "config": {
        "label": "Configuration evidence",
        "required_any": ["api_or_config"],
        "supporting_any": ["entry_points"],
        "missing": "config/property evidence",
    },
    "module_dependency": {
        "label": "Module dependency evidence",
        "required_any": ["module_dependencies", "api_or_config"],
        "supporting_any": ["external_dependencies", "downstream_components", "entry_points"],
        "missing": "build-file dependency evidence such as Maven, Gradle, npm, or module artifact coordinates",
    },
    "message_flow": {
        "label": "Message flow evidence",
        "required_any": ["message_flows"],
        "supporting_any": ["api_or_config", "entry_points", "downstream_components"],
        "missing": "message producer/consumer evidence such as Kafka topic, queue, publisher, or listener",
    },
    "logic": {
        "label": "Rule or error logic evidence",
        "required_any": ["rule_or_error_logic", "entry_points"],
        "supporting_any": ["downstream_components"],
        "missing": "rule/error handling evidence",
    },
    "general": {
        "label": "Specific code evidence",
        "required_any": ["entry_points", "downstream_components", "data_sources", "api_or_config", "rule_or_error_logic"],
        "supporting_any": [],
        "missing": "specific code evidence",
    },
    "static_qa": {
        "label": "Static QA evidence",
        "required_any": ["static_findings"],
        "supporting_any": ["entry_points", "rule_or_error_logic"],
        "missing": "static QA finding evidence such as risky exception handling, hardcoded secret, unsafe SQL, command execution, or TODO/FIXME",
    },
    "impact_analysis": {
        "label": "Impact analysis evidence",
        "required_any": ["impact_surfaces"],
        "supporting_any": ["entry_points", "downstream_components", "api_or_config", "data_sources"],
        "missing": "caller/callee or graph evidence showing upstream and downstream impact surfaces",
    },
    "test_coverage": {
        "label": "Test coverage evidence",
        "required_any": ["test_coverage"],
        "supporting_any": ["entry_points", "downstream_components"],
        "missing": "test file, test case, assertion, mock, or verification evidence covering the target symbol",
    },
    "operational_boundary": {
        "label": "Operational boundary evidence",
        "required_any": ["operational_boundaries"],
        "supporting_any": ["entry_points", "api_or_config", "downstream_components"],
        "missing": "transaction/cache/async/retry/circuit-breaker/security boundary annotation evidence",
    },
}
