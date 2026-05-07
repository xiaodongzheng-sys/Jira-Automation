from __future__ import annotations

import re


HTTPS_URL_PATTERN = re.compile(r"^https://[^/\s]+/.+\.git$")
IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{2,}")
CLASS_DEF_PATTERN = re.compile(r"\b(class|interface|enum)\s+([A-Za-z_][A-Za-z0-9_]*)\b")
JAVA_PACKAGE_PATTERN = re.compile(r"^\s*package\s+([A-Za-z_][A-Za-z0-9_.]*)\s*;")
JAVA_IMPORT_PATTERN = re.compile(r"^\s*import\s+(?:static\s+)?([A-Za-z_][A-Za-z0-9_.*]*)\s*;")
PY_DEF_PATTERN = re.compile(r"^\s*(class|def)\s+([A-Za-z_][A-Za-z0-9_]*)\b")
JS_DEF_PATTERN = re.compile(r"^\s*(?:export\s+)?(?:async\s+)?function\s+([A-Za-z_][A-Za-z0-9_]*)\b|^\s*(?:export\s+)?(?:const|let|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?:async\s*)?\(")
JAVA_METHOD_DEF_PATTERN = re.compile(
    r"^\s*(?:public|private|protected)?\s*(?:static\s+)?(?:final\s+)?"
    r"[A-Za-z_][A-Za-z0-9_<>, ?\[\]]+\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("
)
SETTER_CALL_PATTERN = re.compile(r"\.set([A-Z][A-Za-z0-9_]*)\s*\(([^)]{1,240})\)")
BUILDER_FIELD_PATTERN = re.compile(r"\.([a-z][A-Za-z0-9_]*)\s*\(([^)]{1,240})\)")
ASSIGNMENT_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_.]*)\s*=\s*([^;]{2,240})")
ANNOTATION_ROUTE_PATTERN = re.compile(r"@(RequestMapping|GetMapping|PostMapping|PutMapping|DeleteMapping|PatchMapping)\s*(?:\(([^)]*)\))?")
FEIGN_CLIENT_PATTERN = re.compile(r"@FeignClient\s*\(([^)]*)\)")
MYBATIS_NAMESPACE_PATTERN = re.compile(r"<mapper\b[^>]*\bnamespace\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
MYBATIS_STATEMENT_PATTERN = re.compile(r"<(select|insert|update|delete)\b[^>]*\bid\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
MYBATIS_RESULT_MAP_PATTERN = re.compile(r"<resultMap\b[^>]*\bid\s*=\s*[\"']([^\"']+)[\"'][^>]*", re.IGNORECASE)
MYBATIS_INCLUDE_PATTERN = re.compile(r"<include\b[^>]*\brefid\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
MYBATIS_ATTR_REFERENCE_PATTERN = re.compile(r"\b(parameterType|resultType|resultMap|type|javaType|ofType)\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
HTTP_LITERAL_PATTERN = re.compile(r"[\"'](https?://[^\"']+|/[A-Za-z0-9_./{}:-]{2,})[\"']")
SQL_TABLE_PATTERN = re.compile(r"\b(?:from|join|update|into)\s+([A-Za-z_][A-Za-z0-9_.$]*)", re.IGNORECASE)
SQL_READ_TABLE_PATTERN = re.compile(r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_.$]*)", re.IGNORECASE)
SQL_WRITE_TABLE_PATTERN = re.compile(r"\b(?:insert\s+into|update|delete\s+from)\s+([A-Za-z_][A-Za-z0-9_.$]*)", re.IGNORECASE)
EXACT_LOOKUP_TERM_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:[./:$-][A-Za-z0-9_][A-Za-z0-9_.:$-]*)+")
CODE_USAGE_SUFFIXES = {".java", ".kt", ".kts", ".py", ".js", ".jsx", ".ts", ".tsx", ".go", ".rb", ".php", ".cs", ".scala"}
NON_FUNCTION_USAGE_SUFFIXES = {".md", ".txt", ".properties", ".yaml", ".yml", ".json", ".jsonl", ".xml", ".toml", ".conf", ".sql"}
USAGE_QUERY_HINTS = (
    " used ",
    " usage ",
    " reference ",
    " references ",
    " referenced ",
    " called ",
    " caller ",
    " call ",
    " where is ",
    " where used ",
    "用到",
    "使用",
    "引用",
    "调用",
    "在哪里",
    "在哪",
)
FUNCTION_USAGE_QUERY_HINTS = (" function", " method", "函数", "方法")
COMPLEX_REASONING_QUERY_HINTS = (
    "logic",
    "calculate",
    "calculation",
    "relationship",
    "relation",
    "between",
    "flow",
    "source",
    "upstream",
    "downstream",
    "impact",
    "test",
    "config",
    "why",
    "how",
    "逻辑",
    "计算",
    "关系",
    "链路",
    "来源",
    "上下游",
    "影响",
    "测试",
    "配置",
    "为什么",
    "如何",
)
PROPERTIES_KEY_PATTERN = re.compile(r"^\s*([A-Za-z0-9_.-]{3,})\s*[:=]")
CONFIG_ASSIGNMENT_PATTERN = re.compile(r"^\s*([A-Za-z0-9_.-]{3,})\s*[:=]\s*(.+?)\s*$")
CONFIG_PLACEHOLDER_PATTERN = re.compile(r"\$\{([A-Za-z0-9_.-]+)(?::[^}]*)?\}")
SPRING_VALUE_PATTERN = re.compile(r"@Value\s*\(\s*[\"']\$\{([^}:]+)(?::[^}]*)?\}[\"']\s*\)")
SPRING_QUALIFIER_PATTERN = re.compile(r"@(Qualifier|Resource)\s*(?:\(\s*(?:name\s*=\s*)?[\"']([^\"']+)[\"']\s*\))?")
SPRING_QUALIFIED_VARIABLE_PATTERN = re.compile(
    r"@(?:Qualifier|Resource)\s*(?:\(\s*(?:name\s*=\s*)?[\"']([^\"']+)[\"']\s*\))\s*"
    r"(?:@[A-Za-z_][A-Za-z0-9_]*(?:\([^)]*\))?\s*)*"
    r"(?:final\s+)?[A-Z][A-Za-z0-9_]*(?:<[^;(){}]*>)?(?:\s*\[\])?\s+([a-z][A-Za-z0-9_]*)"
)
SPRING_PROFILE_PATTERN = re.compile(r"@Profile\s*\(([^)]*)\)")
SPRING_CONDITIONAL_ON_PROPERTY_PATTERN = re.compile(r"@ConditionalOnProperty\s*\(([^)]*)\)")
SPRING_BEAN_NAME_PATTERN = re.compile(r"@(Service|Component|Repository|Controller|RestController|Bean)\s*(?:\(\s*(?:value\s*=\s*|name\s*=\s*)?[\"']([^\"']+)[\"']\s*\))?")
SPRING_PRIMARY_PATTERN = re.compile(r"@Primary\b")
SPRING_AOP_PATTERN = re.compile(r"@(Around|Before|After|AfterReturning|AfterThrowing|Pointcut)\s*(?:\(([^)]*)\))?")
SPRING_SCHEDULED_PATTERN = re.compile(r"@Scheduled\s*(?:\(([^)]*)\))?")
SPRING_ASPECT_PATTERN = re.compile(r"@Aspect\b")
SPRING_INTERCEPTOR_PATTERN = re.compile(r"\b(?:implements\s+)?(?:HandlerInterceptor|AsyncHandlerInterceptor)\b")
MESSAGE_LISTENER_PATTERN = re.compile(r"@(KafkaListener|RabbitListener|JmsListener)\s*\(([^)]*)\)")
MESSAGE_SEND_PATTERN = re.compile(r"\b(?:kafkaTemplate|rabbitTemplate|jmsTemplate|streamBridge)\.(?:send|convertAndSend|sendMessage)\s*\(([^)]*)\)", re.IGNORECASE)
EVENT_PUBLISH_PATTERN = re.compile(r"\b(?:publishEvent|eventBus\.post|applicationEventPublisher\.publishEvent)\s*\(([^)]*)\)")
MAVEN_DEPENDENCY_BLOCK_PATTERN = re.compile(r"<dependency\b[^>]*>(.*?)</dependency>", re.IGNORECASE | re.DOTALL)
MAVEN_TAG_PATTERN = re.compile(r"<([A-Za-z0-9_.-]+)>\s*([^<]+?)\s*</\1>", re.IGNORECASE)
GRADLE_COORDINATE_PATTERN = re.compile(
    r"\b(?:implementation|api|compileOnly|runtimeOnly|testImplementation|classpath)\s*(?:\(|\s)\s*[\"']([A-Za-z0-9_.-]+):([A-Za-z0-9_.-]+)(?::[^\"']+)?[\"']"
)
GRADLE_PROJECT_DEPENDENCY_PATTERN = re.compile(r"\b(?:implementation|api|compileOnly|runtimeOnly|testImplementation)\s*(?:\(|\s).*?project\([\"'](:?[^\"')]+)[\"']\)")
GRADLE_INCLUDE_PATTERN = re.compile(r"\binclude\s+(.+)")
RUNTIME_TRACE_FILENAMES = {
    "source-code-qa-runtime-traces.jsonl",
    "source_code_qa_runtime_traces.jsonl",
    "runtime-traces.jsonl",
    "runtime_traces.jsonl",
}
TEST_PATH_MARKERS = (
    "/test/",
    "/tests/",
    "__tests__/",
    ".spec.",
    ".test.",
    "_test.",
    "test_",
    "spec_",
)
TEST_ANNOTATION_PATTERN = re.compile(
    r"@(?:Test|ParameterizedTest|RepeatedTest|SpringBootTest|WebMvcTest|DataJpaTest|ExtendWith|RunWith)\b"
)
TEST_ASSERTION_PATTERN = re.compile(
    r"\b(?:assert[A-Z][A-Za-z0-9_]*|assertThat|expect|verify|when|given|then|should|self\.assert[A-Z][A-Za-z0-9_]*)\s*\("
)
OPERATIONAL_BOUNDARY_PATTERN = re.compile(
    r"@(Transactional|Cacheable|CacheEvict|CachePut|Async|Retryable|Recover|CircuitBreaker|RateLimiter|Bulkhead|TimeLimiter|SchedulerLock|PreAuthorize|PostAuthorize)\b"
    r"(?:\(([^)]*)\))?"
)
FTS_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_./:-]{2,}")
DECLARATION_HINT_PATTERN = re.compile(
    r"^\s*(class|def|function|func|interface|type|enum|const|let|var|public|private|protected|static|final)\b",
    re.IGNORECASE,
)
PATHISH_PATTERN = re.compile(r"/[A-Za-z0-9_./:-]{3,}")
CALL_SYMBOL_PATTERN = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
MEMBER_CALL_PATTERN = re.compile(r"\b([a-z][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\(")
CLASS_CONSTRUCTION_PATTERN = re.compile(r"\bnew\s+([A-Za-z_][A-Za-z0-9_]*)\b")
FIELD_OR_PARAM_TYPE_PATTERN = re.compile(
    r"\b(?:private|protected|public|final|static|@Autowired|@Resource)?\s*"
    r"([A-Z][A-Za-z0-9_]*(?:Service|Client|Integration|Repository|Dao|DAO|Mapper|Adapter|Gateway|Provider|Facade))\b"
)
FIELD_VAR_TYPE_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z0-9_]*(?:Service|Client|Integration|Repository|Dao|DAO|Mapper|Adapter|Gateway|Provider|Facade))\s+([a-z][A-Za-z0-9_]*)\b"
)
GENERIC_FIELD_VAR_TYPE_PATTERN = re.compile(
    r"\b(?:private|protected|public|final|static|\s)*"
    r"[A-Z][A-Za-z0-9_]*(?:<([^;=(){}]+)>)\s+([a-z][A-Za-z0-9_]*)\b"
)
SERVICE_LIKE_TYPE_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z0-9_]*(?:Service|Client|Integration|Repository|Dao|DAO|Mapper|Adapter|Gateway|Provider|Facade))\b"
)
STREAM_LAMBDA_PATTERN = re.compile(
    r"\b([a-z][A-Za-z0-9_]*)\s*(?:\.values\s*\(\s*\))?(?:\.stream\s*\(\s*\))?"
    r"\.(?:forEach|map|flatMap|filter|anyMatch|allMatch|noneMatch|peek)\s*\(\s*\(?\s*([a-z][A-Za-z0-9_]*)\s*\)?\s*->"
)
PROVIDER_CHAIN_CALL_PATTERN = re.compile(
    r"\b([a-z][A-Za-z0-9_]*)\.(?:getObject|getIfAvailable|getIfUnique|get)\s*\(\s*\)\.([A-Za-z_][A-Za-z0-9_]*)\s*\("
)
THIS_FIELD_ASSIGNMENT_PATTERN = re.compile(r"\bthis\.([a-z][A-Za-z0-9_]*)\s*=\s*([a-z][A-Za-z0-9_]*)\s*;")
