from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

from bpmis_jira_tool.config import Settings
from bpmis_jira_tool.source_code_qa import ANSWER_MODE, SourceCodeQAService
from bpmis_jira_tool.user_config import TEAM_PROFILE_DEFAULTS


def _load_cases(path: Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as error:
            raise SystemExit(f"{path}:{line_no}: invalid JSON: {error}") from error
        payload.setdefault("id", f"{path.stem}:{line_no}")
        cases.append(payload)
    return cases


def _case_text(payload: dict[str, Any]) -> str:
    parts = [str(payload.get("summary") or ""), str(payload.get("llm_answer") or "")]
    for match in payload.get("matches") or []:
        parts.extend(
            [
                str(match.get("path") or ""),
                str(match.get("reason") or ""),
                str(match.get("snippet") or ""),
            ]
        )
    return "\n".join(parts).lower()


def _failure_bucket(message: str) -> str:
    lowered = message.lower()
    if "path" in lowered or "retrieval" in lowered or "trace stage" in lowered or "trace path" in lowered or "evidence pack" in lowered:
        return "retrieval"
    if "quality" in lowered or "claim check" in lowered or "answer contract" in lowered:
        return "answer_policy"
    if "term" in lowered:
        return "answer_content"
    if "status expected" in lowered:
        return "query_status"
    return "other"


def _coverage_key(case: dict[str, Any]) -> str:
    return str(case.get("category") or case.get("scenario") or "uncategorized")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _build_fixture_repositories(service: SourceCodeQAService) -> None:
    service.save_mapping(
        pm_team="AF",
        country="All",
        repositories=[
            {"display_name": "Portal Repo", "url": "https://git.example.com/team/portal.git"},
            {"display_name": "Issue Service", "url": "https://git.example.com/team/issue-service.git"},
        ],
    )
    service.save_mapping(
        pm_team="CRMS",
        country="ID",
        repositories=[{"display_name": "Credit Risk", "url": "https://git.example.com/team/credit-risk.git"}],
    )
    af_entries = service.load_config()["mappings"]["AF:All"]
    cr_entry = service.load_config()["mappings"]["CRMS:ID"][0]
    af_repo = service._repo_path("AF:All", type("Entry", (), af_entries[0])())
    issue_service_repo = service._repo_path("AF:All", type("Entry", (), af_entries[1])())
    cr_repo = service._repo_path("CRMS:ID", type("Entry", (), cr_entry)())
    (af_repo / ".git").mkdir(parents=True, exist_ok=True)
    (issue_service_repo / ".git").mkdir(parents=True, exist_ok=True)
    (cr_repo / ".git").mkdir(parents=True, exist_ok=True)

    _write_text(
        af_repo / "bpmis" / "jira_client.py",
        "class BPMISClient:\n"
        "    def batchCreateJiraIssue(self):\n"
        "        return self.post('/api/v1/issues/batchCreateJiraIssue')\n",
    )
    _write_text(
        af_repo / "client" / "IssueRemoteClient.java",
        "@FeignClient(name = \"${issue.service.name}\", url = \"${issue.service.url}\")\n"
        "public interface IssueRemoteClient {\n"
        "    @PostMapping(\"/issue\")\n"
        "    Issue createRemoteIssue(CreateIssueRequest request);\n"
        "}\n",
    )
    _write_text(
        af_repo / "web" / "issue_client.ts",
        "import axios from 'axios';\n"
        "export class IssueApiClient {\n"
        "  createIssue(payload: unknown) {\n"
        "    return axios.post('/remote/issue', payload);\n"
        "  }\n"
        "}\n",
    )
    _write_text(
        af_repo / "api" / "routes.py",
        "from flask import Blueprint\n"
        "from service.issue_service import create_issue\n\n"
        "bp = Blueprint('issue', __name__)\n\n"
        "@bp.route('/python/issues/create', methods=['POST'])\n"
        "def create_issue_route():\n"
        "    return create_issue()\n",
    )
    _write_text(
        af_repo / "controller" / "IssueController.java",
        "public class IssueController {\n"
        "    private IssueService issueService;\n"
        "    @PostMapping(\"/api/issues/create\")\n"
        "    public Issue createIssue(CreateIssueRequest request) {\n"
        "        return issueService.createIssue(request);\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "controller" / "InterfaceIssueController.java",
        "public class InterfaceIssueController {\n"
        "    private IssueWorkflowService issueWorkflowService;\n"
        "    public Issue createWorkflowIssue() {\n"
        "        return issueWorkflowService.createIssue();\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "IssueWorkflowService.java",
        "public interface IssueWorkflowService {\n"
        "    Issue createIssue();\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "IssueWorkflowServiceImpl.java",
        "@Service(\"workflowIssueService\")\n"
        "public class IssueWorkflowServiceImpl implements IssueWorkflowService {\n"
        "    public Issue createIssue() { return new Issue(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "controller" / "QualifiedIssueController.java",
        "public class QualifiedIssueController {\n"
        "    @Qualifier(\"fastIssueService\")\n"
        "    private QualifiedIssueService qualifiedIssueService;\n"
        "    public Issue createQualifiedIssue() {\n"
        "        return qualifiedIssueService.createIssue();\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "QualifiedIssueService.java",
        "public interface QualifiedIssueService {\n"
        "    Issue createIssue();\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "FastQualifiedIssueService.java",
        "@Service(\"fastIssueService\")\n"
        "public class FastQualifiedIssueService implements QualifiedIssueService {\n"
        "    public Issue createIssue() { return new Issue(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "SlowQualifiedIssueService.java",
        "@Service(\"slowIssueService\")\n"
        "public class SlowQualifiedIssueService implements QualifiedIssueService {\n"
        "    public Issue createIssue() { return new Issue(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "controller" / "ConstructorQualifiedIssueController.java",
        "public class ConstructorQualifiedIssueController {\n"
        "    private QualifiedIssueService fastQualifiedIssueService;\n"
        "    private QualifiedIssueService slowQualifiedIssueService;\n"
        "    public ConstructorQualifiedIssueController(@Qualifier(\"fastIssueService\") QualifiedIssueService fastDelegate, @Qualifier(\"slowIssueService\") QualifiedIssueService slowDelegate) {\n"
        "        this.fastQualifiedIssueService = fastDelegate;\n"
        "        this.slowQualifiedIssueService = slowDelegate;\n"
        "    }\n"
        "    public Issue createFastIssue() { return fastQualifiedIssueService.createIssue(); }\n"
        "    public Issue createSlowIssue() { return slowQualifiedIssueService.createIssue(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "controller" / "CollectionQualifiedIssueController.java",
        "import java.util.List;\n"
        "public class CollectionQualifiedIssueController {\n"
        "    @Qualifier(\"fastIssueService\")\n"
        "    private List<QualifiedIssueService> qualifiedIssueServices;\n"
        "    public void createQualifiedIssues() {\n"
        "        qualifiedIssueServices.forEach(qualifiedIssueService -> qualifiedIssueService.createIssue());\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "controller" / "ProviderQualifiedIssueController.java",
        "import org.springframework.beans.factory.ObjectProvider;\n"
        "public class ProviderQualifiedIssueController {\n"
        "    private ObjectProvider<QualifiedIssueService> qualifiedIssueServiceProvider;\n"
        "    public Issue createProviderIssue() {\n"
        "        return qualifiedIssueServiceProvider.getIfAvailable().createIssue();\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "controller" / "StreamQualifiedIssueController.java",
        "import java.util.Map;\n"
        "public class StreamQualifiedIssueController {\n"
        "    private Map<String, QualifiedIssueService> qualifiedIssueServiceMap;\n"
        "    public void createStreamIssues() {\n"
        "        qualifiedIssueServiceMap.values().stream().map(qualifiedIssueService -> qualifiedIssueService.createIssue()).toList();\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "aspect" / "IssueAuditAspect.java",
        "@Aspect\n"
        "@Component\n"
        "public class IssueAuditAspect {\n"
        "    @Pointcut(\"execution(* *..QualifiedIssueService.createIssue(..))\")\n"
        "    public void qualifiedIssueCreatePointcut() {}\n"
        "    @Around(\"qualifiedIssueCreatePointcut()\")\n"
        "    public Object auditQualifiedIssueCreate(ProceedingJoinPoint joinPoint) { return joinPoint.proceed(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "job" / "IssueRetryJob.java",
        "@Component\n"
        "public class IssueRetryJob {\n"
        "    private IssueWorkflowService issueWorkflowService;\n"
        "    @Scheduled(cron = \"0 0/5 * * * ?\")\n"
        "    public void retryIssues() { issueWorkflowService.createIssue(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "web" / "IssueAuthInterceptor.java",
        "public class IssueAuthInterceptor implements HandlerInterceptor {\n"
        "    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {\n"
        "        return true;\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "controller" / "PrimaryIssueController.java",
        "public class PrimaryIssueController {\n"
        "    private PrimarySelectionService primarySelectionService;\n"
        "    public Issue createPrimaryIssue() {\n"
        "        return primarySelectionService.createIssue();\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "PrimarySelectionService.java",
        "public interface PrimarySelectionService {\n"
        "    Issue createIssue();\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "PrimaryIssueService.java",
        "@Primary\n"
        "@Service(\"primaryIssueService\")\n"
        "public class PrimaryIssueService implements PrimarySelectionService {\n"
        "    public Issue createIssue() { return new Issue(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "FallbackIssueService.java",
        "@Service(\"fallbackIssueService\")\n"
        "public class FallbackIssueService implements PrimarySelectionService {\n"
        "    public Issue createIssue() { return new Issue(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "controller" / "ProfileIssueController.java",
        "public class ProfileIssueController {\n"
        "    private ProfileSelectionService profileSelectionService;\n"
        "    public Issue createProfileIssue() {\n"
        "        return profileSelectionService.createIssue();\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "ProfileSelectionService.java",
        "public interface ProfileSelectionService {\n"
        "    Issue createIssue();\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "ProdProfileIssueService.java",
        "@Profile(\"prod\")\n"
        "@Service(\"prodProfileIssueService\")\n"
        "public class ProdProfileIssueService implements ProfileSelectionService {\n"
        "    public Issue createIssue() { return new Issue(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "DevProfileIssueService.java",
        "@Profile(\"dev\")\n"
        "@Service(\"devProfileIssueService\")\n"
        "public class DevProfileIssueService implements ProfileSelectionService {\n"
        "    public Issue createIssue() { return new Issue(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "controller" / "ConditionalIssueController.java",
        "public class ConditionalIssueController {\n"
        "    private ConditionalSelectionService conditionalSelectionService;\n"
        "    public Issue createConditionalIssue() {\n"
        "        return conditionalSelectionService.createIssue();\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "ConditionalSelectionService.java",
        "public interface ConditionalSelectionService {\n"
        "    Issue createIssue();\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "ConditionalFastIssueService.java",
        "@ConditionalOnProperty(name = \"issue.conditional.enabled\", havingValue = \"true\")\n"
        "@Service(\"conditionalFastIssueService\")\n"
        "public class ConditionalFastIssueService implements ConditionalSelectionService {\n"
        "    public Issue createIssue() { return new Issue(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "ConditionalSlowIssueService.java",
        "@ConditionalOnProperty(name = \"issue.conditional.enabled\", havingValue = \"false\")\n"
        "@Service(\"conditionalSlowIssueService\")\n"
        "public class ConditionalSlowIssueService implements ConditionalSelectionService {\n"
        "    public Issue createIssue() { return new Issue(); }\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "IssueService.java",
        "public class IssueService {\n"
        "    private IssueRepository issueRepository;\n"
        "    @Transactional(rollbackFor = ValidationException.class)\n"
        "    @Cacheable(cacheNames = \"issues\", key = \"#request.id\")\n"
        "    public Issue createIssue(CreateIssueRequest request) {\n"
        "        validateCreateIssue(request);\n"
        "        return issueRepository.findIssue();\n"
        "    }\n"
        "    private void validateCreateIssue(CreateIssueRequest request) {\n"
        "        if (request == null) { throw new ValidationException(\"validation error\"); }\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "repository" / "IssueRepository.java",
        "public class IssueRepository {\n"
        "    public Issue findIssue() {\n"
        "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
        "    }\n"
        "    public void saveIssue(Issue issue) {\n"
        "        jdbcTemplate.update(\"insert into issue_table(id) values (?)\", issue.getId());\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "src" / "test" / "java" / "IssueServiceTest.java",
        "import static org.mockito.Mockito.verify;\n"
        "import static org.assertj.core.api.Assertions.assertThat;\n\n"
        "public class IssueServiceTest {\n"
        "    private IssueRepository issueRepository;\n"
        "    private IssueService issueService;\n\n"
        "    @Test\n"
        "    public void createIssueFindsIssueFromRepository() {\n"
        "        Issue issue = issueService.createIssue(new CreateIssueRequest());\n"
        "        verify(issueRepository).findIssue();\n"
        "        assertThat(issue).isNotNull();\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "service" / "RiskyIssueService.java",
        "public class RiskyIssueService {\n"
        "    private static final String PASSWORD = \"plain-secret\";\n"
        "    public void load(String issueId) {\n"
        "        String sql = \"select * from issue_table where id = \" + issueId;\n"
        "        try { callRemote(); } catch (Exception e) { e.printStackTrace(); }\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "events" / "IssueEventPublisher.java",
        "public class IssueEventPublisher {\n"
        "    public void publish(IssueCreatedEvent event) {\n"
        "        kafkaTemplate.send(\"issue.created\", event);\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "events" / "ConfiguredIssueEventPublisher.java",
        "public class ConfiguredIssueEventPublisher {\n"
        "    public void publish(IssueConfiguredEvent event) {\n"
        "        kafkaTemplate.send(\"${issue.configured.topic}\", event);\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        af_repo / "config" / "issue.properties",
        "issue.create.feature.enabled=true\n"
        "issue.create.config.owner=source-code-qa-fixture\n"
        "issue.service.name=issue-service\n"
        "issue.service.url=http://issue-service/remote\n"
        "issue.topic.name=issue.created\n"
        "issue.configured.topic=issue.configured\n"
        "issue.conditional.enabled=false\n"
        "spring.profiles.active=prod\n",
    )
    _write_text(
        af_repo / "config" / "application.yml",
        "spring:\n"
        "  config:\n"
        "    activate:\n"
        "      on-profile: prod\n"
        "issue:\n"
        "  conditional:\n"
        "    enabled: true\n"
        "---\n"
        "spring:\n"
        "  config:\n"
        "    activate:\n"
        "      on-profile: dev\n"
        "issue:\n"
        "  conditional:\n"
        "    enabled: false\n",
    )
    _write_text(
        af_repo / "runtime-traces" / "source-code-qa.jsonl",
        "\n".join(
            json.dumps(row)
            for row in [
                {
                    "kind": "call",
                    "from": "IssueController.createIssue",
                    "to": "IssueServiceImpl.createIssue",
                    "evidence": "observed by integration test trace qa-runtime-1",
                },
                {"kind": "sql", "from": "IssueServiceImpl.createIssue", "table": "issue_table"},
                {"kind": "route", "path": "/api/issues/create", "handler": "IssueController.createIssue"},
                {"kind": "message", "from": "IssueEventPublisher.publish", "topic": "issue.created"},
                {"kind": "config", "key": "feature.issue.fast.enabled", "value": True},
            ]
        )
        + "\n",
    )
    _write_text(
        issue_service_repo / "application.properties",
        "issue.configured.topic=issue.configured\n",
    )
    _write_text(
        af_repo / "pom.xml",
        "<project>\n"
        "  <groupId>com.example</groupId>\n"
        "  <artifactId>portal-web</artifactId>\n"
        "  <dependencies>\n"
        "    <dependency>\n"
        "      <groupId>com.example</groupId>\n"
        "      <artifactId>issue-service-api</artifactId>\n"
        "      <version>1.0.0</version>\n"
        "    </dependency>\n"
        "  </dependencies>\n"
        "</project>\n",
    )
    _write_text(
        af_repo / "settings.gradle",
        "rootProject.name = 'portal'\n"
        "include ':issue-api', ':issue-service'\n",
    )
    _write_text(
        af_repo / "issue-service" / "build.gradle",
        "dependencies {\n"
        "    implementation project(':issue-api')\n"
        "}\n",
    )
    _write_text(
        issue_service_repo / "pom.xml",
        "<project>\n"
        "  <groupId>com.example</groupId>\n"
        "  <artifactId>issue-service-api</artifactId>\n"
        "</project>\n",
    )
    _write_text(
        af_repo / "package.json",
        json.dumps(
            {
                "name": "@example/portal-web",
                "dependencies": {"@example/issue-service-sdk": "^1.0.0"},
            },
            indent=2,
        )
        + "\n",
    )
    _write_text(
        issue_service_repo / "package.json",
        json.dumps({"name": "@example/issue-service-sdk"}, indent=2) + "\n",
    )
    _write_text(
        issue_service_repo / "controller" / "RemoteIssueController.java",
        "@RestController\n"
        "@RequestMapping(\"/remote\")\n"
        "public class RemoteIssueController {\n"
        "    private final RemoteIssueService remoteIssueService;\n"
        "    public RemoteIssueController(RemoteIssueService remoteIssueService) {\n"
        "        this.remoteIssueService = remoteIssueService;\n"
        "    }\n"
        "    @PostMapping(\"/issue\")\n"
        "    public Issue createRemoteIssue(CreateIssueRequest request) {\n"
        "        return remoteIssueService.createRemoteIssue(request);\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        issue_service_repo / "service" / "RemoteIssueService.java",
        "public class RemoteIssueService {\n"
        "    private RemoteIssueRepository remoteIssueRepository;\n"
        "    public Issue createRemoteIssue(CreateIssueRequest request) {\n"
        "        return remoteIssueRepository.insertIssue(request);\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        issue_service_repo / "repository" / "RemoteIssueRepository.java",
        "public class RemoteIssueRepository {\n"
        "    public Issue insertIssue(CreateIssueRequest request) {\n"
        "        return jdbcTemplate.queryForObject(\"select * from remote_issue_table\", mapper);\n"
        "    }\n"
        "    public Issue loadSharedIssue() {\n"
        "        return jdbcTemplate.queryForObject(\"select * from issue_table\", mapper);\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        issue_service_repo / "events" / "IssueEventListener.java",
        "public class IssueEventListener {\n"
        "    @KafkaListener(topics = \"issue.created\")\n"
        "    public void consume(IssueCreatedEvent event) { }\n"
        "}\n",
    )
    _write_text(
        issue_service_repo / "events" / "ConfiguredIssueEventListener.java",
        "public class ConfiguredIssueEventListener {\n"
        "    @KafkaListener(topics = \"${issue.configured.topic}\")\n"
        "    public void consume(IssueConfiguredEvent event) { }\n"
        "}\n",
    )
    _write_text(
        cr_repo / "engine" / "TermLoanPreCheckEngine.java",
        "public class TermLoanPreCheckEngine {\n"
        "    private CustomerRepository customerRepository;\n"
        "    public CustomerProfile run(EngineTermLoanPreCheckLayer1Input input) {\n"
        "        return customerRepository.loadProfile(input.getCustomerId());\n"
        "    }\n"
        "}\n",
    )
    _write_text(
        cr_repo / "repository" / "CustomerRepository.java",
        "public class CustomerRepository {\n"
        "    public CustomerProfile loadProfile(String customerId) {\n"
        "        return jdbcTemplate.queryForObject(\"select * from cr_customer_profile\", mapper, customerId);\n"
        "    }\n"
        "}\n",
    )
    for key, raw_entry, repo_path in (
        ("AF:All", af_entries[0], af_repo),
        ("AF:All", af_entries[1], issue_service_repo),
        ("CRMS:ID", cr_entry, cr_repo),
    ):
        service._build_repo_index(key, type("Entry", (), raw_entry)(), repo_path)


def _evaluate_case(service: SourceCodeQAService, case: dict[str, Any]) -> dict[str, Any]:
    payload = service.query(
        pm_team=str(case.get("pm_team") or ""),
        country=str(case.get("country") or "All"),
        question=str(case.get("question") or ""),
        answer_mode=str(case.get("answer_mode") or ANSWER_MODE),
        llm_budget_mode=str(case.get("llm_budget_mode") or "cheap"),
    )
    matched_paths = {str(match.get("path") or "") for match in payload.get("matches") or []}
    retrievals = {str(match.get("retrieval") or "") for match in payload.get("matches") or []}
    trace_stages = {str(match.get("trace_stage") or "") for match in payload.get("matches") or []}
    trace_paths = payload.get("trace_paths") or []
    structured_answer = payload.get("structured_answer") or {}
    text = _case_text(payload)
    trace_path_text = json.dumps(trace_paths, ensure_ascii=False).lower()
    structured_answer_text = json.dumps(structured_answer, ensure_ascii=False).lower()
    failures: list[str] = []

    if case.get("expected_status") and payload.get("status") != case["expected_status"]:
        failures.append(f"status expected {case['expected_status']!r}, got {payload.get('status')!r}")
    elif not case.get("expected_status") and payload.get("status") != "ok":
        failures.append(f"status expected 'ok', got {payload.get('status')!r}")

    for expected_path in case.get("expected_paths") or []:
        if expected_path not in matched_paths:
            failures.append(f"missing expected path {expected_path!r}")
    for term in case.get("required_terms") or []:
        if str(term).lower() not in text:
            failures.append(f"missing required term {term!r}")
    for term in case.get("forbidden_terms") or []:
        if str(term).lower() in text:
            failures.append(f"found forbidden term {term!r}")
    for retrieval in case.get("expected_retrieval") or []:
        if retrieval not in retrievals:
            failures.append(f"missing retrieval type {retrieval!r}")
    for trace_stage in case.get("expected_trace_stage") or []:
        if trace_stage not in trace_stages:
            failures.append(f"missing trace stage {trace_stage!r}")
    expected_quality = case.get("expected_quality_status")
    if expected_quality and (payload.get("answer_quality") or {}).get("status") != expected_quality:
        failures.append(
            f"quality expected {expected_quality!r}, got {(payload.get('answer_quality') or {}).get('status')!r}"
        )
    for term in case.get("expected_trace_path_terms") or []:
        if str(term).lower() not in trace_path_text:
            failures.append(f"missing trace path term {term!r}")
    min_trace_paths = case.get("min_trace_paths")
    if min_trace_paths is not None and len(trace_paths) < int(min_trace_paths):
        failures.append(f"trace path count expected >= {int(min_trace_paths)}, got {len(trace_paths)}")
    for term in case.get("expected_answer_claim_terms") or []:
        if str(term).lower() not in structured_answer_text:
            failures.append(f"missing structured answer claim term {term!r}")
    expected_claim_status = case.get("expected_claim_check_status")
    if expected_claim_status and (payload.get("answer_claim_check") or {}).get("status") != expected_claim_status:
        failures.append(
            f"claim check expected {expected_claim_status!r}, got {(payload.get('answer_claim_check') or {}).get('status')!r}"
        )
    expected_contract_status = case.get("expected_answer_contract_status")
    if expected_contract_status and (payload.get("answer_contract") or {}).get("status") != expected_contract_status:
        failures.append(
            f"answer contract expected {expected_contract_status!r}, got {(payload.get('answer_contract') or {}).get('status')!r}"
        )
    expected_policy_statuses = case.get("expected_answer_policy_statuses") or {}
    policies = {
        str(policy.get("name") or ""): str(policy.get("status") or "")
        for policy in ((payload.get("answer_contract") or {}).get("policies") or (payload.get("answer_quality") or {}).get("policies") or [])
        if isinstance(policy, dict)
    }
    for policy_name, expected_status in expected_policy_statuses.items():
        if policies.get(str(policy_name)) != str(expected_status):
            failures.append(f"answer policy {policy_name!r} expected {expected_status!r}, got {policies.get(str(policy_name))!r}")
    expected_evidence_pack_terms = case.get("expected_evidence_pack_terms") or []
    evidence_pack = payload.get("evidence_pack") or {}
    evidence_pack_text = json.dumps(evidence_pack, ensure_ascii=False).lower()
    for term in expected_evidence_pack_terms:
        if str(term).lower() not in evidence_pack_text:
            failures.append(f"missing evidence pack term {term!r}")

    repo_status = payload.get("repo_status") or []
    expected_parser_backend = case.get("expected_parser_backend")
    if expected_parser_backend:
        parser_backends = {
            str((item.get("index") or {}).get("parser_backend") or "")
            for item in repo_status
            if isinstance(item, dict)
        }
        if str(expected_parser_backend) not in parser_backends:
            failures.append(f"parser backend expected {expected_parser_backend!r}, got {sorted(parser_backends)!r}")

    min_tree_sitter_files = case.get("min_tree_sitter_files")
    if min_tree_sitter_files is not None:
        indexed_tree_sitter_files = sum(
            int((item.get("index") or {}).get("tree_sitter_files") or 0)
            for item in repo_status
            if isinstance(item, dict)
        )
        if indexed_tree_sitter_files < int(min_tree_sitter_files):
            failures.append(
                f"tree-sitter files expected >= {int(min_tree_sitter_files)}, got {indexed_tree_sitter_files}"
            )

    repo_graph_edges = (payload.get("repo_graph") or {}).get("edges") or []
    for expected_edge in case.get("expected_repo_graph_edges") or []:
        if not _matches_expected_repo_graph_edge(repo_graph_edges, expected_edge):
            failures.append(f"missing repo graph edge {expected_edge!r}")

    expected_symbol_edges = case.get("expected_symbol_edges") or []
    if expected_symbol_edges:
        try:
            symbol_edges = _indexed_symbol_edges(service, service.mapping_key(str(case.get("pm_team") or ""), str(case.get("country") or "All")))
        except Exception:
            symbol_edges = []
        for expected_edge in expected_symbol_edges:
            if not _matches_expected_symbol_edge(symbol_edges, expected_edge):
                failures.append(f"missing symbol edge {expected_edge!r}")

    failure_buckets: dict[str, int] = {}
    for failure in failures:
        bucket = _failure_bucket(failure)
        failure_buckets[bucket] = failure_buckets.get(bucket, 0) + 1

    return {
        "id": case["id"],
        "category": _coverage_key(case),
        "status": "pass" if not failures else "fail",
        "failures": failures,
        "failure_buckets": failure_buckets,
        "matched_paths": sorted(matched_paths),
        "retrievals": sorted(retrievals),
        "trace_stages": sorted(trace_stages),
        "answer_quality": payload.get("answer_quality") or {},
        "trace_paths": trace_paths,
        "structured_answer": structured_answer,
        "answer_claim_check": payload.get("answer_claim_check") or {},
        "answer_contract": payload.get("answer_contract") or {},
        "evidence_pack": evidence_pack,
        "answer_policies": policies,
        "repo_graph_edges": repo_graph_edges,
        "llm_provider": payload.get("llm_provider"),
        "llm_model": payload.get("llm_model"),
        "llm_route": payload.get("llm_route") or {},
        "llm_budget_mode": payload.get("llm_budget_mode"),
        "llm_cached": bool(payload.get("llm_cached")),
        "citations": payload.get("citations") or [],
    }


def _matches_expected_repo_graph_edge(edges: list[dict[str, Any]], expected: dict[str, Any]) -> bool:
    for edge in edges:
        if expected.get("from_repo") and edge.get("from_repo") != expected.get("from_repo"):
            continue
        if expected.get("to_repo") and edge.get("to_repo") != expected.get("to_repo"):
            continue
        if expected.get("edge_kind") and edge.get("edge_kind") != expected.get("edge_kind"):
            continue
        if expected.get("match_reason_contains") and str(expected.get("match_reason_contains")).lower() not in str(edge.get("match_reason") or "").lower():
            continue
        if expected.get("evidence_contains") and str(expected.get("evidence_contains")).lower() not in str(edge.get("evidence") or "").lower():
            continue
        min_confidence = expected.get("min_confidence")
        if min_confidence is not None and float(edge.get("confidence") or 0) < float(min_confidence):
            continue
        return True
    return False


def _matches_expected_symbol_edge(edges: list[dict[str, Any]], expected: dict[str, Any]) -> bool:
    for edge in edges:
        if expected.get("edge_kind") and edge.get("edge_kind") != expected.get("edge_kind"):
            continue
        if expected.get("from_file") and edge.get("from_file") != expected.get("from_file"):
            continue
        if expected.get("to_file") and edge.get("to_file") != expected.get("to_file"):
            continue
        if expected.get("to_name_contains") and str(expected.get("to_name_contains")).lower() not in str(edge.get("to_name") or "").lower():
            continue
        if expected.get("evidence_contains") and str(expected.get("evidence_contains")).lower() not in str(edge.get("evidence") or "").lower():
            continue
        return True
    return False


def _indexed_symbol_edges(service: SourceCodeQAService, key: str) -> list[dict[str, Any]]:
    import sqlite3

    edges: list[dict[str, Any]] = []
    for raw_entry in (service.load_config().get("mappings") or {}).get(key, []):
        entry = service._normalize_entry(raw_entry)
        repo_path = service._repo_path(key, entry)
        index_path = service._index_path(repo_path)
        if not index_path.exists():
            continue
        try:
            with sqlite3.connect(index_path) as connection:
                connection.row_factory = sqlite3.Row
                for table in ("entity_edges", "flow_edges"):
                    for row in connection.execute(
                        f"""
                        select edge_kind, to_name, from_file, from_line, to_file, to_line, evidence
                        from {table}
                        limit 1200
                        """
                    ):
                        edges.append(dict(row))
        except sqlite3.Error:
            continue
    return edges


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Source Code Q&A golden-answer retrieval evals.")
    parser.add_argument(
        "--cases",
        action="append",
        default=None,
        help="JSONL eval case file. Can be passed multiple times.",
    )
    parser.add_argument("--data-root", default=None, help="Override TEAM_PORTAL_DATA_DIR for the indexed repositories.")
    parser.add_argument("--fixture", action="store_true", help="Create deterministic fixture repositories before running cases.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args()

    settings = Settings.from_env()
    service = SourceCodeQAService(
        data_root=Path(args.data_root) if args.data_root else settings.team_portal_data_dir,
        team_profiles=TEAM_PROFILE_DEFAULTS,
        gitlab_token=settings.source_code_qa_gitlab_token,
        gitlab_username=settings.source_code_qa_gitlab_username,
        llm_provider=settings.source_code_qa_llm_provider,
        gemini_api_key=settings.source_code_qa_gemini_api_key,
        gemini_api_base_url=settings.source_code_qa_gemini_api_base_url,
        openai_api_key=settings.source_code_qa_openai_api_key,
        openai_api_base_url=settings.source_code_qa_openai_api_base_url,
        openai_model=settings.source_code_qa_openai_model,
        openai_fast_model=settings.source_code_qa_openai_fast_model,
        openai_deep_model=settings.source_code_qa_openai_deep_model,
        openai_fallback_model=settings.source_code_qa_openai_fallback_model,
        gemini_model=settings.source_code_qa_gemini_model,
        gemini_fast_model=settings.source_code_qa_gemini_fast_model,
        gemini_deep_model=settings.source_code_qa_gemini_deep_model,
        gemini_fallback_model=settings.source_code_qa_gemini_fallback_model,
        query_rewrite_model=settings.source_code_qa_query_rewrite_model,
        planner_model=settings.source_code_qa_planner_model,
        answer_model=settings.source_code_qa_answer_model,
        judge_model=settings.source_code_qa_judge_model,
        repair_model=settings.source_code_qa_repair_model,
        llm_judge_enabled=settings.source_code_qa_llm_judge_enabled,
        semantic_index_model=settings.source_code_qa_embedding_model,
        semantic_index_enabled=settings.source_code_qa_semantic_index_enabled,
        embedding_provider=settings.source_code_qa_embedding_provider,
        embedding_api_key=settings.source_code_qa_embedding_api_key,
        embedding_api_base_url=settings.source_code_qa_embedding_api_base_url,
        llm_cache_ttl_seconds=settings.source_code_qa_llm_cache_ttl_seconds,
        git_timeout_seconds=settings.source_code_qa_git_timeout_seconds,
        max_file_bytes=settings.source_code_qa_max_file_bytes,
    )
    if args.fixture:
        _build_fixture_repositories(service)
    case_paths = [Path(path) for path in (args.cases or ["evals/source_code_qa/golden.jsonl"])]
    cases: list[dict[str, Any]] = []
    for path in case_paths:
        cases.extend(_load_cases(path))
    results = [_evaluate_case(service, case) for case in cases]
    failed = [result for result in results if result["status"] != "pass"]
    failure_buckets: dict[str, int] = {}
    route_buckets: dict[str, int] = {}
    coverage_buckets: dict[str, dict[str, int]] = {}
    for result in results:
        for bucket, count in (result.get("failure_buckets") or {}).items():
            failure_buckets[str(bucket)] = failure_buckets.get(str(bucket), 0) + int(count)
        route = result.get("llm_route") or {}
        route_key = str(route.get("selected") or result.get("llm_budget_mode") or "retrieval_only")
        route_buckets[route_key] = route_buckets.get(route_key, 0) + 1
        category = str(result.get("category") or "uncategorized")
        coverage = coverage_buckets.setdefault(category, {"total": 0, "failed": 0})
        coverage["total"] += 1
        if result["status"] != "pass":
            coverage["failed"] += 1
    summary = {
        "status": "pass" if not failed else "fail",
        "total": len(results),
        "failed": len(failed),
        "failure_buckets": failure_buckets,
        "route_buckets": route_buckets,
        "coverage_buckets": coverage_buckets,
        "results": results,
    }

    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        print(f"Source Code Q&A evals: {summary['status']} ({len(results) - len(failed)}/{len(results)} passed)")
        if failure_buckets:
            print("Failure buckets: " + ", ".join(f"{key}={value}" for key, value in sorted(failure_buckets.items())))
        if coverage_buckets:
            print(
                "Coverage buckets: "
                + ", ".join(
                    f"{key}={value['total'] - value['failed']}/{value['total']}"
                    for key, value in sorted(coverage_buckets.items())
                )
            )
        for result in results:
            marker = "PASS" if result["status"] == "pass" else "FAIL"
            print(f"{marker} {result['id']}")
            for failure in result["failures"]:
                print(f"  - {failure}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
