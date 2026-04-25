(() => {
  const root = document.querySelector('[data-source-code-qa-root]');
  if (!root) return;

  const configUrl = root.dataset.configUrl;
  const saveUrl = root.dataset.saveUrl;
  const syncUrl = root.dataset.syncUrl;
  const queryUrl = root.dataset.queryUrl;
  const feedbackUrl = root.dataset.feedbackUrl;
  const canManage = root.dataset.canManage === 'true';
  const options = JSON.parse(root.dataset.options || '{}');

  const pmTeam = document.querySelector('[data-source-pm-team]');
  const country = document.querySelector('[data-source-country]');
  const answerMode = document.querySelector('[data-source-answer-mode]');
  const countryWrap = document.querySelector('[data-source-country-wrap]');
  const configStatus = document.querySelector('[data-source-config-status]');
  const adminStatus = document.querySelector('[data-source-admin-status]');
  const reposInput = document.querySelector('[data-source-repos-input]');
  const saveButton = document.querySelector('[data-source-save-config]');
  const syncButton = document.querySelector('[data-source-sync]');
  const questionInput = document.querySelector('[data-source-question]');
  const queryButton = document.querySelector('[data-source-query]');
  const queryStatus = document.querySelector('[data-source-query-status]');
  const repoStatus = document.querySelector('[data-source-repo-status]');
  const summary = document.querySelector('[data-source-summary]');
  const results = document.querySelector('[data-source-results]');
  const llmAnswer = document.querySelector('[data-source-llm-answer]');
  const activeMode = document.querySelector('[data-source-active-mode]');
  const activeCache = document.querySelector('[data-source-active-cache]');
  const activeUsage = document.querySelector('[data-source-active-usage]');
  const fallbackNotice = document.querySelector('[data-source-fallback-notice]');
  const feedback = document.querySelector('[data-source-feedback]');
  const feedbackStatus = document.querySelector('[data-source-feedback-status]');
  const evidenceSummary = document.querySelector('[data-source-evidence-summary]');
  const debugTrace = document.querySelector('[data-source-debug-trace]');
  const questionHistory = document.querySelector('[data-source-question-history]');
  const indexHealth = document.querySelector('[data-source-index-health]');
  const releaseGate = document.querySelector('[data-source-release-gate]');
  let config = { mappings: {} };
  let gitAuthReady = false;
  let llmReady = false;
  let llmPolicy = {};
  let indexHealthPayload = {};
  let releaseGatePayload = {};
  let lastPayload = null;
  let conversationContext = null;
  let activeQueryProgress = null;
  const preferenceKey = 'source-code-qa:last-query-config:v1';
  const historyKey = 'source-code-qa:question-history:v1';

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const readJson = async (response) => {
    const contentType = response.headers.get('content-type') || '';
    if (!contentType.includes('application/json')) {
      const text = await response.text();
      const httpStatus = response.status ? `HTTP ${response.status}` : 'non-JSON response';
      const looksHtml = text.includes('<!DOCTYPE') || contentType.includes('text/html');
      throw new Error(looksHtml
        ? `${httpStatus}: the portal returned an HTML error/timeout page instead of JSON. Please retry; if it repeats, check server logs with the request time.`
        : `${httpStatus}: ${text.slice(0, 180)}`);
    }
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.message || 'Request failed.');
    }
    return payload;
  };

  const currentCountry = () => (pmTeam.value === 'CRMS' ? country.value : 'All');
  const currentKey = () => `${pmTeam.value}:${currentCountry()}`;
  const currentRepoCount = () => (config.mappings?.[currentKey()] || []).length;
  const formatElapsed = (startedAt) => {
    const seconds = Math.max(0, (performance.now() - startedAt) / 1000);
    return seconds < 10 ? `${seconds.toFixed(1)}s` : `${Math.round(seconds)}s`;
  };

  const stopQueryProgress = () => {
    if (activeQueryProgress) {
      window.clearInterval(activeQueryProgress);
      activeQueryProgress = null;
    }
  };

  const startQueryProgress = (message = 'Submitting query to server...') => {
    stopQueryProgress();
    const startedAt = performance.now();
    let currentMessage = message;
    const setMessage = (nextMessage) => {
      currentMessage = nextMessage || currentMessage;
      if (queryStatus) {
        queryStatus.textContent = `${currentMessage} elapsed ${formatElapsed(startedAt)}`;
      }
    };
    const update = () => {
      if (queryStatus) {
        queryStatus.textContent = `${currentMessage} elapsed ${formatElapsed(startedAt)}`;
      }
    };
    update();
    activeQueryProgress = window.setInterval(update, 500);
    return { startedAt, setMessage };
  };

  const buildFeedbackReplayContext = (payload) => ({
    trace_id: payload.trace_id || '',
    answer_mode: payload.answer_mode || '',
    llm_budget_mode: payload.llm_budget_mode || payload.llm_requested_budget_mode || '',
    llm_provider: payload.llm_provider || '',
    llm_model: payload.llm_model || '',
    llm_route: payload.llm_route || {},
    llm_finish_reason: payload.llm_finish_reason || '',
    summary: payload.summary || '',
    rendered_answer: payload.llm_answer || '',
    citations: payload.citations || [],
    answer_contract: payload.answer_contract || {},
    evidence_pack: payload.evidence_pack || {},
    tool_trace: (payload.tool_trace || []).slice(0, 30),
    matches_snapshot: (payload.matches || []).slice(0, 10).map((match) => ({
      repo: match.repo,
      path: match.path,
      line_start: match.line_start,
      line_end: match.line_end,
      retrieval: match.retrieval,
      trace_stage: match.trace_stage,
      score: match.score,
      snippet: match.snippet,
    })),
  });

  const selectHasValue = (select, value) => {
    if (!select || value == null) return false;
    return Array.from(select.options).some((option) => option.value === String(value));
  };

  const loadLastQueryConfig = () => {
    try {
      return JSON.parse(window.localStorage.getItem(preferenceKey) || '{}');
    } catch (_error) {
      return {};
    }
  };

  const restoreLastQueryConfig = () => {
    const saved = loadLastQueryConfig();
    if (selectHasValue(pmTeam, saved.pm_team)) {
      pmTeam.value = saved.pm_team;
    }
    updateCountryVisibility();
    if (pmTeam.value === 'CRMS' && selectHasValue(country, saved.country)) {
      country.value = saved.country;
    }
    if (selectHasValue(answerMode, saved.answer_mode)) {
      answerMode.value = saved.answer_mode;
    }
  };

  const rememberLastQueryConfig = (selectedAnswerMode) => {
    try {
      window.localStorage.setItem(preferenceKey, JSON.stringify({
        pm_team: pmTeam.value,
        country: currentCountry(),
        answer_mode: selectedAnswerMode,
      }));
    } catch (_error) {
      // Local storage can be blocked in private/browser-managed contexts.
    }
  };

  const loadQuestionHistory = () => {
    try {
      const rows = JSON.parse(window.localStorage.getItem(historyKey) || '[]');
      return Array.isArray(rows) ? rows : [];
    } catch (_error) {
      return [];
    }
  };

  const saveQuestionHistory = (row) => {
    const question = String(row?.question || '').trim();
    if (!question) return;
    try {
      const rows = loadQuestionHistory()
        .filter((item) => String(item.question || '').trim() !== question);
      rows.unshift({
        question,
        pm_team: row.pm_team,
        country: row.country,
        answer_mode: row.answer_mode,
        status: row.status,
        trace_id: row.trace_id || '',
        summary: String(row.summary || '').slice(0, 160),
        asked_at: new Date().toISOString(),
      });
      window.localStorage.setItem(historyKey, JSON.stringify(rows.slice(0, 12)));
      renderQuestionHistory();
    } catch (_error) {
      // Local history is a convenience only.
    }
  };

  const renderQuestionHistory = () => {
    if (!questionHistory) return;
    const rows = loadQuestionHistory();
    if (!rows.length) {
      questionHistory.hidden = true;
      questionHistory.innerHTML = '';
      return;
    }
    questionHistory.hidden = false;
    questionHistory.innerHTML = `
      <div class="source-qa-history-head">
        <strong>Recent Questions</strong>
        <button class="button button-secondary source-qa-clear-history" type="button" data-source-clear-history>Clear</button>
      </div>
      <div class="source-qa-history-list">
        ${rows.slice(0, 6).map((row) => `
          <button class="source-qa-history-item" type="button" data-source-history-question="${escapeHtml(row.question)}">
            <span>${escapeHtml(row.question)}</span>
            <small>${escapeHtml(row.pm_team || '')} · ${escapeHtml(row.country || 'All')} · ${escapeHtml(row.status || '')}</small>
          </button>
        `).join('')}
      </div>
    `;
    questionHistory.querySelectorAll('[data-source-history-question]').forEach((button) => {
      button.addEventListener('click', () => {
        questionInput.value = button.dataset.sourceHistoryQuestion || '';
        questionInput.focus();
      });
    });
    questionHistory.querySelector('[data-source-clear-history]')?.addEventListener('click', () => {
      window.localStorage.removeItem(historyKey);
      renderQuestionHistory();
    });
  };

  const updateCountryVisibility = () => {
    const isCreditRisk = pmTeam.value === 'CRMS';
    country.disabled = !isCreditRisk;
    countryWrap.classList.toggle('source-qa-country-disabled', !isCreditRisk);
    if (!isCreditRisk) {
      country.value = 'All';
    } else if (!country.value && Array.isArray(options.countries) && options.countries.length) {
      country.value = options.countries[0];
    } else if (country.value === 'All' && Array.isArray(options.countries) && options.countries.length) {
      country.value = options.countries[0];
    }
  };

  const updateAnswerModeState = () => {
    const answerModeValue = answerMode?.value || 'auto';
    const llmSelected = answerModeValue !== 'retrieval_only' && llmReady;
    if (queryButton) {
      queryButton.textContent = llmSelected ? 'Search + Generate Answer' : 'Search Code';
    }
    if (queryStatus) {
      if (answerModeValue !== 'retrieval_only' && !llmReady) {
        const provider = llmPolicy.provider?.provider || 'LLM';
        queryStatus.textContent = `${provider} is unavailable; code search will still run.`;
      } else if (llmSelected) {
        queryStatus.textContent = 'Ready. Smart Answer will search first and call LLM only when useful.';
      } else if (!llmSelected) {
        queryStatus.textContent = 'Ready. Code search only.';
      }
    }
  };

  const renderStatus = (items = []) => {
    if (!repoStatus) return;
    if (!items.length) {
      repoStatus.innerHTML = '<div class="source-qa-empty">No repositories configured for this selection.</div>';
      return;
    }
    repoStatus.innerHTML = items.map((item) => `
      <div class="source-qa-repo-card source-qa-repo-${escapeHtml(item.state)}">
        <strong>${escapeHtml(item.display_name)}</strong>
        <span>${escapeHtml(item.state)} · ${escapeHtml(item.message)}</span>
        <code>${escapeHtml(item.path || item.url)}</code>
      </div>
    `).join('');
  };

  const compactNumber = (value) => {
    const number = Number(value || 0);
    return Number.isFinite(number) ? number.toLocaleString() : '0';
  };

  const renderIndexHealth = (health = {}) => {
    if (!indexHealth) return;
    const totals = health.totals || {};
    const keyHealth = health.keys?.[currentKey()] || {};
    const staleRepos = (keyHealth.freshness?.stale_repos || []).slice(0, 4);
    const state = health.status || 'unknown';
    indexHealth.innerHTML = `
      <div class="source-qa-health-head">
        <strong>Index ${escapeHtml(state)}</strong>
        <span>${compactNumber(totals.ready)} / ${compactNumber(totals.repos)} repos ready</span>
      </div>
      <div class="source-qa-health-metrics">
        <span>${compactNumber(totals.files)} files</span>
        <span>${compactNumber(totals.lines)} lines</span>
        <span>${compactNumber(totals.definitions)} definitions</span>
        <span>${compactNumber(totals.semantic_chunks)} chunks</span>
      </div>
      <p>${escapeHtml(keyHealth.freshness?.warning || (health.newest_indexed_at ? `Newest index: ${health.newest_indexed_at}` : 'No synced index yet.'))}</p>
      ${staleRepos.length ? `<ul>${staleRepos.map((repo) => `<li>${escapeHtml(repo)}</li>`).join('')}</ul>` : ''}
    `;
  };

  const renderReleaseGate = (gate = {}) => {
    if (!releaseGate) return;
    const latestEval = gate.latest_eval || {};
    const evalSummary = latestEval.eval || {};
    const llmSmoke = latestEval.llm_smoke || {};
    const status = gate.status || 'missing';
    releaseGate.innerHTML = `
      <div class="source-qa-health-head">
        <strong>Gate ${escapeHtml(status)}</strong>
        <span>${escapeHtml(gate.updated_at || 'not run')}</span>
      </div>
      <div class="source-qa-health-metrics">
        <span>eval ${escapeHtml(evalSummary.status || 'n/a')}</span>
        <span>${compactNumber(evalSummary.total)} cases</span>
        <span>${compactNumber(evalSummary.failed)} failed</span>
        <span>smoke ${escapeHtml(llmSmoke.status || 'n/a')}</span>
      </div>
      <p>${escapeHtml(gate.summary || 'Run the Source Code Q&A release gate before publishing retrieval or prompt changes.')}</p>
    `;
  };

  const renderSelectedConfig = () => {
    const entries = config.mappings?.[currentKey()] || [];
    const count = entries.length;
    configStatus.textContent = count
      ? `${count} repositories configured for ${currentKey()}.`
      : `No repositories configured for ${currentKey()} yet.`;
    if (reposInput) {
      reposInput.value = entries.map((entry) => `${entry.display_name} | ${entry.url}`).join('\n');
    }
    renderStatus(entries.map((entry) => ({
      ...entry,
      state: 'configured',
      message: 'Configured. Sync status will update after refresh.',
      path: entry.url,
    })));
    renderIndexHealth(indexHealthPayload);
    renderReleaseGate(releaseGatePayload);
  };

  const parseRepoLines = () => {
    return String(reposInput?.value || '')
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => {
        const parts = line.split('|').map((part) => part.trim()).filter(Boolean);
        if (parts.length >= 2) {
          return { display_name: parts[0], url: parts.slice(1).join('|').trim() };
        }
        return { display_name: '', url: line };
      });
  };

  const loadConfig = async () => {
    try {
      const payload = await fetch(configUrl).then(readJson);
      config = payload.config || { mappings: {} };
      gitAuthReady = Boolean(payload.git_auth_ready);
      llmReady = Boolean(payload.llm_ready);
      llmPolicy = payload.llm_policy || {};
      indexHealthPayload = payload.index_health || {};
      releaseGatePayload = payload.release_gate || {};
      if (!gitAuthReady && adminStatus) {
        adminStatus.textContent = 'Set SOURCE_CODE_QA_GITLAB_TOKEN on the server before running Sync / Refresh.';
      } else if (adminStatus && llmPolicy.provider) {
        const provider = llmPolicy.provider.provider || payload.llm_provider || 'llm';
        const routerVersion = llmPolicy.router?.version ? ` · router v${llmPolicy.router.version}` : '';
        adminStatus.textContent = `LLM provider: ${provider}${routerVersion}.`;
      }
      updateAnswerModeState();
      renderSelectedConfig();
      renderIndexHealth(indexHealthPayload);
      renderReleaseGate(releaseGatePayload);
    } catch (error) {
      configStatus.textContent = error.message || 'Repository config could not be loaded.';
    }
  };

  const saveConfig = async () => {
    if (!canManage) return;
    adminStatus.textContent = 'Saving repository mapping...';
    try {
      const payload = await fetch(saveUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pm_team: pmTeam.value,
          country: currentCountry(),
          repositories: parseRepoLines(),
        }),
      }).then(readJson);
      config = payload.config || config;
      adminStatus.textContent = `Saved ${payload.repositories?.length || 0} repositories for ${payload.key}.`;
      renderSelectedConfig();
    } catch (error) {
      adminStatus.textContent = error.message || 'Save failed.';
    }
  };

  const pollSyncJob = async (jobId) => {
    while (jobId) {
      const response = await fetch(`/api/jobs/${encodeURIComponent(jobId)}`, { method: 'GET' });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.message || 'Could not read sync job status.');
      }
      adminStatus.textContent = payload.message || 'Syncing repositories...';
      if (payload.state === 'completed') {
        const result = (payload.results || [])[0] || {};
        renderStatus(result.repo_status || result.results || []);
        adminStatus.textContent = result.status === 'ok'
          ? 'Sync completed.'
          : (payload.notice?.summary || 'Sync completed with issues. Check status cards.');
        return result;
      }
      if (payload.state === 'failed') {
        throw new Error(payload.error || payload.message || 'Sync failed.');
      }
      await new Promise((resolve) => window.setTimeout(resolve, 900));
    }
    return {};
  };

  const syncRepos = async () => {
    if (!canManage) return;
    if (!gitAuthReady) {
      adminStatus.textContent = 'SOURCE_CODE_QA_GITLAB_TOKEN is missing on the server.';
      return;
    }
    adminStatus.textContent = 'Syncing repositories. This can take a minute on first clone...';
    try {
      const payload = await fetch(syncUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pm_team: pmTeam.value, country: currentCountry() }),
      }).then(readJson);
      if (payload.status === 'queued' && payload.job_id) {
        await pollSyncJob(payload.job_id);
        return;
      }
      renderStatus(payload.repo_status || payload.results || []);
      adminStatus.textContent = payload.status === 'ok'
        ? 'Sync completed.'
        : (payload.message || 'Sync completed with issues. Check status cards.');
    } catch (error) {
      adminStatus.textContent = error.message || 'Sync failed.';
    }
  };

  const renderMatches = (matches = [], options = {}) => {
    if (!results) return;
    if (!matches.length) {
      results.innerHTML = '<div class="source-qa-empty">No confident code match for this question.</div>';
      return;
    }
    const body = matches.map((match, index) => `
      <article class="source-qa-match">
        <div class="source-qa-match-head">
          <strong>[S${index + 1}] ${escapeHtml(match.repo)} · ${escapeHtml(match.path)}</strong>
          <span>Lines ${escapeHtml(match.line_start)}-${escapeHtml(match.line_end)} · score ${escapeHtml(match.score)} · ${escapeHtml(match.retrieval || 'file_scan')}</span>
        </div>
        <p>${escapeHtml(match.reason)}</p>
        <pre><code>${escapeHtml(match.snippet)}</code></pre>
      </article>
    `).join('');
    if (options.compact) {
      const openAttr = options.open ? ' open' : '';
      const summary = options.open
        ? `Showing ${matches.length} code references because the answer needs evidence review`
        : `Show ${matches.length} code references used for this answer`;
      results.innerHTML = `
        <details class="source-qa-evidence"${openAttr}>
          <summary>${escapeHtml(summary)}</summary>
          ${body}
        </details>
      `;
      return;
    }
    results.innerHTML = body;
  };

  const buildConversationContext = (payload) => ({
    question: questionInput.value,
    matches: (payload?.matches || []).slice(0, 8).map((match) => ({
      path: match.path,
      snippet: match.snippet,
      repo: match.repo,
      reason: match.reason,
      retrieval: match.retrieval,
    })),
    trace_paths: (payload?.trace_paths || []).slice(0, 5),
    structured_answer: payload?.structured_answer || {},
    answer_contract: payload?.answer_contract || {},
    evidence_pack: payload?.evidence_pack || {},
    answer_quality: payload?.answer_quality || {},
  });

  const shouldOpenEvidence = (payload) => {
    const quality = payload?.answer_quality || {};
    const contract = payload?.answer_contract || {};
    return quality.status === 'needs_more_trace'
      || quality.confidence === 'low'
      || contract.status === 'blocked_missing_source'
      || Boolean((contract.missing_links || []).length);
  };

  const renderDebugTrace = (payload) => {
    if (!debugTrace) return;
    if (!payload || payload.status !== 'ok') {
      debugTrace.hidden = true;
      debugTrace.innerHTML = '';
      return;
    }
    const quality = payload.answer_quality || {};
    const queryComponents = (payload.query_plan?.components || [])
      .map((component) => `<li>${escapeHtml(component.name)}: ${escapeHtml((component.terms || []).slice(0, 8).join(', '))}</li>`)
      .join('');
    const tracePaths = (payload.trace_paths || [])
      .slice(0, 5)
      .map((path) => `<li>${escapeHtml(path.repo)}: ${escapeHtml((path.edges || []).map((edge) => `${edge.edge_kind}:${edge.to_name || edge.to_file}`).join(' -> '))}</li>`)
      .join('');
    const repoEdges = (payload.repo_graph?.edges || [])
      .slice(0, 8)
      .map((edge) => {
        const confidence = Number.isFinite(Number(edge.confidence)) ? ` · ${(Number(edge.confidence) * 100).toFixed(0)}%` : '';
        const reason = edge.match_reason ? ` · ${edge.match_reason}` : '';
        return `<li>${escapeHtml(edge.from_repo)} -> ${escapeHtml(edge.to_repo)} · ${escapeHtml(edge.edge_kind)}${escapeHtml(confidence)}${escapeHtml(reason)} · ${escapeHtml(edge.evidence)}</li>`;
      })
      .join('');
    const contract = payload.answer_contract || {};
    const confirmedSources = (contract.confirmed_sources || [])
      .slice(0, 6)
      .map((item) => `<li>${escapeHtml(item)}</li>`)
      .join('');
    const missingLinks = (contract.missing_links || [])
      .slice(0, 6)
      .map((item) => `<li>${escapeHtml(item)}</li>`)
      .join('');
    const judge = payload.answer_judge || {};
    const judgeIssues = (judge.issues || [])
      .slice(0, 5)
      .map((item) => `<li>${escapeHtml(item)}</li>`)
      .join('');
    const toolTrace = (payload.tool_trace || [])
      .slice(0, 8)
      .map((step) => {
        const label = [
          step.phase || 'trace',
          step.tool || 'tool',
          step.step || '',
        ].filter(Boolean).join(' · ');
        const counts = [
          Number.isFinite(Number(step.matches_found)) ? `found ${step.matches_found}` : '',
          Number.isFinite(Number(step.matches_added)) ? `added ${step.matches_added}` : '',
        ].filter(Boolean).join(', ');
        const terms = (step.terms || []).slice(0, 5).join(', ');
        return `<li>${escapeHtml(label)}${counts ? ` (${escapeHtml(counts)})` : ''}${terms ? `: ${escapeHtml(terms)}` : ''}</li>`;
      })
      .join('');
    debugTrace.hidden = false;
    debugTrace.innerHTML = `
      <details class="source-qa-evidence">
        <summary>Why this answer?</summary>
        <div class="source-qa-debug-grid">
          <section>
            <strong>Quality</strong>
            <p>${escapeHtml(quality.status || 'unknown')} · ${escapeHtml(quality.confidence || 'unknown')} · missing: ${escapeHtml((quality.missing || []).join(', ') || 'none')}</p>
          </section>
          <section>
            <strong>Query plan</strong>
            <ul>${queryComponents || '<li>No decomposition terms.</li>'}</ul>
          </section>
          <section>
            <strong>Trace paths</strong>
            <ul>${tracePaths || '<li>No trace path found.</li>'}</ul>
          </section>
          <section>
            <strong>Repo graph</strong>
            <ul>${repoEdges || '<li>No cross-repo edge found.</li>'}</ul>
          </section>
          <section>
            <strong>Evidence boundary</strong>
            <ul>${confirmedSources || missingLinks || '<li>No answer contract boundary.</li>'}</ul>
          </section>
          <section>
            <strong>Missing links</strong>
            <ul>${missingLinks || '<li>No missing link reported.</li>'}</ul>
          </section>
          <section>
            <strong>Answer judge</strong>
            <p>${escapeHtml(judge.mode || 'deterministic_evidence_judge')} · ${escapeHtml(judge.status || 'unknown')} · checked: ${escapeHtml(String(judge.checked_items ?? 'n/a'))}</p>
            <ul>${judgeIssues || '<li>No judge issue reported.</li>'}</ul>
          </section>
          <section>
            <strong>Investigation tools</strong>
            <ul>${toolTrace || '<li>No planner tool step recorded.</li>'}</ul>
          </section>
        </div>
      </details>
    `;
  };

  const renderEvidenceSummary = (payload) => {
    if (!evidenceSummary) return;
    if (!payload || payload.status !== 'ok') {
      evidenceSummary.hidden = true;
      evidenceSummary.innerHTML = '';
      return;
    }
    const pack = payload.evidence_pack || {};
    const quality = payload.answer_quality || {};
    const contract = payload.answer_contract || {};
    const confirmed = [
      ...(pack.confirmed_facts || []),
      ...(contract.confirmed_sources || []),
    ].filter(Boolean).slice(0, 5);
    const inferred = [
      ...(pack.inferred_facts || []),
      ...(contract.data_carriers || []),
      ...(contract.field_population || []),
    ].filter(Boolean).slice(0, 5);
    const missing = [
      ...(pack.missing_facts || []),
      ...(pack.evidence_limits || []),
      ...(contract.missing_links || []),
      ...(quality.missing || []),
    ].filter(Boolean).slice(0, 5);
    const policies = (quality.policies || [])
      .slice(0, 4)
      .map((policy) => `${policy.name}: ${policy.status}`);
    const freshness = payload.index_freshness?.status
      ? `Index: ${payload.index_freshness.status}`
      : '';
    const confidence = quality.confidence ? `Confidence: ${quality.confidence}` : '';
    const meta = [quality.status ? `Quality: ${quality.status}` : '', confidence, freshness, ...policies]
      .filter(Boolean)
      .join(' · ');
    if (!confirmed.length && !inferred.length && !missing.length && !meta) {
      evidenceSummary.hidden = true;
      evidenceSummary.innerHTML = '';
      return;
    }
    const list = (title, items, emptyText) => `
      <section>
        <strong>${escapeHtml(title)}</strong>
        <ul>${items.length ? items.map((item) => `<li>${escapeHtml(item)}</li>`).join('') : `<li>${escapeHtml(emptyText)}</li>`}</ul>
      </section>
    `;
    evidenceSummary.hidden = false;
    evidenceSummary.innerHTML = `
      <div class="source-qa-evidence-card">
        <div class="source-qa-evidence-head">
          <strong>Evidence Boundary</strong>
          <span>${escapeHtml(meta || 'Evidence summary')}</span>
        </div>
        <div class="source-qa-evidence-grid">
          ${list('Confirmed', confirmed, 'No confirmed source facts extracted yet.')}
          ${list('Inferred', inferred, 'No inferred carrier or call-chain facts extracted.')}
          ${list('Missing', missing, 'No missing evidence reported.')}
        </div>
      </div>
    `;
  };

  const renderInlineCitationList = (citations = []) => {
    const values = (citations || [])
      .map((item) => normalizeCitation(item))
      .filter(Boolean)
      .slice(0, 6);
    if (!values.length) return '';
    return `<span class="source-qa-citations" aria-label="Citations">${values.map((item) => `<span>${escapeHtml(item)}</span>`).join('')}</span>`;
  };

  const normalizeCitation = (value) => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    const match = raw.match(/S?\s*(\d+)/i);
    return match ? `S${match[1]}` : raw.replace(/^\[|\]$/g, '');
  };

  const stripCitationTags = (text) => String(text || '')
    .replace(/\s*\[S\d+\]/gi, '')
    .replace(/\s{2,}/g, ' ')
    .trim();

  const extractCitationTags = (text) => {
    const tags = [];
    String(text || '').replace(/\[S(\d+)\]/gi, (_match, id) => {
      const tag = `S${id}`;
      if (!tags.includes(tag)) tags.push(tag);
      return '';
    });
    return tags;
  };

  const isCitationOnlyText = (text) => {
    const normalized = String(text || '').trim();
    return Boolean(normalized) && /^(?:\[?S\d+\]?\s*)+$/i.test(normalized);
  };

  const cleanDirectAnswer = (directAnswer, fallbackAnswer, hasStructuredSections) => {
    let text = String(directAnswer || fallbackAnswer || '').trim();
    if (!text) return '';
    if (hasStructuredSections) {
      text = text.split(/\n\s*(?:[-*]\s+|Missing evidence:)/i)[0].trim();
      text = text.split(/\s+-\s+(?=(?:The|If|Missing evidence|Specific details|Specific API|Confirmed|Missing)\b)/i)[0].trim();
    }
    return stripCitationTags(text);
  };

  const normalizedClaimsForDisplay = (claims = []) => {
    const seen = new Set();
    const items = [];
    for (const claim of claims || []) {
      const rawText = typeof claim === 'string' ? claim : claim?.text;
      if (isCitationOnlyText(rawText)) continue;
      const text = stripCitationTags(rawText);
      if (!text || seen.has(text.toLowerCase())) continue;
      const citationTags = [
        ...(Array.isArray(claim?.citations) ? claim.citations : []),
        ...extractCitationTags(rawText),
      ];
      items.push({ text, citations: Array.from(new Set(citationTags.map(normalizeCitation).filter(Boolean))) });
      seen.add(text.toLowerCase());
      if (items.length >= 8) break;
    }
    return items;
  };

  const renderReadableAnswerBody = (payload, answer) => {
    const structured = payload?.structured_answer || {};
    const claims = normalizedClaimsForDisplay(Array.isArray(structured.claims) ? structured.claims : []);
    const missing = (Array.isArray(structured.missing_evidence) ? structured.missing_evidence : [])
      .map((item) => String(item || '').trim())
      .filter(Boolean)
      .slice(0, 6);
    const directAnswer = cleanDirectAnswer(structured.direct_answer, answer, Boolean(claims.length || missing.length));
    if (directAnswer || claims.length || missing.length) {
      return `
        <section class="source-qa-answer-section source-qa-answer-direct">
          <strong>Direct Answer</strong>
          ${directAnswer ? `<p>${escapeHtml(directAnswer)}</p>` : '<p>No direct answer returned.</p>'}
        </section>
        ${claims.length ? `
          <section class="source-qa-answer-section">
            <strong>Evidence-backed Claims</strong>
            <div class="source-qa-claim-list">
              ${claims.map((claim) => `
                <div class="source-qa-claim-item">
                  <p>${escapeHtml(claim.text)}</p>
                  ${renderInlineCitationList(claim.citations || [])}
                </div>
              `).join('')}
            </div>
          </section>
        ` : ''}
        ${missing.length ? `
          <section class="source-qa-answer-section source-qa-answer-missing">
            <strong>Missing Evidence</strong>
            <ul>${missing.slice(0, 6).map((item) => `<li>${escapeHtml(item)}</li>`).join('')}</ul>
          </section>
        ` : ''}
      `;
    }
    const paragraphs = answer.split(/\n{2,}/).map((part) => part.trim()).filter(Boolean);
    return paragraphs.map((part) => `<p>${escapeHtml(part)}</p>`).join('');
  };

  const renderLlmAnswer = (payload) => {
    if (!llmAnswer) return;
    const answer = String(payload?.llm_answer || '').trim();
    if (!answer) {
      llmAnswer.hidden = true;
      llmAnswer.innerHTML = '';
      return;
    }
    const usage = payload?.llm_usage || {};
    const meta = [
      payload?.llm_budget_mode ? `budget: ${payload.llm_budget_mode}` : '',
      payload?.llm_provider ? `provider: ${payload.llm_provider}` : '',
      payload?.llm_model ? `model: ${payload.llm_model}` : '',
      payload?.llm_thinking_budget !== undefined ? `thinking: ${payload.llm_thinking_budget}` : '',
      payload?.llm_route?.mode === 'auto' ? `route: ${payload.llm_route.reason}` : '',
      payload?.llm_cost_skip?.skipped ? 'LLM skipped' : (payload?.llm_cached ? 'cache hit' : 'live call'),
      usage?.promptTokenCount || usage?.prompt_tokens ? `prompt: ${usage.promptTokenCount || usage.prompt_tokens}` : '',
      usage?.candidatesTokenCount || usage?.completion_tokens ? `output: ${usage.candidatesTokenCount || usage.completion_tokens}` : '',
      usage?.totalTokenCount || usage?.total_tokens ? `total: ${usage.totalTokenCount || usage.total_tokens}` : '',
    ].filter(Boolean).join(' · ');
    const confidence = payload?.structured_answer?.confidence || payload?.answer_contract?.confidence || payload?.answer_quality?.confidence;
    llmAnswer.hidden = false;
    llmAnswer.innerHTML = `
      <div class="source-qa-llm-card">
        <div class="source-qa-llm-head">
          <div>
            <strong>Answer</strong>
            ${confidence ? `<em>${escapeHtml(confidence)} confidence</em>` : ''}
          </div>
          <span>${escapeHtml(meta)}</span>
        </div>
        <div class="source-qa-answer-body">
          ${renderReadableAnswerBody(payload, answer)}
        </div>
      </div>
    `;
  };

  const renderFallbackNotice = (payload) => {
    if (!fallbackNotice) return;
    const notice = payload?.fallback_notice;
    const freshnessWarning = payload?.index_freshness?.warning;
    if (!notice?.message) {
      if (freshnessWarning) {
        fallbackNotice.hidden = false;
        fallbackNotice.textContent = `Index freshness: ${freshnessWarning}`;
        return;
      }
      fallbackNotice.hidden = true;
      fallbackNotice.textContent = '';
      return;
    }
    fallbackNotice.hidden = false;
    fallbackNotice.textContent = `${notice.title || 'Fallback'}: ${notice.message}`;
  };

  const renderUsageBadges = (payload) => {
    const llmAnswered = ['gemini_flash', 'auto'].includes(payload?.answer_mode) && Boolean(payload?.llm_answer);
    if (activeCache) {
      if (payload?.llm_cost_skip?.skipped) {
        activeCache.hidden = false;
        activeCache.textContent = 'LLM skipped';
      } else if (llmAnswered) {
        activeCache.hidden = false;
        activeCache.textContent = payload?.llm_cached ? 'cache hit' : 'live LLM';
      } else {
        activeCache.hidden = true;
        activeCache.textContent = 'live';
      }
    }
    if (activeUsage) {
      const usage = payload?.llm_usage || {};
      const total = usage?.totalTokenCount || usage?.total_tokens;
      if (llmAnswered && total) {
        activeUsage.hidden = false;
        activeUsage.textContent = `${total} tokens`;
      } else {
        activeUsage.hidden = true;
        activeUsage.textContent = 'tokens';
      }
    }
  };

  const pollQueryJob = async (jobId, progress) => {
    while (jobId) {
      const response = await fetch(`/api/jobs/${encodeURIComponent(jobId)}`, { method: 'GET' });
      const payload = await readJson(response);
      const progressText = payload.total
        ? `${payload.message || 'Processing source-code question.'} (${payload.current || 0}/${payload.total})`
        : (payload.message || 'Processing source-code question.');
      progress.setMessage(progressText);
      if (payload.state === 'completed') {
        return (payload.results || [])[0] || {};
      }
      if (payload.state === 'failed') {
        throw new Error(payload.error || payload.message || 'Source Code Q&A failed.');
      }
      await new Promise((resolve) => window.setTimeout(resolve, 700));
    }
    return {};
  };

  const queryCode = async () => {
    const selectedAnswerMode = answerMode?.value || 'auto';
    const effectiveAnswerMode = selectedAnswerMode !== 'retrieval_only' && !llmReady ? 'retrieval_only' : selectedAnswerMode;
    if (activeMode) activeMode.textContent = effectiveAnswerMode;
    const progress = startQueryProgress('Submitting query to server...');
    if (queryButton) queryButton.disabled = true;
    try {
      const initialPayload = await fetch(queryUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pm_team: pmTeam.value,
          country: currentCountry(),
          question: questionInput.value,
          answer_mode: effectiveAnswerMode,
          llm_budget_mode: 'auto',
          conversation_context: conversationContext,
          async: true,
        }),
      }).then(readJson);
      const payload = initialPayload.status === 'queued' && initialPayload.job_id
        ? await pollQueryJob(initialPayload.job_id, progress)
        : initialPayload;
      lastPayload = payload;
      conversationContext = buildConversationContext(payload);
      rememberLastQueryConfig(effectiveAnswerMode);
      summary.textContent = payload.summary || 'Search completed.';
      if (payload.llm_retryable_error?.retryable) {
        queryStatus.textContent = `LLM quota/rate limit hit; retry is still enabled. Code search completed in ${formatElapsed(progress.startedAt)}.`;
      } else {
        queryStatus.textContent = payload.status === 'ok'
          ? `Search completed in ${formatElapsed(progress.startedAt)}.`
          : `${payload.status} after ${formatElapsed(progress.startedAt)}.`;
      }
      if (activeMode) activeMode.textContent = payload.answer_mode || effectiveAnswerMode;
      renderUsageBadges(payload);
      renderFallbackNotice(payload);
      renderStatus(payload.repo_status || []);
      renderLlmAnswer(payload);
      renderEvidenceSummary(payload);
      renderDebugTrace(payload);
      renderMatches(payload.matches || [], { compact: Boolean(payload.llm_answer), open: shouldOpenEvidence(payload) });
      saveQuestionHistory({
        question: questionInput.value,
        pm_team: pmTeam.value,
        country: currentCountry(),
        answer_mode: payload.answer_mode || selectedAnswerMode,
        status: payload.status,
        trace_id: payload.trace_id,
        summary: payload.summary,
      });
      if (feedback) {
        feedback.hidden = payload.status !== 'ok';
      }
      if (feedbackStatus) {
        feedbackStatus.textContent = '';
      }
    } catch (error) {
      queryStatus.textContent = `${error.message || 'Search failed.'} elapsed ${formatElapsed(progress.startedAt)}`;
      renderUsageBadges({});
      renderFallbackNotice({});
      renderLlmAnswer({});
      renderEvidenceSummary(null);
      renderDebugTrace(null);
      if (feedback) feedback.hidden = true;
    } finally {
      stopQueryProgress();
      if (queryButton) queryButton.disabled = false;
    }
  };

  const sendFeedback = async (rating) => {
    if (!lastPayload || !feedbackUrl) return;
    if (feedbackStatus) feedbackStatus.textContent = 'Saving feedback...';
    try {
      await fetch(feedbackUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          rating,
          pm_team: pmTeam.value,
          country: currentCountry(),
          question: questionInput.value,
          trace_id: lastPayload.trace_id || '',
          answer_mode: lastPayload.answer_mode || '',
          llm_budget_mode: lastPayload.llm_budget_mode || lastPayload.llm_requested_budget_mode || '',
          top_paths: (lastPayload.matches || []).slice(0, 5).map((match) => match.path),
          answer_quality: lastPayload.answer_quality || {},
          replay_context: buildFeedbackReplayContext(lastPayload),
        }),
      }).then(readJson);
      if (feedbackStatus) feedbackStatus.textContent = 'Feedback saved.';
    } catch (error) {
      if (feedbackStatus) feedbackStatus.textContent = error.message || 'Feedback failed.';
    }
  };

  pmTeam.addEventListener('change', () => {
    updateCountryVisibility();
    renderSelectedConfig();
  });
  country.addEventListener('change', renderSelectedConfig);
  answerMode?.addEventListener('change', updateAnswerModeState);
  saveButton?.addEventListener('click', saveConfig);
  syncButton?.addEventListener('click', syncRepos);
  queryButton?.addEventListener('click', queryCode);
  document.querySelectorAll('[data-source-feedback-rating]').forEach((button) => {
    button.addEventListener('click', () => sendFeedback(button.dataset.sourceFeedbackRating));
  });

  restoreLastQueryConfig();
  renderQuestionHistory();
  updateAnswerModeState();
  loadConfig();
})();
