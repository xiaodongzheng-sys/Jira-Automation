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
  let config = { mappings: {} };
  let gitAuthReady = false;
  let llmReady = false;
  let llmPolicy = {};
  let lastPayload = null;
  let conversationContext = null;
  const preferenceKey = 'source-code-qa:last-query-config:v1';

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
      throw new Error(text.includes('<!DOCTYPE') ? 'The portal returned an HTML error page. Please refresh and try again.' : text.slice(0, 180));
    }
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.message || 'Request failed.');
    }
    return payload;
  };

  const currentCountry = () => (pmTeam.value === 'CRMS' ? country.value : 'All');
  const currentKey = () => `${pmTeam.value}:${currentCountry()}`;

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
    const llmSelected = answerModeValue !== 'retrieval_only';
    if (queryButton) {
      queryButton.textContent = llmSelected ? 'Search + Generate Answer' : 'Search Code';
    }
    if (queryStatus) {
      if (answerModeValue === 'gemini_flash' && !llmReady) {
        queryStatus.textContent = 'LLM mode is not configured on the server yet.';
      } else if (!llmSelected) {
        queryStatus.textContent = 'Ready.';
      } else if (answerModeValue === 'auto' && !llmReady) {
        queryStatus.textContent = 'Auto mode will use code-search results until LLM credentials are configured.';
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
      if (!gitAuthReady && adminStatus) {
        adminStatus.textContent = 'Set SOURCE_CODE_QA_GITLAB_TOKEN on the server before running Sync / Refresh.';
      } else if (adminStatus && llmPolicy.provider) {
        const provider = llmPolicy.provider.provider || payload.llm_provider || 'llm';
        const routerVersion = llmPolicy.router?.version ? ` · router v${llmPolicy.router.version}` : '';
        adminStatus.textContent = `LLM provider: ${provider}${routerVersion}.`;
      }
      updateAnswerModeState();
      renderSelectedConfig();
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
      payload?.llm_cached ? 'cache hit' : 'live call',
      usage?.promptTokenCount || usage?.prompt_tokens ? `prompt: ${usage.promptTokenCount || usage.prompt_tokens}` : '',
      usage?.candidatesTokenCount || usage?.completion_tokens ? `output: ${usage.candidatesTokenCount || usage.completion_tokens}` : '',
      usage?.totalTokenCount || usage?.total_tokens ? `total: ${usage.totalTokenCount || usage.total_tokens}` : '',
    ].filter(Boolean).join(' · ');
    llmAnswer.hidden = false;
    llmAnswer.innerHTML = `
      <div class="source-qa-llm-card">
        <div class="source-qa-llm-head">
          <strong>LLM Answer</strong>
          <span>${escapeHtml(meta)}</span>
        </div>
        <pre><code>${escapeHtml(answer)}</code></pre>
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
      if (llmAnswered) {
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

  const queryCode = async () => {
    const selectedAnswerMode = answerMode?.value || 'auto';
    if (selectedAnswerMode === 'gemini_flash' && !llmReady) {
      queryStatus.textContent = 'LLM mode is not configured on the server yet.';
      return;
    }
    if (activeMode) activeMode.textContent = selectedAnswerMode;
    queryStatus.textContent = selectedAnswerMode !== 'retrieval_only'
      ? 'Searching local code and asking LLM...'
      : 'Searching local code index...';
    try {
      const payload = await fetch(queryUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pm_team: pmTeam.value,
          country: currentCountry(),
          question: questionInput.value,
          answer_mode: selectedAnswerMode,
          llm_budget_mode: 'auto',
          conversation_context: conversationContext,
        }),
      }).then(readJson);
      lastPayload = payload;
      conversationContext = buildConversationContext(payload);
      rememberLastQueryConfig(selectedAnswerMode);
      summary.textContent = payload.summary || 'Search completed.';
      queryStatus.textContent = payload.status === 'ok' ? 'Search completed.' : payload.status;
      if (activeMode) activeMode.textContent = payload.answer_mode || selectedAnswerMode;
      renderUsageBadges(payload);
      renderFallbackNotice(payload);
      renderStatus(payload.repo_status || []);
      renderLlmAnswer(payload);
      renderEvidenceSummary(payload);
      renderDebugTrace(payload);
      renderMatches(payload.matches || [], { compact: Boolean(payload.llm_answer), open: shouldOpenEvidence(payload) });
      if (feedback) {
        feedback.hidden = payload.status !== 'ok';
      }
      if (feedbackStatus) {
        feedbackStatus.textContent = '';
      }
    } catch (error) {
      queryStatus.textContent = error.message || 'Search failed.';
      renderUsageBadges({});
      renderFallbackNotice({});
      renderLlmAnswer({});
      renderEvidenceSummary(null);
      renderDebugTrace(null);
      if (feedback) feedback.hidden = true;
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
          top_paths: (lastPayload.matches || []).slice(0, 5).map((match) => match.path),
          answer_quality: lastPayload.answer_quality || {},
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
  updateAnswerModeState();
  loadConfig();
})();
