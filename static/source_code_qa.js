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
  const llmBudget = document.querySelector('[data-source-llm-budget]');
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
  const activeBudget = document.querySelector('[data-source-active-budget]');
  const activeCache = document.querySelector('[data-source-active-cache]');
  const activeUsage = document.querySelector('[data-source-active-usage]');
  const fallbackNotice = document.querySelector('[data-source-fallback-notice]');
  const feedback = document.querySelector('[data-source-feedback]');
  const feedbackStatus = document.querySelector('[data-source-feedback-status]');
  const debugTrace = document.querySelector('[data-source-debug-trace]');
  let config = { mappings: {} };
  let gitAuthReady = false;
  let llmReady = false;
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
    if (selectHasValue(llmBudget, saved.llm_budget_mode)) {
      llmBudget.value = saved.llm_budget_mode;
    }
  };

  const rememberLastQueryConfig = (selectedAnswerMode, selectedBudget) => {
    try {
      window.localStorage.setItem(preferenceKey, JSON.stringify({
        pm_team: pmTeam.value,
        country: currentCountry(),
        answer_mode: selectedAnswerMode,
        llm_budget_mode: selectedBudget,
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
    const llmSelected = answerMode?.value === 'gemini_flash';
    if (llmBudget) {
      llmBudget.disabled = !llmSelected;
    }
    if (queryButton) {
      queryButton.textContent = llmSelected ? 'Search + Generate LLM Answer' : 'Search Code';
    }
    if (queryStatus) {
      if (llmSelected && !llmReady) {
        queryStatus.textContent = 'Gemini mode is not configured on the server yet.';
      } else if (!llmSelected) {
        queryStatus.textContent = 'Ready.';
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
      if (!gitAuthReady && adminStatus) {
        adminStatus.textContent = 'Set SOURCE_CODE_QA_GITLAB_TOKEN on the server before running Sync / Refresh.';
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
      results.innerHTML = `
        <details class="source-qa-evidence">
          <summary>Show ${escapeHtml(matches.length)} code references used for this answer</summary>
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
    })),
    trace_paths: (payload?.trace_paths || []).slice(0, 5),
    structured_answer: payload?.structured_answer || {},
  });

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
      .map((edge) => `<li>${escapeHtml(edge.from_repo)} -> ${escapeHtml(edge.to_repo)} · ${escapeHtml(edge.edge_kind)} · ${escapeHtml(edge.evidence)}</li>`)
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
        </div>
      </details>
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
      payload?.llm_cached ? 'cache hit' : 'live call',
      usage?.promptTokenCount ? `prompt: ${usage.promptTokenCount}` : '',
      usage?.candidatesTokenCount ? `output: ${usage.candidatesTokenCount}` : '',
    ].filter(Boolean).join(' · ');
    llmAnswer.hidden = false;
    llmAnswer.innerHTML = `
      <div class="source-qa-llm-card">
        <div class="source-qa-llm-head">
          <strong>Gemini Answer</strong>
          <span>${escapeHtml(meta)}</span>
        </div>
        <pre><code>${escapeHtml(answer)}</code></pre>
      </div>
    `;
  };

  const renderFallbackNotice = (payload) => {
    if (!fallbackNotice) return;
    const notice = payload?.fallback_notice;
    if (!notice?.message) {
      fallbackNotice.hidden = true;
      fallbackNotice.textContent = '';
      return;
    }
    fallbackNotice.hidden = false;
    fallbackNotice.textContent = `${notice.title || 'Fallback'}: ${notice.message}`;
  };

  const renderUsageBadges = (payload, selectedBudget) => {
    if (activeBudget) {
      activeBudget.textContent = selectedBudget || 'cheap';
    }
    if (activeCache) {
      if (payload?.answer_mode === 'gemini_flash') {
        activeCache.hidden = false;
        activeCache.textContent = payload?.llm_cached ? 'cache hit' : 'live LLM';
      } else {
        activeCache.hidden = true;
        activeCache.textContent = 'live';
      }
    }
    if (activeUsage) {
      const usage = payload?.llm_usage || {};
      const total = usage?.totalTokenCount;
      if (payload?.answer_mode === 'gemini_flash' && total) {
        activeUsage.hidden = false;
        activeUsage.textContent = `${total} tokens`;
      } else {
        activeUsage.hidden = true;
        activeUsage.textContent = 'tokens';
      }
    }
  };

  const queryCode = async () => {
    const selectedAnswerMode = answerMode?.value || 'retrieval_only';
    const selectedBudget = llmBudget?.value || 'cheap';
    if (selectedAnswerMode === 'gemini_flash' && !llmReady) {
      queryStatus.textContent = 'Gemini mode is not configured on the server yet.';
      return;
    }
    activeMode.textContent = selectedAnswerMode;
    queryStatus.textContent = selectedAnswerMode === 'gemini_flash'
      ? 'Searching local code and asking Gemini...'
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
          llm_budget_mode: selectedBudget,
          conversation_context: conversationContext,
        }),
      }).then(readJson);
      lastPayload = payload;
      conversationContext = buildConversationContext(payload);
      rememberLastQueryConfig(selectedAnswerMode, selectedBudget);
      summary.textContent = payload.summary || 'Search completed.';
      queryStatus.textContent = payload.status === 'ok' ? 'Search completed.' : payload.status;
      activeMode.textContent = payload.answer_mode || selectedAnswerMode;
      renderUsageBadges(payload, selectedBudget);
      renderFallbackNotice(payload);
      renderStatus(payload.repo_status || []);
      renderLlmAnswer(payload);
      renderDebugTrace(payload);
      renderMatches(payload.matches || [], { compact: Boolean(payload.llm_answer) });
      if (feedback) {
        feedback.hidden = payload.status !== 'ok';
      }
      if (feedbackStatus) {
        feedbackStatus.textContent = '';
      }
    } catch (error) {
      queryStatus.textContent = error.message || 'Search failed.';
      renderUsageBadges({}, selectedBudget);
      renderFallbackNotice({});
      renderLlmAnswer({});
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
