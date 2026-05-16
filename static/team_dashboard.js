(() => {
  const root = document.querySelector('[data-team-dashboard]');
  if (!root) return;
  const jobsUrlTemplate = root.dataset.jobsUrl || '/api/jobs/__JOB_ID__';

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const formatSingaporeTimestamp = (value) => {
    if (!value) return '';
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Singapore',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hourCycle: 'h23',
    }).formatToParts(date).reduce((acc, part) => {
      acc[part.type] = part.value;
      return acc;
    }, {});
    return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second} SGT`;
  };

  const readJson = async (response, fallbackMessage) => {
    let payload = {};
    try {
      payload = await response.json();
    } catch (error) {
      payload = {};
    }
    if (!response.ok || payload.status === 'error') {
      const error = new Error(payload.message || fallbackMessage);
      error.payload = payload;
      throw error;
    }
    return payload;
  };

  const jobUrl = (jobId) => jobsUrlTemplate.replace('__JOB_ID__', encodeURIComponent(jobId));
  const pollJobResult = async (jobId, { fallbackMessage, onProgress } = {}) => {
    let lastMessage = '';
    while (true) {
      const response = await fetch(jobUrl(jobId), {
        method: 'GET',
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      const payload = await readJson(response, fallbackMessage || 'Could not load job status.');
      const message = payload.message || payload.progress?.message || '';
      if (message && message !== lastMessage) {
        lastMessage = message;
        if (typeof onProgress === 'function') onProgress(message, payload);
      }
      if (payload.state === 'completed') {
        const result = Array.isArray(payload.results) && payload.results[0] ? payload.results[0] : {};
        if (!result || result.status === 'error') throw new Error(result.message || fallbackMessage || 'Job finished without a result.');
        return result;
      }
      if (payload.state === 'failed') {
        throw new Error(payload.error || payload.message || fallbackMessage || 'Job failed.');
      }
      await sleep(1200);
    }
  };

  const externalHref = (value) => {
    const text = String(value || '').trim();
    if (!text) return '';
    if (/^(https?:|mailto:|tel:)/i.test(text)) return text;
    if (text.startsWith('//')) return `https:${text}`;
    return `https://${text}`;
  };

  const isIgnoredSeatalkMappingId = (id) => {
    const value = String(id || '').trim();
    return value === '0' || /^UID\s+0$/i.test(value) || value === 'buddy-0';
  };

  const taskStatus = root.querySelector('[data-team-dashboard-task-status]');
  const taskSummary = root.querySelector('[data-team-dashboard-task-summary]');
  const taskList = root.querySelector('[data-team-dashboard-task-list]');
  const adminForm = root.querySelector('[data-team-dashboard-admin-form]');
  const adminStatus = root.querySelector('[data-team-dashboard-admin-status]');
  const monthlyReportStatus = root.querySelector('[data-monthly-report-status]');
  const monthlyReportGenerateButton = root.querySelector('[data-monthly-report-generate]');
  const monthlyReportSendButton = root.querySelector('[data-monthly-report-send]');
  const monthlyReportDraft = root.querySelector('[data-monthly-report-draft]');
  const monthlyReportPreview = root.querySelector('[data-monthly-report-preview]');
  const monthlyReportRecipient = root.querySelector('[data-monthly-report-recipient]');
  const monthlyReportTopics = root.querySelector('[data-monthly-report-topics]');
  const monthlyReportTopicRows = Array.from(root.querySelectorAll('[data-monthly-report-topic-row]'));
  const monthlyReportPeriodStart = root.querySelector('[data-monthly-report-period-start]');
  const monthlyReportPeriodEnd = root.querySelector('[data-monthly-report-period-end]');
  const monthlyReportProgress = root.querySelector('[data-monthly-report-progress]');
  const monthlyReportProgressFill = root.querySelector('[data-monthly-report-progress-fill]');
  const monthlyReportProgressMessage = root.querySelector('[data-monthly-report-progress-message]');
  const monthlyReportEvidenceReview = root.querySelector('[data-monthly-report-evidence-review]');
  const monthlyReportEvidenceReviewBody = root.querySelector('[data-monthly-report-evidence-review-body]');
  const monthlyReportEvidenceDebug = root.querySelector('[data-monthly-report-evidence-debug]');
  const monthlyReportEvidenceDebugBody = root.querySelector('[data-monthly-report-evidence-debug-body]');
  const monthlyReportTemplateForm = root.querySelector('[data-monthly-report-template-form]');
  const monthlyReportTemplate = root.querySelector('[data-monthly-report-template]');
  const monthlyReportTemplateStatus = root.querySelector('[data-monthly-report-template-status]');
  const dailyBriefStatus = root.querySelector('[data-daily-brief-status]');
  const dailyBriefRows = root.querySelector('[data-daily-brief-rows]');
  const reportIntelligenceForm = root.querySelector('[data-report-intelligence-form]');
  const reportIntelligenceStatus = root.querySelector('[data-report-intelligence-status]');
  const reportIntelligenceVips = root.querySelector('[data-report-intelligence-vips]');
  const reportIntelligenceKeywords = root.querySelector('[data-report-intelligence-keywords]');
  const reportIntelligenceSeatalkBlacklist = root.querySelector('[data-report-intelligence-seatalk-blacklist]');
  const reportIntelligenceGmailSenderBlacklist = root.querySelector('[data-report-intelligence-gmail-sender-blacklist]');
  const reportIntelligenceGmailSubjectHints = root.querySelector('[data-report-intelligence-gmail-subject-hints]');
  const seatalkNameMappingRoot = root.querySelector('[data-seatalk-demo-root]');
  const linkBizProjectStatus = root.querySelector('[data-link-biz-project-status]');
  const linkBizProjectRows = root.querySelector('[data-link-biz-project-rows]');
  const linkBizProjectFindJira = root.querySelector('[data-link-biz-project-find-jira]');
  const linkBizProjectSuggest = root.querySelector('[data-link-biz-project-suggest]');
  const versionPlanContent = root.querySelector('[data-version-plan-content]');
  const versionPlanStatus = root.querySelector('[data-version-plan-status]');
  const versionPlanSyncButton = root.querySelector('[data-version-plan-sync]');
  const canManageKeyProjects = root.dataset.canManageKeyProjects === 'true';
  const teamLabels = {
    AF: 'Anti-fraud',
    CRMS: 'Credit Risk',
    GRC: 'Ops Risk',
  };
  const teamOrder = ['AF', 'CRMS', 'GRC'];
  const jiraPageSize = 10;
  const taskCacheKey = 'team-dashboard:jira-tasks:v8';
  const monthlyReportDraftCacheKey = 'team-dashboard:monthly-report-draft:v2';
  const seatalkNameMappingDefaultPageSize = 20;
  const seatalkNameMappingPageSizeOptions = [20, 50, 100, 200];

  let initialConfig = (() => {
    try {
      return JSON.parse(root.dataset.initialConfig || '{}');
    } catch (error) {
      return {};
    }
  })();
  let taskTeams = [];
  let activeTaskTeamKey = 'AF';
  let keyProjectOnly = false;
  let monthlyReportSubject = 'Monthly Report';
  let monthlyReportLoaded = false;
  let dailyBriefLoaded = false;
  let reportIntelligenceLoaded = false;
  let seatalkNameMappingsLoaded = false;
  let linkBizProjectRowsState = [];
  let linkBizProjectSelectOptions = [];
  let linkBizProjectLoading = false;
  let monthlyReportProgressTimer = null;
  let monthlyReportProgressStartedAt = 0;
  let monthlyReportLastProgress = null;
  let versionPlanLoaded = false;
  let versionPlanState = null;
  let versionPlanPollTimer = null;
  let versionPlanDragRow = null;
  const pmFilterState = {};
  const expandedPanels = {};
  const jiraPageState = {};
  const seatalkNameMappingState = new WeakMap();

  const setStatus = (node, message, tone = 'neutral') => {
    if (!node) return;
    node.textContent = message || '';
    node.dataset.tone = tone;
  };

  const formatDuration = (seconds) => {
    const total = Math.max(0, Math.round(Number(seconds) || 0));
    const minutes = Math.floor(total / 60);
    const remaining = total % 60;
    return minutes ? `${minutes}m ${String(remaining).padStart(2, '0')}s` : `${remaining}s`;
  };

  const formatTokenCount = (value) => {
    const count = Math.max(0, Math.round(Number(value) || 0));
    if (count >= 1000) return `${Math.round(count / 100) / 10}k`;
    return String(count);
  };

  const sleep = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms));

  const monthlyReportJobErrorMessage = (payloadOrError) => {
    const payload = payloadOrError?.payload || payloadOrError?.jobPayload || payloadOrError || {};
    const category = String(payload.error_category || '').toLowerCase();
    const rawMessage = String(payload.message || payload.error || payloadOrError?.message || '').trim();
    if (category === 'codex_timeout_or_rate_limit') {
      return 'Codex timed out or was rate-limited while generating the Monthly Report. Retry once; if it repeats, reduce the report period or source scope.';
    }
    if (category === 'local_agent_offline') {
      return 'Mac local-agent is unavailable. Confirm the host stack is online, then retry Monthly Report generation.';
    }
    return rawMessage || 'Monthly Report draft generation failed.';
  };

  const readJobStatus = async (jobId) => {
    const url = jobsUrlTemplate.replace('__JOB_ID__', encodeURIComponent(jobId));
    const response = await fetch(url, {
      headers: { Accept: 'application/json' },
      credentials: 'same-origin',
    });
    return readJson(response, 'Could not load job status.');
  };

  const normalizeEmailList = (items) => (Array.isArray(items) ? items : [])
    .map((item) => String(item || '').trim().toLowerCase())
    .filter(Boolean)
    .sort();

  const emailSignature = (items) => normalizeEmailList(items).join('|');

  const readTaskCache = () => {
    try {
      const payload = JSON.parse(window.localStorage.getItem(taskCacheKey) || '{}');
      return payload && typeof payload === 'object' ? payload : {};
    } catch (error) {
      return {};
    }
  };

  const writeTaskCache = (payload) => {
    try {
      window.localStorage.setItem(taskCacheKey, JSON.stringify(payload || {}));
    } catch (error) {
      // Browser storage can be disabled or full; Jira loading should still work.
    }
  };

  const clearTaskCache = () => {
    try {
      window.localStorage.removeItem(taskCacheKey);
    } catch (error) {
      writeTaskCache({});
    }
  };

  const readMonthlyReportDraftCache = () => {
    try {
      const payload = JSON.parse(window.localStorage.getItem(monthlyReportDraftCacheKey) || '{}');
      return payload && typeof payload === 'object' ? payload : {};
    } catch (error) {
      return {};
    }
  };

  const writeMonthlyReportDraftCache = (payload) => {
    try {
      window.localStorage.setItem(monthlyReportDraftCacheKey, JSON.stringify({
        draft_markdown: String(payload?.draft_markdown || ''),
        subject: String(payload?.subject || monthlyReportSubject || 'Monthly Report'),
        highlight_topics: Array.isArray(payload?.highlight_topics) ? payload.highlight_topics : readMonthlyReportTopics({ strict: false }),
        highlight_topic_sources: Array.isArray(payload?.highlight_topic_sources) ? payload.highlight_topic_sources : readMonthlyReportTopicSources({ strict: false }),
        evidence_review: Array.isArray(payload?.evidence_review) ? payload.evidence_review : [],
        evidence_debug: Array.isArray(payload?.evidence_debug) ? payload.evidence_debug : [],
        period_start: String(payload?.period_start || monthlyReportPeriodStart?.value || ''),
        period_end: String(payload?.period_end || monthlyReportPeriodEnd?.value || ''),
        saved_at: payload?.saved_at || new Date().toISOString(),
        source: String(payload?.source || 'browser'),
      }));
    } catch (error) {
      // Browser storage can be disabled or full; draft editing should still work.
    }
  };

  const readMonthlyReportTopics = ({ strict = true } = {}) => {
    const sourceNodes = monthlyReportTopicRows.length
      ? monthlyReportTopicRows.map((row) => row.querySelector('[data-monthly-report-topic-input]'))
      : [monthlyReportTopics];
    const topics = sourceNodes
      .map((node) => String(node?.value || '').trim())
      .filter(Boolean);
    const uniqueTopics = [...new Set(topics)];
    if (strict && uniqueTopics.length === 0) {
      throw new Error('Add 1 to 6 highlight topics before generating the Monthly Report.');
    }
    if (strict && uniqueTopics.length > 6) {
      throw new Error('Monthly Report supports at most 6 highlight topics.');
    }
    return uniqueTopics.slice(0, 6);
  };

  const readMonthlyReportTopicSources = ({ strict = true } = {}) => {
    const items = [];
    monthlyReportTopicRows.forEach((row) => {
      const topic = String(row.querySelector('[data-monthly-report-topic-input]')?.value || '').trim();
      if (!topic) return;
      const sources = Array.from(row.querySelectorAll('[data-monthly-report-source]'))
        .filter((input) => input.checked)
        .map((input) => String(input.value || '').trim())
        .filter(Boolean);
      if (strict && sources.length === 0) {
        throw new Error(`Select at least one source for "${topic}".`);
      }
      items.push({ topic, sources });
    });
    return items.slice(0, 6);
  };

  const applyMonthlyReportInputs = (payload = {}, { onlyEmpty = false } = {}) => {
    const topics = Array.isArray(payload.highlight_topics) ? payload.highlight_topics.map((item) => String(item || '').trim()).filter(Boolean).slice(0, 6) : [];
    const rawSourcePayload = payload.highlight_topic_sources && typeof payload.highlight_topic_sources === 'object'
      ? payload.highlight_topic_sources
      : {};
    const sourceEntries = Array.isArray(rawSourcePayload)
      ? rawSourcePayload
          .filter((item) => item && typeof item === 'object')
          .map((item) => [String(item.topic || '').trim(), Array.isArray(item.sources) ? item.sources : []])
      : Object.entries(rawSourcePayload);
    const sourceMap = new Map(
      sourceEntries.map(([topic, sources]) => [
        String(topic || '').trim(),
        Array.isArray(sources) ? sources.map((source) => String(source || '').trim()) : [],
      ]),
    );
    if (monthlyReportTopicRows.length && topics.length && (!onlyEmpty || readMonthlyReportTopics({ strict: false }).length === 0)) {
      monthlyReportTopicRows.forEach((row, index) => {
        const input = row.querySelector('[data-monthly-report-topic-input]');
        if (input) input.value = topics[index] || '';
        const selected = sourceMap.get(topics[index]) || ['seatalk', 'gmail', 'team_dashboard'];
        row.querySelectorAll('[data-monthly-report-source]').forEach((sourceInput) => {
          sourceInput.checked = !topics[index] || selected.includes(String(sourceInput.value || '').trim());
        });
      });
    } else if (monthlyReportTopics && topics.length && (!onlyEmpty || !monthlyReportTopics.value.trim())) {
      monthlyReportTopics.value = topics.join('\n');
    }
    if (monthlyReportPeriodStart && payload.period_start && (!onlyEmpty || !monthlyReportPeriodStart.value)) {
      monthlyReportPeriodStart.value = String(payload.period_start).slice(0, 10);
    }
    if (monthlyReportPeriodEnd && payload.period_end && (!onlyEmpty || !monthlyReportPeriodEnd.value)) {
      monthlyReportPeriodEnd.value = String(payload.period_end).slice(0, 10);
    }
  };

  const monthlyReportRequestPayload = () => {
    const highlightTopics = readMonthlyReportTopics();
    const periodStart = String(monthlyReportPeriodStart?.value || '').trim();
    const periodEnd = String(monthlyReportPeriodEnd?.value || '').trim();
    if (!periodStart || !periodEnd) {
      throw new Error('Monthly Report start date and end date are required.');
    }
    if (periodStart > periodEnd) {
      throw new Error('Monthly Report start date cannot be later than end date.');
    }
    return {
      highlight_topics: highlightTopics,
      highlight_topic_sources: readMonthlyReportTopicSources(),
      period_start: periodStart,
      period_end: periodEnd,
    };
  };

  const cachedTeamFor = (teamKey, memberEmails, sourceCache = null) => {
    const cache = sourceCache || readTaskCache();
    const team = cache?.teams?.[teamKey];
    if (!team || team.email_signature !== emailSignature(memberEmails)) return null;
    return team;
  };

  const saveCachedTeam = (team) => {
    if (!team?.team_key || !team.loaded || team.error) return;
    const cache = readTaskCache();
    const teams = cache.teams && typeof cache.teams === 'object' ? cache.teams : {};
    teams[team.team_key] = {
      ...team,
      loading: false,
      loaded: true,
      error: '',
      progress_text: '',
      email_signature: emailSignature(team.member_emails || []),
      cached_at: new Date().toISOString(),
    };
    writeTaskCache({ version: 1, updated_at: new Date().toISOString(), teams });
  };

  const buildKeyProjectPatch = (isKeyProject, source, override = null) => ({
    is_key_project: Boolean(isKeyProject),
    key_project_source: source || (isKeyProject ? 'manual_on' : 'manual_off'),
    key_project_override: override && typeof override === 'object' ? override : {},
  });

  const updateCachedProjectKeyState = (bpmisId, isKeyProject, source, override = null) => {
    const normalizedBpmisId = String(bpmisId || '').trim();
    if (!normalizedBpmisId) return;
    const patch = buildKeyProjectPatch(isKeyProject, source, override);
    let changed = false;
    const cache = readTaskCache();
    const teams = cache.teams && typeof cache.teams === 'object' ? cache.teams : {};
    Object.values(teams).forEach((team) => {
      ['under_prd', 'pending_live'].forEach((sectionKey) => {
        (Array.isArray(team?.[sectionKey]) ? team[sectionKey] : []).forEach((cachedProject) => {
          if (String(cachedProject?.bpmis_id || '').trim() === normalizedBpmisId) {
            Object.assign(cachedProject, patch);
            changed = true;
          }
        });
      });
    });
    if (changed) writeTaskCache({ ...cache, teams, updated_at: new Date().toISOString() });
  };

  const setupTabs = () => {
    const triggers = [...root.querySelectorAll('[data-team-dashboard-tab]')];
    const panels = [...root.querySelectorAll('[data-team-dashboard-panel]')];
    const activate = (name) => {
      triggers.forEach((trigger) => {
        const active = trigger.dataset.teamDashboardTab === name;
        trigger.classList.toggle('is-active', active);
        trigger.setAttribute('aria-selected', active ? 'true' : 'false');
      });
      panels.forEach((panel) => {
        panel.hidden = panel.dataset.teamDashboardPanel !== name;
      });
      if (name === 'monthly-report') {
        loadMonthlyReportTemplate();
      }
      if (name === 'daily-brief') {
        loadDailyBriefs();
      }
      if (name === 'report-intelligence') {
        loadReportIntelligence();
      }
      if (name === 'seatalk-name-mapping') {
        loadSeaTalkNameMappings(false);
      }
      if (name === 'version-plan') {
        loadVersionPlan();
      }
    };
    triggers.forEach((trigger) => {
      trigger.addEventListener('click', () => activate(trigger.dataset.teamDashboardTab || 'tasks'));
    });
    const requestedTab = new URLSearchParams(window.location.search).get('tab');
    if (requestedTab && triggers.some((trigger) => trigger.dataset.teamDashboardTab === requestedTab)) {
      activate(requestedTab);
      return;
    }
    const activeTrigger = triggers.find((trigger) => trigger.classList.contains('is-active')) || triggers[0];
    if (activeTrigger) {
      activate(activeTrigger.dataset.teamDashboardTab || 'tasks');
    }
  };

  const renderLink = (url, label) => {
    if (!url) return escapeHtml(label || '-');
    return `<a href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${escapeHtml(label || url)}</a>`;
  };

  const versionPlanPriorityOptions = (selected) => {
    const priorities = Array.isArray(versionPlanState?.priority_order) && versionPlanState.priority_order.length
      ? versionPlanState.priority_order
      : ['SP', 'P0', 'P1', 'P2', 'P3'];
    return [
      '<option value="">-</option>',
      ...priorities.map((priority) => (
        `<option value="${escapeHtml(priority)}"${priority === selected ? ' selected' : ''}>${escapeHtml(priority)}</option>`
      )),
    ].join('');
  };

  const versionPlanPmOptions = (selectedValues = []) => {
    const selected = new Set((Array.isArray(selectedValues) ? selectedValues : []).map((item) => String(item || '').trim()));
    const options = Array.isArray(versionPlanState?.pm_options) && versionPlanState.pm_options.length
      ? versionPlanState.pm_options
      : ['Wang Chang', 'Zoey', 'Jireh', 'Ker Yin', 'Rene', 'Jun Wei', 'TBC'];
    return options.map((pm) => (
      `<option value="${escapeHtml(pm)}"${selected.has(pm) ? ' selected' : ''}>${escapeHtml(pm)}</option>`
    )).join('');
  };

  const setVersionPlanStatus = (message, tone = 'neutral') => {
    setStatus(versionPlanStatus, message, tone);
  };

  const versionPlanSyncText = (syncState = {}) => {
    const state = String(syncState.state || '').trim();
    if (state === 'running') return syncState.message || 'Syncing Jira information...';
    if (state === 'error') return syncState.error || syncState.message || 'Sync failed.';
    if (syncState.last_synced_date_sgt) return `Last synced: ${syncState.last_synced_date_sgt} SGT`;
    return 'Cached Version Plan loaded. Jira sync will start when needed.';
  };

  const versionPlanShortDate = (value) => {
    const text = String(value || '').trim();
    if (!text) return '-';
    const date = new Date(`${text.slice(0, 10)}T00:00:00+08:00`);
    if (Number.isNaN(date.getTime())) return text;
    return new Intl.DateTimeFormat('en-GB', {
      timeZone: 'Asia/Singapore',
      day: 'numeric',
      month: 'short',
    }).format(date);
  };

  const versionPlanDbpLine = (prefix, version) => {
    const fallbackMarket = { DBPSG: 'SG', DBPID: 'ID', DBPPH: 'PH' }[prefix] || prefix;
    const rawName = String(version?.version_name || '').trim();
    if (!rawName || rawName === '-') return `${fallbackMarket}_-`;
    return rawName.replace(/^DBP/, '');
  };

  const versionPlanSheetTitle = (title, mappedVersions = {}) => {
    const lines = ['DBPSG', 'DBPID', 'DBPPH'].map((prefix) => versionPlanDbpLine(prefix, mappedVersions[prefix]));
    return `
      <div class="team-dashboard-version-plan-sheet-title">
        <strong>${escapeHtml(title || '-')}</strong>
        ${lines.map((line) => `<span>└ ${escapeHtml(line)}</span>`).join('')}
      </div>
    `;
  };

  const versionPlanRowFeature = (row, readOnly) => {
    if (row.row_type === 'synced') {
      const jiraId = row.jira_id || '-';
      const market = row.market || '-';
      const summary = row.jira_summary || '-';
      return `
        <div class="team-dashboard-version-plan-feature">
          ${renderLink(row.jira_link, `[${jiraId}]`)}
          <span class="team-dashboard-version-plan-market">[${escapeHtml(market)}]</span>
          <span>${escapeHtml(summary)}</span>
        </div>
      `;
    }
    if (readOnly) return escapeHtml(row.feature || '-');
    return `
      <textarea
        rows="1"
        spellcheck="true"
        data-version-plan-cell="feature"
      >${escapeHtml(row.feature || '')}</textarea>
    `;
  };

  const versionPlanManualField = (row, field, readOnly) => {
    if (readOnly || row.row_type !== 'manual') {
      if (field === 'pm') return escapeHtml((Array.isArray(row.pm) ? row.pm : []).join(', ') || '-');
      return escapeHtml(row[field] || '-');
    }
    if (field === 'priority') {
      return `<select data-version-plan-cell="priority" aria-label="Priority">${versionPlanPriorityOptions(row.priority || '')}</select>`;
    }
    if (field === 'pm') {
      return `<select multiple size="3" data-version-plan-cell="pm" aria-label="PM">${versionPlanPmOptions(row.pm || [])}</select>`;
    }
    if (field === 'productization_efforts') {
      return `
        <select data-version-plan-cell="productization_efforts" aria-label="Productization Efforts">
          <option value=""${!row.productization_efforts ? ' selected' : ''}>-</option>
          <option value="Y"${row.productization_efforts === 'Y' ? ' selected' : ''}>Y</option>
          <option value="N"${row.productization_efforts === 'N' ? ' selected' : ''}>N</option>
        </select>
      `;
    }
    return `<textarea rows="1" spellcheck="true" data-version-plan-cell="${escapeHtml(field)}">${escapeHtml(row[field] || '')}</textarea>`;
  };

  const renderVersionPlanRows = (rows, { scope, versionId = '', readOnly = false, title = 'Rows', headerFeature = 'Feature' } = {}) => {
    const rowItems = Array.isArray(rows) ? rows : [];
    const empty = `<div class="team-dashboard-version-plan-empty">No ${escapeHtml(title.toLowerCase())} yet.</div>`;
    const body = rowItems.map((row) => {
      const manual = row.row_type === 'manual' && !readOnly;
      return `
        <div
          class="team-dashboard-version-plan-row${manual ? ' is-manual' : ' is-readonly'}"
          data-version-plan-row-id="${escapeHtml(row.row_id || '')}"
          data-version-plan-scope="${escapeHtml(scope)}"
          data-version-id="${escapeHtml(versionId)}"
          data-version-plan-priority="${escapeHtml(row.priority || '')}"
          ${manual ? 'data-version-plan-manual-row="true" draggable="true"' : ''}
        >
          <div class="team-dashboard-version-plan-cell team-dashboard-version-plan-cell-feature" data-label="Feature">
            ${manual ? '<div class="team-dashboard-version-plan-row-tools"><button class="button button-secondary team-dashboard-version-plan-drag" type="button" aria-label="Drag row">Drag</button><button class="button button-secondary" type="button" data-version-plan-row-action="up">Up</button><button class="button button-secondary" type="button" data-version-plan-row-action="down">Down</button><button class="button button-secondary" type="button" data-version-plan-row-action="delete">Delete</button></div>' : ''}
            ${versionPlanRowFeature(row, readOnly)}
          </div>
          <div class="team-dashboard-version-plan-cell" data-label="Priority">
            ${versionPlanManualField(row, 'priority', readOnly)}
          </div>
          <div class="team-dashboard-version-plan-cell" data-label="PM">
            ${versionPlanManualField(row, 'pm', readOnly)}
          </div>
          <div class="team-dashboard-version-plan-cell" data-label="Remarks">
            ${versionPlanManualField(row, 'remarks', readOnly)}
          </div>
          <div class="team-dashboard-version-plan-cell" data-label="Productization Efforts?">
            ${versionPlanManualField(row, 'productization_efforts', readOnly)}
          </div>
        </div>
      `;
    }).join('');
    return `
      <div class="team-dashboard-version-plan-sheet" data-version-plan-sheet="${escapeHtml(scope)}" data-version-id="${escapeHtml(versionId)}">
        <div class="team-dashboard-version-plan-row team-dashboard-version-plan-header" aria-hidden="true">
          <div>${headerFeature}</div>
          <div>Priority</div>
          <div>PM</div>
          <div>Remarks</div>
          <div>Productization Efforts? (Y/N)</div>
        </div>
        ${body || empty}
      </div>
    `;
  };

  const renderVersionPlanBundle = (bundle) => {
    const mapped = bundle.mapped_versions && typeof bundle.mapped_versions === 'object' ? bundle.mapped_versions : {};
    const manualRows = Array.isArray(bundle.manual_rows) ? bundle.manual_rows : [];
    const syncedRows = Array.isArray(bundle.synced_rows) ? bundle.synced_rows : [];
    const headerFeature = versionPlanSheetTitle(
      `${bundle.af_version_name || 'AF Version'} (PRD Final: ${versionPlanShortDate(bundle.prd_final_date)})`,
      mapped,
    );
    return `
      <section class="team-dashboard-version-plan-bundle">
        ${renderVersionPlanRows([...syncedRows, ...manualRows], { scope: 'bundle', versionId: bundle.version_id, readOnly: false, title: 'rows', headerFeature })}
        <div class="team-dashboard-version-plan-actions">
          <button class="button button-secondary" type="button" data-version-plan-row-action="add" data-version-plan-scope="bundle" data-version-id="${escapeHtml(bundle.version_id || '')}">Add Row</button>
        </div>
      </section>
    `;
  };

  const renderVersionPlanArchivedBundle = (bundle) => {
    const mapped = bundle.mapped_versions && typeof bundle.mapped_versions === 'object' ? bundle.mapped_versions : {};
    const headerFeature = versionPlanSheetTitle(
      `${bundle.af_version_name || 'AF Version'} (PRD Final: ${versionPlanShortDate(bundle.prd_final_date || bundle.af_release_date)})`,
      mapped,
    );
    return `
      <section class="team-dashboard-version-plan-bundle is-archived">
        ${renderVersionPlanRows(bundle.synced_rows || [], { scope: 'archived', versionId: bundle.version_id, readOnly: true, title: 'archived Jira rows', headerFeature })}
      </section>
    `;
  };

  const renderVersionPlan = (payload) => {
    if (!versionPlanContent) return;
    versionPlanState = payload || {};
    const bundles = Array.isArray(payload?.bundles) ? payload.bundles : [];
    const pipelineRows = Array.isArray(payload?.pipeline_rows) ? payload.pipeline_rows : [];
    const archived = Array.isArray(payload?.archived_bundles) ? payload.archived_bundles : [];
    const activeHtml = bundles.length
      ? bundles.map(renderVersionPlanBundle).join('')
      : '<section class="team-dashboard-version-plan-bundle"><p class="productization-inline-status">No active or upcoming AF version bundles in range.</p></section>';
    versionPlanContent.innerHTML = `
      <div class="team-dashboard-version-plan-sections">
        ${activeHtml}
        <section class="team-dashboard-version-plan-bundle team-dashboard-version-plan-pipeline">
          <div class="team-dashboard-version-plan-bundle-head">
            <div>
              <h4>Pipeline Section</h4>
            </div>
            <span class="field-badge">${escapeHtml(pipelineRows.length)} rows</span>
          </div>
          ${renderVersionPlanRows(pipelineRows, { scope: 'pipeline', readOnly: false, title: 'pipeline rows', headerFeature: 'Pipeline Section' })}
          <div class="team-dashboard-version-plan-actions">
            <button class="button button-secondary" type="button" data-version-plan-row-action="add" data-version-plan-scope="pipeline">Add Row</button>
          </div>
        </section>
        <section class="team-dashboard-version-plan-archive">
          <div class="team-dashboard-version-plan-archive-head">
            <h4>Archived Version</h4>
            <span>${escapeHtml(archived.length)} versions</span>
          </div>
          ${archived.length ? archived.map(renderVersionPlanArchivedBundle).join('') : '<p class="productization-inline-status">No archived versions recorded after Version Plan launch yet.</p>'}
        </section>
      </div>
    `;
    setVersionPlanStatus(versionPlanSyncText(payload?.sync_state || {}), payload?.sync_state?.state === 'error' ? 'error' : 'neutral');
  };

  const loadVersionPlan = async ({ force = false } = {}) => {
    if (!versionPlanContent || (versionPlanLoaded && !force)) return;
    versionPlanLoaded = true;
    setVersionPlanStatus('Loading cached Version Plan...', 'neutral');
    try {
      const response = await fetch(root.dataset.versionPlanUrl || '/api/team-dashboard/version-plan/af', {
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      const payload = await readJson(response, 'Could not load Version Plan.');
      renderVersionPlan(payload);
      if (payload?.sync_state?.state === 'running' || payload?.sync_queued) {
        startVersionPlanPolling();
      }
    } catch (error) {
      versionPlanLoaded = false;
      setVersionPlanStatus(error.message || 'Could not load Version Plan.', 'error');
      versionPlanContent.innerHTML = '<p class="productization-inline-status" data-tone="error">Version Plan failed to load.</p>';
    }
  };

  const startVersionPlanPolling = () => {
    if (versionPlanPollTimer) return;
    versionPlanPollTimer = window.setInterval(async () => {
      try {
        const response = await fetch(root.dataset.versionPlanSyncStatusUrl || '/api/team-dashboard/version-plan/af/sync-status', {
          headers: { Accept: 'application/json' },
          credentials: 'same-origin',
        });
        const payload = await readJson(response, 'Could not load Version Plan sync status.');
        const syncState = payload.sync_state || {};
        setVersionPlanStatus(versionPlanSyncText(syncState), syncState.state === 'error' ? 'error' : 'neutral');
        if (syncState.state !== 'running') {
          window.clearInterval(versionPlanPollTimer);
          versionPlanPollTimer = null;
          versionPlanLoaded = false;
          loadVersionPlan({ force: true });
        }
      } catch (error) {
        window.clearInterval(versionPlanPollTimer);
        versionPlanPollTimer = null;
        setVersionPlanStatus(error.message || 'Could not load Version Plan sync status.', 'error');
      }
    }, 1800);
  };

  const syncVersionPlan = async () => {
    if (!versionPlanSyncButton) return;
    versionPlanSyncButton.disabled = true;
    setVersionPlanStatus('Starting Jira sync...', 'neutral');
    try {
      const response = await fetch(root.dataset.versionPlanSyncUrl || '/api/team-dashboard/version-plan/af/sync', {
        method: 'POST',
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      const payload = await readJson(response, 'Could not start Version Plan sync.');
      renderVersionPlan(payload);
      startVersionPlanPolling();
    } catch (error) {
      setVersionPlanStatus(error.message || 'Could not start Version Plan sync.', 'error');
    } finally {
      versionPlanSyncButton.disabled = false;
    }
  };

  const saveVersionPlanCell = async (input) => {
    const row = input.closest('[data-version-plan-row-id]');
    if (!row) return;
    const field = input.dataset.versionPlanCell || '';
    const value = input.multiple ? Array.from(input.selectedOptions).map((option) => option.value) : input.value;
    try {
      const response = await fetch(root.dataset.versionPlanCellUrl || '/api/team-dashboard/version-plan/af/cell', {
        method: 'POST',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({
          scope: row.dataset.versionPlanScope || '',
          version_id: row.dataset.versionId || '',
          row_id: row.dataset.versionPlanRowId || '',
          field,
          value,
        }),
      });
      const payload = await readJson(response, 'Could not save Version Plan cell.');
      renderVersionPlan(payload);
      setVersionPlanStatus('Saved.', 'success');
    } catch (error) {
      setVersionPlanStatus(error.message || 'Could not save Version Plan cell.', 'error');
    }
  };

  const updateVersionPlanRows = async (payload, fallbackMessage = 'Could not update Version Plan rows.') => {
    const response = await fetch(root.dataset.versionPlanRowsUrl || '/api/team-dashboard/version-plan/af/rows', {
      method: 'POST',
      headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
      credentials: 'same-origin',
      body: JSON.stringify(payload),
    });
    const result = await readJson(response, fallbackMessage);
    renderVersionPlan(result);
    setVersionPlanStatus('Saved.', 'success');
  };

  const reorderVersionPlanRows = async (dropRow) => {
    if (!versionPlanDragRow || !dropRow || versionPlanDragRow === dropRow) return;
    if (versionPlanDragRow.dataset.versionPlanScope !== dropRow.dataset.versionPlanScope) return;
    if ((versionPlanDragRow.dataset.versionId || '') !== (dropRow.dataset.versionId || '')) return;
    if ((versionPlanDragRow.dataset.versionPlanPriority || '') !== (dropRow.dataset.versionPlanPriority || '')) return;
    const sheet = dropRow.closest('[data-version-plan-sheet]');
    if (!sheet) return;
    const before = versionPlanDragRow.compareDocumentPosition(dropRow) & Node.DOCUMENT_POSITION_FOLLOWING;
    sheet.insertBefore(versionPlanDragRow, before ? dropRow.nextSibling : dropRow);
    const rowIds = Array.from(sheet.querySelectorAll('[data-version-plan-manual-row="true"]'))
      .filter((row) => (row.dataset.versionPlanPriority || '') === (dropRow.dataset.versionPlanPriority || ''))
      .map((row) => row.dataset.versionPlanRowId || '')
      .filter(Boolean);
    await updateVersionPlanRows({
      action: 'reorder',
      scope: dropRow.dataset.versionPlanScope || '',
      version_id: dropRow.dataset.versionId || '',
      row_ids: rowIds,
    }, 'Could not reorder Version Plan rows.');
  };

  const renderMarkdown = (value) => {
    const html = [];
    let inList = false;
    let table = null;
    const closeList = () => {
      if (inList) {
        html.push('</ul>');
        inList = false;
      }
    };
    const inline = (text) => escapeHtml(text)
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/`(.+?)`/g, '<code>$1</code>');
    const splitTableRow = (line) => {
      let text = String(line || '').trim();
      if (text.startsWith('|')) text = text.slice(1);
      if (text.endsWith('|')) text = text.slice(0, -1);
      return text.split('|').map((cell) => cell.trim());
    };
    const isTableSeparator = (line) => {
      const cells = splitTableRow(line);
      return cells.length > 1 && cells.every((cell) => /^:?-{3,}:?$/.test(cell.replace(/\s+/g, '')));
    };
    const tableColumnWidths = (headers, columnCount) => {
      const normalizedHeaders = headers.map((header) => String(header || '').trim().toLowerCase().replace(/\s+/g, ' '));
      const projectHeaders = ['region', 'priority', 'project', 'current status', 'target tech live date'];
      if (projectHeaders.every((header, index) => normalizedHeaders[index] === header)) {
        const widths = ['12%', '11%', '39%', '16%', '22%'];
        const extraCount = Math.max(0, columnCount - widths.length);
        return widths.concat(Array.from({ length: extraCount }, () => `${100 / Math.max(1, extraCount)}%`));
      }
      return Array.from({ length: columnCount }, () => `${100 / Math.max(1, columnCount)}%`);
    };
    const renderTable = () => {
      if (!table) return;
      const columnCount = Math.max(table.headers.length, ...table.rows.map((row) => row.length), 1);
      const columnWidths = tableColumnWidths(table.headers, columnCount);
      const renderCells = (cells, tag) => Array.from({ length: columnCount }, (_, index) => (
        `<${tag} style="width:${columnWidths[index]};">${inline(cells[index] || '')}</${tag}>`
      )).join('');
      const colgroup = columnWidths.map((width) => `<col style="width:${width};">`).join('');
      html.push(
        '<div class="team-dashboard-markdown-table-wrap"><table class="team-dashboard-markdown-table">'
        + `<colgroup>${colgroup}</colgroup>`
        + `<thead><tr>${renderCells(table.headers, 'th')}</tr></thead>`
        + `<tbody>${table.rows.map((row) => `<tr>${renderCells(row, 'td')}</tr>`).join('')}</tbody>`
        + '</table></div>',
      );
      table = null;
    };
    const closeBlocks = () => {
      closeList();
      renderTable();
    };
    const lines = String(value || '').split(/\r?\n/);
    lines.forEach((line, index) => {
      const trimmed = line.trim();
      if (!trimmed) {
        closeBlocks();
        return;
      }
      const nextLine = lines[index + 1]?.trim() || '';
      if (!table && trimmed.includes('|') && isTableSeparator(nextLine)) {
        closeList();
        table = { headers: splitTableRow(trimmed), rows: [] };
        return;
      }
      if (table) {
        if (isTableSeparator(trimmed)) return;
        if (trimmed.includes('|') && !isTableSeparator(trimmed)) {
          table.rows.push(splitTableRow(trimmed));
          return;
        }
        renderTable();
      }
      const heading = trimmed.match(/^(#{2,4})\s+(.+)$/);
      if (heading) {
        closeList();
        html.push(`<h4>${inline(heading[2])}</h4>`);
        return;
      }
      const listItem = trimmed.match(/^(\d+[.)]|[-*])\s+(.+)$/);
      if (listItem) {
        if (!inList) {
          html.push('<ul>');
          inList = true;
        }
        html.push(`<li>${inline(listItem[2])}</li>`);
        return;
      }
      closeList();
      html.push(`<p>${inline(trimmed)}</p>`);
    });
    closeBlocks();
    return html.join('');
  };

  const renderPrdLinks = (links, jiraItem) => {
    const items = Array.isArray(links) ? links : [];
    if (!items.length) return '-';
    return items.map((item, index) => {
      const url = String(item.url || '').trim();
      const label = items.length > 1 ? `Link ${index + 1}` : 'Link';
      const title = String(item.label || url || `PRD ${index + 1}`).trim();
      return `
        <span class="team-dashboard-prd-link">
          ${url
            ? `<a href="${escapeHtml(url)}" target="_blank" rel="noreferrer" title="${escapeHtml(title)}">${escapeHtml(label)}</a>`
            : escapeHtml(label)}
        </span>
      `;
    }).join('');
  };

  const renderPrdActions = (links, jiraItem) => {
    const items = Array.isArray(links) ? links : [];
    if (!items.length) return '-';
    return items.map((item, index) => {
      const url = String(item.url || '').trim();
      return `
        <div class="team-dashboard-prd-actions">
          <button
            class="button button-secondary team-dashboard-review-button"
            type="button"
            data-prd-action="summary"
            data-jira-id="${escapeHtml(jiraItem?.jira_id || '')}"
            data-jira-link="${escapeHtml(jiraItem?.jira_link || '')}"
            data-prd-url="${escapeHtml(url)}"
            data-prd-index="${index}"
            ${url ? '' : 'disabled'}
          >Summary</button>
          <button
            class="button button-secondary team-dashboard-review-button"
            type="button"
            data-prd-action="review"
            data-jira-id="${escapeHtml(jiraItem?.jira_id || '')}"
            data-jira-link="${escapeHtml(jiraItem?.jira_link || '')}"
            data-prd-url="${escapeHtml(url)}"
            data-prd-index="${index}"
            ${url ? '' : 'disabled'}
          >Review</button>
        </div>
      `;
    }).join('');
  };

  const itemCount = (projects) => (Array.isArray(projects) ? projects : [])
    .reduce((countValue, project) => countValue + (project.jira_tickets || []).length, 0);

  const parseProjectDateSort = (project) => {
    const releaseSort = String(project?.release_date_sort || '').trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(releaseSort)) return releaseSort;
    const text = String(project?.release_date || '').trim();
    let match = text.match(/^(\d{4})[-/](\d{2})[-/](\d{2})/);
    if (match) return `${match[1]}-${match[2]}-${match[3]}`;
    match = text.match(/^(\d{2})[-/](\d{2})[-/](\d{4})/);
    if (match) return `${match[3]}-${match[2]}-${match[1]}`;
    return '';
  };

  const formatReleaseDate = (value) => {
    const text = String(value || '').trim();
    if (!text || text === '-') return text || '-';
    let match = text.match(/^(\d{4})[-/](\d{2})[-/](\d{2})/);
    if (match) return `${match[1]}-${match[2]}-${match[3]}`;
    match = text.match(/^(\d{2})[-/](\d{2})[-/](\d{4})/);
    if (match) return `${match[3]}-${match[2]}-${match[1]}`;
    return text;
  };

  const sortUnderPrdProjects = (projects) => [...(Array.isArray(projects) ? projects : [])].sort((left, right) => {
    const leftDate = parseProjectDateSort(left);
    const rightDate = parseProjectDateSort(right);
    const leftJiraCount = (left?.jira_tickets || []).length;
    const rightJiraCount = (right?.jira_tickets || []).length;
    const bucket = (date, count) => (date ? 0 : count > 0 ? 1 : 2);
    const leftKey = [
      bucket(leftDate, leftJiraCount),
      leftDate,
      String(left?.project_name || '').toLowerCase(),
      String(left?.bpmis_id || '').toLowerCase(),
    ];
    const rightKey = [
      bucket(rightDate, rightJiraCount),
      rightDate,
      String(right?.project_name || '').toLowerCase(),
      String(right?.bpmis_id || '').toLowerCase(),
    ];
    for (let index = 0; index < leftKey.length; index += 1) {
      if (leftKey[index] < rightKey[index]) return -1;
      if (leftKey[index] > rightKey[index]) return 1;
    }
    return 0;
  });

  const projectMatchesPm = (project, selectedPm) => {
    if (!selectedPm) return true;
    const normalizedPm = String(selectedPm || '').trim().toLowerCase();
    const matchedProjectPms = normalizeEmailList(project.matched_pm_emails || []);
    if (matchedProjectPms.includes(normalizedPm)) return true;
    return (project.jira_tickets || []).some((ticket) => String(ticket.pm_email || '').trim().toLowerCase() === normalizedPm);
  };

  const filterProjectsByPm = (projects, selectedPm) => {
    const normalizedPm = String(selectedPm || '').trim().toLowerCase();
    if (!normalizedPm) return Array.isArray(projects) ? projects : [];
    return (Array.isArray(projects) ? projects : [])
      .filter((project) => projectMatchesPm(project, normalizedPm))
      .map((project) => ({
        ...project,
        jira_tickets: (project.jira_tickets || []).filter(
          (ticket) => String(ticket.pm_email || '').trim().toLowerCase() === normalizedPm,
        ),
      }));
  };

  const filterProjectsByKeyProject = (projects) => {
    const items = Array.isArray(projects) ? projects : [];
    if (!keyProjectOnly) return items;
    return items.filter((project) => Boolean(project.is_key_project));
  };

  const updateProjectKeyState = (bpmisId, isKeyProject, source, override = null) => {
    const normalizedBpmisId = String(bpmisId || '').trim();
    if (!normalizedBpmisId) return null;
    const patch = buildKeyProjectPatch(isKeyProject, source, override);
    let updatedProject = null;
    taskTeams = taskTeams.map((team) => {
      const updateSection = (projects) => (Array.isArray(projects) ? projects : []).map((project) => {
        if (String(project?.bpmis_id || '').trim() !== normalizedBpmisId) return project;
        updatedProject = {
          ...project,
          ...patch,
        };
        return updatedProject;
      });
      return {
        ...team,
        under_prd: updateSection(team.under_prd),
        pending_live: updateSection(team.pending_live),
      };
    });
    if (updatedProject) updateCachedProjectKeyState(normalizedBpmisId, isKeyProject, source, override);
    return updatedProject;
  };

  const pmFilterOptions = (team) => {
    const emails = new Set(normalizeEmailList(team.member_emails || []));
    [...(team.under_prd || []), ...(team.pending_live || [])].forEach((project) => {
      normalizeEmailList(project.matched_pm_emails || []).forEach((email) => emails.add(email));
      (project.jira_tickets || []).forEach((ticket) => {
        const email = String(ticket.pm_email || '').trim().toLowerCase();
        if (email) emails.add(email);
      });
    });
    return [...emails].sort();
  };

  const renderJiraRows = (items) => {
    if (!items.length) {
      return '<tr><td colspan="8" class="team-dashboard-empty-cell">No matching Jira tasks.</td></tr>';
    }
    return items.map((item, index) => {
      const reviewPanelId = `prd-review-${String(item.jira_id || index).replace(/[^a-zA-Z0-9_-]/g, '-')}-${index}`;
      return `
      <tr>
        <td>${renderLink(item.jira_link || '', item.jira_id || '-')}</td>
        <td>${escapeHtml(item.jira_title || '-')}</td>
        <td>${escapeHtml(item.pm_email || '-')}</td>
        <td>${escapeHtml(item.jira_status || '-')}</td>
        <td>${escapeHtml(formatReleaseDate(item.release_date))}</td>
        <td>${escapeHtml(item.version || '-')}</td>
        <td>${renderPrdLinks(item.prd_links, item)}</td>
        <td>
          ${renderPrdActions(item.prd_links, item)}
          <button class="button button-secondary team-dashboard-review-toggle" type="button" data-prd-review-toggle="${escapeHtml(reviewPanelId)}" hidden>View Review</button>
        </td>
      </tr>
      <tr class="team-dashboard-review-row" data-prd-review-row="${escapeHtml(reviewPanelId)}" hidden>
        <td colspan="8">
          <div class="team-dashboard-review-panel" data-prd-review-panel="${escapeHtml(reviewPanelId)}"></div>
        </td>
      </tr>
    `;
    }).join('');
  };

  const renderPagination = (pageKey, page, totalPages, totalItems) => {
    if (totalPages <= 1) {
      return '';
    }
    return `
      <div class="team-dashboard-pagination">
        <span>Page ${page} / ${totalPages} · ${totalItems} Jira tasks</span>
        <div class="team-dashboard-pagination-actions">
          <button class="button button-secondary" type="button" data-team-dashboard-page="${escapeHtml(pageKey)}" data-page-delta="-1" ${page <= 1 ? 'disabled' : ''}>Prev</button>
          <button class="button button-secondary" type="button" data-team-dashboard-page="${escapeHtml(pageKey)}" data-page-delta="1" ${page >= totalPages ? 'disabled' : ''}>Next</button>
        </div>
      </div>
    `;
  };

  const renderProject = (project, sectionKey, index) => {
    const tickets = Array.isArray(project.jira_tickets) ? project.jira_tickets : [];
    const panelId = `team-dashboard-${sectionKey}-${index}`;
    const pageKey = `${sectionKey}-${project.bpmis_id || index}`;
    const totalPages = Math.max(1, Math.ceil(tickets.length / jiraPageSize));
    const page = Math.min(Math.max(Number(jiraPageState[pageKey] || 1), 1), totalPages);
    jiraPageState[pageKey] = page;
    const firstIndex = (page - 1) * jiraPageSize;
    const visibleTickets = tickets.slice(firstIndex, firstIndex + jiraPageSize);
    const bpmisId = project.bpmis_id || '-';
    const expanded = Boolean(expandedPanels[panelId]);
    const isKeyProject = Boolean(project.is_key_project);
    const starLabel = isKeyProject ? 'Remove Key Project' : 'Mark as Key Project';
    const sourceLabel = {
      manual_on: 'Manual Key Project',
      manual_off: 'Manually excluded',
      priority_default: 'Default from SP/P0 priority',
      none: 'Not Key Project',
    }[project.key_project_source] || 'Not Key Project';
    const actualMandays = formatMandays(project.actual_mandays);
    return `
      <article class="bpmis-project-card team-dashboard-project-card">
        <div class="bpmis-project-card-main">
          <button class="bpmis-task-toggle" type="button" data-team-dashboard-toggle="${escapeHtml(panelId)}" aria-expanded="${expanded ? 'true' : 'false'}" aria-label="Expand Jira tasks for BPMIS ${escapeHtml(bpmisId)}">${expanded ? '-' : '+'}</button>
          <div class="bpmis-project-card-id">
            <span>BPMIS ID</span>
            <strong>${escapeHtml(bpmisId)}</strong>
          </div>
          <div class="bpmis-project-card-market">
            <span>Live Date</span>
            <strong>${escapeHtml(formatReleaseDate(project.release_date))}</strong>
          </div>
          <div class="bpmis-project-card-name">
            <span>Project Name</span>
            <strong>${escapeHtml(project.project_name || '-')}</strong>
          </div>
          <div class="bpmis-project-card-market">
            <span>Market</span>
            <strong>${escapeHtml(project.market || '-')}</strong>
          </div>
          <div class="bpmis-project-card-market">
            <span>Priority</span>
            <strong>${escapeHtml(project.priority || '-')}</strong>
          </div>
          <div class="bpmis-project-card-name">
            <span>Regional PM PIC</span>
            <strong>${escapeHtml(project.regional_pm_pic || '-')}</strong>
          </div>
          <div class="team-dashboard-project-mandays">
            <span>Actual Mandays</span>
            <strong>${escapeHtml(actualMandays)}</strong>
          </div>
          <div class="team-dashboard-project-count">
            <span>Jira</span>
            <strong>${tickets.length}</strong>
          </div>
          <div class="team-dashboard-key-project">
            <span>Key</span>
            <button
              class="team-dashboard-key-star${isKeyProject ? ' is-key' : ''}"
              type="button"
              data-team-dashboard-key-project="${escapeHtml(bpmisId)}"
              data-key-project-next="${isKeyProject ? 'false' : 'true'}"
              data-key-project-priority="${escapeHtml(project.priority || '')}"
              aria-label="${escapeHtml(starLabel)}"
              title="${escapeHtml(`${starLabel} - ${sourceLabel}`)}"
              ${canManageKeyProjects && bpmisId !== '-' ? '' : 'disabled'}
            ><span class="team-dashboard-key-star-icon" aria-hidden="true">${isKeyProject ? '★' : '☆'}</span></button>
          </div>
        </div>
        <div class="bpmis-task-panel" data-team-dashboard-panel-id="${escapeHtml(panelId)}" ${expanded ? '' : 'hidden'}>
          ${renderPagination(pageKey, page, totalPages, tickets.length)}
          <div class="table-wrap premium-table-wrap">
            <table class="productization-table team-dashboard-table">
              <thead>
                <tr>
                  <th>Jira ID</th>
                  <th>Jira Title</th>
                  <th>Reporter Email</th>
                  <th>Jira Status</th>
                  <th>Release</th>
                  <th>Version</th>
                  <th>PRD Link</th>
                  <th>AI</th>
                </tr>
              </thead>
              <tbody>${renderJiraRows(visibleTickets)}</tbody>
            </table>
          </div>
          ${renderPagination(pageKey, page, totalPages, tickets.length)}
        </div>
      </article>
    `;
  };

  const formatMandays = (value) => {
    if (value === null || value === undefined || value === '') return '-';
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return String(value || '-');
    if (Number.isInteger(numeric)) return String(numeric);
    return String(Math.round(numeric * 10) / 10);
  };

  const renderSection = (title, projects, sectionKey) => {
    const projectItems = Array.isArray(projects) ? projects : [];
    const countValue = itemCount(projectItems);
    return `
      <section class="team-dashboard-task-section">
        <div class="team-dashboard-section-head">
          <h4>${escapeHtml(title)}</h4>
          <span>${projectItems.length} Biz Projects / ${countValue} Jira tasks</span>
        </div>
        <div class="team-dashboard-project-list">
          ${projectItems.length
            ? projectItems.map((project, index) => renderProject(project, sectionKey, index)).join('')
            : '<div class="empty-state"><p>No matching Biz Projects.</p></div>'}
        </div>
      </section>
    `;
  };

  const renderTeamTabs = (teams) => `
    <div class="workspace-tabs team-dashboard-track-tabs" role="tablist" aria-label="Task List tracks">
      ${teams.map((team) => {
        const teamKey = team.team_key || '';
        const active = teamKey === activeTaskTeamKey;
        return `
          <button
            class="workspace-tab${active ? ' is-active' : ''}"
            type="button"
            role="tab"
            aria-selected="${active ? 'true' : 'false'}"
            data-team-dashboard-track="${escapeHtml(teamKey)}"
          >${escapeHtml(team.label || teamLabels[teamKey] || teamKey)}</button>
        `;
      }).join('')}
    </div>
  `;

  const renderPmFilter = (team, selectedPm) => {
    const options = pmFilterOptions(team);
    return `
      <label class="team-dashboard-pm-filter">
        <span>PM email</span>
        <select data-team-dashboard-pm-filter="${escapeHtml(team.team_key || '')}">
          <option value="">All PMs</option>
          ${options.map((email) => `<option value="${escapeHtml(email)}" ${email === selectedPm ? 'selected' : ''}>${escapeHtml(email)}</option>`).join('')}
        </select>
      </label>
    `;
  };

  const renderTeamLoadMeta = (team) => {
    if (!team.loaded || team.loading || team.error) return '';
    const parts = [];
    const elapsed = Number(team.elapsed_seconds || team.timing_stats?.total || 0);
    if (elapsed > 0) parts.push(`Loaded in ${formatDuration(elapsed)}`);
    const apiCalls = Number(team.fetch_stats?.api_call_count || 0);
    if (apiCalls > 0) parts.push(`${apiCalls} upstream calls`);
    const rowsScanned = Number(team.fetch_stats?.issue_rows_scanned || 0);
    const issuePages = Number(team.fetch_stats?.issue_list_page_count || 0);
    const treeRows = Number(team.fetch_stats?.issue_tree_rows_scanned || 0);
    const treePages = Number(team.fetch_stats?.issue_tree_page_count || 0);
    const releaseVersions = Number(team.fetch_stats?.release_version_count || 0);
    const parentBulk = Number(team.fetch_stats?.issue_detail_bulk_lookup_count || 0);
    const singleFallback = Number(team.fetch_stats?.issue_detail_single_fallback_count || 0);
    const jiraBulk = Number(team.fetch_stats?.jira_live_bulk_lookup_count || 0);
    const fallbackCandidates = Number(team.fetch_stats?.team_dashboard_zero_jira_fallback_candidate_count || 0);
    const treeFallback = Number(team.fetch_stats?.issue_tree_fallback_count || 0);
    const releaseFilterUsed = Number(team.fetch_stats?.bpmis_release_query_filter_used_count || 0);
    const bottlenecks = [];
    if (treePages > 0) bottlenecks.push(`BPMIS tree pages ${treePages}`);
    if (treeRows > 0) bottlenecks.push(`tree rows ${treeRows}`);
    if (releaseVersions > 0) bottlenecks.push(`versions ${releaseVersions}`);
    if (issuePages > 0) bottlenecks.push(`BPMIS pages ${issuePages}`);
    if (rowsScanned > 0) bottlenecks.push(`rows ${rowsScanned}`);
    if (parentBulk > 0) bottlenecks.push(`parent bulk ${parentBulk}`);
    if (singleFallback > 0) bottlenecks.push(`single fallback ${singleFallback}`);
    if (jiraBulk > 0) bottlenecks.push(`Jira bulk ${jiraBulk}`);
    if (fallbackCandidates > 0) bottlenecks.push(`fallback ${fallbackCandidates}`);
    if (treeFallback > 0) bottlenecks.push(`tree fallback ${treeFallback}`);
    if (releaseFilterUsed > 0) bottlenecks.push('release filter on');
    if (bottlenecks.length) parts.push(bottlenecks.slice(0, 6).join(' / '));
    if (!parts.length && team.cached_at) parts.push(`Restored ${formatSingaporeTimestamp(team.cached_at)}`);
    return parts.length ? `<p class="productization-inline-status" data-tone="neutral">${escapeHtml(parts.join(' · '))}</p>` : '';
  };

  const renderTeam = (team) => {
    const underPrd = Array.isArray(team.under_prd) ? team.under_prd : [];
    const pendingLive = Array.isArray(team.pending_live) ? team.pending_live : [];
    const error = team.error ? `<p class="productization-inline-status" data-tone="error">${escapeHtml(team.error)}</p>` : '';
    const loading = team.loading ? `<p class="productization-inline-status" data-tone="neutral">${escapeHtml(team.progress_text || 'Loading team Jira tasks...')}</p>` : '';
    const notLoaded = !team.loaded && !team.loading && !team.error;
    const teamKey = team.team_key || 'team';
    const selectedPm = String(pmFilterState[teamKey] || '').trim().toLowerCase();
    const filteredUnderPrd = sortUnderPrdProjects(filterProjectsByKeyProject(filterProjectsByPm(underPrd, selectedPm)));
    const filteredPendingLive = filterProjectsByKeyProject(filterProjectsByPm(pendingLive, selectedPm));
    const anyTeamLoading = taskTeams.some((item) => item.loading);
    const anyTeamLoadedOrErrored = taskTeams.some((item) => item.loaded || item.error);
    const actionLabel = anyTeamLoadedOrErrored ? 'Reload Jira' : 'Load Jira';
    return `
      <section class="team-dashboard-team${team.loading ? ' is-loading' : ''}" data-team-dashboard-track-panel="${escapeHtml(teamKey)}">
        <div class="team-dashboard-team-head">
          <div>
            <h3>${escapeHtml(team.label || team.team_key || 'Team')}</h3>
            <span>${escapeHtml((team.member_emails || []).join(', ') || 'No configured members')}</span>
          </div>
          <div class="team-dashboard-team-actions">
            <label class="team-dashboard-key-filter">
              <input type="checkbox" data-team-dashboard-key-filter ${keyProjectOnly ? 'checked' : ''}>
              <span>Key Project</span>
            </label>
            ${renderPmFilter(team, selectedPm)}
            <button
              class="button button-secondary"
              type="button"
              data-team-dashboard-load-team="${escapeHtml(teamKey)}"
              ${anyTeamLoading ? 'disabled' : ''}
            >${escapeHtml(actionLabel)}</button>
          </div>
        </div>
        ${error}
        ${loading}
        ${renderTeamLoadMeta(team)}
        ${notLoaded ? '<p class="productization-inline-status" data-tone="neutral">Not loaded. Click Load Jira to fetch all teams.</p>' : ''}
        ${team.loading || notLoaded ? '' : renderSection('Under PRD', filteredUnderPrd, `${teamKey}-under-prd-${selectedPm || 'all'}`)}
        ${team.loading || notLoaded ? '' : renderSection('Pending Live', filteredPendingLive, `${teamKey}-pending-live-${selectedPm || 'all'}`)}
      </section>
    `;
  };

  const renderTeams = (teams) => {
    if (!taskList) return;
    if (!teams.length) {
      taskList.innerHTML = '<div class="empty-state"><p>No configured teams.</p></div>';
      return;
    }
    if (!teams.some((team) => team.team_key === activeTaskTeamKey)) {
      activeTaskTeamKey = teams[0].team_key || '';
    }
    const orderIndex = (teamKey) => {
      const index = teamOrder.indexOf(teamKey);
      return index >= 0 ? index : teamOrder.length;
    };
    const orderedTeams = [...teams].sort((left, right) => orderIndex(left.team_key) - orderIndex(right.team_key));
    const activeTeam = orderedTeams.find((team) => team.team_key === activeTaskTeamKey) || orderedTeams[0];
    taskList.innerHTML = `
      ${renderTeamTabs(orderedTeams)}
      ${activeTeam ? renderTeam(activeTeam) : '<div class="empty-state"><p>No configured teams.</p></div>'}
    `;
  };

  const renderLinkBizRows = (rows) => {
    if (!linkBizProjectRows) return;
    const items = Array.isArray(rows) ? rows : [];
    if (!items.length) {
      linkBizProjectRows.innerHTML = '<tr><td colspan="6" class="team-dashboard-empty-cell">No unlinked Jira tickets found.</td></tr>';
      return;
    }
    const selectOptions = Array.isArray(linkBizProjectSelectOptions) ? linkBizProjectSelectOptions : [];
    linkBizProjectRows.innerHTML = items.map((row) => {
      const rowSelectOptions = Array.isArray(row.select_biz_project_options) ? row.select_biz_project_options : selectOptions;
      const bpmisId = String(row.suggested_bpmis_id || '').trim();
      const selectedBpmisId = String(row.selected_bpmis_id || '').trim();
      const effectiveBpmisId = selectedBpmisId || bpmisId;
      const disabled = effectiveBpmisId ? '' : 'disabled';
      const suggestedTitle = String(row.suggested_project_title || '').trim();
      const selectedProjectTitle = String(row.selected_project_title || '').trim();
      const matchSource = String(row.match_source || '').trim();
      const matchScore = Number(row.match_score || 0);
      const suggestionLabel = suggestedTitle || 'Not matched yet';
      const suggestionMeta = suggestedTitle && matchSource
        ? `<span class="team-dashboard-link-biz-match-meta">${escapeHtml(matchSource)} · ${Math.round(matchScore * 100)}%</span>`
        : '';
      const selectHtml = `
        <select class="team-dashboard-link-biz-select" data-link-biz-project-select>
          <option value="">Use suggested match</option>
          ${rowSelectOptions.map((option) => {
            const optionId = String(option.bpmis_id || '').trim();
            const optionTitle = String(option.project_name || '').trim();
            if (!optionId || !optionTitle) return '';
            return `<option value="${escapeHtml(optionId)}" ${optionId === selectedBpmisId ? 'selected' : ''}>${escapeHtml(optionTitle)}</option>`;
          }).join('')}
        </select>
      `;
      return `
        <tr data-link-biz-project-row="${escapeHtml(row.jira_id || '')}">
          <td>${renderLink(row.jira_link || '', row.jira_id || '-')}</td>
          <td>${escapeHtml(row.jira_title || '-')}</td>
          <td>${escapeHtml(row.reporter_email || '-')}</td>
          <td>${escapeHtml(suggestionLabel)}${suggestionMeta}</td>
          <td>${selectHtml}</td>
          <td>
            <button
              class="button button-secondary"
              type="button"
              data-link-biz-project-action
              data-jira-id="${escapeHtml(row.jira_id || '')}"
              data-jira-link="${escapeHtml(row.jira_link || '')}"
              data-jira-title="${escapeHtml(row.jira_title || '')}"
              data-reporter-email="${escapeHtml(row.reporter_email || '')}"
              data-suggested-bpmis-id="${escapeHtml(effectiveBpmisId)}"
              data-suggested-project-title="${escapeHtml(selectedProjectTitle || row.suggested_project_title || '')}"
              ${disabled}
            >Link</button>
            <span class="team-dashboard-link-biz-row-status" data-link-biz-project-row-status></span>
          </td>
        </tr>
      `;
    }).join('');
  };

  const linkBizProjectLoadedTeams = () => taskTeams.filter((team) => team && team.loaded && !team.error);

  const linkBizTitleExcluded = (title) => {
    const normalized = String(title || '').toLowerCase();
    return [
      'sync af productization',
      'productisation upgrade',
      'deployment of productization',
    ].some((phrase) => normalized.includes(phrase));
  };

  const linkBizRowsFromLoadedTeams = (teams) => {
    const rows = [];
    const seen = new Set();
    (Array.isArray(teams) ? teams : []).forEach((team) => {
      const teamKey = String(team?.team_key || '').trim();
      ['under_prd', 'pending_live'].forEach((sectionKey) => {
        (team?.[sectionKey] || []).forEach((project) => {
          const projectBpmisId = String(project?.bpmis_id || '').trim();
          const unavailableProject = !projectBpmisId || String(project?.project_name || '').trim().toLowerCase() === 'bpmis unavailable';
          if (!unavailableProject) return;
          (project?.jira_tickets || []).forEach((ticket) => {
            const jiraId = String(ticket?.jira_id || ticket?.issue_id || '').trim();
            const jiraTitle = String(ticket?.jira_title || '').trim();
            if (!jiraId || seen.has(jiraId) || linkBizTitleExcluded(jiraTitle)) return;
            seen.add(jiraId);
            rows.push({
              team_key: teamKey,
              jira_id: jiraId,
              jira_link: String(ticket?.jira_link || `https://jira.shopee.io/browse/${jiraId}`).trim(),
              jira_title: jiraTitle,
              reporter_email: String(ticket?.pm_email || ticket?.reporter_email || '').trim().toLowerCase(),
              suggested_bpmis_id: '',
              suggested_project_title: '',
              match_score: 0,
              match_source: '',
            });
          });
        });
      });
    });
    return rows.sort((left, right) => (
      `${left.team_key || ''}:${left.jira_id || ''}`.localeCompare(`${right.team_key || ''}:${right.jira_id || ''}`)
    ));
  };

  const loadLinkBizJira = async () => {
    if (!linkBizProjectRows || linkBizProjectLoading) return;
    linkBizProjectLoading = true;
    const originalFindLabel = linkBizProjectFindJira?.textContent || 'Find Unlinked Jira';
    if (linkBizProjectFindJira) linkBizProjectFindJira.disabled = true;
    if (linkBizProjectFindJira) linkBizProjectFindJira.textContent = 'Finding...';
    if (linkBizProjectSuggest) linkBizProjectSuggest.disabled = true;
    linkBizProjectRows.innerHTML = '<tr><td colspan="6" class="team-dashboard-empty-cell">Finding unlinked Jira tickets...</td></tr>';
    setStatus(linkBizProjectStatus, 'Finding unlinked Jira tickets...', 'neutral');
    await new Promise((resolve) => window.setTimeout(resolve, 0));
    const cachedTeams = linkBizProjectLoadedTeams();
    if (cachedTeams.length) {
      linkBizProjectRowsState = linkBizRowsFromLoadedTeams(cachedTeams);
      linkBizProjectSelectOptions = [];
      renderLinkBizRows(linkBizProjectRowsState);
      if (linkBizProjectSuggest) linkBizProjectSuggest.disabled = !linkBizProjectRowsState.length;
      const refreshedAt = formatSingaporeTimestamp(new Date());
      setStatus(
        linkBizProjectStatus,
        `Refreshed at ${refreshedAt}. ${linkBizProjectRowsState.length} unlinked Jira tickets found from ${cachedTeams.length} loaded Task List team${cachedTeams.length === 1 ? '' : 's'}.`,
        'success',
      );
      linkBizProjectLoading = false;
      if (linkBizProjectFindJira) linkBizProjectFindJira.disabled = false;
      if (linkBizProjectFindJira) linkBizProjectFindJira.textContent = originalFindLabel;
      return;
    }
    try {
      const response = await fetch(root.dataset.linkBizProjectJiraUrl || '/api/team-dashboard/link-biz-projects/jira', {
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      const payload = await readJson(response, 'Could not load unlinked Jira tickets.');
      linkBizProjectRowsState = payload.rows || [];
      linkBizProjectSelectOptions = [];
      renderLinkBizRows(linkBizProjectRowsState);
      if (linkBizProjectSuggest) linkBizProjectSuggest.disabled = !linkBizProjectRowsState.length;
      const elapsed = payload.elapsed_seconds ? ` in ${formatDuration(payload.elapsed_seconds)}` : '';
      setStatus(linkBizProjectStatus, `${linkBizProjectRowsState.length} unlinked Jira tickets found${elapsed}.`, 'success');
    } catch (error) {
      linkBizProjectRows.innerHTML = '<tr><td colspan="6" class="team-dashboard-empty-cell">Could not load unlinked Jira tickets.</td></tr>';
      setStatus(linkBizProjectStatus, error.message || 'Could not load unlinked Jira tickets.', 'error');
    } finally {
      linkBizProjectLoading = false;
      if (linkBizProjectFindJira) linkBizProjectFindJira.disabled = false;
      if (linkBizProjectFindJira) linkBizProjectFindJira.textContent = originalFindLabel;
    }
  };

  const suggestLinkBizProjects = async () => {
    if (!linkBizProjectRows || linkBizProjectLoading || !linkBizProjectRowsState.length) return;
    linkBizProjectLoading = true;
    if (linkBizProjectFindJira) linkBizProjectFindJira.disabled = true;
    if (linkBizProjectSuggest) linkBizProjectSuggest.disabled = true;
    setStatus(linkBizProjectStatus, 'Searching BPMIS Biz Projects and matching suggestions...', 'neutral');
    try {
      const response = await fetch(root.dataset.linkBizProjectSuggestionsUrl || '/api/team-dashboard/link-biz-projects/suggestions', {
        method: 'POST',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ rows: linkBizProjectRowsState, team_payloads: linkBizProjectLoadedTeams() }),
      });
      const payload = await readJson(response, 'Could not suggest BPMIS Biz Projects.');
      linkBizProjectRowsState = payload.rows || [];
      linkBizProjectSelectOptions = payload.select_biz_project_options || [];
      renderLinkBizRows(linkBizProjectRowsState);
      const elapsed = payload.elapsed_seconds ? ` in ${formatDuration(payload.elapsed_seconds)}` : '';
      const keywordCount = Number(payload.keyword_search_count || 0);
      setStatus(
        linkBizProjectStatus,
        `${payload.matched_count || 0}/${linkBizProjectRowsState.length} Jira tickets matched${elapsed}. ${payload.team_candidate_count || 0} team candidates, ${payload.keyword_candidate_count || 0} keyword candidates from ${keywordCount} keyword searches.`,
        'success',
      );
    } catch (error) {
      setStatus(linkBizProjectStatus, error.message || 'Could not suggest BPMIS Biz Projects.', 'error');
    } finally {
      linkBizProjectLoading = false;
      if (linkBizProjectFindJira) linkBizProjectFindJira.disabled = false;
      if (linkBizProjectSuggest) linkBizProjectSuggest.disabled = !linkBizProjectRowsState.length;
    }
  };

  const configuredTeams = async () => {
    const response = await fetch(root.dataset.configUrl || '/api/team-dashboard/config', {
      headers: { Accept: 'application/json' },
      credentials: 'same-origin',
    });
    const payload = await readJson(response, 'Could not load Team Dashboard config.');
    const serverTaskCache = payload.config?.task_cache && typeof payload.config.task_cache === 'object'
      ? payload.config.task_cache
      : null;
    return Object.entries(payload.config?.teams || {}).map(([teamKey, team]) => {
      const memberEmails = Array.isArray(team.member_emails) ? team.member_emails : [];
      const baseTeam = {
        team_key: teamKey,
        label: team.label || teamKey,
        member_emails: memberEmails,
        under_prd: [],
        pending_live: [],
        progress_text: '',
        loading: false,
        loaded: false,
      };
      const cached = cachedTeamFor(teamKey, memberEmails, serverTaskCache) || cachedTeamFor(teamKey, memberEmails);
      if (!cached) return baseTeam;
      return {
        ...baseTeam,
        under_prd: Array.isArray(cached.under_prd) ? cached.under_prd : [],
        pending_live: Array.isArray(cached.pending_live) ? cached.pending_live : [],
        loaded: true,
        cached_at: cached.cached_at || '',
        elapsed_seconds: cached.elapsed_seconds || 0,
        fetch_stats: cached.fetch_stats || {},
        timing_stats: cached.timing_stats || {},
      };
    });
  };

  const teamTaskUrl = (teamKey = '', reload = false) => {
    const url = new URL(root.dataset.tasksUrl || '/api/team-dashboard/tasks', window.location.origin);
    if (teamKey) {
      url.searchParams.set('team', teamKey);
    }
    if (reload) {
      url.searchParams.set('reload', '1');
      url.searchParams.set('_reload', String(Date.now()));
    }
    return url.toString();
  };

  const updateTaskSummary = (teams) => {
    const teamItems = Array.isArray(teams) ? teams : [];
    const loaded = teamItems.filter((team) => team.loaded).length;
    const failed = teamItems.filter((team) => team.error && !team.loading).length;
    const total = teamItems.reduce((count, team) => count + itemCount(team.under_prd || []) + itemCount(team.pending_live || []), 0);
    if (taskSummary) {
      taskSummary.textContent = `Loaded ${loaded}/${teamItems.length} teams; ${total} Jira tasks so far${failed ? `; ${failed} failed` : ''}.`;
    }
  };

  const loadConfiguredTeams = async () => {
    try {
      taskTeams = await configuredTeams();
      renderTeams(taskTeams);
      updateTaskSummary(taskTeams);
      const restored = taskTeams.filter((team) => team.loaded).length;
      setStatus(
        taskStatus,
        restored ? `Restored saved Jira tasks for ${restored} team${restored === 1 ? '' : 's'}. Click Reload Jira to refresh.` : '',
        'neutral',
      );
    } catch (error) {
      setStatus(taskStatus, error.message || 'Could not load Team Dashboard config.', 'error');
      renderTeams([]);
    }
  };

  const mergeLoadedTeam = (currentTeam, loadedTeam, payloadStatus = 'ok') => {
    const safeTeam = loadedTeam && typeof loadedTeam === 'object' ? loadedTeam : {};
    const hadTeamError = Boolean(safeTeam.error || payloadStatus === 'partial');
    return {
      ...safeTeam,
      team_key: safeTeam.team_key || currentTeam.team_key,
      label: safeTeam.label || currentTeam.label,
      member_emails: safeTeam.member_emails || currentTeam.member_emails || [],
      under_prd: Array.isArray(safeTeam.under_prd) ? safeTeam.under_prd : [],
      pending_live: Array.isArray(safeTeam.pending_live) ? safeTeam.pending_live : [],
      loading: false,
      loaded: !hadTeamError,
      error: safeTeam.error || '',
      progress_text: hadTeamError ? 'Failed' : 'Done',
    };
  };

  const loadAllTeamTasks = async () => {
    if (!taskTeams.length) return;
    const loadingCount = taskTeams.length;
    taskTeams = taskTeams.map((team) => ({
      ...team,
      loading: true,
      error: '',
      progress_text: `Loading ${team.label || team.team_key} Jira tasks...`,
    }));
    renderTeams(taskTeams);
    updateTaskSummary(taskTeams);
    setStatus(taskStatus, `Loading Jira tasks for ${loadingCount} teams...`, 'neutral');
    try {
      const response = await fetch(teamTaskUrl('', true), {
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
        cache: 'no-store',
      });
      const payload = await readJson(response, 'Could not load team Jira tasks.');
      const loadedTeams = Array.isArray(payload.teams) ? payload.teams : [];
      const loadedByKey = new Map(loadedTeams.map((team) => [String(team?.team_key || ''), team]));
      let loadedCount = 0;
      let failedCount = 0;
      taskTeams = taskTeams.map((currentTeam) => {
        const loadedTeam = loadedByKey.get(String(currentTeam.team_key || ''));
        if (!loadedTeam) {
          failedCount += 1;
          return {
            ...currentTeam,
            loading: false,
            error: `No Jira payload returned for ${currentTeam.label || currentTeam.team_key}.`,
            progress_text: 'Failed',
          };
        }
        const mergedTeam = mergeLoadedTeam(currentTeam, loadedTeam);
        if (mergedTeam.error) {
          failedCount += 1;
        } else {
          loadedCount += 1;
          saveCachedTeam(mergedTeam);
        }
        return mergedTeam;
      });
      setStatus(
        taskStatus,
        failedCount
          ? `Reloaded Jira for ${loadedCount}/${loadingCount} teams; ${failedCount} failed.`
          : `Reloaded Jira for ${loadedCount} teams.`,
        failedCount ? 'error' : 'success',
      );
    } catch (error) {
      taskTeams = taskTeams.map((team) => ({
        ...team,
        loading: false,
        error: error.message || 'Could not load team Jira tasks.',
        progress_text: 'Failed',
      }));
      setStatus(taskStatus, error.message || 'Could not load team Jira tasks.', 'error');
    }
    renderTeams(taskTeams);
    updateTaskSummary(taskTeams);
  };

  const loadTeamTasks = async () => {
    await loadAllTeamTasks();
  };

  const emailsFromTextarea = (node) => String(node?.value || '')
    .split(/\s|,|;/)
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);

  const saveMembers = async (event) => {
    event.preventDefault();
    const teams = {};
    root.querySelectorAll('[data-team-dashboard-members]').forEach((textarea) => {
      const teamKey = textarea.dataset.teamDashboardMembers || '';
      if (!teamKey) return;
      teams[teamKey] = { member_emails: emailsFromTextarea(textarea) };
    });
    setStatus(adminStatus, 'Saving team emails...', 'neutral');
    try {
      const response = await fetch(root.dataset.saveUrl || '/admin/team-dashboard/members', {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ teams }),
      });
      const payload = await readJson(response, 'Could not save team emails.');
      Object.entries(payload.config?.teams || {}).forEach(([teamKey, team]) => {
        const textarea = root.querySelector(`[data-team-dashboard-members="${CSS.escape(teamKey)}"]`);
        if (textarea) textarea.value = (team.member_emails || []).join('\n');
      });
      initialConfig = payload.config || initialConfig;
      clearTaskCache();
      setStatus(adminStatus, 'Team emails saved.', 'success');
    } catch (error) {
      setStatus(adminStatus, error.message || 'Could not save team emails.', 'error');
    }
  };

  const updateMonthlyReportPreview = ({ persist = false } = {}) => {
    if (!monthlyReportDraft || !monthlyReportPreview) return;
    const value = monthlyReportDraft.value || '';
    monthlyReportPreview.innerHTML = value.trim()
      ? renderMarkdown(value)
      : '<p>Generate a draft to preview the email body.</p>';
    if (monthlyReportSendButton) {
      monthlyReportSendButton.disabled = !value.trim();
    }
    if (persist && value.trim()) {
      writeMonthlyReportDraftCache({
        draft_markdown: value,
        subject: monthlyReportSubject,
        highlight_topics: readMonthlyReportTopics({ strict: false }),
        source: 'browser',
      });
    }
  };

  const renderMonthlyReportEvidenceDebug = (items) => {
    if (!monthlyReportEvidenceDebug || !monthlyReportEvidenceDebugBody) return;
    const rows = Array.isArray(items) ? items.filter((item) => item && typeof item === 'object') : [];
    monthlyReportEvidenceDebug.hidden = rows.length === 0;
    if (!rows.length) {
      monthlyReportEvidenceDebugBody.innerHTML = '';
      return;
    }
    monthlyReportEvidenceDebugBody.innerHTML = rows.map((item) => {
      const counts = item.source_counts || {};
      const groups = Array.isArray(item.qualifier_marker_groups) ? item.qualifier_marker_groups : [];
      const aliases = Array.isArray(item.alias_sample) ? item.alias_sample.slice(0, 12) : [];
      const conversations = Array.isArray(item.seatalk_conversation_labels) ? item.seatalk_conversation_labels : [];
      const gaps = Array.isArray(item.gaps) ? item.gaps : [];
      const glossary = Array.isArray(item.glossary_matches) ? item.glossary_matches : [];
      const countText = [
        `SeaTalk ${Number(counts.seatalk || 0)}`,
        `Gmail ${Number(counts.gmail || 0)}`,
        `Sheet ${Number(counts.google_sheet || 0)}`,
        `Projects ${Number(counts.project || 0)}`,
        `PRD ${Number(counts.prd || 0)}`,
      ].join(' · ');
      const groupText = groups.length ? groups.map((group) => (Array.isArray(group) ? group.join(' / ') : String(group || ''))).join(' | ') : '-';
      const glossaryText = glossary.length
        ? glossary.map((entry) => [entry.domain, entry.canonical || entry.id].filter(Boolean).join(': ')).join(' | ')
        : '-';
      return `
        <article class="team-dashboard-monthly-report-debug-card">
          <div class="team-dashboard-monthly-report-debug-head">
            <strong>${escapeHtml(item.topic || 'Highlight')}</strong>
            <span>${escapeHtml(String(item.confidence || 'none'))}</span>
          </div>
          <dl>
            <div><dt>Intent</dt><dd>${escapeHtml(item.topic_intent || '-')}</dd></div>
            <div><dt>Sources</dt><dd>${escapeHtml(countText)}</dd></div>
            <div><dt>SeaTalk Matches</dt><dd>${escapeHtml(`${Number(item.seatalk_raw_match_count || 0)} raw / ${Number(item.seatalk_filtered_match_count || 0)} filtered / ${Number(item.seatalk_compact_count || 0)} shown`)}</dd></div>
            <div><dt>Qualifiers</dt><dd>${escapeHtml(groupText)}</dd></div>
            <div><dt>Glossary</dt><dd>${escapeHtml(glossaryText)}</dd></div>
            <div><dt>Groups</dt><dd>${escapeHtml(conversations.join(' | ') || '-')}</dd></div>
            <div><dt>Aliases</dt><dd>${escapeHtml(aliases.join(' | ') || '-')}</dd></div>
            <div><dt>Gaps</dt><dd>${escapeHtml(gaps.join(' | ') || '-')}</dd></div>
          </dl>
        </article>
      `;
    }).join('');
  };

  const renderMonthlyReportEvidenceReview = (items) => {
    if (!monthlyReportEvidenceReview || !monthlyReportEvidenceReviewBody) return;
    const rows = Array.isArray(items) ? items.filter((item) => item && typeof item === 'object') : [];
    monthlyReportEvidenceReview.hidden = rows.length === 0;
    if (!rows.length) {
      monthlyReportEvidenceReviewBody.innerHTML = '';
      return;
    }
    monthlyReportEvidenceReviewBody.innerHTML = rows.map((item) => {
      const counts = item.source_counts || {};
      const sourceText = [
        `SeaTalk ${Number(counts.seatalk || 0)}`,
        `Gmail ${Number(counts.gmail || 0)}`,
        `Sheet ${Number(counts.google_sheet || 0)}`,
        `Projects ${Number(counts.project || 0)}`,
        `PRD ${Number(counts.prd || 0)}`,
      ].join(' · ');
      const groups = Array.isArray(item.seatalk_conversation_labels) ? item.seatalk_conversation_labels : [];
      const gaps = Array.isArray(item.gaps) ? item.gaps : [];
      const glossary = Array.isArray(item.glossary_matches) ? item.glossary_matches : [];
      const glossaryText = glossary.length
        ? glossary.map((entry) => [entry.domain, entry.canonical || entry.id].filter(Boolean).join(': ')).join(' | ')
        : '-';
      return `
        <article class="team-dashboard-monthly-report-review-card" data-status="${escapeHtml(item.status || 'ready')}">
          <div class="team-dashboard-monthly-report-review-head">
            <strong>${escapeHtml(item.topic || 'Highlight')}</strong>
            <span>${escapeHtml(`${item.status || 'ready'} · ${item.confidence || 'none'}`)}</span>
          </div>
          <dl>
            <div><dt>Primary Topic</dt><dd>${escapeHtml(item.primary_topic || item.topic || '-')}</dd></div>
            <div><dt>Intent</dt><dd>${escapeHtml(item.intent || '-')}</dd></div>
            <div><dt>Evidence</dt><dd>${escapeHtml(sourceText)}</dd></div>
            <div><dt>SeaTalk Groups</dt><dd>${escapeHtml(groups.join(' | ') || '-')}</dd></div>
            <div><dt>Glossary</dt><dd>${escapeHtml(glossaryText)}</dd></div>
            <div><dt>Gaps</dt><dd>${escapeHtml(gaps.join(' | ') || '-')}</dd></div>
          </dl>
        </article>
      `;
    }).join('');
  };

  const setMonthlyReportProgressStep = (activeStep, percent, message) => {
    if (!monthlyReportProgress) return;
    monthlyReportProgress.hidden = false;
    if (monthlyReportProgressFill) {
      monthlyReportProgressFill.style.width = `${Math.max(0, Math.min(100, percent))}%`;
    }
    monthlyReportProgress.querySelectorAll('[data-monthly-report-progress-step]').forEach((node) => {
      const step = node.dataset.monthlyReportProgressStep || '';
      if (step === activeStep) {
        node.dataset.state = 'loading';
      } else if (
        (activeStep === 'compact' && step === 'prepare')
        || (activeStep === 'draft' && ['prepare', 'compact'].includes(step))
        || activeStep === 'done'
      ) {
        node.dataset.state = 'done';
      } else if (activeStep === 'error') {
        node.dataset.state = step === 'draft' ? 'error' : node.dataset.state || '';
      } else {
        node.dataset.state = '';
      }
    });
    if (monthlyReportProgressMessage) {
      monthlyReportProgressMessage.textContent = message || '';
    }
  };

  const monthlyReportProgressText = (progress) => {
    const elapsedSeconds = monthlyReportProgressStartedAt ? Math.floor((Date.now() - monthlyReportProgressStartedAt) / 1000) : 0;
    const message = progress?.message || 'Preparing Monthly Report generation.';
    const batchText = progress?.total ? ` (${Number(progress.current || 0)}/${Number(progress.total || 0)})` : '';
    const tokenText = progress?.estimated_prompt_tokens
      ? ` Approx. input: ${formatTokenCount(progress.estimated_prompt_tokens)} tokens${progress.token_risk ? `, risk: ${progress.token_risk}` : ''}.`
      : '';
    return `${message}${batchText} Elapsed ${formatDuration(elapsedSeconds)}.${tokenText}`;
  };

  const monthlyReportProgressStageMeta = (stage, current, total) => {
    const normalizedStage = String(stage || 'preparing_sources');
    const scale = (start, end) => {
      if (!total) return start;
      const ratio = Math.max(0, Math.min(1, Number(current || 0) / Number(total || 1)));
      return Math.round(start + ((end - start) * ratio));
    };
    if (normalizedStage.includes('final') || normalizedStage.includes('draft') || normalizedStage.includes('codex')) {
      return { activeStep: 'draft', percent: scale(88, 96) };
    }
    if (
      (normalizedStage.includes('summarizing_') && !normalizedStage.includes('summarizing_prd_scope'))
      || normalizedStage.includes('merging')
      || normalizedStage.includes('compressing')
    ) {
      if (normalizedStage.includes('merging')) return { activeStep: 'compact', percent: scale(78, 84) };
      if (normalizedStage.includes('compressing')) return { activeStep: 'compact', percent: scale(82, 88) };
      return { activeStep: 'compact', percent: scale(52, 78) };
    }
    const prepareRanges = [
      { match: 'collecting_seatalk', start: 12, end: 18 },
      { match: 'searching_vip_gmail', start: 18, end: 28 },
      { match: 'searching_topic_gmail', start: 28, end: 42 },
      { match: 'ingesting_prd', start: 42, end: 48 },
      { match: 'summarizing_prd_scope', start: 48, end: 52 },
      { match: 'building_evidence', start: 52, end: 56 },
    ];
    const matchedRange = prepareRanges.find((range) => normalizedStage.includes(range.match));
    if (matchedRange) {
      return { activeStep: 'prepare', percent: scale(matchedRange.start, matchedRange.end) };
    }
    return { activeStep: 'prepare', percent: scale(8, 12) };
  };

  const renderMonthlyReportProgress = (progress) => {
    monthlyReportLastProgress = progress || monthlyReportLastProgress || {};
    const stage = String(monthlyReportLastProgress.stage || 'preparing_sources');
    const total = Number(monthlyReportLastProgress.total || 0);
    const current = Number(monthlyReportLastProgress.current || 0);
    if (String(monthlyReportLastProgress.state || '') === 'completed') {
      setMonthlyReportProgressStep('done', 100, monthlyReportProgressText(monthlyReportLastProgress));
      return;
    }
    const meta = monthlyReportProgressStageMeta(stage, current, total);
    setMonthlyReportProgressStep(meta.activeStep, meta.percent, monthlyReportProgressText(monthlyReportLastProgress));
  };

  const startMonthlyReportProgress = () => {
    monthlyReportProgressStartedAt = Date.now();
    monthlyReportLastProgress = {
      stage: 'preparing_sources',
      message: 'Preparing Monthly Report sources.',
      current: 0,
      total: 0,
      estimated_prompt_tokens: 0,
      token_risk: '',
    };
    window.clearInterval(monthlyReportProgressTimer);
    renderMonthlyReportProgress(monthlyReportLastProgress);
    monthlyReportProgressTimer = window.setInterval(() => renderMonthlyReportProgress(monthlyReportLastProgress), 1000);
  };

  const stopMonthlyReportProgress = (state, message) => {
    window.clearInterval(monthlyReportProgressTimer);
    monthlyReportProgressTimer = null;
    if (state === 'done') {
      setMonthlyReportProgressStep('done', 100, message);
    } else if (state === 'error') {
      setMonthlyReportProgressStep('error', 100, message);
    }
  };

  const monthlyReportGenerationMessage = (payload, projectCount, ticketCount) => {
    const summary = payload.generation_summary || {};
    const seconds = Number(summary.elapsed_seconds || 0);
    const tokens = Number(summary.estimated_prompt_tokens || 0);
    const risk = String(summary.token_risk || 'normal');
    const base = `Draft generated in ${formatDuration(seconds)} from ${projectCount} Key Project${projectCount === 1 ? '' : 's'} and ${ticketCount} Jira ticket${ticketCount === 1 ? '' : 's'}.`;
    if (!tokens) return base;
    const tokenText = `Approx. input size: ${formatTokenCount(tokens)} tokens.`;
    if (risk === 'high') {
      return `${base} ${tokenText} Token risk was high, but generation completed after compaction.`;
    }
    if (risk === 'warning') {
      return `${base} ${tokenText} Context was large, so generation may be slower than usual.`;
    }
    return `${base} ${tokenText} Token risk normal.`;
  };

  const pollMonthlyReportJob = async (jobId) => {
    while (jobId) {
      const payload = await readJobStatus(jobId);
      renderMonthlyReportProgress(payload.progress || payload);
      if (payload.state === 'completed') {
        return (payload.results || [])[0] || {};
      }
      if (payload.state === 'failed') {
        const error = new Error(monthlyReportJobErrorMessage(payload));
        error.jobPayload = payload;
        throw error;
      }
      await sleep(1000);
    }
    return {};
  };

  const loadMonthlyReportTemplate = async () => {
    if (monthlyReportLoaded) return;
    monthlyReportLoaded = true;
    try {
      const response = await fetch(root.dataset.monthlyReportTemplateUrl || '/api/team-dashboard/monthly-report/template', {
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      const payload = await readJson(response, 'Could not load Monthly Report template.');
      monthlyReportSubject = payload.subject || monthlyReportSubject;
      if (monthlyReportTemplate && !monthlyReportTemplate.value.trim()) {
        monthlyReportTemplate.value = payload.template || '';
      }
      if (monthlyReportRecipient) {
        monthlyReportRecipient.textContent = payload.recipient || 'xiaodong.zheng@npt.sg';
      }
      applyMonthlyReportInputs(payload, { onlyEmpty: true });
    } catch (error) {
      setStatus(monthlyReportStatus, error.message || 'Could not load Monthly Report template.', 'error');
    }
  };

  const restoreMonthlyReportDraft = async () => {
    if (!monthlyReportDraft) return;
    const cached = readMonthlyReportDraftCache();
    const cachedDraft = String(cached.draft_markdown || '').trim();
    const cachedSavedAt = Date.parse(cached.saved_at || '') || 0;
    if (String(cached.draft_markdown || '').trim()) {
      monthlyReportSubject = cached.subject || monthlyReportSubject;
      applyMonthlyReportInputs(cached);
      monthlyReportDraft.value = cached.draft_markdown || '';
      updateMonthlyReportPreview();
      renderMonthlyReportEvidenceReview(cached.evidence_review || []);
      renderMonthlyReportEvidenceDebug(cached.evidence_debug || []);
      setStatus(monthlyReportStatus, 'Restored the last Monthly Report draft from this browser.', 'neutral');
    }
    try {
      const response = await fetch(root.dataset.monthlyReportLatestDraftUrl || '/api/team-dashboard/monthly-report/latest-draft', {
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      const payload = await readJson(response, 'Could not load the latest Monthly Report draft.');
      if (!String(payload.draft_markdown || '').trim()) return;
      const serverGeneratedAt = Number(payload.generated_at || 0) * 1000;
      if (cachedDraft && cachedSavedAt >= serverGeneratedAt) return;
      monthlyReportSubject = payload.subject || monthlyReportSubject;
      applyMonthlyReportInputs(payload);
      monthlyReportDraft.value = payload.draft_markdown || '';
      writeMonthlyReportDraftCache({
        draft_markdown: payload.draft_markdown,
        subject: monthlyReportSubject,
        highlight_topics: payload.highlight_topics || [],
        highlight_topic_sources: payload.highlight_topic_sources || payload.generation_summary?.highlight_topic_sources || [],
        evidence_review: payload.evidence_review || [],
        evidence_debug: payload.evidence_debug || payload.highlight_evidence_debug || [],
        period_start: payload.period_start || '',
        period_end: payload.period_end || '',
        saved_at: payload.generated_at ? new Date(Number(payload.generated_at) * 1000).toISOString() : undefined,
        source: 'server',
      });
      updateMonthlyReportPreview();
      renderMonthlyReportEvidenceReview(payload.evidence_review || []);
      renderMonthlyReportEvidenceDebug(payload.evidence_debug || payload.highlight_evidence_debug || []);
      setStatus(monthlyReportStatus, 'Restored the latest generated Monthly Report draft.', 'neutral');
    } catch (error) {
      // Missing historical drafts should not block the Team Dashboard.
    }
  };

  const generateMonthlyReport = async () => {
    if (!monthlyReportGenerateButton || !monthlyReportDraft) return;
    let requestPayload = {};
    try {
      requestPayload = monthlyReportRequestPayload();
    } catch (error) {
      setStatus(monthlyReportStatus, error.message || 'Monthly Report input is invalid.', 'error');
      return;
    }
    monthlyReportGenerateButton.disabled = true;
    monthlyReportGenerateButton.textContent = 'Generating...';
    const previousDraft = monthlyReportDraft.value.trim();
    setStatus(
      monthlyReportStatus,
      previousDraft
        ? 'Generating Monthly Report draft. The previous draft remains visible until the new draft is ready.'
        : 'Generating Monthly Report draft. Keep this page open; Send Email stays disabled until the draft is ready.',
      'neutral',
    );
    startMonthlyReportProgress();
    try {
      const response = await fetch(root.dataset.monthlyReportDraftUrl || '/api/team-dashboard/monthly-report/draft', {
        method: 'POST',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify(requestPayload),
      });
      const initialPayload = await readJson(response, 'Could not generate Monthly Report draft.');
      const payload = initialPayload.status === 'queued' && initialPayload.job_id
        ? await pollMonthlyReportJob(initialPayload.job_id)
        : initialPayload;
      monthlyReportDraft.value = payload.draft_markdown || '';
      writeMonthlyReportDraftCache({
        draft_markdown: monthlyReportDraft.value,
        subject: monthlyReportSubject,
        highlight_topics: payload.highlight_topics || requestPayload.highlight_topics,
        highlight_topic_sources: payload.highlight_topic_sources || payload.generation_summary?.highlight_topic_sources || requestPayload.highlight_topic_sources,
        evidence_review: payload.evidence_review || [],
        evidence_debug: payload.evidence_debug || payload.highlight_evidence_debug || [],
        period_start: payload.generation_summary?.period_start || requestPayload.period_start,
        period_end: payload.generation_summary?.period_end || requestPayload.period_end,
        source: 'generate',
      });
      updateMonthlyReportPreview();
      renderMonthlyReportEvidenceReview(payload.evidence_review || []);
      renderMonthlyReportEvidenceDebug(payload.evidence_debug || payload.highlight_evidence_debug || []);
      const evidence = payload.evidence_summary || {};
      const projectCount = Number(evidence.key_project_count || 0);
      const ticketCount = Number(evidence.jira_ticket_count || 0);
      const successMessage = monthlyReportGenerationMessage(payload, projectCount, ticketCount);
      setStatus(monthlyReportStatus, successMessage, 'success');
      stopMonthlyReportProgress('done', successMessage);
    } catch (error) {
      const message = monthlyReportJobErrorMessage(error);
      setStatus(monthlyReportStatus, message, 'error');
      stopMonthlyReportProgress('error', message);
    } finally {
      monthlyReportGenerateButton.disabled = false;
      monthlyReportGenerateButton.textContent = 'Generate Monthly Report Draft';
    }
  };

  const sendMonthlyReport = async () => {
    if (!monthlyReportSendButton || !monthlyReportDraft) return;
    const draft = monthlyReportDraft.value.trim();
    if (!draft) {
      setStatus(monthlyReportStatus, 'Monthly Report draft is empty.', 'error');
      return;
    }
    monthlyReportSendButton.disabled = true;
    monthlyReportSendButton.textContent = 'Sending...';
    setStatus(monthlyReportStatus, 'Sending Monthly Report email...', 'neutral');
    try {
      const response = await fetch(root.dataset.monthlyReportSendUrl || '/api/team-dashboard/monthly-report/send', {
        method: 'POST',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({
          draft_markdown: draft,
          subject: monthlyReportSubject,
          recipient: monthlyReportRecipient?.textContent || 'xiaodong.zheng@npt.sg',
        }),
      });
      const payload = await readJson(response, 'Could not send Monthly Report email.');
      setStatus(monthlyReportStatus, `Monthly Report sent to ${payload.recipient || 'xiaodong.zheng@npt.sg'}.`, 'success');
    } catch (error) {
      setStatus(monthlyReportStatus, error.message || 'Could not send Monthly Report email.', 'error');
    } finally {
      monthlyReportSendButton.textContent = 'Send Email';
      monthlyReportSendButton.disabled = !monthlyReportDraft.value.trim();
    }
  };

  const renderDailyBriefRows = (briefs) => {
    if (!dailyBriefRows) return;
    const items = Array.isArray(briefs) ? briefs : [];
    if (!items.length) {
      dailyBriefRows.innerHTML = '<tr><td colspan="2" class="team-dashboard-empty-cell">No Daily Brief emails archived yet.</td></tr>';
      return;
    }
    dailyBriefRows.innerHTML = items.map((item) => {
      const downloadUrl = String(item.download_url || '').trim();
      const disabled = downloadUrl ? '' : 'disabled';
      return `
        <tr>
          <td>${escapeHtml(item.time_period || '-')}</td>
          <td>
            <a
              class="button button-secondary"
              href="${escapeHtml(downloadUrl || '#')}"
              ${downloadUrl ? 'download' : 'aria-disabled="true"'}
              ${disabled}
            >Download</a>
          </td>
        </tr>
      `;
    }).join('');
  };

  const loadDailyBriefs = async ({ force = false } = {}) => {
    if (!dailyBriefRows || (!force && dailyBriefLoaded)) return;
    dailyBriefLoaded = true;
    dailyBriefRows.innerHTML = '<tr><td colspan="2" class="team-dashboard-empty-cell">Loading Daily Brief emails...</td></tr>';
    setStatus(dailyBriefStatus, 'Loading Daily Brief emails...', 'neutral');
    try {
      const response = await fetch(root.dataset.dailyBriefsUrl || '/api/team-dashboard/daily-briefs', {
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      const payload = await readJson(response, 'Could not load Daily Brief emails.');
      renderDailyBriefRows(payload.briefs || []);
      const count = Array.isArray(payload.briefs) ? payload.briefs.length : 0;
      setStatus(dailyBriefStatus, count ? `Loaded ${count} Daily Brief email${count === 1 ? '' : 's'}.` : 'No Daily Brief emails archived yet.', count ? 'success' : 'neutral');
    } catch (error) {
      dailyBriefLoaded = false;
      renderDailyBriefRows([]);
      setStatus(dailyBriefStatus, error.message || 'Could not load Daily Brief emails.', 'error');
    }
  };

  const saveMonthlyReportTemplate = async (event) => {
    event.preventDefault();
    if (!monthlyReportTemplate) return;
    setStatus(monthlyReportTemplateStatus, 'Saving Monthly Report template...', 'neutral');
    try {
      const response = await fetch(root.dataset.monthlyReportTemplateSaveUrl || '/admin/team-dashboard/monthly-report-template', {
        method: 'POST',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ template: monthlyReportTemplate.value || '' }),
      });
      const payload = await readJson(response, 'Could not save Monthly Report template.');
      monthlyReportTemplate.value = payload.template || monthlyReportTemplate.value;
      monthlyReportLoaded = false;
      setStatus(monthlyReportTemplateStatus, 'Monthly Report template saved.', 'success');
    } catch (error) {
      setStatus(monthlyReportTemplateStatus, error.message || 'Could not save Monthly Report template.', 'error');
    }
  };

  const splitLines = (value) => String(value || '')
    .split(/[\r\n;,]+/)
    .map((item) => item.trim())
    .filter(Boolean);

  const renderReportIntelligence = (config) => {
    const intelligence = config?.report_intelligence_config || {};
    if (reportIntelligenceVips) {
      reportIntelligenceVips.value = (intelligence.vip_people || []).map((vip) => [
        vip.display_name || '',
        (vip.role_tags || []).join(', '),
        (vip.emails || []).join(', '),
        (vip.seatalk_ids || []).join(', '),
        (vip.aliases || []).join(', '),
      ].join(' | ')).join('\n');
    }
    if (reportIntelligenceKeywords) reportIntelligenceKeywords.value = (intelligence.priority_keywords || []).join('\n');
    const noise = intelligence.noise || {};
    if (reportIntelligenceSeatalkBlacklist) reportIntelligenceSeatalkBlacklist.value = (noise.seatalk_group_blacklist || []).join('\n');
    if (reportIntelligenceGmailSenderBlacklist) reportIntelligenceGmailSenderBlacklist.value = (noise.gmail_sender_blacklist || []).join('\n');
    if (reportIntelligenceGmailSubjectHints) reportIntelligenceGmailSubjectHints.value = (noise.gmail_subject_hints || []).join('\n');
  };

  const parseVipRows = () => splitLines(reportIntelligenceVips?.value || '').map((line) => {
    const parts = line.split('|').map((item) => item.trim());
    return {
      display_name: parts[0] || '',
      role_tags: splitLines(parts[1] || ''),
      emails: splitLines(parts[2] || ''),
      seatalk_ids: splitLines(parts[3] || ''),
      aliases: splitLines(parts[4] || ''),
    };
  }).filter((item) => item.display_name || item.emails.length || item.seatalk_ids.length || item.aliases.length);

  const collectReportIntelligence = () => ({
    vip_people: parseVipRows(),
    priority_keywords: splitLines(reportIntelligenceKeywords?.value || ''),
    noise: {
      seatalk_group_blacklist: splitLines(reportIntelligenceSeatalkBlacklist?.value || ''),
      gmail_sender_blacklist: splitLines(reportIntelligenceGmailSenderBlacklist?.value || ''),
      gmail_subject_hints: splitLines(reportIntelligenceGmailSubjectHints?.value || ''),
    },
  });

  const loadReportIntelligence = async () => {
    if (reportIntelligenceLoaded || !reportIntelligenceForm) return;
    reportIntelligenceLoaded = true;
    try {
      if (!initialConfig?.report_intelligence_config) {
        const response = await fetch(root.dataset.configUrl || '/api/team-dashboard/config', {
          headers: { Accept: 'application/json' },
          credentials: 'same-origin',
        });
        const payload = await readJson(response, 'Could not load Report Intelligence config.');
        initialConfig = payload.config || initialConfig;
      }
      renderReportIntelligence(initialConfig);
    } catch (error) {
      setStatus(reportIntelligenceStatus, error.message || 'Could not load Report Intelligence config.', 'error');
    }
  };

  const saveReportIntelligence = async (event) => {
    event.preventDefault();
    setStatus(reportIntelligenceStatus, 'Saving Report Intelligence rules...', 'neutral');
    try {
      const response = await fetch(root.dataset.reportIntelligenceSaveUrl || '/admin/team-dashboard/report-intelligence', {
        method: 'POST',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ report_intelligence_config: collectReportIntelligence() }),
      });
      const payload = await readJson(response, 'Could not save Report Intelligence rules.');
      initialConfig = {
        ...initialConfig,
        report_intelligence_config: payload.report_intelligence_config || collectReportIntelligence(),
      };
      renderReportIntelligence(initialConfig);
      setStatus(reportIntelligenceStatus, 'Report Intelligence rules saved.', 'success');
    } catch (error) {
      setStatus(reportIntelligenceStatus, error.message || 'Could not save Report Intelligence rules.', 'error');
    }
  };

  const seatalkMappingStateFor = (mappingRoot) => {
    if (!mappingRoot) return { rows: [], mappings: {}, page: 1 };
    if (!seatalkNameMappingState.has(mappingRoot)) {
      seatalkNameMappingState.set(mappingRoot, { rows: [], mappings: {}, page: 1, pageSize: seatalkNameMappingDefaultPageSize });
    }
    return seatalkNameMappingState.get(mappingRoot);
  };

  const syncVisibleNameMappingInputs = (mappingRoot) => {
    const state = seatalkMappingStateFor(mappingRoot);
    mappingRoot?.querySelectorAll('[data-seatalk-mapping-row]').forEach((row) => {
      const id = row.dataset.seatalkMappingId || '';
      const input = row.querySelector('[data-seatalk-mapping-input]');
      if (id && input) state.mappings[id] = String(input.value || '');
    });
  };

  const renderNameMappingPage = () => {
    const mappingRoot = seatalkNameMappingRoot;
    const body = mappingRoot?.querySelector('[data-seatalk-name-mapping-body]');
    if (!mappingRoot || !body) return;
    const actionContainers = [...mappingRoot.querySelectorAll('[data-seatalk-name-mapping-actions]')];
    const state = seatalkMappingStateFor(mappingRoot);
    const rows = Array.isArray(state.rows) ? state.rows : [];
    const pageSize = seatalkNameMappingPageSizeOptions.includes(Number(state.pageSize))
      ? Number(state.pageSize)
      : seatalkNameMappingDefaultPageSize;
    state.pageSize = pageSize;
    const totalPages = Math.max(1, Math.ceil(rows.length / pageSize));
    state.page = Math.min(Math.max(Number(state.page || 1), 1), totalPages);
    const start = (state.page - 1) * pageSize;
    const pageRows = rows.slice(start, start + pageSize);
    if (!rows.length) {
      body.innerHTML = '<article class="seatalk-insight-item"><p>No frequent or recently surfaced SeaTalk source IDs were found.</p></article>';
      body.hidden = false;
      actionContainers.forEach((actions) => { actions.hidden = true; });
      return;
    }
    body.innerHTML = `
      <div class="seatalk-mapping-pagination" data-seatalk-name-mapping-pagination>
        <div class="seatalk-mapping-page-summary">
          <span>Showing ${start + 1}-${start + pageRows.length} of ${rows.length}</span>
          <label class="seatalk-mapping-page-size">
            <span>Rows per page</span>
            <select data-seatalk-name-mapping-page-size aria-label="Rows per page">
              ${seatalkNameMappingPageSizeOptions.map((option) => `
                <option value="${option}" ${option === pageSize ? 'selected' : ''}>${option}</option>
              `).join('')}
            </select>
          </label>
        </div>
        <div class="button-row">
          <button class="button button-secondary" type="button" data-seatalk-name-mapping-prev ${state.page <= 1 ? 'disabled' : ''}>Previous</button>
          <span>Page ${state.page} / ${totalPages}</span>
          <button class="button button-secondary" type="button" data-seatalk-name-mapping-next ${state.page >= totalPages ? 'disabled' : ''}>Next</button>
        </div>
      </div>
      ${pageRows.map((row) => `
        <div class="seatalk-mapping-row" data-seatalk-mapping-row data-seatalk-mapping-id="${escapeHtml(row.id)}">
          <div class="seatalk-mapping-id">
            <strong>${escapeHtml(row.id)}</strong>
            <span>${escapeHtml(row.priorityReason || row.type || 'Frequent unknown ID')}</span>
          </div>
          <div class="seatalk-mapping-count">
            <strong>${Number(row.count || 0)}</strong>
            <span>mentions</span>
          </div>
          <div>
            <input type="text" value="${escapeHtml(state.mappings[row.id] || '')}" placeholder="Display name" data-seatalk-mapping-input aria-label="Display name for ${escapeHtml(row.id)}">
            ${row.example ? `<div class="seatalk-mapping-example">${escapeHtml(row.example)}</div>` : ''}
          </div>
        </div>
      `).join('')}
    `;
    body.hidden = false;
    actionContainers.forEach((actions) => { actions.hidden = false; });
    body.querySelectorAll('[data-seatalk-mapping-input]').forEach((input) => {
      input.addEventListener('input', () => syncVisibleNameMappingInputs(mappingRoot));
    });
    body.querySelector('[data-seatalk-name-mapping-page-size]')?.addEventListener('change', (event) => {
      syncVisibleNameMappingInputs(mappingRoot);
      const currentStart = (state.page - 1) * pageSize;
      const nextPageSize = Number(event.target.value || seatalkNameMappingDefaultPageSize);
      state.pageSize = seatalkNameMappingPageSizeOptions.includes(nextPageSize) ? nextPageSize : seatalkNameMappingDefaultPageSize;
      state.page = Math.floor(currentStart / state.pageSize) + 1;
      renderNameMappingPage();
    });
    body.querySelector('[data-seatalk-name-mapping-prev]')?.addEventListener('click', () => {
      syncVisibleNameMappingInputs(mappingRoot);
      state.page -= 1;
      renderNameMappingPage();
    });
    body.querySelector('[data-seatalk-name-mapping-next]')?.addEventListener('click', () => {
      syncVisibleNameMappingInputs(mappingRoot);
      state.page += 1;
      renderNameMappingPage();
    });
  };

  const renderNameMappings = (payload) => {
    const mappingRoot = seatalkNameMappingRoot;
    if (!mappingRoot) return;
    const mappings = payload?.mappings && typeof payload.mappings === 'object' ? payload.mappings : {};
    const unknownRows = Array.isArray(payload?.unknown_ids) ? payload.unknown_ids : [];
    const rowsById = new Map();
    const personAlias = (id) => {
      const value = String(id || '');
      if (value.startsWith('buddy-')) return `UID ${value.slice('buddy-'.length)}`;
      if (value.startsWith('UID ')) return `buddy-${value.slice('UID '.length)}`;
      return '';
    };
    const canonicalMappingId = (id) => String(id || '').startsWith('buddy-') ? `UID ${String(id).slice('buddy-'.length)}` : String(id || '');
    const mappingValueFor = (id) => mappings[id] || mappings[personAlias(id)] || '';
    unknownRows.forEach((row) => {
      if (!row?.id || isIgnoredSeatalkMappingId(row.id)) return;
      const canonicalId = canonicalMappingId(row.id);
      const existing = rowsById.get(canonicalId);
      if (existing) {
        existing.count += Number(row.count || 0);
        if (!existing.example && row.example) existing.example = row.example;
        return;
      }
      rowsById.set(canonicalId, {
        id: canonicalId,
        type: row.type || 'uid',
        count: Number(row.count || 0),
        example: row.example || '',
        priorityReason: row.priority_reason || 'Frequent unknown ID',
      });
    });
    Object.entries(mappings).forEach(([id, value]) => {
      const displayName = String(value || '').trim();
      if (!displayName || isIgnoredSeatalkMappingId(id)) return;
      const canonicalId = canonicalMappingId(id);
      if (!canonicalId || rowsById.has(canonicalId)) return;
      rowsById.set(canonicalId, {
        id: canonicalId,
        type: canonicalId.startsWith('group-') ? 'group' : 'uid',
        count: 0,
        example: '',
        priorityReason: 'Saved mapping',
      });
    });
    const rows = Array.from(rowsById.values());
    const state = seatalkMappingStateFor(mappingRoot);
    state.rows = rows;
    state.mappings = {};
    rows.forEach((row) => {
      state.mappings[row.id] = mappingValueFor(row.id);
    });
    state.page = 1;
    state.pageSize = seatalkNameMappingPageSizeOptions.includes(Number(state.pageSize))
      ? Number(state.pageSize)
      : seatalkNameMappingDefaultPageSize;
    renderNameMappingPage();
  };

  const loadSeaTalkNameMappings = async (forceRefresh = false) => {
    const mappingRoot = seatalkNameMappingRoot;
    const mappingsUrl = root.dataset.reportIntelligenceSeatalkNameMappingsUrl || mappingRoot?.dataset.seatalkNameMappingsUrl || '';
    if (!mappingRoot || !mappingsUrl || mappingRoot.dataset.seatalkConfigured !== 'true') return;
    if (seatalkNameMappingsLoaded && !forceRefresh) return;
    const refreshButton = mappingRoot.querySelector('[data-seatalk-name-mapping-refresh]');
    const originalButtonText = refreshButton?.textContent || 'Refresh Candidates';
    if (refreshButton && forceRefresh) {
      refreshButton.disabled = true;
      refreshButton.textContent = 'Refreshing...';
    }
    const mappingStatus = mappingRoot.querySelector('[data-seatalk-mapping-status]');
    if (mappingStatus) mappingStatus.hidden = false;
    setStatus(mappingStatus, forceRefresh ? 'Refreshing recent SeaTalk IDs...' : 'Loading frequent unknown IDs...', 'neutral');
    try {
      const url = forceRefresh ? `${mappingsUrl}${mappingsUrl.includes('?') ? '&' : '?'}refresh=1` : mappingsUrl;
      const response = await fetch(url, { headers: { Accept: 'application/json' }, credentials: 'same-origin' });
      const payload = await readJson(response, 'Could not load SeaTalk name mappings.');
      renderNameMappings(payload);
      const statusNode = mappingRoot.querySelector('[data-seatalk-mapping-status]');
      if (statusNode) statusNode.hidden = true;
      seatalkNameMappingsLoaded = true;
    } catch (error) {
      setStatus(mappingRoot.querySelector('[data-seatalk-mapping-status]'), error.message || 'Could not load SeaTalk name mappings.', 'error');
    } finally {
      if (refreshButton && forceRefresh) {
        refreshButton.disabled = false;
        refreshButton.textContent = originalButtonText;
      }
    }
  };

  const collectNameMappings = () => {
    const mappingRoot = seatalkNameMappingRoot;
    if (!mappingRoot) return {};
    syncVisibleNameMappingInputs(mappingRoot);
    const state = seatalkMappingStateFor(mappingRoot);
    const mappings = {};
    Object.entries(state.mappings || {}).forEach(([id, value]) => {
      const trimmed = String(value || '').trim();
      if (id && trimmed && !isIgnoredSeatalkMappingId(id)) mappings[id] = trimmed;
    });
    return mappings;
  };

  const saveSeaTalkNameMappings = async () => {
    const mappingRoot = seatalkNameMappingRoot;
    const mappingsUrl = root.dataset.reportIntelligenceSeatalkNameMappingsUrl || mappingRoot?.dataset.seatalkNameMappingsUrl || '';
    if (!mappingRoot || !mappingsUrl) return;
    const saveButtons = [...mappingRoot.querySelectorAll('[data-seatalk-name-mapping-save]')];
    const feedbackNode = mappingRoot.querySelector('[data-seatalk-name-mapping-save-feedback]');
    saveButtons.forEach((button) => {
      button.disabled = true;
      button.textContent = 'Saving...';
    });
    try {
      const response = await fetch(mappingsUrl, {
        method: 'POST',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ mappings: collectNameMappings() }),
      });
      await readJson(response, 'Could not save SeaTalk name mappings.');
      if (feedbackNode) {
        feedbackNode.textContent = 'Saved. Reports will use these names on the next load.';
        feedbackNode.dataset.tone = 'success';
      }
      seatalkNameMappingsLoaded = false;
      window.setTimeout(() => loadSeaTalkNameMappings(false), 500);
    } catch (error) {
      if (feedbackNode) {
        feedbackNode.textContent = error.message || 'Could not save SeaTalk name mappings.';
        feedbackNode.dataset.tone = 'error';
      }
    } finally {
      saveButtons.forEach((button) => {
        button.disabled = false;
        button.textContent = 'Save Mappings';
      });
    }
  };

  setupTabs();
  loadConfiguredTeams();
  restoreMonthlyReportDraft();
  adminForm?.addEventListener('submit', saveMembers);
  reportIntelligenceForm?.addEventListener('submit', saveReportIntelligence);
  seatalkNameMappingRoot?.querySelector('[data-seatalk-name-mapping-refresh]')?.addEventListener('click', () => loadSeaTalkNameMappings(true));
  seatalkNameMappingRoot?.querySelectorAll('[data-seatalk-name-mapping-save]').forEach((button) => {
    button.addEventListener('click', saveSeaTalkNameMappings);
  });
  monthlyReportDraft?.addEventListener('input', () => updateMonthlyReportPreview({ persist: true }));
  [
    monthlyReportTopics,
    ...monthlyReportTopicRows.flatMap((row) => [
      row.querySelector('[data-monthly-report-topic-input]'),
      ...Array.from(row.querySelectorAll('[data-monthly-report-source]')),
    ]),
    monthlyReportPeriodStart,
    monthlyReportPeriodEnd,
  ].forEach((node) => {
    node?.addEventListener('change', () => {
      writeMonthlyReportDraftCache({
        draft_markdown: monthlyReportDraft?.value || '',
        subject: monthlyReportSubject,
        highlight_topics: readMonthlyReportTopics({ strict: false }),
        highlight_topic_sources: readMonthlyReportTopicSources({ strict: false }),
        source: 'inputs',
      });
    });
  });
  monthlyReportGenerateButton?.addEventListener('click', generateMonthlyReport);
  monthlyReportSendButton?.addEventListener('click', sendMonthlyReport);
  monthlyReportTemplateForm?.addEventListener('submit', saveMonthlyReportTemplate);
  versionPlanSyncButton?.addEventListener('click', syncVersionPlan);
  versionPlanContent?.addEventListener('change', (event) => {
    const input = event.target.closest('[data-version-plan-cell]');
    if (input) saveVersionPlanCell(input);
  });
  versionPlanContent?.addEventListener('click', async (event) => {
    const button = event.target.closest('[data-version-plan-row-action]');
    if (!button) return;
    const action = button.dataset.versionPlanRowAction || '';
    const scope = button.dataset.versionPlanScope || button.closest('[data-version-plan-row-id]')?.dataset.versionPlanScope || '';
    const versionId = button.dataset.versionId || button.closest('[data-version-plan-row-id]')?.dataset.versionId || '';
    const row = button.closest('[data-version-plan-row-id]');
    try {
      if (action === 'add') {
        await updateVersionPlanRows({ action: 'add', scope, version_id: versionId }, 'Could not add Version Plan row.');
        return;
      }
      if (action === 'delete' && row) {
        await updateVersionPlanRows({
          action: 'delete',
          scope,
          version_id: versionId,
          row_id: row.dataset.versionPlanRowId || '',
        }, 'Could not delete Version Plan row.');
        return;
      }
      if ((action === 'up' || action === 'down') && row) {
        const sheet = row.closest('[data-version-plan-sheet]');
        const groupRows = Array.from(sheet?.querySelectorAll('[data-version-plan-manual-row="true"]') || [])
          .filter((item) => (item.dataset.versionPlanPriority || '') === (row.dataset.versionPlanPriority || ''));
        const index = groupRows.indexOf(row);
        const swapWith = action === 'up' ? groupRows[index - 1] : groupRows[index + 1];
        if (!swapWith) return;
        if (action === 'up') sheet.insertBefore(row, swapWith);
        else sheet.insertBefore(swapWith, row);
        const rowIds = groupRows
          .map((item) => item.dataset.versionPlanRowId || '')
          .filter(Boolean);
        const moved = row.dataset.versionPlanRowId || '';
        const target = swapWith.dataset.versionPlanRowId || '';
        const movedIndex = rowIds.indexOf(moved);
        const targetIndex = rowIds.indexOf(target);
        if (movedIndex >= 0 && targetIndex >= 0) {
          rowIds[movedIndex] = target;
          rowIds[targetIndex] = moved;
        }
        await updateVersionPlanRows({
          action: 'reorder',
          scope,
          version_id: versionId,
          row_ids: rowIds,
        }, 'Could not reorder Version Plan rows.');
      }
    } catch (error) {
      setVersionPlanStatus(error.message || 'Could not update Version Plan rows.', 'error');
    }
  });
  versionPlanContent?.addEventListener('dragstart', (event) => {
    const row = event.target.closest('[data-version-plan-manual-row="true"]');
    if (!row) return;
    versionPlanDragRow = row;
    row.classList.add('is-dragging');
    event.dataTransfer.effectAllowed = 'move';
    event.dataTransfer.setData('text/plain', row.dataset.versionPlanRowId || '');
  });
  versionPlanContent?.addEventListener('dragend', () => {
    versionPlanDragRow?.classList.remove('is-dragging');
    versionPlanDragRow = null;
  });
  versionPlanContent?.addEventListener('dragover', (event) => {
    const row = event.target.closest('[data-version-plan-manual-row="true"]');
    if (!row || !versionPlanDragRow) return;
    if (row.dataset.versionPlanScope !== versionPlanDragRow.dataset.versionPlanScope) return;
    if ((row.dataset.versionId || '') !== (versionPlanDragRow.dataset.versionId || '')) return;
    if ((row.dataset.versionPlanPriority || '') !== (versionPlanDragRow.dataset.versionPlanPriority || '')) return;
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  });
  versionPlanContent?.addEventListener('drop', async (event) => {
    const row = event.target.closest('[data-version-plan-manual-row="true"]');
    if (!row) return;
    event.preventDefault();
    try {
      await reorderVersionPlanRows(row);
    } catch (error) {
      setVersionPlanStatus(error.message || 'Could not reorder Version Plan rows.', 'error');
    }
  });
  linkBizProjectFindJira?.addEventListener('click', loadLinkBizJira);
  linkBizProjectSuggest?.addEventListener('click', suggestLinkBizProjects);
  linkBizProjectRows?.addEventListener('change', (event) => {
    const select = event.target.closest('[data-link-biz-project-select]');
    if (!select) return;
    const row = select.closest('[data-link-biz-project-row]');
    const jiraId = row?.dataset.linkBizProjectRow || '';
    const selectedBpmisId = String(select.value || '').trim();
    const rowState = linkBizProjectRowsState.find((item) => String(item.jira_id || '') === jiraId) || {};
    const rowOptions = Array.isArray(rowState.select_biz_project_options)
      ? rowState.select_biz_project_options
      : linkBizProjectSelectOptions;
    const selectedOption = rowOptions.find(
      (option) => String(option.bpmis_id || '').trim() === selectedBpmisId,
    ) || {};
    linkBizProjectRowsState = linkBizProjectRowsState.map((item) => {
      if (String(item.jira_id || '') !== jiraId) return item;
      return {
        ...item,
        selected_bpmis_id: selectedBpmisId,
        selected_project_title: selectedBpmisId ? String(selectedOption.project_name || '').trim() : '',
      };
    });
    renderLinkBizRows(linkBizProjectRowsState);
  });
  linkBizProjectRows?.addEventListener('click', async (event) => {
    const button = event.target.closest('[data-link-biz-project-action]');
    if (!button) return;
    const row = button.closest('[data-link-biz-project-row]');
    const rowStatus = row?.querySelector('[data-link-biz-project-row-status]');
    const jiraId = button.dataset.jiraId || '';
    const suggestedBpmisId = button.dataset.suggestedBpmisId || '';
    if (!jiraId || !suggestedBpmisId) return;
    button.disabled = true;
    button.textContent = 'Linking...';
    if (rowStatus) rowStatus.textContent = '';
    try {
      const response = await fetch(root.dataset.linkBizProjectUrl || '/api/team-dashboard/link-biz-projects', {
        method: 'POST',
        headers: { Accept: 'application/json', 'Content-Type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({
          jira_id: jiraId,
          jira_link: button.dataset.jiraLink || '',
          jira_title: button.dataset.jiraTitle || '',
          reporter_email: button.dataset.reporterEmail || '',
          suggested_bpmis_id: suggestedBpmisId,
          suggested_project_title: button.dataset.suggestedProjectTitle || '',
          selected_bpmis_id: suggestedBpmisId,
          selected_project_title: button.dataset.suggestedProjectTitle || '',
        }),
      });
      await readJson(response, 'Could not link Jira ticket to BPMIS Biz Project.');
      clearTaskCache();
      linkBizProjectRowsState = linkBizProjectRowsState.filter((item) => String(item.jira_id || '') !== jiraId);
      row?.remove();
      setStatus(linkBizProjectStatus, `${jiraId} linked to BPMIS ${suggestedBpmisId}. Task List cache was cleared.`, 'success');
      if (linkBizProjectRows && !linkBizProjectRows.querySelector('[data-link-biz-project-row]')) {
        renderLinkBizRows([]);
        if (linkBizProjectSuggest) linkBizProjectSuggest.disabled = true;
      }
    } catch (error) {
      button.disabled = false;
      button.textContent = 'Retry';
      if (rowStatus) rowStatus.textContent = error.message || 'Link failed.';
      setStatus(linkBizProjectStatus, error.message || 'Could not link Jira ticket to BPMIS Biz Project.', 'error');
    }
  });
  taskList?.addEventListener('click', (event) => {
    const trackButton = event.target.closest('[data-team-dashboard-track]');
    if (trackButton) {
      activeTaskTeamKey = trackButton.dataset.teamDashboardTrack || activeTaskTeamKey;
      renderTeams(taskTeams);
      return;
    }

    const loadButton = event.target.closest('[data-team-dashboard-load-team]');
    if (loadButton) {
      loadTeamTasks(loadButton.dataset.teamDashboardLoadTeam || '');
      return;
    }

    const pageButton = event.target.closest('[data-team-dashboard-page]');
    if (pageButton) {
      const pageKey = pageButton.dataset.teamDashboardPage || '';
      const delta = Number(pageButton.dataset.pageDelta || 0);
      jiraPageState[pageKey] = Math.max(1, Number(jiraPageState[pageKey] || 1) + delta);
      renderTeams(taskTeams);
      return;
    }

    const keyButton = event.target.closest('[data-team-dashboard-key-project]');
    if (keyButton) {
      if (!canManageKeyProjects || keyButton.disabled) return;
      const bpmisId = keyButton.dataset.teamDashboardKeyProject || '';
      const nextValue = keyButton.dataset.keyProjectNext === 'true';
      const priority = keyButton.dataset.keyProjectPriority || '';
      keyButton.disabled = true;
      setStatus(taskStatus, nextValue ? 'Marking Key Project...' : 'Removing Key Project...', 'neutral');
      fetch(root.dataset.keyProjectUrl || '/api/team-dashboard/key-projects', {
        method: 'POST',
        headers: {
          Accept: 'application/json',
          'Content-Type': 'application/json',
        },
        credentials: 'same-origin',
        body: JSON.stringify({ bpmis_id: bpmisId, is_key_project: nextValue, priority }),
      })
        .then((response) => readJson(response, 'Could not save Key Project.'))
        .then((payload) => {
          updateProjectKeyState(
            payload.bpmis_id || bpmisId,
            payload.is_key_project,
            payload.key_project_source,
            payload.override || {},
          );
          renderTeams(taskTeams);
          setStatus(taskStatus, nextValue ? 'Key Project marked.' : 'Key Project removed.', 'success');
        })
        .catch((error) => {
          keyButton.disabled = false;
          setStatus(taskStatus, error.message || 'Could not save Key Project.', 'error');
        });
      return;
    }

    const button = event.target.closest('[data-team-dashboard-toggle]');
    if (!button) return;
    const panelId = button.dataset.teamDashboardToggle || '';
    const panel = taskList.querySelector(`[data-team-dashboard-panel-id="${CSS.escape(panelId)}"]`);
    if (!panel) return;
    const nextHidden = !panel.hidden ? true : false;
    panel.hidden = nextHidden;
    expandedPanels[panelId] = !nextHidden;
    button.textContent = nextHidden ? '+' : '-';
    button.setAttribute('aria-expanded', nextHidden ? 'false' : 'true');
  });

  taskList?.addEventListener('click', async (event) => {
    const toggle = event.target.closest('[data-prd-review-toggle]');
    if (toggle) {
      const row = taskList.querySelector(`[data-prd-review-row="${CSS.escape(toggle.dataset.prdReviewToggle || '')}"]`);
      if (!row) return;
      row.hidden = !row.hidden;
      toggle.textContent = row.hidden ? 'View Review' : 'Hide Review';
      return;
    }

    const actionButton = event.target.closest('[data-prd-action]');
    const button = actionButton;
    if (!button) return;
    const action = button.dataset.prdAction === 'summary' ? 'summary' : 'review';
    const isSummary = action === 'summary';
    const jiraId = button.dataset.jiraId || '';
    const jiraLink = button.dataset.jiraLink || '';
    const prdUrl = externalHref(button.dataset.prdUrl || '');
    if (!jiraId || !prdUrl) return;
    const row = button.closest('tr');
    const panelRow = row?.nextElementSibling?.matches('[data-prd-review-row]') ? row.nextElementSibling : null;
    const panel = panelRow?.querySelector('[data-prd-review-panel]');
    const toggleButton = row?.querySelector('[data-prd-review-toggle]');
    if (!panel || !panelRow) return;

    const forceRefresh = button.dataset.forceRefresh === 'true';
    button.dataset.forceRefresh = 'false';
    button.disabled = true;
    button.textContent = isSummary ? 'Summarizing...' : 'Reviewing...';
    panelRow.hidden = false;
    panel.innerHTML = `<div class="team-dashboard-review-loading">${isSummary ? 'Summarizing PRD...' : 'Reviewing PRD...'}</div>`;
    try {
      const response = await fetch(`/api/team-dashboard/prd-${action}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ jira_id: jiraId, jira_link: jiraLink, prd_url: prdUrl, force_refresh: forceRefresh, async: true }),
      });
      let payload = await readJson(response, isSummary ? 'Could not summarize PRD.' : 'Could not review PRD.');
      if (payload.status === 'queued' && payload.job_id) {
        payload = await pollJobResult(payload.job_id, {
          fallbackMessage: isSummary ? 'Could not summarize PRD.' : 'Could not review PRD.',
          onProgress: (message, progressPayload) => {
            const tokens = Number(progressPayload?.estimated_prompt_tokens || progressPayload?.progress?.estimated_prompt_tokens || 0);
            const risk = String(progressPayload?.token_risk || progressPayload?.progress?.token_risk || '');
            const tokenText = tokens ? ` Approx. input: ${formatTokenCount(tokens)} tokens${risk ? `, risk: ${risk}` : ''}.` : '';
            panel.innerHTML = `<div class="team-dashboard-review-loading">${escapeHtml(`${message}${tokenText}`)}</div>`;
          },
        });
      }
      const result = isSummary ? (payload.summary || {}) : (payload.review || {});
      const coverage = payload.coverage || {};
      const coverageLine = coverage.mode
        ? `<span>Coverage: ${escapeHtml(coverage.mode)} · ${escapeHtml(coverage.sections_covered || coverage.sections_assessed || 0)}/${escapeHtml(coverage.sections_total || coverage.selected_sections_total || 0)} sections${coverage.truncated ? ' · truncated' : ''}${coverage.estimated_prompt_tokens ? ` · approx ${escapeHtml(formatTokenCount(coverage.estimated_prompt_tokens))} input tokens${coverage.token_risk ? ` (${escapeHtml(coverage.token_risk)})` : ''}` : ''}</span>`
        : '';
      const templateTotal = Number(coverage.report_templates_total ?? coverage.linked_artifacts_total);
      const templateReviewed = Number(coverage.report_templates_reviewed ?? coverage.linked_artifacts_reviewed ?? 0);
      const templateCacheHits = Number(coverage.report_templates_cache_hits ?? coverage.linked_artifacts_cache_hits ?? 0);
      const templateLine = !isSummary && Number.isFinite(templateTotal) && templateTotal > 0
        ? `<span>Report templates reviewed: ${escapeHtml(templateReviewed)}/${escapeHtml(templateTotal)}${templateCacheHits ? ` · ${escapeHtml(templateCacheHits)} cached` : ''}</span>`
        : '';
      const confluenceTablesTotal = Number(coverage.confluence_tables_total || 0);
      const confluenceTablesReviewed = Number(coverage.confluence_tables_reviewed || 0);
      const confluenceTableLine = !isSummary && Number.isFinite(confluenceTablesTotal) && confluenceTablesTotal > 0
        ? `<span>Confluence tables reviewed: ${escapeHtml(confluenceTablesReviewed)}/${escapeHtml(confluenceTablesTotal)}${coverage.table_truncated ? ` · compacted ${escapeHtml(coverage.table_rows_included || 0)} rows, omitted ${escapeHtml(coverage.table_rows_omitted || 0)}` : ''}</span>`
        : '';
      const sheetScreenshotTotal = Number(coverage.google_sheet_screenshots_total || 0);
      const sheetScreenshotReviewed = Number(coverage.google_sheet_screenshots_reviewed || 0);
      const sheetScreenshotFailed = Number(coverage.google_sheet_screenshots_failed || 0);
      const sheetScreenshotLine = !isSummary && Number.isFinite(sheetScreenshotTotal) && sheetScreenshotTotal > 0
        ? `<span>Google Sheet screenshots reviewed: ${escapeHtml(sheetScreenshotReviewed)}/${escapeHtml(sheetScreenshotTotal)}${sheetScreenshotFailed ? ` · ${escapeHtml(sheetScreenshotFailed)} not reviewed` : ''}</span>`
        : '';
      panel.innerHTML = `
        <div class="team-dashboard-review-meta">
          <strong>${escapeHtml(payload.cached ? `Cached PRD ${isSummary ? 'Summary' : 'Review'}` : `PRD ${isSummary ? 'Summary' : 'Review'}`)}</strong>
          <span>${escapeHtml(formatSingaporeTimestamp(result.updated_at || ''))}</span>
          ${coverageLine}
          ${confluenceTableLine}
          ${templateLine}
          ${sheetScreenshotLine}
        </div>
        <div class="team-dashboard-review-markdown">${renderMarkdown(result.result_markdown || '')}</div>
        <div class="team-dashboard-review-actions">
          <button class="button button-secondary team-dashboard-review-refresh" type="button" data-prd-refresh>Regenerate</button>
        </div>
      `;
      button.textContent = isSummary ? 'View Summary' : 'View Review';
      if (toggleButton) {
        toggleButton.hidden = false;
        toggleButton.textContent = `Hide ${isSummary ? 'Summary' : 'Review'}`;
      }
      panel.querySelector('[data-prd-refresh]')?.addEventListener('click', () => {
        button.dataset.forceRefresh = 'true';
        button.click();
      });
    } catch (error) {
      panel.innerHTML = `<p class="productization-inline-status" data-tone="error">${escapeHtml(error.message || (isSummary ? 'Could not summarize PRD.' : 'Could not review PRD.'))}</p>`;
      button.textContent = 'Retry';
    } finally {
      button.disabled = false;
    }
  });

  taskList?.addEventListener('change', (event) => {
    const keyFilter = event.target.closest('[data-team-dashboard-key-filter]');
    if (keyFilter) {
      keyProjectOnly = Boolean(keyFilter.checked);
      renderTeams(taskTeams);
      return;
    }

    const filter = event.target.closest('[data-team-dashboard-pm-filter]');
    if (!filter) return;
    const teamKey = filter.dataset.teamDashboardPmFilter || '';
    pmFilterState[teamKey] = String(filter.value || '').trim().toLowerCase();
    renderTeams(taskTeams);
  });
})();
