(() => {
  const gmailRoot = document.querySelector('[data-gmail-demo-root]');
  const seatalkRoot = document.querySelector('[data-seatalk-demo-root]');
  if (!gmailRoot && !seatalkRoot) return;

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const formatNumber = (value) => new Intl.NumberFormat('en-US').format(Number(value || 0));
  const formatDateTime = (value) => {
    if (!value) return 'Unknown';
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return String(value);
    return new Intl.DateTimeFormat('en-US', {
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit',
    }).format(parsed);
  };

  const parseDashboardResponse = async (response) => {
    const contentType = response.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      return response.json();
    }
    const text = await response.text();
    if (response.redirected || contentType.includes('text/html')) {
      throw new Error('The dashboard request returned an HTML page instead of API data. Please refresh the portal and sign in again if needed.');
    }
    throw new Error(text || 'Could not load dashboard data.');
  };

  const setScopedStatus = (root, selector, message, tone = 'neutral') => {
    const node = root?.querySelector(selector);
    if (!node) return;
    node.dataset.tone = tone;
    node.innerHTML = `<p>${escapeHtml(message)}</p>`;
    node.hidden = false;
  };

  const hideScopedStatus = (root, selector) => {
    const node = root?.querySelector(selector);
    if (!node) return;
    node.hidden = true;
  };

  const renderCards = (container, cards) => {
    if (!container) return;
    container.innerHTML = cards.map((card) => `
      <article class="mail-demo-scorecard">
        <strong>${escapeHtml(card.label)}</strong>
        <span>${escapeHtml(card.value)}</span>
        <small>${escapeHtml(card.detail)}</small>
      </article>
    `).join('');
  };

  const renderSourceTags = (container, tags) => {
    if (!container) return;
    const rows = (Array.isArray(tags) ? tags : []).filter(Boolean);
    container.innerHTML = rows.map((tag) => `<span class="mail-demo-source-tag">${escapeHtml(tag)}</span>`).join('');
  };

  const renderEmptyInsights = (container, message) => {
    if (!container) return;
    container.innerHTML = `
      <article class="seatalk-insight-item">
        <p>${escapeHtml(message)}</p>
      </article>
    `;
  };

  const renderProjectUpdates = (container, updates) => {
    if (!container) return;
    const rows = Array.isArray(updates) ? updates : [];
    if (!rows.length) {
      renderEmptyInsights(container, 'No confident project updates were found in the last 7 days.');
      container.hidden = false;
      return;
    }
    container.innerHTML = rows.map((item) => `
      <article class="seatalk-insight-item">
        <div class="seatalk-insight-meta">
          <span>${escapeHtml(item.domain || 'Unknown')}</span>
          <span>${escapeHtml(String(item.status || 'unknown').replaceAll('_', ' '))}</span>
        </div>
        <h4>${escapeHtml(item.title || 'Untitled update')}</h4>
        <p>${escapeHtml(item.summary || '')}</p>
        ${item.evidence ? `<div class="seatalk-insight-evidence">${escapeHtml(item.evidence)}</div>` : ''}
      </article>
    `).join('');
    container.hidden = false;
  };

  const postCompletedTodo = async (completeUrl, todo) => {
    if (!completeUrl) return;
    const response = await fetch(completeUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ todo }),
    });
    const payload = await parseDashboardResponse(response);
    if (!response.ok) throw new Error(payload.message || 'Could not mark the to-do complete.');
  };

  const renderTodos = (container, todos, completeUrl) => {
    if (!container) return;
    const rows = Array.isArray(todos) ? todos : [];
    if (!rows.length) {
      renderEmptyInsights(container, 'No open to-dos were found.');
      return;
    }
    container.innerHTML = rows.map((item, index) => `
      <article class="seatalk-insight-item seatalk-todo-item" data-seatalk-todo-item data-seatalk-todo-index="${index}">
        <input type="checkbox" data-seatalk-todo-complete aria-label="Mark to-do complete">
        <div>
          <div class="seatalk-insight-meta">
            <span>${escapeHtml(item.domain || 'Unknown')}</span>
            <span>${escapeHtml(item.priority || 'unknown')}</span>
            <span>Deadline: ${escapeHtml(item.due || 'unknown')}</span>
          </div>
          <h4>${escapeHtml(item.task || 'Untitled task')}</h4>
          ${item.evidence ? `<div class="seatalk-insight-evidence">${escapeHtml(item.evidence)}</div>` : ''}
        </div>
      </article>
    `).join('');
    container.querySelectorAll('[data-seatalk-todo-complete]').forEach((checkbox) => {
      checkbox.addEventListener('change', async (event) => {
        const itemNode = event.currentTarget.closest('[data-seatalk-todo-item]');
        const index = Number(itemNode?.dataset.seatalkTodoIndex || -1);
        const todo = rows[index];
        if (!todo) return;
        event.currentTarget.disabled = true;
        try {
          await postCompletedTodo(completeUrl, todo);
          itemNode.remove();
          if (!container.querySelector('[data-seatalk-todo-item]')) {
            renderEmptyInsights(container, 'No open to-dos were found.');
          }
        } catch (error) {
          event.currentTarget.checked = false;
          event.currentTarget.disabled = false;
          window.alert(error.message || 'Could not mark the to-do complete.');
        }
      });
    });
  };

  const renderNameMappings = (root, payload) => {
    const body = root.querySelector('[data-seatalk-name-mapping-body]');
    const actions = root.querySelector('[data-seatalk-name-mapping-actions]');
    if (!body) return;
    const mappings = payload?.mappings && typeof payload.mappings === 'object' ? payload.mappings : {};
    const unknownRows = Array.isArray(payload?.unknown_ids) ? payload.unknown_ids : [];
    const rowsById = new Map();
    const personAlias = (id) => {
      const value = String(id || '');
      if (value.startsWith('buddy-')) return `UID ${value.slice('buddy-'.length)}`;
      if (value.startsWith('UID ')) return `buddy-${value.slice('UID '.length)}`;
      return '';
    };
    const canonicalMappingId = (id) => {
      const value = String(id || '');
      if (value.startsWith('buddy-')) return `UID ${value.slice('buddy-'.length)}`;
      return value;
    };
    const mappingValueFor = (id) => mappings[id] || mappings[personAlias(id)] || '';
    unknownRows.forEach((row) => {
      if (!row?.id) return;
      rowsById.set(String(row.id), {
        id: String(row.id),
        type: row.type || 'uid',
        count: Number(row.count || 0),
        example: row.example || '',
        priorityReason: row.priority_reason || 'Frequent unknown ID',
      });
    });
    const savedCanonicalIds = new Set(Array.from(rowsById.keys()).map(canonicalMappingId));
    Object.keys(mappings).sort().forEach((id) => {
      const canonicalId = canonicalMappingId(id);
      if (!savedCanonicalIds.has(canonicalId)) {
        savedCanonicalIds.add(canonicalId);
        rowsById.set(id, {
          id,
          type: id.startsWith('group-') ? 'group' : id.startsWith('buddy-') ? 'buddy' : 'uid',
          count: 0,
          example: '',
          priorityReason: 'Saved mapping',
        });
      }
    });
    const rows = Array.from(rowsById.values());
    if (!rows.length) {
      body.innerHTML = `
        <article class="seatalk-insight-item">
          <p>No frequent unknown SeaTalk IDs were found in the last 7 days.</p>
        </article>
      `;
      body.hidden = false;
      if (actions) actions.hidden = true;
      return;
    }
    body.innerHTML = rows.map((row) => `
      <div class="seatalk-mapping-row" data-seatalk-mapping-row data-seatalk-mapping-id="${escapeHtml(row.id)}">
        <div class="seatalk-mapping-id">
          <strong>${escapeHtml(row.id)}</strong>
          <span>${escapeHtml(row.priorityReason || row.type || 'Frequent unknown ID')}</span>
        </div>
        <div class="seatalk-mapping-count">
          <strong>${formatNumber(row.count || 0)}</strong>
          <span>recent mentions</span>
        </div>
        <div>
          <input
            type="text"
            value="${escapeHtml(mappingValueFor(row.id))}"
            placeholder="Display name"
            data-seatalk-mapping-input
            aria-label="Display name for ${escapeHtml(row.id)}"
          >
          ${row.example ? `<div class="seatalk-mapping-example">${escapeHtml(row.example)}</div>` : ''}
        </div>
      </div>
    `).join('');
    body.hidden = false;
    if (actions) actions.hidden = false;
  };

  const collectNameMappings = (root) => {
    const mappings = {};
    root.querySelectorAll('[data-seatalk-mapping-row]').forEach((row) => {
      const id = row.dataset.seatalkMappingId || '';
      const input = row.querySelector('[data-seatalk-mapping-input]');
      const value = String(input?.value || '').trim();
      if (id && value) mappings[id] = value;
    });
    return mappings;
  };

  const loadSeaTalkNameMappings = async (root, mappingsUrl) => {
    if (!mappingsUrl) return;
    setScopedStatus(root, '[data-seatalk-mapping-status]', 'Loading frequent unknown IDs…', 'neutral');
    try {
      const response = await fetch(mappingsUrl, { method: 'GET' });
      const payload = await parseDashboardResponse(response);
      if (!response.ok) throw new Error(payload.message || 'Could not load SeaTalk name mappings.');
      renderNameMappings(root, payload);
      hideScopedStatus(root, '[data-seatalk-mapping-status]');
      root.dataset.seatalkMappingsLoaded = 'true';
    } catch (error) {
      setScopedStatus(root, '[data-seatalk-mapping-status]', error.message || 'Could not load SeaTalk name mappings.', 'error');
    }
  };

  const saveSeaTalkNameMappings = async (root, mappingsUrl) => {
    if (!mappingsUrl) return;
    setScopedStatus(root, '[data-seatalk-mapping-status]', 'Saving name mappings…', 'neutral');
    try {
      const response = await fetch(mappingsUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mappings: collectNameMappings(root) }),
      });
      const payload = await parseDashboardResponse(response);
      if (!response.ok) throw new Error(payload.message || 'Could not save SeaTalk name mappings.');
      setScopedStatus(root, '[data-seatalk-mapping-status]', 'Saved. Exports and Codex evidence will use these names on the next load.', 'success');
      root.dataset.seatalkMappingsLoaded = '';
      window.setTimeout(() => loadSeaTalkNameMappings(root, mappingsUrl), 300);
    } catch (error) {
      setScopedStatus(root, '[data-seatalk-mapping-status]', error.message || 'Could not save SeaTalk name mappings.', 'error');
    }
  };

  const renderChart = (container, series) => {
    if (!container) return;
    const rows = Array.isArray(series) ? series : [];
    if (!rows.length) {
      container.innerHTML = '<div class="mail-demo-chart-empty"><p>No activity was found for this time range.</p></div>';
      return;
    }
    const width = 680;
    const height = 230;
    const padding = { top: 20, right: 22, bottom: 28, left: 24 };
    const usableWidth = width - padding.left - padding.right;
    const usableHeight = height - padding.top - padding.bottom;
    const maxValue = Math.max(...rows.map((row) => Number(row.count || 0)), 1);
    const points = rows.map((row, index) => {
      const x = padding.left + (rows.length === 1 ? usableWidth / 2 : (usableWidth * index) / (rows.length - 1));
      const y = padding.top + usableHeight - ((Number(row.count || 0) / maxValue) * usableHeight);
      return { x, y, count: Number(row.count || 0), label: row.label || row.date || '' };
    });
    const peakPoint = points.reduce((best, point) => (point.count > best.count ? point : best), points[0]);
    const polyline = points.map((point) => `${point.x},${point.y}`).join(' ');
    const horizontalGrid = [0, 0.25, 0.5, 0.75, 1].map((ratio) => {
      const y = padding.top + usableHeight - (ratio * usableHeight);
      return `<line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" stroke="rgba(96, 120, 162, 0.14)" stroke-width="1" />`;
    }).join('');
    const circles = points.map((point) => `
      <g>
        <circle cx="${point.x}" cy="${point.y}" r="4.5" fill="#2c6de6" />
        <title>${escapeHtml(`${point.label}: ${point.count}`)}</title>
      </g>
    `).join('');
    const startLabel = rows[0]?.label || '';
    const endLabel = rows[rows.length - 1]?.label || '';
    const peakText = `Peak: ${peakPoint.count}`;
    const peakLabelWidth = Math.max(76, 18 + (peakText.length * 6.4));
    const peakLabelX = Math.min(width - padding.right - (peakLabelWidth / 2), Math.max(padding.left + (peakLabelWidth / 2), peakPoint.x));
    const peakAboveY = peakPoint.y - 18;
    const peakBelowY = peakPoint.y + 28;
    const peakLabelY = peakAboveY >= padding.top + 12 ? peakAboveY : Math.min(height - padding.bottom - 10, peakBelowY);
    const connectorY = peakLabelY < peakPoint.y ? peakLabelY + 6 : peakLabelY - 18;
    container.innerHTML = `
      <svg class="mail-demo-chart" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="Activity trend">
        ${horizontalGrid}
        <polyline fill="none" stroke="#2c6de6" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" points="${polyline}" />
        ${circles}
        <text x="${padding.left}" y="${height - 8}" fill="#7b8aab" font-size="11">${escapeHtml(startLabel)}</text>
        <text x="${width - padding.right}" y="${height - 8}" fill="#7b8aab" font-size="11" text-anchor="end">${escapeHtml(endLabel)}</text>
        <g>
          <line x1="${peakPoint.x}" y1="${peakPoint.y}" x2="${peakLabelX}" y2="${connectorY}" stroke="rgba(96,120,162,0.28)" stroke-width="1.5" />
          <rect x="${peakLabelX - (peakLabelWidth / 2)}" y="${peakLabelY - 13}" width="${peakLabelWidth}" height="20" rx="10" fill="rgba(255,255,255,0.92)" stroke="rgba(96,120,162,0.18)" />
          <text x="${peakLabelX}" y="${peakLabelY}" fill="#42577f" font-size="11" font-weight="600" text-anchor="middle">${escapeHtml(peakText)}</text>
        </g>
      </svg>
    `;
  };

  const buildMetricValue = (value, availability) => {
    if (value === null || value === undefined) {
      return 'N/A';
    }
    if (typeof value === 'number') {
      return formatNumber(value);
    }
    return String(value);
  };

  const renderGmailExportButtons = (container, exportUrl, manifest) => {
    if (!container) return;
    const batchCount = Number(manifest?.batch_count || 0);
    const totalMessages = Number(manifest?.total_messages || 0);
    const batchSize = Number(manifest?.batch_size || 100);
    const estimated = manifest?.estimated === true;
    if (!batchCount || !totalMessages) {
      container.innerHTML = `
        <span class="help-text">No recent inbox emails were available for Gmail download batches.</span>
      `;
      return;
    }
    const buttons = Array.from({ length: batchCount }, (_value, index) => {
      const batchNumber = index + 1;
      const start = (index * batchSize) + 1;
      const end = Math.min(totalMessages, start + batchSize - 1);
      return `
        <a class="button button-secondary" data-gmail-export-button href="${escapeHtml(`${exportUrl}?batch=${batchNumber}`)}">
          Download Emails ${start}-${end}
        </a>
      `;
    }).join('');
    container.innerHTML = `
      <div class="button-row">${buttons}</div>
      <span class="help-text">${escapeHtml(
        estimated
          ? `${formatNumber(totalMessages)} recent inbox emails were counted to prepare download batches quickly. Final batch contents are filtered during download.`
          : `${formatNumber(totalMessages)} exportable inbox emails found in the last 7 days.`
      )}</span>
    `;
  };

  const renderGmail = async () => {
    if (!gmailRoot) return;
    const dashboardUrl = gmailRoot.dataset.dashboardUrl || '';
    const exportUrl = gmailRoot.dataset.gmailExportUrl || '';
    const exportManifestUrl = gmailRoot.dataset.gmailExportManifestUrl || '';
    const gmailScopeReady = gmailRoot.dataset.gmailScopeReady === 'true';
    const contentNode = gmailRoot.querySelector('[data-mail-demo-content]');
    const scorecardsNode = gmailRoot.querySelector('[data-mail-demo-scorecards]');
    const receivedChartNode = gmailRoot.querySelector('[data-mail-demo-chart="received"]');
    const sentChartNode = gmailRoot.querySelector('[data-mail-demo-chart="sent"]');
    const exportButtonsNode = gmailRoot.querySelector('[data-gmail-export-buttons]');
    if (!gmailScopeReady) return;
    setScopedStatus(gmailRoot, '[data-mail-demo-status]', 'Loading Gmail dashboard data…', 'neutral');
    try {
      const dashboardResponse = await fetch(dashboardUrl, { method: 'GET' });
      const payload = await parseDashboardResponse(dashboardResponse);
      if (!dashboardResponse.ok) throw new Error(payload.message || 'Could not load Gmail dashboard data.');
      renderCards(scorecardsNode, [
        {
          label: 'Received Today',
          value: buildMetricValue(payload.summary?.received_today),
          detail: `${formatNumber(payload.summary?.received_period_total)} inbound messages in the last 7 days`,
        },
        {
          label: 'Current Unread',
          value: buildMetricValue(payload.summary?.current_unread),
          detail: 'Current unread Gmail inbox messages',
        },
        {
          label: 'Read Rate',
          value: payload.summary?.read_rate_percent === null || payload.summary?.read_rate_percent === undefined
            ? 'N/A'
            : `${buildMetricValue(payload.summary?.read_rate_percent)}%`,
          detail: 'Calculated from inbox messages over the last 7 days',
        },
      ]);
      renderChart(receivedChartNode, payload.trends?.received || []);
      renderChart(sentChartNode, payload.trends?.sent || []);
      if (exportButtonsNode) {
        exportButtonsNode.innerHTML = '<span class="help-text">Preparing Gmail download batches…</span>';
      }
      if (exportManifestUrl) {
        const loadExportManifest = () => {
          fetch(exportManifestUrl, { method: 'GET' })
            .then(parseDashboardResponse)
            .then((exportManifest) => {
              renderGmailExportButtons(exportButtonsNode, exportUrl, exportManifest);
            })
            .catch(() => {
              if (exportButtonsNode) {
                exportButtonsNode.innerHTML = '<span class="help-text">Gmail download batches are temporarily unavailable. The mailbox overview is still up to date.</span>';
              }
            });
        };
        if ('requestIdleCallback' in window) {
          window.requestIdleCallback(loadExportManifest, { timeout: 800 });
        } else {
          window.setTimeout(loadExportManifest, 120);
        }
      }
      if (contentNode) contentNode.hidden = false;
      hideScopedStatus(gmailRoot, '[data-mail-demo-status]');
    } catch (error) {
      if (contentNode) contentNode.hidden = true;
      setScopedStatus(gmailRoot, '[data-mail-demo-status]', error.message || 'Could not load Gmail dashboard data.', 'error');
    }
  };

  const renderSeaTalk = async () => {
    if (!seatalkRoot) return;
    const seatalkConfigured = seatalkRoot.dataset.seatalkConfigured === 'true';
    const insightsUrl = seatalkRoot.dataset.seatalkInsightsUrl || '';
    const todoCompleteUrl = seatalkRoot.dataset.seatalkTodoCompleteUrl || '';
    const nameMappingsUrl = seatalkRoot.dataset.seatalkNameMappingsUrl || '';
    const contentNode = seatalkRoot.querySelector('[data-seatalk-content]');
    const insightsStatusNode = seatalkRoot.querySelector('[data-seatalk-insights-status]');
    const projectUpdatesNode = seatalkRoot.querySelector('[data-seatalk-project-updates]');
    const todosNode = seatalkRoot.querySelector('[data-seatalk-todos]');
    const myTodosNode = seatalkRoot.querySelector('[data-seatalk-my-todos]');
    if (!seatalkConfigured) return;
    if (contentNode) contentNode.hidden = false;
    hideScopedStatus(seatalkRoot, '[data-seatalk-status]');
    seatalkRoot.querySelectorAll('[data-seatalk-tab]').forEach((tab) => {
      tab.addEventListener('click', () => {
        const target = tab.dataset.seatalkTab || 'summary';
        seatalkRoot.querySelectorAll('[data-seatalk-tab]').forEach((button) => {
          button.setAttribute('aria-selected', button === tab ? 'true' : 'false');
        });
        seatalkRoot.querySelectorAll('[data-seatalk-panel]').forEach((panel) => {
          panel.hidden = panel.dataset.seatalkPanel !== target;
        });
        if (target === 'mapping' && seatalkRoot.dataset.seatalkMappingsLoaded !== 'true') {
          loadSeaTalkNameMappings(seatalkRoot, nameMappingsUrl);
        }
      });
    });
    const saveMappingsButton = seatalkRoot.querySelector('[data-seatalk-name-mapping-save]');
    if (saveMappingsButton) {
      saveMappingsButton.addEventListener('click', () => saveSeaTalkNameMappings(seatalkRoot, nameMappingsUrl));
    }
    if (!insightsUrl || !insightsStatusNode) return;
    setScopedStatus(seatalkRoot, '[data-seatalk-insights-status]', 'Loading SeaTalk summary…', 'neutral');
    try {
      const insightsResponse = await fetch(insightsUrl, { method: 'GET' });
      const insightsPayload = await parseDashboardResponse(insightsResponse);
      if (!insightsResponse.ok) throw new Error(insightsPayload.message || 'Could not load SeaTalk summary.');
      renderTodos(myTodosNode, insightsPayload.my_todos || [], todoCompleteUrl);
      renderProjectUpdates(projectUpdatesNode, insightsPayload.project_updates || []);
      if (todosNode) todosNode.hidden = false;
      hideScopedStatus(seatalkRoot, '[data-seatalk-insights-status]');
    } catch (error) {
      setScopedStatus(seatalkRoot, '[data-seatalk-insights-status]', error.message || 'Could not load SeaTalk summary.', 'error');
    }
  };

  renderGmail();
  renderSeaTalk();
})();
