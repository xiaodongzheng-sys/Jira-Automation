(() => {
  const root = document.querySelector('[data-team-dashboard]');
  if (!root) return;

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const readJson = async (response, fallbackMessage) => {
    let payload = {};
    try {
      payload = await response.json();
    } catch (error) {
      payload = {};
    }
    if (!response.ok || payload.status === 'error') {
      throw new Error(payload.message || fallbackMessage);
    }
    return payload;
  };

  const externalHref = (value) => {
    const text = String(value || '').trim();
    if (!text) return '';
    if (/^(https?:|mailto:|tel:)/i.test(text)) return text;
    if (text.startsWith('//')) return `https:${text}`;
    return `https://${text}`;
  };

  const taskStatus = root.querySelector('[data-team-dashboard-task-status]');
  const taskSummary = root.querySelector('[data-team-dashboard-task-summary]');
  const taskProgress = root.querySelector('[data-team-dashboard-progress]');
  const taskList = root.querySelector('[data-team-dashboard-task-list]');
  const updateButton = root.querySelector('[data-team-dashboard-update]');
  const adminForm = root.querySelector('[data-team-dashboard-admin-form]');
  const adminStatus = root.querySelector('[data-team-dashboard-admin-status]');
  const teamLabels = {
    AF: 'Anti-fraud',
    CRMS: 'Credit Risk',
    GRC: 'Ops Risk',
  };

  let initialConfig = (() => {
    try {
      return JSON.parse(root.dataset.initialConfig || '{}');
    } catch (error) {
      return {};
    }
  })();

  const setStatus = (node, message, tone = 'neutral') => {
    if (!node) return;
    node.textContent = message || '';
    node.dataset.tone = tone;
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
    };
    triggers.forEach((trigger) => {
      trigger.addEventListener('click', () => activate(trigger.dataset.teamDashboardTab || 'tasks'));
    });
  };

  const renderLink = (url, label) => {
    if (!url) return escapeHtml(label || '-');
    return `<a href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${escapeHtml(label || url)}</a>`;
  };

  const renderMarkdown = (value) => {
    const html = [];
    let inList = false;
    const closeList = () => {
      if (inList) {
        html.push('</ul>');
        inList = false;
      }
    };
    const inline = (text) => escapeHtml(text)
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/`(.+?)`/g, '<code>$1</code>');
    String(value || '').split(/\r?\n/).forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed) {
        closeList();
        return;
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
    closeList();
    return html.join('');
  };

  const renderPrdLinks = (links, jiraItem) => {
    const items = Array.isArray(links) ? links : [];
    if (!items.length) return '-';
    return items.map((item, index) => {
      const url = String(item.url || '').trim();
      const label = String(item.label || url || `PRD ${index + 1}`).trim();
      return `
        <div class="team-dashboard-prd-link">
          ${renderLink(url, label)}
          <button
            class="button button-secondary team-dashboard-review-button"
            type="button"
            data-prd-review
            data-jira-id="${escapeHtml(jiraItem?.jira_id || '')}"
            data-jira-link="${escapeHtml(jiraItem?.jira_link || '')}"
            data-prd-url="${escapeHtml(url)}"
            data-prd-index="${index}"
            ${url ? '' : 'disabled'}
          >AI Review</button>
        </div>
      `;
    }).join('<br>');
  };

  const itemCount = (projects) => (Array.isArray(projects) ? projects : [])
    .reduce((countValue, project) => countValue + (project.jira_tickets || []).length, 0);

  const renderJiraRows = (items) => {
    if (!items.length) {
      return '<tr><td colspan="7" class="team-dashboard-empty-cell">No matching Jira tasks.</td></tr>';
    }
    return items.map((item, index) => {
      const reviewPanelId = `prd-review-${String(item.jira_id || index).replace(/[^a-zA-Z0-9_-]/g, '-')}-${index}`;
      return `
      <tr>
        <td>${renderLink(item.jira_link || '', item.jira_id || '-')}</td>
        <td>${escapeHtml(item.jira_title || '-')}</td>
        <td>${escapeHtml(item.pm_email || '-')}</td>
        <td>${escapeHtml(item.jira_status || '-')}</td>
        <td>${escapeHtml(item.version || '-')}</td>
        <td>${renderPrdLinks(item.prd_links, item)}</td>
        <td>
          <button class="button button-secondary team-dashboard-review-toggle" type="button" data-prd-review-toggle="${escapeHtml(reviewPanelId)}" hidden>View Review</button>
        </td>
      </tr>
      <tr class="team-dashboard-review-row" data-prd-review-row="${escapeHtml(reviewPanelId)}" hidden>
        <td colspan="7">
          <div class="team-dashboard-review-panel" data-prd-review-panel="${escapeHtml(reviewPanelId)}"></div>
        </td>
      </tr>
    `;
    }).join('');
  };

  const renderProject = (project, sectionKey, index) => {
    const tickets = Array.isArray(project.jira_tickets) ? project.jira_tickets : [];
    const panelId = `team-dashboard-${sectionKey}-${index}`;
    const bpmisId = project.bpmis_id || '-';
    return `
      <article class="bpmis-project-card team-dashboard-project-card">
        <div class="bpmis-project-card-main">
          <button class="bpmis-task-toggle" type="button" data-team-dashboard-toggle="${escapeHtml(panelId)}" aria-expanded="false" aria-label="Expand Jira tasks for BPMIS ${escapeHtml(bpmisId)}">+</button>
          <div class="bpmis-project-card-id">
            <span>BPMIS ID</span>
            <strong>${escapeHtml(bpmisId)}</strong>
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
          <div class="team-dashboard-project-count">
            <span>Jira</span>
            <strong>${tickets.length}</strong>
          </div>
        </div>
        <div class="bpmis-task-panel" data-team-dashboard-panel-id="${escapeHtml(panelId)}" hidden>
          <div class="table-wrap premium-table-wrap">
            <table class="productization-table team-dashboard-table">
              <thead>
                <tr>
                  <th>Jira ID</th>
                  <th>Jira Title</th>
                  <th>PM Email</th>
                  <th>Jira Status</th>
                  <th>Version</th>
                  <th>PRD Link</th>
                  <th>AI</th>
                </tr>
              </thead>
              <tbody>${renderJiraRows(tickets)}</tbody>
            </table>
          </div>
        </div>
      </article>
    `;
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

  const renderTeamProgress = (teams, completed, failed) => {
    if (!taskProgress) return;
    const teamItems = Array.isArray(teams) ? teams : [];
    taskProgress.hidden = !teamItems.length;
    if (!teamItems.length) {
      taskProgress.innerHTML = '';
      return;
    }
    taskProgress.innerHTML = `
      <div class="team-dashboard-progress-bar" aria-hidden="true">
        <span style="width: ${escapeHtml(String(Math.round((completed / teamItems.length) * 100)))}%"></span>
      </div>
      <div class="team-dashboard-progress-items">
        ${teamItems.map((team) => `
          <div class="team-dashboard-progress-item" data-state="${escapeHtml(team.state || 'queued')}">
            <strong>${escapeHtml(team.label || team.team_key || 'Team')}</strong>
            <span>${escapeHtml(team.progress_text || 'Queued')}</span>
          </div>
        `).join('')}
      </div>
      <p>${escapeHtml(`Progress: ${completed}/${teamItems.length} teams loaded${failed ? `, ${failed} failed` : ''}.`)}</p>
    `;
  };

  const renderTeams = (teams) => {
    if (!taskList) return;
    if (!teams.length) {
      taskList.innerHTML = '<div class="empty-state"><p>No team dashboard data loaded.</p></div>';
      return;
    }
    taskList.innerHTML = teams.map((team) => {
      const underPrd = Array.isArray(team.under_prd) ? team.under_prd : [];
      const pendingLive = Array.isArray(team.pending_live) ? team.pending_live : [];
      const error = team.error ? `<p class="productization-inline-status" data-tone="error">${escapeHtml(team.error)}</p>` : '';
      const loading = team.loading ? `<p class="productization-inline-status" data-tone="neutral">${escapeHtml(team.progress_text || 'Loading team Jira tasks...')}</p>` : '';
      const totalTasks = itemCount(underPrd) + itemCount(pendingLive);
      const teamKey = team.team_key || 'team';
      return `
        <section class="team-dashboard-team${team.loading ? ' is-loading' : ''}">
          <div class="team-dashboard-team-head">
            <div>
              <h3>${escapeHtml(team.label || team.team_key || 'Team')}</h3>
              <span>${escapeHtml((team.member_emails || []).join(', ') || 'No configured members')}</span>
            </div>
            <strong>${totalTasks}</strong>
          </div>
          ${error}
          ${loading}
          ${team.loading ? '' : renderSection('Under PRD', underPrd, `${teamKey}-under-prd`)}
          ${team.loading ? '' : renderSection('Pending Live', pendingLive, `${teamKey}-pending-live`)}
        </section>
      `;
    }).join('');
  };

  const configuredTeams = async () => {
    const response = await fetch(root.dataset.configUrl || '/api/team-dashboard/config', {
      headers: { Accept: 'application/json' },
      credentials: 'same-origin',
    });
    const payload = await readJson(response, 'Could not load Team Dashboard config.');
    return Object.entries(payload.config?.teams || {}).map(([teamKey, team]) => ({
      team_key: teamKey,
      label: team.label || teamKey,
      member_emails: Array.isArray(team.member_emails) ? team.member_emails : [],
      under_prd: [],
      pending_live: [],
      state: 'queued',
      progress_text: 'Queued',
      loading: true,
    }));
  };

  const teamTaskUrl = (teamKey) => {
    const url = new URL(root.dataset.tasksUrl || '/api/team-dashboard/tasks', window.location.origin);
    url.searchParams.set('team', teamKey);
    return url.toString();
  };

  const updateTaskSummary = (teams, completed, failed, final = false) => {
    const teamItems = Array.isArray(teams) ? teams : [];
    const total = teamItems.reduce((count, team) => count + itemCount(team.under_prd || []) + itemCount(team.pending_live || []), 0);
    if (taskSummary) {
      taskSummary.textContent = final
        ? `Loaded ${total} Jira tasks from ${completed - failed}/${teamItems.length} teams${failed ? `; ${failed} failed` : ''}.`
        : `Loaded ${completed}/${teamItems.length} teams; ${total} Jira tasks so far.`;
    }
  };

  const loadTasks = async () => {
    if (!updateButton) return;
    updateButton.disabled = true;
    setStatus(taskStatus, 'Loading team Jira tasks...', 'neutral');
    try {
      const teams = await configuredTeams();
      let completed = 0;
      let failed = 0;
      renderTeamProgress(teams, completed, failed);
      renderTeams(teams);
      updateTaskSummary(teams, completed, failed);

      await Promise.all(teams.map(async (team, index) => {
        teams[index] = { ...team, state: 'loading', progress_text: 'Loading...', loading: true };
        renderTeamProgress(teams, completed, failed);
        renderTeams(teams);
        try {
          const response = await fetch(teamTaskUrl(team.team_key), {
            headers: { Accept: 'application/json' },
            credentials: 'same-origin',
          });
          const payload = await readJson(response, `Could not load ${team.label || team.team_key} tasks.`);
          const loadedTeam = payload.team || (Array.isArray(payload.teams) ? payload.teams[0] : null) || {};
          const hadTeamError = Boolean(loadedTeam.error || payload.status === 'partial');
          if (hadTeamError) failed += 1;
          teams[index] = {
            ...loadedTeam,
            team_key: loadedTeam.team_key || team.team_key,
            label: loadedTeam.label || team.label,
            member_emails: loadedTeam.member_emails || team.member_emails || [],
            state: hadTeamError ? 'error' : 'done',
            progress_text: hadTeamError ? 'Failed' : 'Done',
            loading: false,
          };
        } catch (error) {
          failed += 1;
          teams[index] = {
            ...team,
            state: 'error',
            progress_text: 'Failed',
            loading: false,
            error: error.message || `Could not load ${team.label || team.team_key} tasks.`,
          };
        } finally {
          completed += 1;
          renderTeamProgress(teams, completed, failed);
          renderTeams(teams);
          updateTaskSummary(teams, completed, failed);
        }
      }));

      updateTaskSummary(teams, completed, failed, true);
      setStatus(
        taskStatus,
        failed ? `Updated with ${failed} team error${failed === 1 ? '' : 's'}.` : 'All teams updated.',
        failed ? 'error' : 'success',
      );
    } catch (error) {
      setStatus(taskStatus, error.message || 'Could not load Team Dashboard tasks.', 'error');
      if (taskProgress) taskProgress.hidden = true;
    } finally {
      updateButton.disabled = false;
    }
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
      setStatus(adminStatus, 'Team emails saved.', 'success');
    } catch (error) {
      setStatus(adminStatus, error.message || 'Could not save team emails.', 'error');
    }
  };

  setupTabs();
  updateButton?.addEventListener('click', loadTasks);
  adminForm?.addEventListener('submit', saveMembers);
  taskList?.addEventListener('click', (event) => {
    const button = event.target.closest('[data-team-dashboard-toggle]');
    if (!button) return;
    const panelId = button.dataset.teamDashboardToggle || '';
    const panel = taskList.querySelector(`[data-team-dashboard-panel-id="${CSS.escape(panelId)}"]`);
    if (!panel) return;
    const nextHidden = !panel.hidden ? true : false;
    panel.hidden = nextHidden;
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

    const reviewButton = event.target.closest('[data-prd-review]');
    const button = reviewButton;
    if (!button) return;
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
    button.textContent = 'Reviewing...';
    panelRow.hidden = false;
    panel.innerHTML = '<div class="team-dashboard-review-loading">Reviewing PRD...</div>';
    try {
      const response = await fetch('/api/team-dashboard/prd-review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ jira_id: jiraId, jira_link: jiraLink, prd_url: prdUrl, force_refresh: forceRefresh }),
      });
      const payload = await readJson(response, 'Could not review PRD.');
      const result = payload.review || {};
      panel.innerHTML = `
        <div class="team-dashboard-review-meta">
          <strong>${escapeHtml(payload.cached ? 'Cached PRD Review' : 'PRD Review')}</strong>
          <span>${escapeHtml(result.updated_at || '')}</span>
        </div>
        <div class="team-dashboard-review-markdown">${renderMarkdown(result.result_markdown || '')}</div>
        <div class="team-dashboard-review-actions">
          <button class="button button-secondary team-dashboard-review-refresh" type="button" data-prd-refresh>Regenerate</button>
        </div>
      `;
      button.textContent = 'View Review';
      if (toggleButton) {
        toggleButton.hidden = false;
        toggleButton.textContent = 'Hide Review';
      }
      panel.querySelector('[data-prd-refresh]')?.addEventListener('click', () => {
        button.dataset.forceRefresh = 'true';
        button.click();
      });
    } catch (error) {
      panel.innerHTML = `<p class="productization-inline-status" data-tone="error">${escapeHtml(error.message || 'Could not review PRD.')}</p>`;
      button.textContent = 'Retry';
    } finally {
      button.disabled = false;
    }
  });
})();
