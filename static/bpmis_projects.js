(() => {
  const root = document.querySelector('[data-bpmis-projects]');
  if (!root) return;

  const projectsUrl = root.dataset.projectsUrl || '/api/bpmis-projects';
  const versionUrl = root.dataset.versionUrl || '/api/productization-upgrade-summary/versions';
  const body = root.querySelector('[data-bpmis-project-body]');
  const tableWrap = root.querySelector('[data-bpmis-project-table-wrap]');
  const empty = root.querySelector('[data-bpmis-project-empty]');
  const count = root.querySelector('[data-bpmis-project-count]');
  const status = root.querySelector('[data-bpmis-project-status]');
  const modal = document.querySelector('[data-jira-modal-backdrop]');
  const modalTitle = document.querySelector('[data-jira-wizard-title]');
  const modalKicker = document.querySelector('[data-jira-wizard-kicker]');
  const stepOne = document.querySelector('[data-jira-wizard-step-one]');
  const stepTwo = document.querySelector('[data-jira-wizard-step-two]');
  const componentList = document.querySelector('[data-jira-component-list]');
  const formList = document.querySelector('[data-jira-ticket-form-list]');
  const wizardStatus = document.querySelector('[data-jira-wizard-status]');
  const cancelButton = document.querySelector('[data-jira-wizard-cancel]');
  const backButton = document.querySelector('[data-jira-wizard-back]');
  const nextButton = document.querySelector('[data-jira-wizard-next]');
  const submitButton = document.querySelector('[data-jira-wizard-submit]');

  if (!body || !tableWrap || !empty || !count || !status || !modal || !modalTitle || !modalKicker || !stepOne || !stepTwo || !componentList || !formList || !wizardStatus || !cancelButton || !backButton || !nextButton || !submitButton) {
    return;
  }

  if (modal.parentElement !== document.body) {
    document.body.appendChild(modal);
  }

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const cssEscape = (value) => window.CSS && window.CSS.escape
    ? window.CSS.escape(String(value ?? ''))
    : String(value ?? '').replace(/["\\]/g, '\\$&');

  const externalHref = (value) => {
    const text = String(value || '').trim();
    if (!text) return '';
    if (/^(https?:|mailto:|tel:)/i.test(text)) return text;
    if (text.startsWith('//')) return `https:${text}`;
    return `https://${text}`;
  };

  const readJson = async (response, fallbackMessage) => {
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload.status === 'error') {
      throw new Error(payload.message || fallbackMessage);
    }
    return payload;
  };

  let projects = [];
  let activeProject = null;
  let activeOptions = null;
  let versionController = null;
  let expandedProjectId = '';

  const setStatus = (message, tone = 'neutral') => {
    status.textContent = message || '';
    status.dataset.tone = tone;
  };

  const setWizardStatus = (message, tone = 'neutral') => {
    wizardStatus.textContent = message || '';
    wizardStatus.dataset.tone = tone;
  };

  const ticketCount = (project) => Array.isArray(project?.jira_tickets) ? project.jira_tickets.length : 0;

  const ticketLabel = (countValue) => {
    if (!countValue) return 'Tasks';
    return `Tasks (${countValue})`;
  };

  const displayVersionName = (item) => {
    const rawName = item?.version_name || item?.fullName || item?.name || item?.versionName || item?.label || '';
    return String(rawName || '').replace(/\s+·\s+\d+\s*$/, '').trim();
  };

  const taskStatusClass = (value) => {
    const normalized = String(value || '').trim().toLowerCase();
    if (!normalized) return '';
    if (normalized.includes('loading')) return ' is-loading';
    if (normalized.includes('waiting')) return ' is-waiting';
    if (normalized.includes('done') || normalized.includes('closed') || normalized.includes('resolved')) return ' is-done';
    if (normalized.includes('progress') || normalized.includes('doing')) return ' is-progress';
    return '';
  };

  const displayTaskStatus = (value) => {
    const text = String(value || '').trim();
    if (!text || text.toLowerCase() === 'created') return 'Loading status';
    return text;
  };

  const taskMarkup = (tickets) => {
    if (!Array.isArray(tickets) || !tickets.length) {
      return '<div class="bpmis-task-empty">No Jira tasks created for this project yet.</div>';
    }
    const rows = tickets.map((ticket) => {
      const key = escapeHtml(ticket.ticket_key || ticket.ticket_link || 'Jira');
      const title = escapeHtml(ticket.live_jira_title || ticket.jira_title || '-');
      const rawStatus = displayTaskStatus(ticket.live_jira_status || ticket.status);
      const statusText = escapeHtml(rawStatus);
      const version = escapeHtml(ticket.live_fix_version || ticket.fix_version_name || '-');
      const market = escapeHtml(ticket.market || '-');
      const component = escapeHtml(ticket.component || '-');
      const link = ticket.ticket_link
        ? `<a href="${escapeHtml(ticket.ticket_link)}" target="_blank" rel="noreferrer">${key}</a>`
        : `<span>${key}</span>`;
      const liveError = ticket.live_error ? `<p class="bpmis-task-warning">${escapeHtml(ticket.live_error)}</p>` : '';
      return `
        <article class="bpmis-task-card" data-task-card="${escapeHtml(ticket.id || '')}">
          <div class="bpmis-task-cell bpmis-task-ticket" data-label="Ticket">${link}</div>
          <div class="bpmis-task-cell bpmis-task-title" data-label="Title" title="${title}">${title}</div>
          <div class="bpmis-task-cell" data-label="Market">${market}</div>
          <div class="bpmis-task-cell" data-label="Status">
            <span class="bpmis-task-status${taskStatusClass(rawStatus)}">${statusText}</span>
          </div>
          <div class="bpmis-task-cell" data-label="Version">${version}</div>
          <div class="bpmis-task-cell" data-label="Component">${component}</div>
          <div class="bpmis-task-cell bpmis-task-actions">
            <button class="button button-secondary danger-button bpmis-task-delink" type="button" data-delink-task="${escapeHtml(ticket.id || '')}" data-delink-project="${escapeHtml(ticket.bpmis_id || '')}">Delink</button>
          </div>
          ${liveError}
        </article>
      `;
    }).join('');
    return `
      <div class="bpmis-task-list">
        <div class="bpmis-task-list-head" aria-hidden="true">
          <span>Ticket</span>
          <span>Title</span>
          <span>Market</span>
          <span>Status</span>
          <span>Version</span>
          <span>Component</span>
          <span></span>
        </div>
        ${rows}
      </div>
    `;
  };

  const projectById = (bpmisId) => projects.find((project) => String(project.bpmis_id || '') === String(bpmisId || ''));

  const brdMarkup = (value) => {
    const links = String(value || '').split(/\n+/).map((item) => item.trim()).filter(Boolean);
    if (!links.length) return '-';
    return links.map((link, index) => `<a href="${escapeHtml(externalHref(link))}" target="_blank" rel="noreferrer">BRD ${index + 1}</a>`).join('<br>');
  };

  const renderProjects = () => {
    count.textContent = `${projects.length} project${projects.length === 1 ? '' : 's'}`;
    if (!projects.length) {
      tableWrap.hidden = true;
      empty.hidden = false;
      body.innerHTML = '';
      return;
    }
    tableWrap.hidden = false;
    empty.hidden = true;
    body.innerHTML = projects.map((project) => {
      const countValue = ticketCount(project);
      const taskButtonLabel = countValue ? `Expand ${countValue} Jira task${countValue === 1 ? '' : 's'}` : 'Expand Jira tasks';
      return `
      <tr class="bpmis-project-row" data-project-row="${escapeHtml(project.bpmis_id)}">
        <td>
          <button class="bpmis-task-toggle" type="button" data-toggle-tasks="${escapeHtml(project.bpmis_id)}" aria-expanded="false" aria-label="${escapeHtml(taskButtonLabel)}">+</button>
        </td>
        <td>${escapeHtml(project.bpmis_id || '-')}</td>
        <td>${escapeHtml(project.project_name || '-')}</td>
        <td>${brdMarkup(project.brd_link)}</td>
        <td>${escapeHtml(project.market || '-')}</td>
        <td>
          <div class="button-row bpmis-project-actions">
            <button class="button button-secondary" type="button" data-create-jira="${escapeHtml(project.bpmis_id)}">Create Jira</button>
            <button class="button button-secondary danger-button" type="button" data-delete-project="${escapeHtml(project.bpmis_id)}">Delete</button>
          </div>
        </td>
      </tr>
      <tr class="bpmis-task-row" data-task-row="${escapeHtml(project.bpmis_id)}" hidden>
        <td colspan="6">
          <div class="bpmis-task-panel" data-task-panel="${escapeHtml(project.bpmis_id)}">
            <div class="bpmis-task-loading">Loading Jira tasks...</div>
          </div>
        </td>
      </tr>
    `;
    }).join('');
  };

  const loadTasks = async (bpmisId, { force = false } = {}) => {
    const panel = body.querySelector(`[data-task-panel="${cssEscape(bpmisId)}"]`);
    if (!panel) return;
    const project = projectById(bpmisId);
    if (panel.dataset.loaded === 'true' && !force) {
      if (ticketCount(project) > 0 && panel.dataset.liveLoaded !== 'true') {
        void refreshLiveTasks(bpmisId, panel);
      }
      return;
    }
    panel.innerHTML = taskMarkup(Array.isArray(project?.jira_tickets) ? project.jira_tickets : []);
    panel.dataset.loaded = 'true';
    if (ticketCount(project) > 0) {
      void refreshLiveTasks(bpmisId, panel);
    }
  };

  const refreshLiveTasks = async (bpmisId, panel) => {
    if (panel.dataset.liveLoading === 'true') return;
    panel.dataset.liveLoading = 'true';
    try {
      const response = await fetch(`${projectsUrl}/${encodeURIComponent(bpmisId)}/jira-tickets?live=1`, {
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      const payload = await readJson(response, 'Could not load live Jira status.');
      const liveTickets = Array.isArray(payload.tickets) ? payload.tickets : [];
      panel.innerHTML = taskMarkup(liveTickets);
      panel.dataset.liveLoaded = 'true';
    } catch (error) {
      const warning = document.createElement('p');
      warning.className = 'bpmis-task-warning';
      warning.textContent = error.message || 'Could not load live Jira status.';
      panel.appendChild(warning);
    } finally {
      panel.dataset.liveLoading = 'false';
    }
  };

  const toggleTasks = async (button) => {
    const bpmisId = button.dataset.toggleTasks || '';
    const row = body.querySelector(`[data-task-row="${cssEscape(bpmisId)}"]`);
    if (!row) return;
    const willOpen = row.hidden;
    row.hidden = !willOpen;
    button.setAttribute('aria-expanded', willOpen ? 'true' : 'false');
    button.textContent = willOpen ? '-' : '+';
    expandedProjectId = willOpen ? bpmisId : '';
    if (willOpen) {
      await loadTasks(bpmisId);
    }
  };

  const loadProjects = async () => {
    try {
      setStatus('Loading BPMIS projects...');
      const response = await fetch(projectsUrl, { headers: { Accept: 'application/json' }, credentials: 'same-origin' });
      const payload = await readJson(response, 'Could not load BPMIS projects.');
      projects = Array.isArray(payload.projects) ? payload.projects : [];
      renderProjects();
      if (expandedProjectId) {
        const row = body.querySelector(`[data-task-row="${cssEscape(expandedProjectId)}"]`);
        const button = body.querySelector(`[data-toggle-tasks="${cssEscape(expandedProjectId)}"]`);
        if (row && button) {
          row.hidden = false;
          button.setAttribute('aria-expanded', 'true');
          button.textContent = '-';
          await loadTasks(expandedProjectId, { force: true });
        }
      }
      setStatus('');
    } catch (error) {
      setStatus(error.message || 'Could not load BPMIS projects.', 'error');
    }
  };

  const closeModal = () => {
    modal.hidden = true;
    modal.classList.remove('is-visible');
    document.body.classList.remove('modal-open');
    activeProject = null;
    activeOptions = null;
    componentList.innerHTML = '';
    formList.innerHTML = '';
    setWizardStatus('');
  };

  const showStep = (step) => {
    const onStepOne = step === 1;
    stepOne.hidden = !onStepOne;
    stepTwo.hidden = onStepOne;
    backButton.hidden = onStepOne;
    nextButton.hidden = !onStepOne;
    submitButton.hidden = onStepOne;
    modalTitle.textContent = onStepOne ? 'Select Components' : 'Review Jira Details';
  };

  const defaultTitle = (projectName, system) => `[Feature][${system}]${projectName || ''}`;

  const renderComponentChoices = () => {
    const components = activeOptions?.components || [];
    componentList.innerHTML = components.map((entry, index) => {
      const markets = entry.markets || [];
      const marketControl = markets.length > 1
        ? `<select data-component-market="${index}">${markets.map((market) => `<option value="${escapeHtml(market.market)}">${escapeHtml(market.market)} / ${escapeHtml(market.system)}</option>`).join('')}</select>`
        : `<input type="hidden" data-component-market="${index}" value="${escapeHtml(markets[0]?.market || '')}"><span class="field-badge">${escapeHtml(markets[0]?.market || '-')}</span>`;
      return `
        <label class="jira-component-option">
          <input type="checkbox" data-component-choice="${index}">
          <span>
            <strong>${escapeHtml(entry.component || '-')}</strong>
            ${marketControl}
          </span>
        </label>
      `;
    }).join('');
  };

  const selectedComponents = () => [...componentList.querySelectorAll('[data-component-choice]:checked')].map((checkbox) => {
    const index = Number(checkbox.dataset.componentChoice);
    const entry = activeOptions.components[index];
    const market = componentList.querySelector(`[data-component-market="${index}"]`)?.value || '';
    const route = (entry.markets || []).find((item) => item.market === market) || {};
    return {
      component: entry.component,
      market,
      system: route.system || '',
      fix_version: entry.defaults?.fix_version || '',
    };
  });

  const bindVersionSearch = (input, menu) => {
    let timer = null;
    input.addEventListener('input', () => {
      window.clearTimeout(timer);
      const query = input.value.trim();
      if (query.length < 2) {
        menu.hidden = true;
        menu.innerHTML = '';
        return;
      }
      timer = window.setTimeout(async () => {
        if (versionController) versionController.abort();
        versionController = new AbortController();
        try {
          const response = await fetch(`${versionUrl}?q=${encodeURIComponent(query)}`, {
            headers: { Accept: 'application/json' },
            credentials: 'same-origin',
            signal: versionController.signal,
          });
          const payload = await readJson(response, 'Could not search Fix Versions.');
          const items = Array.isArray(payload.items) ? payload.items : [];
          menu.hidden = false;
          menu.innerHTML = items.length
            ? items.slice(0, 8).map((item) => {
              const versionName = displayVersionName(item);
              return `<button type="button" data-version-name="${escapeHtml(versionName)}">${escapeHtml(versionName)}</button>`;
            }).join('')
            : '<div class="productization-typeahead-empty">No matching versions.</div>';
        } catch (error) {
          if (error.name !== 'AbortError') {
            menu.hidden = false;
            menu.innerHTML = `<div class="productization-typeahead-empty">${escapeHtml(error.message || 'Could not search Fix Versions.')}</div>`;
          }
        }
      }, 250);
    });
    menu.addEventListener('click', (event) => {
      const option = event.target.closest('[data-version-name]');
      if (!option) return;
      input.value = option.dataset.versionName || '';
      menu.hidden = true;
    });
  };

  const renderJiraForms = () => {
    const selections = selectedComponents();
    if (!selections.length) {
      setWizardStatus('Choose at least one Component.', 'error');
      return false;
    }
    formList.innerHTML = selections.map((item, index) => `
      <article class="jira-ticket-form" data-ticket-form="${index}" data-component="${escapeHtml(item.component)}" data-market="${escapeHtml(item.market)}">
        <div class="field-label-row">
          <h4>${escapeHtml(item.component)} / ${escapeHtml(item.market)}</h4>
          <span class="field-badge">${escapeHtml(item.system)}</span>
        </div>
        <input type="hidden" data-ticket-field="component" value="${escapeHtml(item.component)}">
        <input type="hidden" data-ticket-field="market" value="${escapeHtml(item.market)}">
        <div class="field-group">
          <label>Jira Title</label>
          <input data-ticket-field="jira_title" value="${escapeHtml(defaultTitle(activeProject.project_name, item.system))}">
        </div>
        <div class="field-grid">
          <div class="field-group productization-input-shell">
            <label>Fix Version/s</label>
            <input data-ticket-field="fix_version" value="${escapeHtml(item.fix_version)}" autocomplete="off">
            <div class="productization-typeahead jira-version-typeahead" data-version-menu hidden></div>
          </div>
          <div class="field-group">
            <label>PRD Link</label>
            <input data-ticket-field="prd_link" value="">
          </div>
        </div>
        <div class="field-group">
          <label>Description</label>
          <textarea data-ticket-field="description" rows="3"></textarea>
        </div>
      </article>
    `).join('');
    formList.querySelectorAll('[data-ticket-form]').forEach((formNode) => {
      const input = formNode.querySelector('[data-ticket-field="fix_version"]');
      const menu = formNode.querySelector('[data-version-menu]');
      if (input && menu) bindVersionSearch(input, menu);
    });
    setWizardStatus(`${selections.length} selected Component${selections.length === 1 ? '' : 's'} ready for Jira creation.`, 'neutral');
    showStep(2);
    modal.scrollTop = 0;
    return true;
  };

  const openCreateJira = async (bpmisId) => {
    activeProject = projects.find((project) => String(project.bpmis_id || '') === String(bpmisId || ''));
    if (!activeProject) return;
    modal.hidden = false;
    modal.classList.add('is-visible');
    document.body.classList.add('modal-open');
    modalKicker.textContent = `BPMIS ${activeProject.bpmis_id}`;
    showStep(1);
    setWizardStatus('Loading Component options...');
    try {
      const response = await fetch(`${projectsUrl}/${encodeURIComponent(bpmisId)}/jira-options`, {
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      activeOptions = await readJson(response, 'Could not load Jira options.');
      renderComponentChoices();
      setWizardStatus('');
    } catch (error) {
      setWizardStatus(error.message || 'Could not load Jira options.', 'error');
    }
  };

  const collectTicketItems = () => [...formList.querySelectorAll('[data-ticket-form]')].map((formNode) => {
    const read = (field) => formNode.querySelector(`[data-ticket-field="${field}"]`)?.value?.trim() || '';
    return {
      component: read('component'),
      market: read('market'),
      jira_title: read('jira_title'),
      fix_version: read('fix_version'),
      prd_link: read('prd_link'),
      description: read('description'),
    };
  });

  const submitJira = async () => {
    if (!activeProject) return;
    submitButton.disabled = true;
    backButton.disabled = true;
    cancelButton.disabled = true;
    submitButton.textContent = 'Creating...';
    setWizardStatus('Creating Jira tickets... Please wait.', 'info');
    try {
      const response = await fetch(`${projectsUrl}/${encodeURIComponent(activeProject.bpmis_id)}/jira-tickets`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ items: collectTicketItems() }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok && !Array.isArray(payload.results)) {
        throw new Error(payload.message || 'Could not create Jira tickets.');
      }
      const created = (payload.results || []).filter((item) => item.status === 'created').length;
      const errors = (payload.results || []).filter((item) => item.status === 'error');
      if (errors.length) {
        setWizardStatus(`${created} created, ${errors.length} failed: ${errors[0].message || 'validation failed'}`, created ? 'warning' : 'error');
      } else {
        setWizardStatus(`${created} Jira ticket${created === 1 ? '' : 's'} created successfully.`, 'success');
      }
      await loadProjects();
      if (created) {
        window.setTimeout(closeModal, 1800);
      }
    } catch (error) {
      setWizardStatus(error.message || 'Could not create Jira tickets.', 'error');
    } finally {
      submitButton.disabled = false;
      backButton.disabled = false;
      cancelButton.disabled = false;
      submitButton.textContent = 'Create Jira';
    }
  };

  const delinkTask = async (button) => {
    const bpmisId = button.dataset.delinkProject || '';
    const ticketId = button.dataset.delinkTask || '';
    if (!bpmisId || !ticketId) return;
    if (!window.confirm('Delink this Jira from the BPMIS project?')) return;
    button.disabled = true;
    try {
      const response = await fetch(`${projectsUrl}/${encodeURIComponent(bpmisId)}/jira-tickets/${encodeURIComponent(ticketId)}`, {
        method: 'DELETE',
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      await readJson(response, 'Could not delink Jira task.');
      expandedProjectId = bpmisId;
      await loadProjects();
      setStatus('Jira task delinked from BPMIS project.', 'success');
    } catch (error) {
      setStatus(error.message || 'Could not delink Jira task.', 'error');
    } finally {
      button.disabled = false;
    }
  };

  body.addEventListener('click', async (event) => {
    const createButton = event.target.closest('[data-create-jira]');
    if (createButton) {
      await openCreateJira(createButton.dataset.createJira || '');
      return;
    }
    const taskButton = event.target.closest('[data-toggle-tasks]');
    if (taskButton) {
      await toggleTasks(taskButton);
      return;
    }
    const delinkButton = event.target.closest('[data-delink-task]');
    if (delinkButton) {
      await delinkTask(delinkButton);
      return;
    }
    const deleteButton = event.target.closest('[data-delete-project]');
    if (!deleteButton) return;
    const bpmisId = deleteButton.dataset.deleteProject || '';
    if (!window.confirm(`Delete BPMIS project ${bpmisId} from this portal?`)) return;
    deleteButton.disabled = true;
    try {
      const response = await fetch(`${projectsUrl}/${encodeURIComponent(bpmisId)}`, {
        method: 'DELETE',
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      await readJson(response, 'Could not delete BPMIS project.');
      await loadProjects();
    } catch (error) {
      setStatus(error.message || 'Could not delete BPMIS project.', 'error');
    } finally {
      deleteButton.disabled = false;
    }
  });

  nextButton.addEventListener('click', renderJiraForms);
  backButton.addEventListener('click', () => showStep(1));
  submitButton.addEventListener('click', submitJira);
  cancelButton.addEventListener('click', closeModal);
  modal.addEventListener('click', (event) => {
    if (event.target === modal) closeModal();
  });
  window.addEventListener('bpmis-job-completed', loadProjects);
  loadProjects();
})();
