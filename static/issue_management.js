(() => {
  const root = document.querySelector('[data-issue-management]');
  if (!root) return;

  const STORAGE_KEY = 'risk-pm:issue-management:v1-demo';
  const today = new Date().toISOString().slice(0, 10);
  const isCreatePage = root.dataset.issueMode === 'create';
  const isEditPage = root.dataset.issueMode === 'edit';
  const isViewPage = root.dataset.issueMode === 'view';
  const isActionPlanViewPage = root.dataset.issueMode === 'action-plan-view';
  const MAX_DOCUMENTS = 100;
  const MAX_DOCUMENT_SIZE = 20 * 1024 * 1024;
  const ACCEPTED_EXTENSIONS = new Set(['pdf', 'docx', 'txt', 'csv', 'xlsx', 'jpg', 'jpeg', 'ppt', 'pptx', 'zip']);
  const issueTypes = [
    ['Self-Assessment', 'Self-Assessment'],
    ['Outsourcing', 'Outsourcing'],
    ['External Audit', 'External Audit'],
    ['Internal Audit', 'Internal Audit'],
    ['Compliance Assurance', 'Compliance Assurance'],
  ];
  const typeCodes = {
    'Self-Assessment': 'SA',
    Outsourcing: 'OS',
    'External Audit': 'EA',
    'Internal Audit': 'IA',
    'Compliance Assurance': 'CA',
  };
  const seed = [
    { issue_id: 'SA2026-IS00004', creator: 'Marketing', impacted_unit: 'Marketing', type: 'Self-Assessment', impact: 'Medium-High', date_of_issue: '2026-01-12', title: 'Q1 customer due diligence evidence gap', description: 'Required evidence was not consistently retained for the sampled onboarding cases.', parties: 'Marketing, Compliance', revised_tcd: '2026-09-05', documents: [], status: 'Open', action_plans: [{ ap_id: 'SA2026-AP00006', owner: 'Marketing', title: 'Complete evidence retention checklist', description: 'Publish the checklist and complete the outstanding sample review.', tcd: '2026-08-28', status: 'Open', documents: [] }] },
    { issue_id: 'EA2026-IS00003', creator: 'Operations', impacted_unit: 'Operations', type: 'External Audit', impact: 'High', date_of_issue: '2026-02-03', title: 'External audit observation: access recertification', description: 'Quarterly access recertification evidence needs a single accountable owner.', parties: 'Operations, IT Controls', documents: [], status: 'Pending Approval', action_plans: [{ ap_id: 'EA2026-AP00005', owner: 'Operations', title: 'Centralise access recertification evidence', description: 'Create the evidence pack and approval checklist.', tcd: '2026-09-14', status: 'Pending Approval', documents: [] }] },
    { issue_id: 'IA2026-IS00002', creator: 'Internal Audit', impacted_unit: 'Credit Risk', type: 'Internal Audit', impact: 'High', date_of_issue: '2026-03-21', title: 'Credit risk override monitoring follow-up', description: 'The monitoring control requires a documented escalation path for repeated overrides.', parties: 'Internal Audit, Credit Risk', documents: [], status: 'Draft', action_plans: [{ ap_id: 'IA2026-AP00003', owner: 'Credit Risk', title: 'Define override escalation playbook', description: 'Document trigger thresholds and evidence expectations.', tcd: '2026-10-02', status: 'Draft', documents: [] }] },
    { issue_id: 'CA2026-IS00001', creator: 'Compliance & AML/CTF Assurance', impacted_unit: 'Fraud Risk', type: 'Compliance Assurance', impact: 'Medium-High', date_of_issue: '2026-04-18', title: 'KYC quality assurance sampling variance', description: 'Sampling results show inconsistent closure evidence between review teams.', parties: 'Compliance Assurance, Fraud Risk', documents: [], status: 'Open', action_plans: [{ ap_id: 'CA2026-AP00001', owner: 'Fraud Risk', title: 'Standardise KYC QA closure evidence', description: 'Add a closure evidence checklist to the operating procedure.', tcd: '2026-09-30', status: 'Open', documents: [] }] },
    { issue_id: 'OS2025-IS00009', creator: 'Operations', impacted_unit: 'Operations', type: 'Outsourcing', impact: 'Medium-Low', date_of_issue: '2025-11-07', title: 'Outsourcing incident action tracking', description: 'Vendor incident follow-up actions were completed and evidenced.', parties: 'Operations, Vendor Management', documents: [], status: 'Closed', action_plans: [{ ap_id: 'OS2025-AP00013', owner: 'Operations', title: 'Close vendor incident actions', description: 'Archive evidence and record final owner sign-off.', tcd: '2026-01-15', status: 'Closed', documents: [] }] },
    { issue_id: 'SA2026-IS00001', creator: 'Fraud Risk', impacted_unit: 'Fraud Risk', type: 'Self-Assessment', impact: '', date_of_issue: '2026-08-09', title: '', description: '', parties: '', documents: [], status: 'Draft', action_plans: [] },
  ];

  const $ = (selector) => root.querySelector(selector);
  const $$ = (selector) => [...root.querySelectorAll(selector)];
  const clone = (value) => JSON.parse(JSON.stringify(value));
  const escapeHtml = (value) => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;');
  const replaceLegacyFunctionalUnit = (value) => typeof value === 'string' ? value.replaceAll('Retail Finance', 'Marketing') : value;
  const migrateFunctionalUnits = (records) => records.map((issue) => ({
    ...issue,
    creator: replaceLegacyFunctionalUnit(issue.creator),
    impacted_unit: replaceLegacyFunctionalUnit(issue.impacted_unit),
    parties: replaceLegacyFunctionalUnit(issue.parties),
    action_plans: (issue.action_plans || []).map((ap) => ({ ...ap, owner: replaceLegacyFunctionalUnit(ap.owner) })),
  }));
  const readState = () => {
    try {
      const parsed = JSON.parse(window.localStorage.getItem(STORAGE_KEY) || '');
      return Array.isArray(parsed) ? migrateFunctionalUnits(parsed) : clone(seed);
    } catch (_error) { return clone(seed); }
  };
  let issues = readState();
  let editingId = '';
  let draftActionPlans = [];
  let selectedDocuments = [];
  let selectedApDocuments = [];
  let editingApIndex = -1;
  let viewEditing = false;
  let currentPage = 1;
  let pageSize = 10;
  let sortKey = 'issue_id';
  let sortOrder = 'descend';
  let toastTimer = null;
  let validationToastTimer = null;
  let pendingWithdrawal = null;
  let formDirty = false;

  const saveState = () => window.localStorage.setItem(STORAGE_KEY, JSON.stringify(issues));
  const codeFor = (type) => typeCodes[type] || 'IS';
  const statusClass = (status) => ({ 'Draft': 'issue-status-draft', 'Pending Approval': 'issue-status-pending', Open: 'issue-status-open', 'Reopen (Open) - Pending Approval': 'issue-status-pending', 'Reopen (Draft) - Pending Approval': 'issue-status-pending', Closed: 'issue-status-closed', 'Withdraw - Pending Approval': 'issue-status-pending', Withdrawn: 'issue-status-withdrawn' }[status] || 'issue-status-draft');
  const issueTcd = (issue) => {
    const dates = (issue?.action_plans || []).filter((ap) => String(ap.status || '').toLowerCase() !== 'withdrawn' && ap.tcd).map((ap) => ap.tcd).sort();
    return dates[dates.length - 1] || '';
  };
  const isActionPlanOverdue = (ap) => Boolean(ap?.tcd && ap.status !== 'Closed' && ap.status !== 'Withdrawn' && ap.tcd < today);
  const isOverdue = (issue) => Boolean(issueTcd(issue) && issue.status !== 'Closed' && issue.status !== 'Withdrawn' && issueTcd(issue) < today);
  const formatDate = (value) => value ? new Intl.DateTimeFormat('en-SG', { day: '2-digit', month: 'short', year: 'numeric' }).format(new Date(`${value}T00:00:00`)) : '-';
  const formatOverviewDate = (value) => {
    if (!value) return '-';
    const [year, month, day] = String(value).slice(0, 10).split('-');
    return year && month && day ? `${day}-${month}-${year}` : '-';
  };
  const formatFileSize = (bytes) => {
    if (!bytes) return '0 B';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };
  const showToast = (message) => {
    const node = $('[data-issue-toast]');
    if (!node) return;
    node.textContent = message;
    node.hidden = false;
    window.clearTimeout(toastTimer);
    toastTimer = window.setTimeout(() => { node.hidden = true; }, 5000);
  };
  const showValidationToast = (message) => {
    const node = $('[data-validation-toast]');
    const text = $('[data-validation-toast-message]');
    if (!node || !text) return;
    text.textContent = message;
    node.hidden = false;
    window.clearTimeout(validationToastTimer);
    validationToastTimer = window.setTimeout(() => { node.hidden = true; }, 5000);
  };

  const closeSubmitConfirmation = () => {
    const modal = $('[data-submit-confirm]');
    if (modal) modal.hidden = true;
    document.body.classList.remove('modal-open');
  };
  const openSubmitConfirmation = () => {
    const modal = $('[data-submit-confirm]');
    if (!modal) return;
    modal.hidden = false;
    document.body.classList.add('modal-open');
  };
  const closeCancelConfirmation = () => {
    const modal = $('[data-cancel-confirm]');
    if (modal) modal.hidden = true;
    document.body.classList.remove('modal-open');
  };
  const openCancelConfirmation = () => {
    const modal = $('[data-cancel-confirm]');
    if (!modal) return;
    modal.hidden = false;
    document.body.classList.add('modal-open');
  };
  const navigateToOverview = () => window.location.assign(root.dataset.overviewUrl || '/issue-management');
  const navigateAfterCancel = () => {
    if (isEditPage) {
      const viewUrl = root.dataset.viewUrl?.replace('__ISSUE_ID__', encodeURIComponent(root.dataset.editIssueId || editingId || ''));
      if (viewUrl) {
        window.location.assign(viewUrl);
        return;
      }
    }
    navigateToOverview();
  };
  const markFormDirty = () => {
    if (isCreatePage || isEditPage || (isViewPage && viewEditing)) formDirty = true;
  };
  const handleFormBack = () => {
    if (formDirty) openCancelConfirmation();
    else navigateAfterCancel();
  };
  const closeWithdrawalModals = () => {
    ['[data-withdraw-justification]', '[data-withdraw-confirmation]'].forEach((selector) => {
      const modal = $(selector);
      if (modal) modal.hidden = true;
    });
    pendingWithdrawal = null;
    document.body.classList.remove('modal-open');
  };
  const openWithdrawJustification = (kind, index = -1) => {
    const issue = editingId ? issues.find((candidate) => candidate.issue_id === editingId) : null;
    const isActionPlan = kind === 'action_plan';
    const ap = isActionPlan ? issue?.action_plans?.[index] : null;
    if (!issue || issue.status !== 'Draft' || (isActionPlan && (!ap || ap.status !== 'Draft'))) return;
    pendingWithdrawal = { kind, index };
    const label = isActionPlan ? 'Action Plan' : 'Issue';
    $('[data-withdraw-justification-title]').textContent = `Withdraw ${label}`;
    $('[data-withdraw-justification-input]').value = '';
    $('[data-withdraw-justification-error]').hidden = true;
    $('[data-withdraw-justification]').hidden = false;
    document.body.classList.add('modal-open');
  };
  const openWithdrawConfirmation = () => {
    if (!pendingWithdrawal) return;
    const label = pendingWithdrawal.kind === 'action_plan' ? 'Action Plan' : 'Issue';
    const title = $('[data-withdraw-confirmation-title]');
    if (title) title.textContent = 'Confirm Withdrawal';
    $('[data-withdraw-confirmation-message]').textContent = `Confirm to Withdraw ${label}? Once ${label} is withdrawn, it is irreversible.`;
    $('[data-withdraw-justification]').hidden = true;
    $('[data-withdraw-confirmation]').hidden = false;
  };
  const finalizeWithdrawal = () => {
    if (!pendingWithdrawal) return;
    const { kind, index, justification } = pendingWithdrawal;
    const issue = editingId ? issues.find((candidate) => candidate.issue_id === editingId) : null;
    if (!issue || issue.status !== 'Draft') { closeWithdrawalModals(); return; }
    if (kind === 'action_plan') {
      const ap = issue.action_plans?.[index];
      if (!ap || ap.status !== 'Draft') { closeWithdrawalModals(); return; }
      ap.status = 'Withdrawn';
      ap.withdrawal_date = today;
      ap.withdrawal_justification = justification;
      draftActionPlans = clone(issue.action_plans);
      syncIssueTargetDateFields(issue, draftActionPlans);
      saveState();
      closeWithdrawalModals();
      if (isActionPlanViewPage) initializeActionPlanViewPage(); else renderActionPlans(viewEditing);
      showToast(`${ap.ap_id || 'Action Plan'} withdrawn.`);
      return;
    }
    issue.status = 'Withdrawn';
    issue.withdrawal_date = today;
    issue.withdrawal_justification = justification;
    saveState();
    closeWithdrawalModals();
    window.location.assign(root.dataset.overviewUrl || '/issue-management');
  };

  const multiSelectNode = (key) => root.querySelector(`[data-search-multi="${key}"]`);
  const multiSelectValues = (key) => {
    const node = multiSelectNode(key);
    return node ? [...node.querySelectorAll('[data-search-multi-option]:checked')].map((option) => option.value) : [];
  };
  const closeMultiSelectMenus = (except = null) => {
    $$('[data-search-multi]').forEach((node) => {
      if (node === except) return;
      const trigger = node.querySelector('[data-search-multi-trigger]');
      const menu = node.querySelector('[data-search-multi-menu]');
      if (menu) menu.hidden = true;
      if (trigger) trigger.setAttribute('aria-expanded', 'false');
    });
  };
  const renderMultiSelect = (node) => {
    if (!node) return;
    const selected = [...node.querySelectorAll('[data-search-multi-option]:checked')].map((option) => ({
      value: option.value,
      label: option.closest('.issue-multi-select-option')?.querySelector('span')?.textContent.trim() || option.value,
    }));
    const trigger = node.querySelector('[data-search-multi-trigger]');
    const label = node.querySelector('[data-search-multi-label]');
    const placeholder = node.dataset.placeholder || 'Select';
    if (label) {
      label.innerHTML = selected.length
        ? `${selected.slice(0, 2).map((item) => `<span class="issue-multi-select-tag">${escapeHtml(item.label)}</span>`).join('')}${selected.length > 2 ? `<span class="issue-multi-select-tag">+${selected.length - 2}</span>` : ''}`
        : escapeHtml(placeholder);
    }
    if (trigger) {
      trigger.classList.toggle('issue-filter-empty', !selected.length);
      trigger.title = selected.length ? selected.map((item) => item.label).join(', ') : placeholder;
      trigger.setAttribute('aria-label', `${placeholder}: ${selected.length ? selected.map((item) => item.label).join(', ') : 'none selected'}`);
    }
  };
  const setMultiSelectOptions = (key, options) => {
    const node = multiSelectNode(key);
    const menu = node?.querySelector('[data-search-multi-menu]');
    if (!node || !menu) return;
    const selected = new Set(multiSelectValues(key));
    menu.innerHTML = options.map(({ value, label }) => `<label class="issue-multi-select-option"><input type="checkbox" data-search-multi-option value="${escapeHtml(value)}"${selected.has(value) ? ' checked' : ''}><span>${escapeHtml(label)}</span></label>`).join('');
    menu.querySelectorAll('[data-search-multi-option]').forEach((option) => option.addEventListener('change', () => { renderMultiSelect(node); syncSearchFilterTone(); }));
    renderMultiSelect(node);
  };
  const initializeMultiSelectFilters = () => {
    $$('[data-search-multi]').forEach((node) => {
      const trigger = node.querySelector('[data-search-multi-trigger]');
      if (trigger) trigger.addEventListener('click', (event) => {
        event.preventDefault();
        const menu = node.querySelector('[data-search-multi-menu]');
        const expanded = menu ? menu.hidden : true;
        closeMultiSelectMenus(node);
        if (menu) menu.hidden = !expanded;
        trigger.setAttribute('aria-expanded', String(expanded));
      });
      node.querySelectorAll('[data-search-multi-option]').forEach((option) => option.addEventListener('change', () => { renderMultiSelect(node); syncSearchFilterTone(); }));
      renderMultiSelect(node);
    });
    root.addEventListener('click', (event) => { if (!event.target.closest('[data-search-multi]')) closeMultiSelectMenus(); });
  };
  const searchFilterSelector = '[data-search-id], [data-search-title], [data-search-issue-date-from], [data-search-issue-date-to], [data-search-tcd-from], [data-search-tcd-to], [data-search-rtcd-from], [data-search-rtcd-to]';
  const syncSearchFilterTone = () => {
    $$(searchFilterSelector).forEach((node) => node.classList.toggle('issue-filter-empty', !node.value));
    $$('[data-search-multi]').forEach((node) => {
      const trigger = node.querySelector('[data-search-multi-trigger]');
      if (trigger) trigger.classList.toggle('issue-filter-empty', !node.querySelector('[data-search-multi-option]:checked'));
    });
  };
  const populateFilters = () => {
    setMultiSelectOptions('creator', allFunctionalUnits.map((value) => ({ value, label: value })));
    setMultiSelectOptions('unit', allFunctionalUnits.map((value) => ({ value, label: value })));
    $$('[data-search-multi]').forEach(renderMultiSelect);
    syncSearchFilterTone();
  };
  const sortDateValue = (left, right) => {
    const a = left || null;
    const b = right || null;
    if (a === null && b === null) return 0;
    if (a === null) return 1;
    if (b === null) return -1;
    if (a === b) return 0;
    return a < b ? -1 : 1;
  };
  const compareIssuesForOverview = (left, right) => {
    if (sortKey === 'date_of_issue') return sortDateValue(left.date_of_issue, right.date_of_issue);
    if (sortKey === 'tcd') return sortDateValue(issueTcd(left), issueTcd(right));
    if (sortKey === 'revised_tcd') return sortDateValue(left.revised_tcd, right.revised_tcd);
    return String(left.issue_id || '').localeCompare(String(right.issue_id || ''));
  };
  const updateSortIndicators = () => {
    $$('[data-issue-sort]').forEach((button) => {
      const active = sortKey === button.dataset.issueSort;
      const order = active ? sortOrder : 'none';
      const indicator = button.querySelector('.issue-sort-indicator');
      const header = button.closest('th');
      button.dataset.sortOrder = order;
      button.setAttribute('aria-label', `Sort by ${button.querySelector('span')?.textContent || 'date'} ${order === 'ascend' ? 'descending' : 'ascending'}`);
      if (header) header.setAttribute('aria-sort', order === 'ascend' ? 'ascending' : order === 'descend' ? 'descending' : 'none');
      if (indicator) indicator.dataset.sortOrder = order;
    });
  };
  const toggleOverviewSort = (key) => {
    if (sortKey !== key) {
      sortKey = key;
      sortOrder = 'ascend';
    } else if (sortOrder === 'ascend') {
      sortOrder = 'descend';
    } else {
      sortKey = 'issue_id';
      sortOrder = 'descend';
    }
    currentPage = 1;
    updateSortIndicators();
    renderRows();
  };
  const filteredIssues = () => {
    const issueId = ($('[data-search-id]').value || '').trim().toLowerCase();
    const issueTitle = ($('[data-search-title]').value || '').trim().toLowerCase();
    const creator = multiSelectValues('creator');
    const unit = multiSelectValues('unit');
    const type = multiSelectValues('type');
    const status = multiSelectValues('status');
    const impact = multiSelectValues('impact');
    const issueDateFrom = $('[data-search-issue-date-from]').value;
    const issueDateTo = $('[data-search-issue-date-to]').value;
    const tcdFrom = $('[data-search-tcd-from]').value;
    const tcdTo = $('[data-search-tcd-to]').value;
    const rtcdFrom = $('[data-search-rtcd-from]').value;
    const rtcdTo = $('[data-search-rtcd-to]').value;
    const inRange = (value, from, to) => (!from || (value && value >= from)) && (!to || (value && value <= to));
    return issues.filter((issue) => {
      const issueIdMatch = !issueId || String(issue.issue_id || '').toLowerCase().includes(issueId);
      const issueTitleMatch = !issueTitle || String(issue.title || '').toLowerCase().includes(issueTitle);
      const matchesAny = (selected, value) => !selected.length || selected.includes(value);
      return issueIdMatch && issueTitleMatch && matchesAny(creator, issue.creator) && matchesAny(unit, issue.impacted_unit) && matchesAny(type, issue.type) && matchesAny(status, issue.status) && matchesAny(impact, issue.impact) && inRange(issue.date_of_issue, issueDateFrom, issueDateTo) && inRange(issueTcd(issue), tcdFrom, tcdTo) && inRange(issue.revised_tcd || '', rtcdFrom, rtcdTo);
    }).sort((a, b) => {
      const comparison = compareIssuesForOverview(a, b);
      return sortOrder === 'ascend' ? comparison : -comparison;
    });
  };
  const renderRows = () => {
    const allRows = filteredIssues();
    const totalPages = Math.max(1, Math.ceil(allRows.length / pageSize));
    currentPage = Math.min(currentPage, totalPages);
    const start = (currentPage - 1) * pageSize;
    const rows = allRows.slice(start, start + pageSize);
    $('[data-issue-empty]').hidden = allRows.length > 0;
    const totalItems = $('[data-total-items]');
    if (totalItems) totalItems.textContent = `Total ${allRows.length} items`;
    const pageNumber = $('[data-page-number]');
    if (pageNumber) pageNumber.textContent = String(currentPage);
    const previousPage = $('[data-prev-page]');
    if (previousPage) previousPage.disabled = currentPage <= 1;
    const nextPage = $('[data-next-page]');
    if (nextPage) nextPage.disabled = currentPage >= totalPages;
    $('[data-issue-rows]').innerHTML = rows.map((issue, index) => {
      const tcd = issueTcd(issue);
      return `<tr>
        <td>${start + index + 1}</td><td><button class="issue-id-link" type="button" data-open-issue="${escapeHtml(issue.issue_id)}">${escapeHtml(issue.issue_id)}</button></td><td>${escapeHtml(issue.type || '-')}</td><td>${escapeHtml(issue.creator || '-')}</td><td>${escapeHtml(issue.impacted_unit || '-')}</td>
        <td class="issue-title-cell" title="${escapeHtml(issue.title || 'Untitled draft')}">${escapeHtml(issue.title || 'Untitled draft')}</td>
        <td>${formatOverviewDate(issue.date_of_issue)}</td><td>${escapeHtml(issue.impact || '-')}</td><td><span class="issue-status-pill ${statusClass(issue.status)}">${escapeHtml(issue.status)}</span></td>
        <td>${formatOverviewDate(tcd)}</td><td>${formatOverviewDate(issue.revised_tcd)}</td><td class="${isOverdue(issue) ? 'issue-overdue' : 'issue-on-track'}">${tcd ? (isOverdue(issue) ? 'Yes' : 'No') : '-'}</td>
      </tr>`;
    }).join('');
    $$('[data-open-issue]').forEach((button) => button.addEventListener('click', () => {
      const viewUrl = root.dataset.viewUrl?.replace('__ISSUE_ID__', encodeURIComponent(button.dataset.openIssue));
      if (viewUrl) window.location.assign(viewUrl);
      else openModal(button.dataset.openIssue);
    }));
  };
  const renderOverview = () => { populateFilters(); updateSortIndicators(); renderRows(); };

  const formFieldMap = {
    issueId: '[data-form-issue-id]', status: '[data-form-status]', creator: '[data-form-creator]', type: '[data-form-type]', unit: '[data-form-unit]', impact: '[data-form-impact]', date: '[data-form-date]', tcd: '[data-form-tcd]', rtcd: '[data-form-rtcd]', rtcdCount: '[data-form-rtcd-count]', withdrawDate: '[data-form-withdraw-date]', withdrawJustification: '[data-form-withdraw-justification]', overdue: '[data-form-overdue]', title: '[data-form-title]', description: '[data-form-description]', parties: '[data-form-parties]', documents: '[data-form-documents]',
  };
  const apFieldMap = {
    owner: '[data-ap-form-owner]', id: '[data-ap-form-id]', status: '[data-ap-form-status]', title: '[data-ap-form-title]', description: '[data-ap-form-description]', tcd: '[data-ap-form-tcd]', rtcd: '[data-ap-form-rtcd]', overdue: '[data-ap-form-overdue]', rtcdCount: '[data-ap-form-rtcd-count]', withdrawDate: '[data-ap-form-withdraw-date]', withdrawJustification: '[data-ap-form-withdraw-justification]', documents: '[data-ap-form-documents]',
  };
  const setField = (key, value) => { const node = $(formFieldMap[key]); if (node) node.value = value || ''; };
  const getField = (key) => { const node = $(formFieldMap[key]); return node ? node.value.trim() : ''; };
  const setApField = (key, value) => { const node = $(apFieldMap[key]); if (node) node.value = value || ''; };
  const getApField = (key) => { const node = $(apFieldMap[key]); return node ? node.value.trim() : ''; };
  const syncIssueTargetDateFields = (issue = null, actionPlans = null) => {
    const current = issue || (editingId ? issues.find((candidate) => candidate.issue_id === editingId) : null);
    const source = current
      ? { ...current, action_plans: actionPlans || current.action_plans || [] }
      : { status: 'Draft', action_plans: actionPlans || draftActionPlans };
    const tcd = issueTcd(source);
    setField('tcd', tcd);
    setField('overdue', tcd ? (isOverdue(source) ? 'Yes' : 'No') : '-');
  };
  const syncIssueWithdrawalFields = (issue) => {
    const visible = issue?.status === 'Withdrawn';
    $$('[data-issue-withdraw-field]').forEach((node) => { node.hidden = !visible; });
    setField('withdrawDate', visible ? (issue?.withdrawal_date || today) : '');
    setField('withdrawJustification', visible ? (issue?.withdrawal_justification || 'Withdrawn by the Issue Creator.') : '');
  };
  const syncActionPlanWithdrawalFields = (ap) => {
    const visible = ap?.status === 'Withdrawn';
    $$('[data-ap-withdraw-field]').forEach((node) => { node.hidden = !visible; });
    setApField('withdrawDate', visible ? (ap?.withdrawal_date || today) : '');
    setApField('withdrawJustification', visible ? (ap?.withdrawal_justification || 'Withdrawn by the Action Plan Owner.') : '');
  };
  const setFormError = (message = '') => {
    const node = $('[data-form-error]');
    if (node) {
      node.textContent = message;
      node.hidden = true;
    }
    if (message) showValidationToast(message);
  };
  const setApFormError = (message = '') => { const node = $('[data-ap-form-error]'); if (node) { node.textContent = message; node.hidden = !message; } };

  const allowedIssueTypesForCreator = (creator) => {
    const common = ['Self-Assessment', 'Outsourcing', 'External Audit'];
    if (creator === 'Internal Audit') return ['Internal Audit', ...common];
    if (creator === 'Compliance & AML/CTF Assurance') return ['Compliance Assurance', ...common];
    return common;
  };
  const syncIssueTypeOptions = () => {
    const select = $(formFieldMap.type);
    if (!select) return;
    const current = select.value;
    const allowed = allowedIssueTypesForCreator(getField('creator'));
    select.innerHTML = `<option value="">Select Issue Type</option>${issueTypes.filter(([value]) => allowed.includes(value)).map(([value, label]) => `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`).join('')}`;
    select.value = allowed.includes(current) ? current : '';
  };
  const allFunctionalUnits = ['Credit Risk', 'Fraud Risk', 'Operations', 'Marketing', 'Internal Audit', 'Compliance & AML/CTF Assurance'];
  const syncActionPlanOwnerOptions = (editable = true) => {
    const select = $(apFieldMap.owner);
    if (!select) return;
    const current = select.value;
    const creator = getField('creator');
    const type = getField('type');
    const fixedOwner = ['Self-Assessment', 'Outsourcing', 'External Audit'].includes(type) && creator;
    const options = fixedOwner ? [creator] : allFunctionalUnits;
    select.innerHTML = `<option value="">Select Owner FU</option>${options.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join('')}`;
    select.value = fixedOwner ? creator : (options.includes(current) ? current : '');
    select.disabled = !editable || Boolean(fixedOwner);
  };
  const syncTypeDrivenFields = () => {
    syncIssueTypeOptions();
    const type = getField('type');
    const creator = getField('creator');
    const unit = $(formFieldMap.unit);
    const fixedImpactedUnit = ['Self-Assessment', 'Outsourcing', 'External Audit'].includes(type) && creator;
    if (fixedImpactedUnit) unit.value = creator;
    const issue = editingId ? issues.find((candidate) => candidate.issue_id === editingId) : null;
    const editableContext = isCreatePage || (!isViewPage && (!editingId || issue?.status === 'Draft')) || (isViewPage && viewEditing);
    unit.disabled = Boolean(fixedImpactedUnit) || !editableContext || Boolean(issue && issue.status !== 'Draft');
    syncActionPlanOwnerOptions();
  };
  const syncIssueIdentityFieldLocks = () => {
    if (!isCreatePage && !isEditPage) return;
    const creator = $(formFieldMap.creator);
    const type = $(formFieldMap.type);
    const issueId = getField('issueId');
    const hasCreatedIssueId = Boolean(issueId && issueId !== 'Generated after Save or Submit');
    // The creator is fixed by the picker step.  Issue Type remains selectable
    // until the first Save/Submit creates the Issue ID, then both identity
    // fields are immutable on this create/edit surface.
    if (creator) creator.disabled = true;
    if (type) type.disabled = hasCreatedIssueId;
  };

  const renderDocumentList = (selector, docs, removeAttribute, editable) => {
    const node = $(selector);
    if (!node) return;
    node.innerHTML = docs.length ? docs.map((doc, index) => `<div class="issue-document-item"><span>📎 ${escapeHtml(doc.name)} <small>${formatFileSize(doc.size)}</small></span>${editable ? `<button type="button" data-${removeAttribute}="${index}" aria-label="Remove ${escapeHtml(doc.name)}">×</button>` : ''}</div>`).join('') : '<span class="issue-document-empty">No documents selected.</span>';
  };
  const documentError = (selector, message = '') => { const node = $(selector); if (node) { node.textContent = message; node.hidden = !message; } };
  const fileMetadata = (file) => ({ name: file.name, size: file.size, type: file.type || '' });
  const validateFiles = (files, existingCount) => {
    if (existingCount + files.length > MAX_DOCUMENTS) return `You can upload up to ${MAX_DOCUMENTS} files.`;
    for (const file of files) {
      const extension = file.name.toLowerCase().split('.').pop();
      if (!ACCEPTED_EXTENSIONS.has(extension)) return `${file.name} is not a supported document type.`;
      if (file.size > MAX_DOCUMENT_SIZE) return `${file.name} exceeds the 20 MB file size limit.`;
    }
    return '';
  };
  const renderIssueDocuments = (editable) => renderDocumentList('[data-document-list]', selectedDocuments, 'remove-document', editable);
  const renderApDocuments = (editable) => renderDocumentList('[data-ap-document-list]', selectedApDocuments, 'remove-ap-document', editable);
  const handleDocumentInput = (event, isApDocument) => {
    const files = [...event.target.files];
    const current = isApDocument ? selectedApDocuments : selectedDocuments;
    const errorSelector = isApDocument ? '[data-ap-documents-error]' : '[data-documents-error]';
    const error = validateFiles(files, current.length);
    if (error) {
      documentError(errorSelector, error);
      event.target.value = '';
      return;
    }
    documentError(errorSelector, '');
    const next = [...current, ...files.map(fileMetadata)];
    if (isApDocument) { selectedApDocuments = next; renderApDocuments(true); } else { selectedDocuments = next; renderIssueDocuments(true); }
    event.target.value = '';
  };

  const renderActionPlans = (editable = true) => {
    const rows = $('[data-ap-rows]');
    const empty = $('[data-ap-empty]');
    const plans = draftActionPlans || [];
    if (!rows || !empty) return;
    empty.hidden = plans.length > 0;
    const issue = editingId ? issues.find((candidate) => candidate.issue_id === editingId) : null;
    rows.innerHTML = plans.map((ap, index) => {
      const apIsDraft = (ap.status || 'Draft') === 'Draft';
      const draftEditActions = isEditPage && issue?.status === 'Draft' && apIsDraft;
      const canEditActionPlan = !isViewPage && editable && apIsDraft && (!editingId || issue?.status === 'Draft');
      const action = isViewPage
        ? `<button type="button" data-edit-ap="${index}">View</button>`
        : draftEditActions
          ? `<button type="button" data-edit-ap="${index}">Edit</button><button type="button" class="issue-ap-withdraw" data-withdraw-ap="${index}">Withdraw</button>`
          : canEditActionPlan
          ? `<button type="button" data-edit-ap="${index}">Edit</button>`
          : `<button type="button" data-edit-ap="${index}">View</button>`;
      const canOpenActionPlan = isViewPage && ap.ap_id;
      const actionPlanIdentity = canOpenActionPlan
        ? `<button class="issue-ap-id-link" type="button" data-view-action-plan="${index}">${escapeHtml(ap.ap_id)}</button>`
        : `<strong>${escapeHtml(ap.ap_id || 'Generated after Save')}</strong>`;
      return `<tr>
        <td>${index + 1}</td><td>${actionPlanIdentity}<span>${escapeHtml(ap.title || 'Untitled Action Plan')}</span></td><td>${escapeHtml(ap.owner || '-')}</td><td>${formatDate(ap.tcd)}</td><td>${formatDate(ap.revised_tcd)}</td><td>${escapeHtml(ap.rtcd_count || 0)}</td><td><span class="issue-status-pill ${statusClass(ap.status || 'Draft')}">${escapeHtml(ap.status || 'Draft')}</span></td><td class="${isActionPlanOverdue(ap) ? 'issue-overdue' : 'issue-on-track'}">${ap.tcd ? (isActionPlanOverdue(ap) ? 'Yes' : 'No') : '-'}</td><td class="issue-ap-actions">${action}</td>
      </tr>`;
    }).join('');
    const actionPlanState = $('[data-ap-state]');
    if (actionPlanState) actionPlanState.textContent = `${plans.length} Action Plan${plans.length === 1 ? '' : 's'}`;
  };

  const openActionPlanModal = (index = -1, forceEditable = false, forceReadOnly = false) => {
    editingApIndex = index;
    const ap = index >= 0 ? draftActionPlans[index] || {} : {};
    selectedApDocuments = clone(ap.documents || []);
    setApField('id', ap.ap_id || 'Generated after Save');
    setApField('status', ap.status || 'Draft');
    setApField('owner', ap.owner || '');
    setApField('title', ap.title || '');
    setApField('description', ap.description || '');
    setApField('tcd', ap.tcd || '');
    const targetCompletionDate = $(apFieldMap.tcd);
    if (targetCompletionDate) targetCompletionDate.min = today;
    setApField('rtcd', ap.revised_tcd || '');
    setApField('rtcdCount', ap.rtcd_count || 0);
    setApField('overdue', ap.tcd ? (isActionPlanOverdue(ap) ? 'Yes' : 'No') : '-');
    syncActionPlanWithdrawalFields(ap);
    const issue = editingId ? issues.find((candidate) => candidate.issue_id === editingId) : null;
    const apIsDraft = (ap.status || 'Draft') === 'Draft';
    const editableIssueContext = !isViewPage && (!editingId || issue?.status === 'Draft') && apIsDraft;
    const editable = !forceReadOnly && Boolean(forceEditable || isCreatePage || editableIssueContext || (isViewPage && viewEditing && apIsDraft));
    $('[data-ap-modal-title]').textContent = index >= 0 ? (editable ? 'Edit Action Plan' : 'View Action Plan') : 'Add Action Plan';
    const modalKicker = $('[data-ap-modal-kicker]');
    const modalSubtitle = $('[data-ap-modal-subtitle]');
    if (modalKicker) modalKicker.hidden = true;
    if (modalSubtitle) modalSubtitle.hidden = true;
    setApFormError('');
    [apFieldMap.owner, apFieldMap.title, apFieldMap.description, apFieldMap.tcd, apFieldMap.documents].forEach((selector) => { $(selector).disabled = !editable; });
    syncActionPlanOwnerOptions(editable);
    $('[data-save-ap]').hidden = !editable;
    $('[data-save-ap]').disabled = !editable;
    renderApDocuments(editable);
    $('[data-ap-modal]').hidden = false;
    document.body.classList.add('modal-open');
  };
  const closeActionPlanModal = () => {
    const modal = $('[data-ap-modal]');
    if (!modal) return;
    modal.hidden = true;
    editingApIndex = -1;
    document.body.classList.remove('modal-open');
  };
  const saveActionPlan = () => {
    const owner = getApField('owner');
    const title = getApField('title');
    const description = getApField('description');
    const tcd = getApField('tcd');
    if (!owner) return setApFormError('Action Plan Owner is required.');
    if (!title) return setApFormError('Action Plan Title is required.');
    if (!description) return setApFormError('Description of Action is required.');
    if (!tcd) return setApFormError('Action Plan Target Completion Date is required.');
    if (tcd < today) return setApFormError('Action Plan Target Completion Date must be today or a future date.');
    const next = { ap_id: editingApIndex >= 0 ? draftActionPlans[editingApIndex]?.ap_id || '' : '', owner, title, description, tcd, revised_tcd: getApField('rtcd'), rtcd_count: Number(getApField('rtcdCount') || 0), status: editingApIndex >= 0 ? (draftActionPlans[editingApIndex]?.status || 'Draft') : 'Draft', documents: clone(selectedApDocuments) };
    const savedActionPlanLabel = next.ap_id || 'Action Plan';
    if (editingApIndex >= 0) draftActionPlans[editingApIndex] = next; else draftActionPlans.push(next);
    if (isViewPage && editingId) {
      const issue = issues.find((candidate) => candidate.issue_id === editingId);
      if (issue) { issue.action_plans = clone(draftActionPlans); saveState(); }
    }
    const issue = editingId ? issues.find((candidate) => candidate.issue_id === editingId) : null;
    syncIssueTargetDateFields(issue, draftActionPlans);
    renderActionPlans(isCreatePage || !isViewPage || viewEditing);
    closeActionPlanModal();
    markFormDirty();
    showToast(`${savedActionPlanLabel} Saved`);
  };

  const openCreatePicker = () => {
    const picker = $('[data-create-picker]');
    if (!picker) return;
    $('[data-create-picker-error]').hidden = true;
    picker.hidden = false;
    document.body.classList.add('modal-open');
  };
  const closeCreatePicker = () => {
    const picker = $('[data-create-picker]');
    if (!picker) return;
    picker.hidden = true;
    document.body.classList.remove('modal-open');
  };
  const confirmCreatePicker = () => {
    const creator = $('[data-create-creator]')?.value || '';
    if (!creator) { $('[data-create-picker-error]').hidden = false; return; }
    const createUrl = root.dataset.createUrl;
    if (createUrl) window.location.assign(`${createUrl}?creator=${encodeURIComponent(creator)}`);
  };

  const closeModal = () => {
    const modal = $('[data-issue-modal]');
    if (!modal) return;
    modal.hidden = true;
    document.body.classList.remove('modal-open');
    editingId = '';
  };
  const openModal = (issueId = '') => {
    editingId = issueId;
    const issue = issueId ? issues.find((candidate) => candidate.issue_id === issueId) : null;
    if (!issue) return;
    const editable = issue.status === 'Draft';
    const ap = issue.action_plans?.[0] || {};
    draftActionPlans = clone(issue.action_plans || []);
    selectedDocuments = clone(issue.documents || []);
    $('[data-modal-kicker]').textContent = 'Issue & Action Plan Details';
    $('[data-modal-title]').textContent = `${issue.issue_id} ${editable ? 'Edit' : 'Details'}`;
    $('[data-modal-subtitle]').textContent = editable ? 'Update Draft fields, then save or submit for approval.' : 'This Issue is read-only after submission.';
    setField('issueId', issue.issue_id); setField('status', issue.status || 'Draft'); setField('creator', issue.creator); setField('type', issue.type); setField('unit', issue.impacted_unit); setField('impact', issue.impact); setField('date', issue.date_of_issue || today); setField('tcd', issueTcd(issue)); setField('rtcd', issue.revised_tcd); setField('rtcdCount', issue.rtcd_count || (issue.revised_tcd ? 1 : 0)); setField('overdue', issueTcd(issue) ? (isOverdue(issue) ? 'Yes' : 'No') : '-'); setField('title', issue.title); setField('description', issue.description); setField('parties', issue.parties);
    syncIssueWithdrawalFields(issue);
    syncTypeDrivenFields();
    [formFieldMap.creator, formFieldMap.type].forEach((selector) => { $(selector).disabled = true; });
    [formFieldMap.unit, formFieldMap.impact, formFieldMap.title, formFieldMap.description, formFieldMap.parties, formFieldMap.documents].forEach((selector) => { $(selector).disabled = !editable; });
    $('[data-save-issue]').disabled = !editable; $('[data-submit-issue]').disabled = !editable; $('[data-add-ap]').disabled = !editable;
    renderIssueDocuments(editable); renderActionPlans(editable); setFormError('');
    $('[data-issue-modal]').hidden = false;
    document.body.classList.add('modal-open');
  };

  const initializeCreatePage = () => {
    formDirty = false;
    editingId = '';
    draftActionPlans = [];
    selectedDocuments = [];
    setField('creator', root.dataset.initialCreator || '');
    setField('issueId', 'Generated after Save or Submit'); setField('status', 'Draft');
    syncIssueWithdrawalFields(null);
    setField('date', today); setField('tcd', ''); setField('rtcd', ''); setField('rtcdCount', '0'); setField('overdue', '-');
    syncIssueTargetDateFields();
    renderIssueDocuments(true); renderActionPlans(true); syncTypeDrivenFields(); syncIssueIdentityFieldLocks(); setFormError('');
  };
  const initializeEditPage = () => {
    const issue = issues.find((candidate) => candidate.issue_id === root.dataset.editIssueId);
    const card = $('.issue-edit-card');
    if (!issue || issue.status !== 'Draft') {
      if (card) card.hidden = true;
      const error = $('[data-edit-error]');
      if (error) { error.textContent = `Draft Issue ${root.dataset.editIssueId || ''} could not be found in this browser demo.`; error.hidden = false; }
      return;
    }
    editingId = issue.issue_id;
    formDirty = false;
    draftActionPlans = clone(issue.action_plans || []);
    selectedDocuments = clone(issue.documents || []);
    setField('issueId', issue.issue_id); setField('status', issue.status || 'Draft'); setField('creator', issue.creator); setField('type', issue.type); setField('unit', issue.impacted_unit); setField('impact', issue.impact); setField('date', issue.date_of_issue || today); setField('tcd', issueTcd(issue)); setField('rtcd', issue.revised_tcd); setField('rtcdCount', issue.rtcd_count || (issue.revised_tcd ? 1 : 0)); setField('overdue', issueTcd(issue) ? (isOverdue(issue) ? 'Yes' : 'No') : '-'); setField('title', issue.title); setField('description', issue.description); setField('parties', issue.parties);
    syncIssueWithdrawalFields(issue);
    const subtitle = $('[data-edit-subtitle]');
    if (subtitle) subtitle.textContent = `${issue.issue_id} · ${issue.creator || '-'} · ${issue.type || 'Issue'}`;
    [formFieldMap.creator, formFieldMap.type].forEach((selector) => { $(selector).disabled = true; });
    [formFieldMap.unit, formFieldMap.impact, formFieldMap.title, formFieldMap.description, formFieldMap.parties, formFieldMap.documents].forEach((selector) => { $(selector).disabled = false; });
    syncTypeDrivenFields();
    syncIssueIdentityFieldLocks();
    renderIssueDocuments(true); renderActionPlans(true); setFormError('');
  };
  const setViewEditing = (enabled) => {
    const issue = editingId ? issues.find((candidate) => candidate.issue_id === editingId) : null;
    viewEditing = Boolean(enabled && issue?.status === 'Draft');
    if (!viewEditing) formDirty = false;
    const editable = viewEditing;
    [formFieldMap.creator, formFieldMap.type].forEach((selector) => { $(selector).disabled = true; });
    [formFieldMap.unit, formFieldMap.impact, formFieldMap.title, formFieldMap.description, formFieldMap.parties, formFieldMap.documents].forEach((selector) => { $(selector).disabled = !editable; });
    syncTypeDrivenFields();
    if (isViewPage) {
      $('[data-save-issue]').hidden = !editable;
      $('[data-submit-issue]').hidden = !editable;
      $('[data-add-ap]').hidden = !editable;
      $('[data-view-submit]').hidden = !issue || issue.status !== 'Draft';
      const withdrawButton = $('[data-view-withdraw]');
      if (withdrawButton) withdrawButton.hidden = !issue || issue.status !== 'Draft' || editable;
    }
    $('[data-save-issue]').disabled = !editable;
    $('[data-submit-issue]').disabled = !editable;
    $('[data-add-ap]').disabled = !editable;
    renderIssueDocuments(editable);
    renderActionPlans(editable);
    const editButton = $('[data-view-edit]');
    if (editButton) editButton.textContent = editable ? 'Save' : 'Edit Issue';
  };
  const initializeViewPage = () => {
    const issue = issues.find((candidate) => candidate.issue_id === root.dataset.viewIssueId);
    const card = $('.issue-view-card');
    if (!issue) {
      if (card) card.hidden = true;
      const error = $('[data-view-error]');
      if (error) { error.textContent = `Issue ${root.dataset.viewIssueId || ''} could not be found in this browser demo.`; error.hidden = false; }
      return;
    }
    editingId = issue.issue_id;
    formDirty = false;
    draftActionPlans = clone(issue.action_plans || []);
    selectedDocuments = clone(issue.documents || []);
    setField('issueId', issue.issue_id); setField('status', issue.status || 'Draft'); setField('creator', issue.creator); setField('type', issue.type); setField('unit', issue.impacted_unit); setField('impact', issue.impact); setField('date', issue.date_of_issue || today); setField('tcd', issueTcd(issue)); setField('rtcd', issue.revised_tcd); setField('rtcdCount', issue.rtcd_count || (issue.revised_tcd ? 1 : 0)); setField('overdue', issueTcd(issue) ? (isOverdue(issue) ? 'Yes' : 'No') : '-'); setField('title', issue.title); setField('description', issue.description); setField('parties', issue.parties);
    syncIssueWithdrawalFields(issue);
    $('[data-view-subtitle]').textContent = `${issue.issue_id} · ${issue.creator || '-'} · ${issue.type || 'Issue'}`;
    const viewHeading = $('[data-view-heading]');
    if (viewHeading) viewHeading.textContent = issue.title ? `${issue.issue_id} · ${issue.title}` : `${issue.issue_id} · Issue Details`;
    const status = $('[data-view-status]');
    if (status) { status.textContent = issue.status || 'Draft'; status.className = `issue-status-pill ${statusClass(issue.status || 'Draft')}`; }
    const isDraft = issue.status === 'Draft';
    ['[data-view-edit]', '[data-view-submit]', '[data-view-withdraw]'].forEach((selector) => { const node = $(selector); if (node) node.hidden = !isDraft; });
    const subtitle = $('[data-view-subtitle]');
    if (subtitle && !isDraft && issue.status !== 'Withdrawn') subtitle.textContent += ' · Read-only after submission';
    setViewEditing(false);
    setFormError('');
  };
  const initializeActionPlanViewPage = () => {
    const issue = issues.find((candidate) => candidate.issue_id === root.dataset.viewIssueId);
    const ap = issue?.action_plans?.find((candidate) => candidate.ap_id === root.dataset.viewApId);
    const card = $('.issue-view-card');
    if (!issue || !ap) {
      if (card) card.hidden = true;
      const subtitle = $('[data-ap-view-subtitle]');
      if (subtitle) subtitle.textContent = 'This Action Plan could not be found in this browser demo.';
      return;
    }
    editingId = issue.issue_id;
    draftActionPlans = clone(issue.action_plans || []);
    $('[data-ap-view-subtitle]').textContent = `${ap.ap_id || 'Action Plan'} · ${issue.issue_id} · ${ap.owner || '-'}`;
    $('[data-ap-view-heading]').textContent = ap.title ? `${ap.ap_id || 'Action Plan'} · ${ap.title}` : `${ap.ap_id || 'Action Plan'} · Action Plan Details`;
    $('[data-ap-view-owner]').value = ap.owner || '-';
    $('[data-ap-view-id]').value = ap.ap_id || 'Generated after Save';
    $('[data-ap-view-status-field]').value = ap.status || 'Draft';
    $('[data-ap-view-tcd]').value = formatDate(ap.tcd);
    $('[data-ap-view-rtcd]').value = formatDate(ap.revised_tcd);
    $('[data-ap-view-rtcd-count]').value = String(ap.rtcd_count || 0);
    $('[data-ap-view-overdue]').value = ap.tcd ? (isActionPlanOverdue(ap) ? 'Yes' : 'No') : '-';
    $('[data-ap-view-withdraw-date]').value = ap.status === 'Withdrawn' ? formatDate(ap.withdrawal_date || today) : '-';
    $('[data-ap-view-withdraw-justification]').value = ap.status === 'Withdrawn' ? (ap.withdrawal_justification || 'Withdrawn by the Action Plan Owner.') : '';
    $('[data-ap-view-title]').value = ap.title || 'Untitled Action Plan';
    $('[data-ap-view-description]').value = ap.description || '';
    const status = $('[data-ap-view-status]');
    if (status) { status.textContent = ap.status || 'Draft'; status.className = `issue-status-pill ${statusClass(ap.status || 'Draft')}`; }
    const withdraw = $('[data-ap-view-withdraw]');
    if (withdraw) withdraw.hidden = issue.status !== 'Draft' || ap.status !== 'Draft';
    $$('[data-ap-view-withdraw-field]').forEach((node) => { node.hidden = ap.status !== 'Withdrawn'; });
    renderDocumentList('[data-ap-view-document-list]', ap.documents || [], '', false);
  };
  const makeIssueId = (type) => { const prefix = `${codeFor(type)}${new Date().getFullYear()}-IS`; const numbers = issues.map((issue) => Number(String(issue.issue_id || '').split('IS').pop())).filter(Number.isFinite); return `${prefix}${String(Math.max(0, ...numbers) + 1).padStart(5, '0')}`; };
  const makeApId = (issueId) => { const prefix = `${String(issueId).split('-IS')[0]}-AP`; const numbers = issues.flatMap((issue) => issue.action_plans || []).map((ap) => Number(String(ap.ap_id || '').split('-AP').pop())).filter(Number.isFinite); return `${prefix}${String(Math.max(0, ...numbers) + 1).padStart(5, '0')}`; };
  const readForm = () => ({ creator: getField('creator'), type: getField('type'), impacted_unit: getField('unit'), impact: getField('impact'), date_of_issue: getField('date') || today, title: getField('title'), description: getField('description'), parties: getField('parties'), documents: clone(selectedDocuments), action_plans: clone(draftActionPlans) });
  const validate = (submit, payload) => {
    if (!payload.creator || !payload.type) return 'Issue Creator and Issue Type are required.';
    if (!submit) return '';
    const required = [['Impacted Unit', payload.impacted_unit], ['Potential Impact', payload.impact], ['Issue Title', payload.title], ['Description of Issue', payload.description], ['Parties Involved', payload.parties]];
    const missing = required.find(([, value]) => !value);
    if (missing) return `${missing[0]} is required before Submit.`;
    if (!payload.action_plans.length) return 'Please add at least one Action Plan before submitting the Issue & Action Plan for approval.';
    return '';
  };
  const persistForm = (submit, confirmed = false) => {
    const payload = readForm(); const error = validate(submit, payload); if (error) { setFormError(error); return false; }
    if (submit && !confirmed) { openSubmitConfirmation(); return false; }
    let issue = editingId ? issues.find((candidate) => candidate.issue_id === editingId) : null;
    if (!issue) { issue = { issue_id: makeIssueId(payload.type), status: 'Draft', action_plans: [] }; issues.push(issue); editingId = issue.issue_id; }
    Object.assign(issue, { creator: payload.creator, type: payload.type, impacted_unit: payload.impacted_unit, impact: payload.impact, date_of_issue: payload.date_of_issue, title: payload.title, description: payload.description, parties: payload.parties, documents: payload.documents });
    issue.action_plans = clone(payload.action_plans);
    issue.action_plans.forEach((ap) => { if (!ap.ap_id) ap.ap_id = makeApId(issue.issue_id); });
    if (submit) { issue.status = 'Pending Approval'; issue.action_plans.forEach((ap) => { ap.status = 'Pending Approval'; }); } else { issue.status = 'Draft'; issue.action_plans.forEach((ap) => { if (!ap.status || ap.status === 'Pending Approval') ap.status = 'Draft'; }); }
    draftActionPlans = clone(issue.action_plans);
    saveState();
    formDirty = false;
    if (isCreatePage) {
      if (submit) { window.location.assign(root.dataset.overviewUrl || '/issue-management'); return true; }
      const issueIdField = $('[data-form-issue-id]');
      if (issueIdField) issueIdField.value = issue.issue_id;
      syncIssueIdentityFieldLocks();
      renderActionPlans(true);
      showToast(`${issue.issue_id} saved as Draft.`);
      return true;
    }
    if (isViewPage) {
      if (submit) { window.location.assign(root.dataset.overviewUrl || '/issue-management'); return true; }
      initializeViewPage();
      showToast(`${issue.issue_id} saved.`);
      return true;
    }
    if (isEditPage) {
      if (submit) { window.location.assign(root.dataset.overviewUrl || '/issue-management'); return true; }
      initializeEditPage();
      showToast(`${issue.issue_id} saved.`);
      return true;
    }
    renderOverview(); closeModal(); showToast(submit ? `${issue.issue_id} submitted for approval.` : `${issue.issue_id} saved as Draft.`); return true;
  };

  $$('[data-close-submit-confirm]').forEach((node) => node.addEventListener('click', closeSubmitConfirmation));
  $('[data-submit-confirm]').addEventListener('click', (event) => { if (event.target === $('[data-submit-confirm]')) closeSubmitConfirmation(); });
  $('[data-confirm-submit]').addEventListener('click', () => { closeSubmitConfirmation(); persistForm(true, true); });
  $$('[data-close-cancel-confirm]').forEach((node) => node.addEventListener('click', closeCancelConfirmation));
  $('[data-cancel-confirm]').addEventListener('click', (event) => { if (event.target === $('[data-cancel-confirm]')) closeCancelConfirmation(); });
  $('[data-confirm-cancel]').addEventListener('click', () => { closeCancelConfirmation(); navigateAfterCancel(); });
  $$('[data-close-withdraw-justification]').forEach((node) => node.addEventListener('click', closeWithdrawalModals));
  $('[data-withdraw-justification]').addEventListener('click', (event) => { if (event.target === $('[data-withdraw-justification]')) closeWithdrawalModals(); });
  $('[data-confirm-withdraw-justification]').addEventListener('click', () => {
    const input = $('[data-withdraw-justification-input]');
    const error = $('[data-withdraw-justification-error]');
    if (!input.value.trim()) { error.hidden = false; input.focus(); return; }
    pendingWithdrawal.justification = input.value.trim();
    error.hidden = true;
    openWithdrawConfirmation();
  });
  $$('[data-close-withdraw-confirmation]').forEach((node) => node.addEventListener('click', closeWithdrawalModals));
  $('[data-withdraw-confirmation]').addEventListener('click', (event) => { if (event.target === $('[data-withdraw-confirmation]')) closeWithdrawalModals(); });
  $('[data-confirm-withdraw]').addEventListener('click', finalizeWithdrawal);

  if (isCreatePage) {
    $('[data-issue-form]').addEventListener('submit', (event) => { event.preventDefault(); persistForm(true); });
    $('[data-save-issue]').addEventListener('click', () => persistForm(false));
    [formFieldMap.creator, formFieldMap.type].forEach((selector) => $(selector).addEventListener('change', syncTypeDrivenFields));
    $('[data-form-back]').addEventListener('click', handleFormBack);
    $('[data-add-ap]').addEventListener('click', () => openActionPlanModal());
    initializeCreatePage();
  } else if (isEditPage) {
    $('[data-issue-form]').addEventListener('submit', (event) => { event.preventDefault(); persistForm(true); });
    $('[data-save-issue]').addEventListener('click', () => persistForm(false));
    [formFieldMap.creator, formFieldMap.type].forEach((selector) => $(selector).addEventListener('change', syncTypeDrivenFields));
    $('[data-form-back]').addEventListener('click', handleFormBack);
    $('[data-add-ap]').addEventListener('click', () => openActionPlanModal(-1, true));
    initializeEditPage();
  } else if (isViewPage) {
    $('[data-view-edit]').addEventListener('click', () => {
      const editUrl = root.dataset.editUrl?.replace('__ISSUE_ID__', encodeURIComponent(root.dataset.viewIssueId || ''));
      if (editUrl) window.location.assign(editUrl); else if (viewEditing) persistForm(false); else setViewEditing(true);
    });
    $('[data-view-submit]').addEventListener('click', () => persistForm(true));
    $('[data-view-withdraw]').addEventListener('click', () => {
      openWithdrawJustification('issue');
    });
    $('[data-save-issue]').addEventListener('click', () => persistForm(false));
    $('[data-issue-form]').addEventListener('submit', (event) => { event.preventDefault(); if (viewEditing) persistForm(true); });
    [formFieldMap.creator, formFieldMap.type].forEach((selector) => $(selector).addEventListener('change', syncTypeDrivenFields));
    $('[data-form-back]').addEventListener('click', () => { if (viewEditing) handleFormBack(); else navigateToOverview(); });
    $('[data-add-ap]').addEventListener('click', () => openActionPlanModal(-1, true));
    initializeViewPage();
  } else if (isActionPlanViewPage) {
    $('[data-ap-view-back]').addEventListener('click', () => {
      const issueUrl = root.dataset.issueViewUrl?.replace('__ISSUE_ID__', encodeURIComponent(root.dataset.viewIssueId || ''));
      window.location.assign(issueUrl || root.dataset.overviewUrl || '/issue-management');
    });
    $('[data-ap-view-withdraw]').addEventListener('click', () => openWithdrawJustification('action_plan', (issues.find((candidate) => candidate.issue_id === editingId)?.action_plans || []).findIndex((candidate) => candidate.ap_id === root.dataset.viewApId)));
    initializeActionPlanViewPage();
  } else {
    $('[data-create-issue]').addEventListener('click', openCreatePicker);
    $('[data-confirm-create]').addEventListener('click', confirmCreatePicker);
    $$('[data-cancel-create]').forEach((node) => node.addEventListener('click', closeCreatePicker));
    $('[data-create-picker]').addEventListener('click', (event) => { if (event.target === $('[data-create-picker]')) closeCreatePicker(); });
    $$('[data-close-modal]').forEach((node) => node.addEventListener('click', closeModal));
    $('[data-issue-modal]').addEventListener('click', (event) => { if (event.target === $('[data-issue-modal]')) closeModal(); });
    $('[data-save-issue]').addEventListener('click', () => persistForm(false));
    $('[data-issue-form]').addEventListener('submit', (event) => { event.preventDefault(); persistForm(true); });
    [formFieldMap.creator, formFieldMap.type].forEach((selector) => $(selector).addEventListener('change', syncTypeDrivenFields));
    initializeMultiSelectFilters();
    $('[data-run-search]').addEventListener('click', () => { currentPage = 1; renderRows(); });
    $$('[data-issue-sort]').forEach((button) => button.addEventListener('click', () => toggleOverviewSort(button.dataset.issueSort)));
    $('[data-toggle-date-filters]').addEventListener('click', () => { const filters = $('[data-date-filters]'); const expanded = filters.hidden; filters.hidden = !expanded; $('[data-toggle-date-filters]').setAttribute('aria-expanded', String(expanded)); $('[data-toggle-date-filters]').setAttribute('aria-label', expanded ? 'Collapse Target Completion Date / Revised Target Completion Date filters' : 'Expand Target Completion Date / Revised Target Completion Date filters'); $('[data-toggle-date-filters] span').textContent = expanded ? '▴' : '▾'; });
    $('[data-clear-search]').addEventListener('click', () => { $$(searchFilterSelector).forEach((node) => { node.value = ''; }); $$('[data-search-multi-option]').forEach((node) => { node.checked = false; }); $$('[data-search-multi]').forEach(renderMultiSelect); closeMultiSelectMenus(); syncSearchFilterTone(); currentPage = 1; renderRows(); });
    $('[data-prev-page]').addEventListener('click', () => { if (currentPage > 1) { currentPage -= 1; renderRows(); } });
    $('[data-next-page]').addEventListener('click', () => { const totalPages = Math.max(1, Math.ceil(filteredIssues().length / pageSize)); if (currentPage < totalPages) { currentPage += 1; renderRows(); } });
    $('[data-page-size]').addEventListener('change', (event) => { pageSize = Number(event.target.value) || 10; currentPage = 1; renderRows(); });
    $('[data-add-ap]').addEventListener('click', () => openActionPlanModal());
    renderOverview();
  }

  $$(searchFilterSelector).forEach((node) => {
    node.addEventListener('input', syncSearchFilterTone);
    node.addEventListener('change', syncSearchFilterTone);
  });
  syncSearchFilterTone();

  const issueForm = $('[data-issue-form]');
  if (issueForm) {
    issueForm.addEventListener('input', markFormDirty);
    issueForm.addEventListener('change', (event) => {
      if (event.target.matches(formFieldMap.documents)) handleDocumentInput(event, false);
      markFormDirty();
    });
  }
  const actionPlanForm = $('[data-ap-form]');
  if (actionPlanForm) actionPlanForm.addEventListener('submit', (event) => { event.preventDefault(); saveActionPlan(); });
  const actionPlanTargetCompletionDate = $(apFieldMap.tcd);
  if (actionPlanTargetCompletionDate) actionPlanTargetCompletionDate.min = today;
  $$('[data-close-ap-modal]').forEach((node) => node.addEventListener('click', closeActionPlanModal));
  const actionPlanModal = $('[data-ap-modal]');
  if (actionPlanModal) actionPlanModal.addEventListener('click', (event) => { if (event.target === actionPlanModal) closeActionPlanModal(); });
  const actionPlanDocuments = $('[data-ap-form-documents]');
  if (actionPlanDocuments) actionPlanDocuments.addEventListener('change', (event) => handleDocumentInput(event, true));
  root.addEventListener('click', (event) => {
    const viewActionPlan = event.target.closest('[data-view-action-plan]');
    if (viewActionPlan) {
      openActionPlanModal(Number(viewActionPlan.dataset.viewActionPlan), false, true);
      return;
    }
    const removeIssue = event.target.closest('[data-remove-document]');
    if (removeIssue) { selectedDocuments.splice(Number(removeIssue.dataset.removeDocument), 1); renderIssueDocuments(true); }
    const removeAp = event.target.closest('[data-remove-ap-document]');
    if (removeAp) { selectedApDocuments.splice(Number(removeAp.dataset.removeApDocument), 1); renderApDocuments(true); }
    const editAp = event.target.closest('[data-edit-ap]');
    if (editAp) {
      const index = Number(editAp.dataset.editAp);
      const ap = draftActionPlans[index];
      const issue = editingId ? issues.find((candidate) => candidate.issue_id === editingId) : null;
      const canEdit = !isViewPage && (isCreatePage || (!editingId || issue?.status === 'Draft')) && (ap?.status || 'Draft') === 'Draft';
      openActionPlanModal(index, canEdit);
    }
    const withdrawAp = event.target.closest('[data-withdraw-ap]');
    if (withdrawAp && (isViewPage || isEditPage)) {
      openWithdrawJustification('action_plan', Number(withdrawAp.dataset.withdrawAp));
    }
  });
  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Escape') return;
    if (!isCreatePage && $('[data-create-picker]') && !$('[data-create-picker]').hidden) closeCreatePicker();
    if ($('[data-submit-confirm]') && !$('[data-submit-confirm]').hidden) closeSubmitConfirmation();
    if ($('[data-cancel-confirm]') && !$('[data-cancel-confirm]').hidden) closeCancelConfirmation();
    if ($('[data-withdraw-justification]') && !$('[data-withdraw-justification]').hidden) closeWithdrawalModals();
    if ($('[data-withdraw-confirmation]') && !$('[data-withdraw-confirmation]').hidden) closeWithdrawalModals();
    if ($('[data-ap-modal]') && !$('[data-ap-modal]').hidden) closeActionPlanModal();
    if (!isCreatePage && $('[data-issue-modal]') && !$('[data-issue-modal]').hidden) closeModal();
  });
})();
