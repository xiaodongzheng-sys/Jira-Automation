(() => {
  const root = document.querySelector('[data-productization-upgrade-summary]');
  if (!root) return;

  const form = root.querySelector('[data-productization-version-form]');
  const input = root.querySelector('[data-productization-version-input]');
  const status = root.querySelector('[data-productization-status]');
  const candidatesNode = root.querySelector('[data-productization-candidates]');
  const candidatesEmpty = root.querySelector('[data-productization-candidates-empty]');
  const tableWrap = root.querySelector('[data-productization-table-wrap]');
  const resultsBody = root.querySelector('[data-productization-results-body]');
  const resultsEmpty = root.querySelector('[data-productization-results-empty]');

  if (!form || !input || !status || !candidatesNode || !candidatesEmpty || !tableWrap || !resultsBody || !resultsEmpty) {
    return;
  }

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const setStatus = (message, tone = 'neutral') => {
    status.textContent = message;
    status.dataset.tone = tone;
  };

  const resetResults = (message = 'No upgrade tickets loaded yet.') => {
    resultsBody.innerHTML = '';
    tableWrap.hidden = true;
    resultsEmpty.hidden = false;
    const textNode = resultsEmpty.querySelector('p');
    if (textNode) textNode.textContent = message;
  };

  const resetCandidates = (message = 'No version results yet.') => {
    candidatesNode.innerHTML = '';
    candidatesEmpty.hidden = false;
    const textNode = candidatesEmpty.querySelector('p');
    if (textNode) textNode.textContent = message;
  };

  const renderResults = (items) => {
    if (!Array.isArray(items) || items.length === 0) {
      resetResults('No Jira tickets matched the selected version.');
      return;
    }

    resultsEmpty.hidden = true;
    tableWrap.hidden = false;
    resultsBody.innerHTML = items.map((item) => {
      const ticket = item.jira_ticket_url
        ? `<a href="${escapeHtml(item.jira_ticket_url)}" target="_blank" rel="noreferrer">${escapeHtml(item.jira_ticket_number || '-')}</a>`
        : escapeHtml(item.jira_ticket_number || '-');
      const prdLinks = Array.isArray(item.prd_links) && item.prd_links.length
        ? item.prd_links.map((link) => `<a href="${escapeHtml(link.url)}" target="_blank" rel="noreferrer">${escapeHtml(link.label || link.url)}</a>`).join('<br>')
        : '-';
      return `
        <tr>
          <td>${ticket}</td>
          <td>${escapeHtml(item.feature_summary || '-')}</td>
          <td>${escapeHtml(item.detailed_feature || '-')}</td>
          <td>${escapeHtml(item.pm || '-')}</td>
          <td>${prdLinks}</td>
        </tr>
      `;
    }).join('');
  };

  const loadIssues = async (versionId, label, candidateButton) => {
    setStatus(`Loading Jira tickets for ${label}...`);
    resetResults('Loading Jira tickets...');
    candidatesNode.querySelectorAll('.productization-candidate').forEach((node) => {
      node.classList.toggle('is-selected', node === candidateButton);
    });

    try {
      const response = await fetch(`/api/productization-upgrade-summary/issues?version_id=${encodeURIComponent(versionId)}`);
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.message || 'Could not load Jira tickets for that version.');
      }
      renderResults(payload.items || []);
      setStatus(`Loaded Jira tickets for ${label}.`, 'success');
    } catch (error) {
      resetResults(error.message || 'Could not load Jira tickets for that version.');
      setStatus(error.message || 'Could not load Jira tickets for that version.', 'error');
    }
  };

  const renderCandidates = (items) => {
    if (!Array.isArray(items) || items.length === 0) {
      resetCandidates('No matching versions found.');
      return;
    }

    candidatesEmpty.hidden = true;
    candidatesNode.innerHTML = items.map((item) => `
      <button
        class="productization-candidate"
        type="button"
        data-version-id="${escapeHtml(item.version_id)}"
        data-version-label="${escapeHtml(item.label || item.version_name || item.version_id)}"
      >
        <strong>${escapeHtml(item.version_name || item.version_id)}</strong>
        <span>${escapeHtml(item.market || 'No market label')}</span>
      </button>
    `).join('');

    candidatesNode.querySelectorAll('.productization-candidate').forEach((button) => {
      button.addEventListener('click', () => {
        loadIssues(button.dataset.versionId || '', button.dataset.versionLabel || button.dataset.versionId || 'selected version', button);
      });
    });
  };

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const query = input.value.trim();
    if (!query) {
      setStatus('Version keyword is required.', 'error');
      resetCandidates('No version results yet.');
      resetResults();
      return;
    }

    setStatus(`Searching versions matching "${query}"...`);
    resetCandidates('Searching versions...');
    resetResults();

    try {
      const response = await fetch(`/api/productization-upgrade-summary/versions?q=${encodeURIComponent(query)}`);
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.message || 'Could not search versions.');
      }
      renderCandidates(payload.items || []);
      setStatus(`Select one exact version from ${payload.items?.length || 0} match${(payload.items?.length || 0) === 1 ? '' : 'es'}.`, 'success');
    } catch (error) {
      resetCandidates(error.message || 'Could not search versions.');
      setStatus(error.message || 'Could not search versions.', 'error');
    }
  });
})();
