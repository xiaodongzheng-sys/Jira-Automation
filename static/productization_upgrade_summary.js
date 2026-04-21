(() => {
  const root = document.querySelector('[data-productization-upgrade-summary]');
  if (!root) return;

  const form = root.querySelector('[data-productization-version-form]');
  const input = root.querySelector('[data-productization-version-input]');
  const typeahead = root.querySelector('[data-productization-typeahead]');
  const selectedNode = root.querySelector('[data-productization-selected]');
  const selectedEmpty = root.querySelector('[data-productization-selected-empty]');
  const status = root.querySelector('[data-productization-status]');
  const tableWrap = root.querySelector('[data-productization-table-wrap]');
  const resultsBody = root.querySelector('[data-productization-results-body]');
  const resultsEmpty = root.querySelector('[data-productization-results-empty]');
  const resultsTitle = root.querySelector('[data-productization-results-title]');
  const copyButton = root.querySelector('[data-productization-copy-button]');

  if (!form || !input || !typeahead || !selectedNode || !selectedEmpty || !status || !tableWrap || !resultsBody || !resultsEmpty || !resultsTitle || !copyButton) {
    return;
  }

  const MAX_SELECTIONS = 2;
  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const readJsonOrThrow = async (response, fallbackMessage) => {
    const contentType = response.headers.get('content-type') || '';
    if (contentType.includes('application/json')) {
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.message || fallbackMessage);
      }
      return payload;
    }
    const text = await response.text();
    const compact = text.replace(/\s+/g, ' ').trim();
    throw new Error(compact || fallbackMessage);
  };

  const copyText = async (text) => {
    const helper = document.createElement('textarea');
    helper.value = text;
    helper.setAttribute('readonly', '');
    helper.style.position = 'absolute';
    helper.style.left = '-9999px';
    document.body.appendChild(helper);
    helper.select();
    document.execCommand('copy');
    document.body.removeChild(helper);
  };

  const quoteTsvCell = (value) => {
    const text = String(value ?? '').replace(/\t/g, ' ').replace(/\r/g, '');
    if (!/[\"\n]/.test(text)) {
      return text;
    }
    return `"${text.replaceAll('"', '""')}"`;
  };

  const formatDetailedFeatureText = (value) => {
    const text = String(value ?? '').trim();
    if (!text || text === '-') return '-';

    return text
      .replace(/\(?P\d+\)?/gi, ' ')
      .replace(/\s+(?=\d+\.\s*[A-Za-z\u4e00-\u9fff(])/g, '\n')
      .replace(/\s+(?=scenario\d+\b)/gi, '\n')
      .replace(/\s+(?=\d+\s*-\s*[A-Za-z\u4e00-\u9fff(])/g, '\n')
      .replace(/\s*;\s*/g, ';\n')
      .replace(/\s*；\s*/g, '；\n')
      .replace(/\s{2,}/g, ' ')
      .replace(/\n{3,}/g, '\n\n')
      .trim();
  };

  const buildDetailedFeatureSegments = (value) => {
    const text = formatDetailedFeatureText(value);
    if (text === '-') {
      return [{ type: 'plain', text: '-' }];
    }

    let numberedIndex = 0;
    return text
      .split(/\n+/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => {
        const normalizedLine = line.replace(/^\(?P\d+\)?\s*/i, '').trim();
        const numberedMatch = normalizedLine.match(/^(\d+\.)\s*(.+)$/);
        if (numberedMatch) {
          numberedIndex += 1;
          return { type: 'numbered', marker: `${numberedIndex}.`, text: numberedMatch[2] || '' };
        }
        return { type: 'plain', text: normalizedLine };
      });
  };

  const formatDetailedFeatureHtml = (value) => buildDetailedFeatureSegments(value).map((segment) => {
    if (segment.type === 'numbered') {
      return `
        <div class="productization-detail-line productization-detail-line-numbered">
          <span class="productization-detail-marker">${escapeHtml(segment.marker)}</span>
          <span>${escapeHtml(segment.text)}</span>
        </div>
      `;
    }
    return `<div class="productization-detail-line">${escapeHtml(segment.text)}</div>`;
  }).join('');

  const setStatus = (message, tone = 'neutral') => {
    status.textContent = message;
    status.dataset.tone = tone;
  };

  const resetResults = (message = 'No upgrade tickets loaded yet.') => {
    resultsTitle.textContent = 'Upgrade Tickets';
    resultsBody.innerHTML = '';
    tableWrap.hidden = true;
    resultsEmpty.hidden = false;
    copyButton.disabled = true;
    const textNode = resultsEmpty.querySelector('p');
    if (textNode) textNode.textContent = message;
  };

  let searchController = null;
  let inputTimer = null;
  let searchToken = 0;
  let activeSuggestionIndex = -1;
  let lastSuggestions = [];
  const selectedVersions = [];

  const buildCopyRows = () => {
    const header = ['Jira Link', 'Feature Summary', 'Detailed Feature'];
    const rows = [header];
    selectedVersions.forEach((entry, index) => {
      rows.push([entry.label, '', '']);
      (entry.items || []).forEach((item) => {
        rows.push([
          item.jira_ticket_number || '-',
          item.feature_summary || '-',
          formatDetailedFeatureText(item.detailed_feature || '-'),
        ]);
      });
    });
    return rows;
  };

  const buildCopyText = () => buildCopyRows()
    .map((row) => row.map((cell) => quoteTsvCell(cell)).join('\t'))
    .join('\n');

  const buildCopyHtml = () => {
    const rows = buildCopyRows();
    if (!rows.length) return '';

    const headerCells = rows[0]
      .map((cell) => `<th style="text-align:left;font-weight:700;">${escapeHtml(cell)}</th>`)
      .join('');
    const bodyRows = selectedVersions.map((entry, index) => {
      const sectionRow = `<tr><td colspan="3" style="font-weight:700;background:#eef4ff;">${escapeHtml(entry.label)}</td></tr>`;
      const itemRows = (entry.items || []).map((item) => {
        const jiraCell = item.jira_ticket_url
          ? `<a href="${escapeHtml(item.jira_ticket_url)}">${escapeHtml(item.jira_ticket_number || '-')}</a>`
          : escapeHtml(item.jira_ticket_number || '-');
        return `
          <tr>
            <td style="white-space:pre-wrap;vertical-align:top;">${jiraCell}</td>
            <td style="white-space:pre-wrap;vertical-align:top;">${escapeHtml(item.feature_summary || '-').replaceAll('\n', '<br>')}</td>
            <td style="white-space:pre-wrap;vertical-align:top;">${escapeHtml(formatDetailedFeatureText(item.detailed_feature || '-')).replaceAll('\n', '<br>')}</td>
          </tr>
        `;
      }).join('');
      return `${sectionRow}${itemRows}`;
    }).join('');

    return `
      <table border="1" cellspacing="0" cellpadding="6" style="border-collapse:collapse;">
        <thead><tr>${headerCells}</tr></thead>
        <tbody>${bodyRows}</tbody>
      </table>
    `.trim();
  };

  const hideSuggestions = () => {
    typeahead.hidden = true;
    typeahead.innerHTML = '';
    activeSuggestionIndex = -1;
    lastSuggestions = [];
  };

  const renderResults = () => {
    const readySections = selectedVersions.filter((entry) => Array.isArray(entry.items) && entry.items.length);
    if (!readySections.length) {
      resetResults(selectedVersions.length ? 'Selected versions have no matching Jira tickets.' : 'No upgrade tickets loaded yet.');
      return;
    }

    resultsTitle.textContent = 'Upgrade Tickets';
    copyButton.disabled = false;
    resultsEmpty.hidden = true;
    tableWrap.hidden = false;
    resultsBody.innerHTML = readySections.map((entry) => {
      const rows = entry.items.map((item) => {
        const ticket = item.jira_ticket_url
          ? `<a href="${escapeHtml(item.jira_ticket_url)}" target="_blank" rel="noreferrer">${escapeHtml(item.jira_ticket_number || '-')}</a>`
          : escapeHtml(item.jira_ticket_number || '-');
        return `
          <tr>
            <td>${ticket}</td>
            <td>${escapeHtml(item.feature_summary || '-')}</td>
            <td>${formatDetailedFeatureHtml(item.detailed_feature || '-')}</td>
          </tr>
        `;
      }).join('');
      return `
        <tr class="productization-table-section-row">
          <td colspan="3">${escapeHtml(entry.label)}</td>
        </tr>
        ${rows}
      `;
    }).join('');
  };

  const renderSelectedVersions = () => {
    if (!selectedVersions.length) {
      selectedNode.innerHTML = '';
      selectedEmpty.hidden = false;
      const textNode = selectedEmpty.querySelector('p');
      if (textNode) textNode.textContent = 'No versions selected yet.';
      return;
    }

    selectedEmpty.hidden = true;
    selectedNode.innerHTML = selectedVersions.map((entry, index) => `
      <article class="productization-selection-card">
        <div class="productization-selection-copy">
          <strong>Version ${index + 1}</strong>
          <span>${escapeHtml(entry.label)}</span>
        </div>
        <button
          class="productization-selection-remove"
          type="button"
          data-remove-version-index="${index}"
          aria-label="Remove ${escapeHtml(entry.label)}"
        >
          Remove
        </button>
      </article>
    `).join('');

    selectedNode.querySelectorAll('[data-remove-version-index]').forEach((button) => {
      button.addEventListener('click', () => {
        const index = Number(button.dataset.removeVersionIndex || -1);
        if (index < 0) return;
        const removed = selectedVersions.splice(index, 1)[0];
        renderSelectedVersions();
        renderResults();
        setStatus(
          selectedVersions.length
            ? `Removed ${removed?.label || 'version'}. You can select another version now.`
            : 'Removed selected version. Type a version keyword to begin.',
          'success',
        );
        if (selectedVersions.length < MAX_SELECTIONS) {
          input.disabled = false;
          input.placeholder = 'Type a version keyword';
        }
      });
    });
  };

  const setActiveSuggestion = (index) => {
    activeSuggestionIndex = index;
    Array.from(typeahead.querySelectorAll('.productization-typeahead-option')).forEach((node, nodeIndex) => {
      node.classList.toggle('is-active', nodeIndex === index);
    });
  };

  const renderSuggestions = (items, query) => {
    lastSuggestions = Array.isArray(items) ? items : [];
    if (!lastSuggestions.length) {
      typeahead.innerHTML = `<div class="productization-typeahead-empty">No matching versions for "${escapeHtml(query)}".</div>`;
      typeahead.hidden = false;
      activeSuggestionIndex = -1;
      return;
    }

    typeahead.hidden = false;
    typeahead.innerHTML = lastSuggestions.map((item, index) => `
      <button
        class="productization-typeahead-option"
        type="button"
        data-suggestion-index="${index}"
      >
        <strong>${escapeHtml(item.version_name || item.version_id)}</strong>
        <span>${escapeHtml(item.market || 'No market label')}</span>
      </button>
    `).join('');
    setActiveSuggestion(0);

    typeahead.querySelectorAll('[data-suggestion-index]').forEach((button) => {
      button.addEventListener('mousedown', (event) => {
        event.preventDefault();
      });
      button.addEventListener('click', () => {
        const index = Number(button.dataset.suggestionIndex || -1);
        if (index >= 0) {
          selectSuggestion(lastSuggestions[index]);
        }
      });
    });
  };

  const loadIssuesForSelection = async (selection) => {
    setStatus(`Loading Jira tickets for ${selection.label}...`);
    try {
      const response = await fetch(`/api/productization-upgrade-summary/issues?version_id=${encodeURIComponent(selection.version_id)}`);
      const payload = await readJsonOrThrow(response, 'Could not load Jira tickets for that version.');
      selection.items = payload.items || [];
      renderSelectedVersions();
      renderResults();
      setStatus(
        selectedVersions.length > 1
          ? `Loaded ${selectedVersions.length} versions into one combined table.`
          : `Loaded Jira tickets for ${selection.label}.`,
        'success',
      );
    } catch (error) {
      selectedVersions.splice(selectedVersions.findIndex((entry) => entry.version_id === selection.version_id), 1);
      renderSelectedVersions();
      renderResults();
      setStatus(error.message || 'Could not load Jira tickets for that version.', 'error');
    }
  };

  const selectSuggestion = async (item) => {
    if (!item || selectedVersions.length >= MAX_SELECTIONS) {
      return;
    }

    const alreadySelected = selectedVersions.some((entry) => entry.version_id === item.version_id);
    if (alreadySelected) {
      hideSuggestions();
      input.value = '';
      setStatus('That version is already selected.', 'error');
      return;
    }

    hideSuggestions();
    searchToken += 1;
    if (searchController) {
      searchController.abort();
      searchController = null;
    }
    input.value = '';
    const selection = {
      version_id: item.version_id,
      label: item.version_name || item.version_id,
      items: null,
    };
    selectedVersions.push(selection);
    renderSelectedVersions();
    renderResults();

    if (selectedVersions.length >= MAX_SELECTIONS) {
      input.disabled = true;
      input.placeholder = 'Maximum 2 versions selected';
    }

    await loadIssuesForSelection(selection);
  };

  const searchVersions = async (rawQuery) => {
    const query = String(rawQuery || '').trim();
    if (!query) {
      hideSuggestions();
      if (!selectedVersions.length) {
        setStatus('Type a version keyword to begin.');
      }
      return;
    }
    if (selectedVersions.length >= MAX_SELECTIONS) {
      hideSuggestions();
      setStatus('You can compare up to 2 versions only.', 'error');
      return;
    }

    searchToken += 1;
    const requestToken = searchToken;
    if (searchController) searchController.abort();
    searchController = new AbortController();
    setStatus(`Searching versions matching "${query}"...`);

    try {
      const response = await fetch(`/api/productization-upgrade-summary/versions?q=${encodeURIComponent(query)}`, {
        signal: searchController.signal,
      });
      const payload = await readJsonOrThrow(response, 'Could not search versions.');
      if (requestToken !== searchToken) return;
      renderSuggestions(payload.items || [], query);
      const count = payload.items?.length || 0;
      setStatus(count ? `Select one exact version from ${count} match${count === 1 ? '' : 'es'}.` : `No matching versions for "${query}".`, count ? 'success' : 'error');
    } catch (error) {
      if (error.name === 'AbortError') return;
      hideSuggestions();
      setStatus(error.message || 'Could not search versions.', 'error');
    }
  };

  form.addEventListener('submit', (event) => {
    event.preventDefault();
  });

  input.addEventListener('input', () => {
    window.clearTimeout(inputTimer);
    inputTimer = window.setTimeout(() => {
      searchVersions(input.value);
    }, 220);
  });

  input.addEventListener('keydown', (event) => {
    if (typeahead.hidden || !lastSuggestions.length) {
      if (event.key === 'Enter') {
        event.preventDefault();
      }
      return;
    }

    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setActiveSuggestion((activeSuggestionIndex + 1) % lastSuggestions.length);
      return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      setActiveSuggestion((activeSuggestionIndex - 1 + lastSuggestions.length) % lastSuggestions.length);
      return;
    }
    if (event.key === 'Enter') {
      event.preventDefault();
      if (activeSuggestionIndex >= 0 && lastSuggestions[activeSuggestionIndex]) {
        selectSuggestion(lastSuggestions[activeSuggestionIndex]);
      }
      return;
    }
    if (event.key === 'Escape') {
      hideSuggestions();
      return;
    }
  });

  input.addEventListener('blur', () => {
    window.setTimeout(() => {
      hideSuggestions();
    }, 120);
  });

  copyButton.addEventListener('click', async () => {
    const readySelections = selectedVersions.filter((entry) => Array.isArray(entry.items) && entry.items.length);
    if (!readySelections.length) {
      return;
    }
    try {
      const text = buildCopyText();
      const html = buildCopyHtml();
      if (navigator.clipboard?.write && typeof ClipboardItem !== 'undefined') {
        await navigator.clipboard.write([
          new ClipboardItem({
            'text/plain': new Blob([text], { type: 'text/plain' }),
            'text/html': new Blob([html], { type: 'text/html' }),
          }),
        ]);
      } else {
        await copyText(text);
      }
      const totalRows = readySelections.reduce((sum, entry) => sum + entry.items.length, 0);
      setStatus(`Copied ${totalRows} rows across ${readySelections.length} selected version${readySelections.length === 1 ? '' : 's'}.`, 'success');
    } catch (error) {
      setStatus(error.message || 'Could not copy this table.', 'error');
    }
  });

  renderSelectedVersions();
  resetResults();
})();
