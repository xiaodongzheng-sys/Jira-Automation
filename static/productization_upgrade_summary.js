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
  const llmDescriptionButton = root.querySelector('[data-productization-llm-description-button]');
  const showAllToggle = root.querySelector('[data-productization-show-all-toggle]');

  if (!form || !input || !typeahead || !selectedNode || !selectedEmpty || !status || !tableWrap || !resultsBody || !resultsEmpty || !resultsTitle || !copyButton || !llmDescriptionButton || !showAllToggle) {
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
    return text.replace(/\r/g, '').replace(/\n{3,}/g, '\n\n').trim();
  };

  const stripListMarker = (value) => String(value || '').replace(/^(\d+[.)]|[-*•])\s+/, '').trim();

  const splitDetailedFeatureItems = (value) => {
    const text = formatDetailedFeatureText(value);
    if (text === '-') return ['-'];
    const explicitLines = text
      .replace(/\s+(\d+[.)])\s+/g, '\n$1 ')
      .split(/\n+/)
      .map((line) => stripListMarker(line))
      .filter(Boolean);
    if (explicitLines.length > 1) return explicitLines;

    const sentenceLike = text
      .match(/.+?(?:[.;](?=\s+[A-Z0-9])|$)/g)
      ?.map((item) => stripListMarker(item))
      .filter(Boolean) || [];
    if (sentenceLike.length > 1 && text.length > 140) return sentenceLike;
    return [text];
  };

  const formatDetailedFeatureHtml = (value) => {
    const items = splitDetailedFeatureItems(value);
    if (items.length <= 1) {
      return `<div class="productization-detail-line">${escapeHtml(items[0] || '-')}</div>`;
    }
    return `
      <ul class="productization-detail-list">
        ${items.map((item) => `<li>${escapeHtml(item)}</li>`).join('')}
      </ul>
    `;
  };

  const formatDetailedFeatureCopyText = (value) => {
    const items = splitDetailedFeatureItems(value);
    if (items.length <= 1) return items[0] || '-';
    return items.map((item) => `- ${item}`).join('\n');
  };

  const formatDetailedFeatureCopyHtml = (value) => {
    const items = splitDetailedFeatureItems(value);
    if (items.length <= 1) {
      return escapeHtml(items[0] || '-');
    }
    return items.map((item) => `<div>- ${escapeHtml(item)}</div>`).join('');
  };

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
    llmDescriptionButton.disabled = true;
    const textNode = resultsEmpty.querySelector('p');
    if (textNode) textNode.textContent = message;
  };

  let searchController = null;
  let inputTimer = null;
  let searchToken = 0;
  let activeSuggestionIndex = -1;
  let lastSuggestions = [];
  const selectedVersions = [];
  const isShowAllEnabled = () => Boolean(showAllToggle.checked);

  const buildCopyRows = () => {
    const header = ['Jira Link', 'Feature Summary', 'Detailed Feature'];
    const rows = [header];
    selectedVersions.forEach((entry, index) => {
      rows.push([entry.label, '', '']);
      (entry.items || []).forEach((item) => {
        rows.push([
          item.jira_ticket_number || '-',
          item.feature_summary || '-',
          formatDetailedFeatureCopyText(item.detailed_feature || '-'),
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
            <td style="white-space:pre-wrap;vertical-align:top;">${formatDetailedFeatureCopyHtml(item.detailed_feature || '-')}</td>
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
    llmDescriptionButton.disabled = false;
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
      const response = await fetch(
        `/api/productization-upgrade-summary/issues?version_id=${encodeURIComponent(selection.version_id)}&show_all_before_team_filtering=${isShowAllEnabled() ? '1' : '0'}`
      );
      const payload = await readJsonOrThrow(response, 'Could not load Jira tickets for that version.');
      selection.items = payload.items || [];
      selection.rawCount = Number(payload.raw_count || 0);
      selection.filteredCount = Number(payload.filtered_count || 0);
      selection.teamFilterApplied = Boolean(payload.team_filter_applied);
      selection.showAllBeforeTeamFiltering = Boolean(payload.show_all_before_team_filtering);
      renderSelectedVersions();
      renderResults();
      if (selectedVersions.length > 1) {
        setStatus(`Loaded ${selectedVersions.length} versions into one combined table.`, 'success');
      } else if (selection.teamFilterApplied && selection.rawCount > selection.filteredCount) {
        setStatus(
          selection.filteredCount > 0
            ? `Loaded ${selection.filteredCount} matching Jira tickets for ${selection.label} after team filtering (${selection.rawCount} before filtering).`
            : `Loaded ${selection.rawCount} Jira tickets for ${selection.label}, but 0 matched the current team filter. Turn on "Show all tickets before team filtering" to inspect the raw list.`,
          'success',
        );
      } else if (selection.showAllBeforeTeamFiltering) {
        setStatus(`Loaded ${selection.rawCount} Jira tickets for ${selection.label} before team filtering.`, 'success');
      } else {
        setStatus(`Loaded Jira tickets for ${selection.label}.`, 'success');
      }
    } catch (error) {
      selectedVersions.splice(selectedVersions.findIndex((entry) => entry.version_id === selection.version_id), 1);
      renderSelectedVersions();
      renderResults();
      setStatus(error.message || 'Could not load Jira tickets for that version.', 'error');
    }
  };

  const generateLlmDescriptions = async () => {
    const readySelections = selectedVersions.filter((entry) => Array.isArray(entry.items) && entry.items.length);
    if (!readySelections.length) return;

    llmDescriptionButton.disabled = true;
    copyButton.disabled = true;
    setStatus(
      readySelections.length > 1
        ? `Generating LLM Description for ${readySelections.length} versions in parallel...`
        : `Generating LLM Description for ${readySelections[0].label}...`,
      'neutral',
    );
    try {
      const results = await Promise.allSettled(readySelections.map(async (selection) => {
        const response = await fetch(
          `/api/productization-upgrade-summary/llm-descriptions?version_id=${encodeURIComponent(selection.version_id)}&show_all_before_team_filtering=${isShowAllEnabled() ? '1' : '0'}`
        );
        const payload = await readJsonOrThrow(response, 'Could not generate LLM Description.');
        return { selection, payload };
      }));

      let generatedTotal = 0;
      const failed = [];
      results.forEach((result, index) => {
        if (result.status === 'rejected') {
          failed.push({
            label: readySelections[index]?.label || `Version ${index + 1}`,
            message: result.reason?.message || 'Could not generate LLM Description.',
          });
          return;
        }
        const { selection, payload } = result.value;
        selection.items = payload.items || [];
        selection.rawCount = Number(payload.raw_count || 0);
        selection.filteredCount = Number(payload.filtered_count || 0);
        selection.teamFilterApplied = Boolean(payload.team_filter_applied);
        selection.showAllBeforeTeamFiltering = Boolean(payload.show_all_before_team_filtering);
        selection.llmDescriptionGenerated = Boolean(payload.llm_description_generated);
        selection.llmGeneratedCount = Number(payload.llm_generated_count || 0);
        generatedTotal += selection.llmGeneratedCount;
      });
      renderResults();

      if (failed.length) {
        setStatus(
          generatedTotal
            ? `LLM generated ${generatedTotal} Description value${generatedTotal === 1 ? '' : 's'}, but ${failed.length} version${failed.length === 1 ? '' : 's'} failed: ${failed[0].label}.`
            : `${failed.length} LLM Description request${failed.length === 1 ? '' : 's'} failed: ${failed[0].message}`,
          generatedTotal ? 'warning' : 'error',
        );
        return;
      }
      setStatus(`LLM generated ${generatedTotal} Description value${generatedTotal === 1 ? '' : 's'} across ${readySelections.length} version${readySelections.length === 1 ? '' : 's'}.`, 'success');
    } catch (error) {
      setStatus(error.message || 'Could not generate LLM Description.', 'error');
    } finally {
      renderResults();
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

  showAllToggle.addEventListener('change', async () => {
    if (!selectedVersions.length) {
      setStatus(
        showAllToggle.checked
          ? 'Show-all mode is on. Select a version to inspect raw tickets before team filtering.'
          : 'Team filtering mode is on when applicable. Select a version to inspect tickets.',
        'success',
      );
      return;
    }

    const selections = selectedVersions.map((selection) => ({
      version_id: selection.version_id,
      label: selection.label,
    }));
    selectedVersions.length = 0;
    renderSelectedVersions();
    renderResults();
    setStatus('Reloading selected versions...', 'neutral');

    for (const selection of selections) {
      const nextSelection = {
        version_id: selection.version_id,
        label: selection.label,
        items: null,
      };
      selectedVersions.push(nextSelection);
      renderSelectedVersions();
      renderResults();
      await loadIssuesForSelection(nextSelection);
    }
  });

  llmDescriptionButton.addEventListener('click', () => {
    void generateLlmDescriptions();
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

  window.addEventListener('portal:tab-activated', (event) => {
    if (event.detail?.tabName === 'productization-upgrade-summary') {
      renderSelectedVersions();
      renderResults();
    }
  });

  renderSelectedVersions();
  resetResults();
})();
