(() => {
  const root = document.querySelector('[data-prd-self-assessment]');
  if (!root) return;

  const form = root.querySelector('[data-prd-self-assessment-form]');
  const urlInput = root.querySelector('[data-prd-self-assessment-url]');
  const languageSelect = root.querySelector('[data-prd-self-assessment-language]');
  const statusNode = root.querySelector('[data-prd-self-assessment-status]');
  const resultPanel = root.querySelector('[data-prd-self-assessment-result]');
  const buttons = [...root.querySelectorAll('[data-prd-self-assessment-action]')];
  const loadSectionsButton = root.querySelector('[data-prd-self-assessment-load-sections]');
  const selectAllButton = root.querySelector('[data-prd-self-assessment-select-all]');
  const clearAllButton = root.querySelector('[data-prd-self-assessment-clear-all]');
  const sectionSummary = root.querySelector('[data-prd-self-assessment-section-summary]');
  const sectionList = root.querySelector('[data-prd-self-assessment-section-list]');
  const jobsUrlTemplate = root.dataset.jobsUrl || '/api/jobs/__JOB_ID__';

  const STORAGE_KEY = 'prd-self-assessment:last-form:v1';
  let lastAction = 'summary';
  let sectionState = {
    loadedUrl: '',
    prdTitle: '',
    sections: [],
  };

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const isValidHttpUrl = (value) => /^https?:\/\/\S+/i.test(String(value || '').trim());

  const readSavedForm = () => {
    try {
      const parsed = JSON.parse(window.localStorage.getItem(STORAGE_KEY) || '{}');
      return parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
      return {};
    }
  };

  const saveForm = () => {
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify({
        prd_url: String(urlInput?.value || '').trim(),
        language: languageSelect?.value === 'en' ? 'en' : 'zh',
      }));
    } catch {
      // Persistence is best-effort; the API action should still work.
    }
  };

  const restoreForm = () => {
    const saved = readSavedForm();
    if (urlInput && saved.prd_url) urlInput.value = String(saved.prd_url);
    if (languageSelect) languageSelect.value = saved.language === 'en' ? 'en' : 'zh';
  };

  const setStatus = (message, tone = 'neutral') => {
    if (!statusNode) return;
    statusNode.innerHTML = `<p>${escapeHtml(message)}</p>`;
    statusNode.dataset.tone = tone;
  };

  const setSectionSummary = (message, tone = 'neutral') => {
    if (!sectionSummary) return;
    sectionSummary.textContent = message;
    sectionSummary.dataset.tone = tone;
  };

  const renderMarkdown = (value) => {
    const lines = String(value || '').split(/\r?\n/);
    let inList = false;
    const html = [];
    const closeList = () => {
      if (inList) {
        html.push('</ul>');
        inList = false;
      }
    };
    const inline = (text) => escapeHtml(text)
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/`(.+?)`/g, '<code>$1</code>');
    lines.forEach((line) => {
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

  const parseJsonResponse = async (response) => {
    const contentType = String(response.headers.get('content-type') || '').toLowerCase();
    if (contentType.includes('application/json')) return response.json();
    const text = await response.text().catch(() => '');
    if (response.redirected || text.trim().startsWith('<!DOCTYPE') || text.trim().startsWith('<html')) {
      throw new Error('Your session expired or requires sign-in. Refresh the page and try again.');
    }
    throw new Error(`Unexpected API response format (${contentType || 'unknown'}).`);
  };

  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  const jobUrl = (jobId) => jobsUrlTemplate.replace('__JOB_ID__', encodeURIComponent(jobId));

  const pollJobResult = async (jobId, action) => {
    let lastMessage = '';
    while (true) {
      const response = await fetch(jobUrl(jobId), {
        method: 'GET',
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      const payload = await parseJsonResponse(response);
      if (!response.ok || payload.status === 'error') throw new Error(payload.message || 'Could not load PRD job status.');
      const message = payload.message || payload.progress?.message || '';
      if (message && message !== lastMessage) {
        lastMessage = message;
        setStatus(message);
        if (resultPanel) {
          resultPanel.innerHTML = `<div class="briefing-review-loading">${escapeHtml(message)}</div>`;
        }
      }
      if (payload.state === 'completed') {
        const result = Array.isArray(payload.results) && payload.results[0] ? payload.results[0] : {};
        if (!result || result.status === 'error') throw new Error(result.message || 'PRD job finished without a result.');
        return result;
      }
      if (payload.state === 'failed') {
        throw new Error(payload.error || payload.message || 'PRD job failed.');
      }
      await sleep(1200);
    }
  };

  const selectedSectionIndexes = () => {
    if (!sectionState.sections.length || !sectionList) return null;
    return [...sectionList.querySelectorAll('input[type="checkbox"][data-section-index]:checked')]
      .map((input) => Number(input.dataset.sectionIndex))
      .filter((index) => Number.isInteger(index) && index > 0);
  };

  const updateReviewButtonState = () => {
    const reviewButton = buttons.find((button) => button.dataset.prdSelfAssessmentAction === 'review');
    if (!reviewButton || reviewButton.dataset.loading === 'true') return;
    const selected = selectedSectionIndexes();
    const hasLoadedSections = Array.isArray(selected);
    reviewButton.disabled = hasLoadedSections && selected.length === 0;
    if (hasLoadedSections && selected.length === 0) {
      reviewButton.title = 'Select at least one PRD section to review.';
    } else {
      reviewButton.removeAttribute('title');
    }
    if (hasLoadedSections && sectionState.sections.length) {
      setSectionSummary(`${selected.length}/${sectionState.sections.length} sections selected for AI PRD Review.`);
    }
  };

  const setLoading = (action, isLoading) => {
    buttons.forEach((button) => {
      const buttonAction = button.dataset.prdSelfAssessmentAction;
      button.dataset.loading = isLoading ? 'true' : 'false';
      button.disabled = isLoading;
      if (!isLoading) {
        button.textContent = buttonAction === 'summary' ? 'Generate PRD Summary' : 'Generate AI PRD Review';
      } else if (buttonAction === action) {
        button.textContent = action === 'summary' ? 'Summarizing...' : 'Reviewing...';
      }
    });
    if (loadSectionsButton) loadSectionsButton.disabled = isLoading;
    if (selectAllButton) selectAllButton.disabled = isLoading;
    if (clearAllButton) clearAllButton.disabled = isLoading;
    if (!isLoading) updateReviewButtonState();
  };

  const endpointFor = (action) => (
    action === 'summary'
      ? (root.dataset.summaryUrl || '/api/prd-self-assessment/summary')
      : (root.dataset.reviewUrl || '/api/prd-self-assessment/review')
  );

  const latestEndpoint = () => root.dataset.latestUrl || '/api/prd-self-assessment/latest';
  const sectionsEndpoint = () => root.dataset.sectionsUrl || '/api/prd-self-assessment/sections';

  const clearSections = ({ message = 'Load sections to choose which PRD areas need review.' } = {}) => {
    sectionState = { loadedUrl: '', prdTitle: '', sections: [] };
    if (sectionList) {
      sectionList.hidden = true;
      sectionList.innerHTML = '';
    }
    if (selectAllButton) selectAllButton.hidden = true;
    if (clearAllButton) clearAllButton.hidden = true;
    setSectionSummary(message);
    updateReviewButtonState();
  };

  const renderSections = (payload) => {
    const sections = Array.isArray(payload.sections) ? payload.sections : [];
    sectionState = {
      loadedUrl: String(urlInput?.value || '').trim(),
      prdTitle: payload.prd?.title || 'PRD',
      sections,
    };
    if (!sectionList) return;
    sectionList.hidden = false;
    sectionList.innerHTML = sections.map((section) => {
      const index = Number(section.index);
      const title = section.title || `Section ${index}`;
      const charCount = Number(section.char_count || 0);
      const linkedSpreadsheetCount = Number(section.linked_spreadsheet_count || 0);
      const linkedLabel = linkedSpreadsheetCount
        ? `${linkedSpreadsheetCount.toLocaleString()} linked spreadsheet${linkedSpreadsheetCount === 1 ? '' : 's'}`
        : '';
      return `
        <label class="prd-section-option">
          <input type="checkbox" data-section-index="${escapeHtml(index)}" checked>
          <span>
            <strong>${escapeHtml(index)}. ${escapeHtml(title)}</strong>
            <span>${escapeHtml(charCount.toLocaleString())} chars${linkedLabel ? ` · ${escapeHtml(linkedLabel)}` : ''}</span>
          </span>
          ${section.long ? '<span class="prd-section-badge">Long</span>' : ''}
          ${linkedSpreadsheetCount ? '<span class="prd-section-badge">Spreadsheet</span>' : ''}
        </label>
      `;
    }).join('');
    sectionList.querySelectorAll('input[type="checkbox"][data-section-index]').forEach((input) => {
      input.addEventListener('change', updateReviewButtonState);
    });
    if (selectAllButton) selectAllButton.hidden = false;
    if (clearAllButton) clearAllButton.hidden = false;
    setSectionSummary(`${sections.length}/${sections.length} sections selected for AI PRD Review.`);
    updateReviewButtonState();
  };

  const loadSections = async () => {
    const prdUrl = String(urlInput?.value || '').trim();
    const language = languageSelect?.value === 'en' ? 'en' : 'zh';
    saveForm();
    if (!isValidHttpUrl(prdUrl)) {
      const message = 'Enter a valid Confluence page URL before loading sections.';
      setSectionSummary(message, 'error');
      setStatus(message, 'error');
      return;
    }
    if (loadSectionsButton) {
      loadSectionsButton.disabled = true;
      loadSectionsButton.textContent = 'Loading...';
    }
    setSectionSummary('Reading PRD sections...');
    try {
      const response = await fetch(sectionsEndpoint(), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ prd_url: prdUrl, language }),
      });
      const payload = await parseJsonResponse(response);
      if (!response.ok || payload.status === 'error') throw new Error(payload.message || 'Could not load PRD sections.');
      renderSections(payload);
      setStatus(`Loaded ${payload.sections?.length || 0} PRD sections.`, 'success');
    } catch (error) {
      const message = error.message || 'Could not load PRD sections.';
      clearSections({ message });
      setStatus(message, 'error');
    } finally {
      if (loadSectionsButton) {
        loadSectionsButton.disabled = false;
        loadSectionsButton.textContent = 'Load Sections';
      }
    }
  };

  const renderResult = (action, payload) => {
    if (!resultPanel) return;
    const isSummary = action === 'summary';
    const result = isSummary ? (payload.summary || {}) : (payload.review || {});
    const prd = payload.prd || {};
    const language = payload.language === 'en' ? 'English' : 'Chinese';
    const coverage = payload.coverage || {};
    const titles = Array.isArray(coverage.selected_section_titles) ? coverage.selected_section_titles : [];
    const coverageLine = !isSummary && coverage.sections_assessed
      ? `<span>Reviewed sections: ${escapeHtml(coverage.sections_assessed)}/${escapeHtml(coverage.selected_sections_total || coverage.sections_assessed)} selected${coverage.sections_total ? ` · ${escapeHtml(coverage.sections_total)} total` : ''}</span>`
      : '';
    const generationCoverageLine = coverage.mode
      ? `<span>Coverage: ${escapeHtml(coverage.mode)} · ${escapeHtml(coverage.sections_covered || coverage.sections_assessed || 0)}/${escapeHtml(coverage.sections_total || coverage.selected_sections_total || 0)} sections${coverage.truncated ? ' · truncated' : ''}</span>`
      : '';
    const reportTemplatesTotal = Number(coverage.report_templates_total ?? coverage.linked_artifacts_total);
    const reportTemplatesReviewed = Number(coverage.report_templates_reviewed ?? coverage.linked_artifacts_reviewed ?? 0);
    const reportTemplatesFailed = Number(coverage.report_templates_failed ?? coverage.linked_artifacts_failed ?? 0);
    const linkedCoverageLine = !isSummary && Number.isFinite(reportTemplatesTotal) && reportTemplatesTotal > 0
      ? `<span>Report templates reviewed: ${escapeHtml(reportTemplatesReviewed)}/${escapeHtml(reportTemplatesTotal)}${reportTemplatesFailed ? ` · ${escapeHtml(reportTemplatesFailed)} not reviewed` : ''}</span>`
      : '';
    const linkedArtifacts = Array.isArray(coverage.report_templates) ? coverage.report_templates : (Array.isArray(coverage.linked_artifacts) ? coverage.linked_artifacts : []);
    const linkedArtifactLine = !isSummary && linkedArtifacts.length
      ? `<p class="help-text">${escapeHtml(linkedArtifacts.slice(0, 4).map((item) => `${item.status === 'ok' ? 'Reviewed' : 'Not reviewed'}: ${item.title || item.url || 'Report template'}${item.status === 'ok' ? ` (${item.sheet_count || 0} sheets${item.skipped_sheet_count ? `, ${item.skipped_sheet_count} skipped` : ''})` : ` - ${item.reason || 'unavailable'}`}`).join(' · '))}${linkedArtifacts.length > 4 ? ` · +${escapeHtml(linkedArtifacts.length - 4)} more` : ''}</p>`
      : '';
    const sheetScreenshotTotal = Number(coverage.google_sheet_screenshots_total || 0);
    const sheetScreenshotReviewed = Number(coverage.google_sheet_screenshots_reviewed || 0);
    const sheetScreenshotFailed = Number(coverage.google_sheet_screenshots_failed || 0);
    const sheetScreenshotLine = !isSummary && Number.isFinite(sheetScreenshotTotal) && sheetScreenshotTotal > 0
      ? `<span>Google Sheet screenshots reviewed: ${escapeHtml(sheetScreenshotReviewed)}/${escapeHtml(sheetScreenshotTotal)}${sheetScreenshotFailed ? ` · ${escapeHtml(sheetScreenshotFailed)} not reviewed` : ''}</span>`
      : '';
    const sheetScreenshotImages = Array.isArray(coverage.google_sheet_screenshot_images) ? coverage.google_sheet_screenshot_images : [];
    const sheetScreenshotVisible = sheetScreenshotImages.filter((item) => item.status !== 'skipped' || item.reason !== 'not_google_sheet_screenshot');
    const sheetScreenshotArtifactLine = !isSummary && sheetScreenshotVisible.length
      ? `<p class="help-text">${escapeHtml(sheetScreenshotVisible.slice(0, 4).map((item) => `${item.status === 'ok' ? 'Reviewed' : 'Not reviewed'}: ${item.image_id || 'Screenshot'}${item.status === 'ok' ? '' : ` - ${item.reason || 'unavailable'}`}`).join(' · '))}${sheetScreenshotVisible.length > 4 ? ` · +${escapeHtml(sheetScreenshotVisible.length - 4)} more` : ''}</p>`
      : '';
    const titleLine = !isSummary && titles.length
      ? `<p class="help-text">${escapeHtml(titles.slice(0, 6).join(' · '))}${titles.length > 6 ? ` · +${escapeHtml(titles.length - 6)} more` : ''}</p>`
      : '';
    resultPanel.hidden = false;
    resultPanel.dataset.tone = 'success';
    resultPanel.innerHTML = `
      <div class="briefing-review-meta">
        <div>
          <strong>${escapeHtml(payload.cached ? `Cached PRD ${isSummary ? 'Summary' : 'Review'}` : `PRD ${isSummary ? 'Summary' : 'Review'}`)}</strong>
          <span>${escapeHtml(language)} · ${escapeHtml(prd.title || 'PRD')}</span>
          ${generationCoverageLine}
          ${coverageLine}
          ${linkedCoverageLine}
          ${sheetScreenshotLine}
          ${titleLine}
          ${linkedArtifactLine}
          ${sheetScreenshotArtifactLine}
        </div>
        <span>${escapeHtml(result.updated_at || '')}</span>
      </div>
      <div class="briefing-review-markdown">${renderMarkdown(result.result_markdown || '')}</div>
      <div class="briefing-review-actions">
        <button class="button button-secondary" type="button" data-prd-self-assessment-regenerate>Regenerate</button>
      </div>
    `;
    resultPanel.querySelector('[data-prd-self-assessment-regenerate]')?.addEventListener('click', () => {
      generate(action, { forceRefresh: true });
    });
  };

  const renderError = (message) => {
    if (!resultPanel) return;
    resultPanel.hidden = false;
    resultPanel.dataset.tone = 'error';
    resultPanel.innerHTML = `<p>${escapeHtml(message || 'Could not generate PRD output right now.')}</p>`;
  };

  const generate = async (action, { forceRefresh = false } = {}) => {
    lastAction = action === 'review' ? 'review' : 'summary';
    const prdUrl = String(urlInput?.value || '').trim();
    const language = languageSelect?.value === 'en' ? 'en' : 'zh';
    const selected = lastAction === 'review' ? selectedSectionIndexes() : null;
    saveForm();
    if (!isValidHttpUrl(prdUrl)) {
      const message = 'Enter a valid Confluence page URL.';
      setStatus(message, 'error');
      renderError(message);
      return;
    }
    if (Array.isArray(selected) && selected.length === 0) {
      const message = 'Select at least one PRD section to review.';
      setStatus(message, 'error');
      renderError(message);
      updateReviewButtonState();
      return;
    }
    setLoading(lastAction, true);
    setStatus(lastAction === 'summary' ? 'Reading PRD and generating summary...' : 'Reading PRD and generating AI review...');
    if (resultPanel) {
      resultPanel.hidden = false;
      resultPanel.dataset.tone = 'neutral';
      resultPanel.innerHTML = `<div class="briefing-review-loading">${lastAction === 'summary' ? 'Summarizing PRD with Codex...' : 'Reviewing PRD with Codex...'}</div>`;
    }
    try {
      const response = await fetch(endpointFor(lastAction), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({
          prd_url: prdUrl,
          language,
          force_refresh: forceRefresh,
          async: true,
          ...(Array.isArray(selected) ? { selected_section_indexes: selected } : {}),
        }),
      });
      let payload = await parseJsonResponse(response);
      if (!response.ok) throw new Error(payload.message || 'Could not generate PRD output right now.');
      if (payload.status === 'queued' && payload.job_id) {
        payload = await pollJobResult(payload.job_id, lastAction);
      }
      setStatus(lastAction === 'summary' ? 'PRD summary generated.' : 'AI PRD review generated.', 'success');
      renderResult(lastAction, payload);
    } catch (error) {
      const message = error.message || 'Could not generate PRD output right now.';
      setStatus(message, 'error');
      renderError(message);
    } finally {
      setLoading(lastAction, false);
    }
  };

  const restoreLatestResult = async () => {
    try {
      const response = await fetch(latestEndpoint(), {
        method: 'GET',
        headers: { Accept: 'application/json' },
        credentials: 'same-origin',
      });
      const payload = await parseJsonResponse(response);
      if (!response.ok || payload.status === 'error') return;
      const latest = payload.latest || {};
      const latestPayload = latest.payload || {};
      const action = latestPayload.action === 'review' ? 'review' : (latestPayload.action === 'summary' ? 'summary' : '');
      const resultPayload = latestPayload.payload || {};
      if (!action || !resultPayload || resultPayload.status !== 'ok') return;
      lastAction = action;
      if (resultPayload.prd?.source_url && urlInput) urlInput.value = resultPayload.prd.source_url;
      if (languageSelect) languageSelect.value = resultPayload.language === 'en' ? 'en' : 'zh';
      saveForm();
      setStatus(action === 'summary' ? 'Showing the latest PRD summary.' : 'Showing the latest AI PRD review.', 'success');
      renderResult(action, resultPayload);
    } catch {
      // Latest output is a convenience only; leave the empty state if it cannot be loaded.
    }
  };

  restoreForm();
  restoreLatestResult();
  urlInput?.addEventListener('input', () => {
    saveForm();
    if (sectionState.loadedUrl && String(urlInput.value || '').trim() !== sectionState.loadedUrl) {
      clearSections({ message: 'PRD URL changed. Load sections again before choosing review scope.' });
    }
  });
  languageSelect?.addEventListener('change', saveForm);
  loadSectionsButton?.addEventListener('click', loadSections);
  selectAllButton?.addEventListener('click', () => {
    sectionList?.querySelectorAll('input[type="checkbox"][data-section-index]').forEach((input) => {
      input.checked = true;
    });
    updateReviewButtonState();
  });
  clearAllButton?.addEventListener('click', () => {
    sectionList?.querySelectorAll('input[type="checkbox"][data-section-index]').forEach((input) => {
      input.checked = false;
    });
    updateReviewButtonState();
  });
  form?.addEventListener('submit', (event) => {
    event.preventDefault();
    generate(lastAction);
  });
  buttons.forEach((button) => {
    button.addEventListener('click', () => {
      generate(button.dataset.prdSelfAssessmentAction || 'summary');
    });
  });
})();
