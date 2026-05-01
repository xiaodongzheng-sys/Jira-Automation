(() => {
  const root = document.querySelector('[data-prd-self-assessment]');
  if (!root) return;

  const form = root.querySelector('[data-prd-self-assessment-form]');
  const urlInput = root.querySelector('[data-prd-self-assessment-url]');
  const languageSelect = root.querySelector('[data-prd-self-assessment-language]');
  const statusNode = root.querySelector('[data-prd-self-assessment-status]');
  const resultPanel = root.querySelector('[data-prd-self-assessment-result]');
  const buttons = [...root.querySelectorAll('[data-prd-self-assessment-action]')];

  const STORAGE_KEY = 'prd-self-assessment:last-form:v1';
  let lastAction = 'summary';

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

  const setLoading = (action, isLoading) => {
    buttons.forEach((button) => {
      const buttonAction = button.dataset.prdSelfAssessmentAction;
      button.disabled = isLoading;
      if (!isLoading) {
        button.textContent = buttonAction === 'summary' ? 'Generate PRD Summary' : 'Generate AI PRD Review';
      } else if (buttonAction === action) {
        button.textContent = action === 'summary' ? 'Summarizing...' : 'Reviewing...';
      }
    });
  };

  const endpointFor = (action) => (
    action === 'summary'
      ? (root.dataset.summaryUrl || '/api/prd-self-assessment/summary')
      : (root.dataset.reviewUrl || '/api/prd-self-assessment/review')
  );

  const renderResult = (action, payload) => {
    if (!resultPanel) return;
    const isSummary = action === 'summary';
    const result = isSummary ? (payload.summary || {}) : (payload.review || {});
    const prd = payload.prd || {};
    const language = payload.language === 'en' ? 'English' : 'Chinese';
    resultPanel.hidden = false;
    resultPanel.dataset.tone = 'success';
    resultPanel.innerHTML = `
      <div class="briefing-review-meta">
        <div>
          <strong>${escapeHtml(payload.cached ? `Cached PRD ${isSummary ? 'Summary' : 'Review'}` : `PRD ${isSummary ? 'Summary' : 'Review'}`)}</strong>
          <span>${escapeHtml(language)} · ${escapeHtml(prd.title || 'PRD')}</span>
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
    saveForm();
    if (!isValidHttpUrl(prdUrl)) {
      const message = 'Enter a valid Confluence page URL.';
      setStatus(message, 'error');
      renderError(message);
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
        body: JSON.stringify({ prd_url: prdUrl, language, force_refresh: forceRefresh }),
      });
      const payload = await parseJsonResponse(response);
      if (!response.ok) throw new Error(payload.message || 'Could not generate PRD output right now.');
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

  restoreForm();
  urlInput?.addEventListener('input', saveForm);
  languageSelect?.addEventListener('change', saveForm);
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
