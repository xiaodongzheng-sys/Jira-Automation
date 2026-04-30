(() => {
  const sessionForm = document.querySelector('[data-briefing-session-form]');
  const chatForm = document.querySelector('[data-chat-form]');
  const statusNode = document.querySelector('[data-briefing-status]');
  const walkthroughStatusNode = document.querySelector('[data-walkthrough-status]');
  const sectionListNode = document.querySelector('[data-section-list]');
  const sectionDetailNode = document.querySelector('[data-section-detail]');
  const chatLogNode = document.querySelector('[data-chat-log]');
  const narrateButton = document.querySelector('[data-play-section]');
  const readerModeToggle = document.querySelector('[data-reader-mode-toggle]');
  const noImageModeToggle = document.querySelector('[data-no-image-mode-toggle]');
  const quickQuestionButtons = document.querySelectorAll('[data-quick-question]');
  const imageLightbox = document.querySelector('[data-image-lightbox]');
  const imageLightboxMedia = document.querySelector('[data-image-lightbox-media]');
  const imageLightboxClose = document.querySelector('[data-image-lightbox-close]');
  const imageLightboxOpen = document.querySelector('[data-image-lightbox-open]');
  const sessionSubmitButton = sessionForm?.querySelector('button[type="submit"]');
  const briefingLanguage = document.querySelector('[data-briefing-language]');
  const prdReviewButton = document.querySelector('[data-prd-review-generate]');
  const prdReviewPanel = document.querySelector('[data-prd-review-panel]');
  const chatSubmitButton = chatForm?.querySelector('button[type="submit"]');
  const CACHED_NARRATION_DELAY_MS = 0;
  const MAX_SOURCE_HTML_RENDER_CHARS = 70000;

  let state = {
    sessionId: null,
    sections: [],
    briefingBlocks: [],
    currentSectionIndex: 0,
    currentBlockIndex: 0,
    messages: [],
    isNarrating: false,
    currentAudio: null,
    readerMode: false,
    noImageMode: false,
    briefingLanguage: 'zh',
  };

  const READER_MODE_STORAGE_KEY = 'prd_briefing_reader_mode';
  const NO_IMAGE_MODE_STORAGE_KEY = 'prd_briefing_no_image_mode';
  const NO_IMAGE_MODE_POSITION_STORAGE_KEY = 'prd_briefing_no_image_mode_position';

  const isValidHttpUrl = (value) => /^https?:\/\/\S+/i.test(String(value || '').trim());

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const sanitizePrdHtmlFragment = (value) => {
    const template = document.createElement('template');
    template.innerHTML = String(value || '');

    template.content.querySelectorAll([
      'script',
      'style',
      'link',
      'meta',
      'iframe',
      'object',
      'embed',
      'form',
      'input',
      'button',
      'textarea',
      'select',
      'dialog',
    ].join(',')).forEach((node) => node.remove());

    template.content.querySelectorAll('*').forEach((node) => {
      Array.from(node.attributes || []).forEach((attribute) => {
        const name = attribute.name.toLowerCase();
        if (
          name.startsWith('on')
          || ['style', 'srcdoc', 'autofocus', 'hidden'].includes(name)
        ) {
          node.removeAttribute(attribute.name);
        }
      });

      if (node.tagName === 'A') {
        const href = node.getAttribute('href') || '';
        if (/^\s*javascript:/i.test(href)) {
          node.removeAttribute('href');
        }
        node.setAttribute('target', '_blank');
        node.setAttribute('rel', 'noreferrer');
      }

      if (node.tagName === 'IMG') {
        const src = node.getAttribute('src') || '';
        if (/^\s*javascript:/i.test(src)) {
          node.removeAttribute('src');
        }
        node.setAttribute('loading', 'lazy');
        node.setAttribute('decoding', 'async');
      }
    });

    return template.innerHTML;
  };

  const renderPlainSourceContent = (section) => (section.content || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => `<p>${escapeHtml(line)}</p>`)
    .join('');

  const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

  const normalizeDecorativeText = (value) => String(value || '')
    .replace(/[\u200b-\u200d\ufeff]/g, '')
    .replace(/\s+/g, '');

  const fastNodeText = (node) => String(node?.textContent || '').replace(/\s+/g, ' ').trim();

  const isDecorativeArrowText = (value) => {
    const text = normalizeDecorativeText(value);
    return Boolean(text) && /^[↓↑←→↕↔↘↙↖↗⇩⇧⇦⇨⇣⇡⇠⇢⇓⇑⇒⇐▼▲◀▶▾▴⌄⌃⌵⏷⏶\-–—|.,:;()]+$/.test(text);
  };

  const isDecorativeMarkerText = (value) => {
    const text = normalizeDecorativeText(value);
    if (!text) return true;
    if (isDecorativeArrowText(text)) return true;
    return /^(?:[0-9]+|[ivxlcdmIVXLCDM]+|[a-zA-Z])[.)、:]?(?:(?:[0-9]+|[ivxlcdmIVXLCDM]+|[a-zA-Z])[.)、:]?)*$/.test(text);
  };

  const setStatus = (message, tone = 'neutral') => {
    if (!statusNode) return;
    statusNode.innerHTML = `<p>${escapeHtml(message)}</p>`;
    statusNode.dataset.tone = tone;
  };

  const setWalkthroughStatus = (message, tone = 'neutral') => {
    if (!walkthroughStatusNode) return;
    walkthroughStatusNode.hidden = false;
    walkthroughStatusNode.innerHTML = `<p>${escapeHtml(message)}</p>`;
    walkthroughStatusNode.dataset.tone = tone;
  };

  const clearWalkthroughStatus = () => {
    if (!walkthroughStatusNode) return;
    walkthroughStatusNode.hidden = true;
    walkthroughStatusNode.innerHTML = '<p>Narration status appears here.</p>';
    delete walkthroughStatusNode.dataset.tone;
  };

  const wait = (durationMs) => new Promise((resolve) => {
    window.setTimeout(resolve, durationMs);
  });

  const briefingLanguageLabel = () => (state.briefingLanguage === 'en' ? 'English' : 'Chinese');

  const setSessionSubmitLoading = (isLoading) => {
    if (!sessionSubmitButton) return;
    sessionSubmitButton.disabled = isLoading;
    sessionSubmitButton.textContent = isLoading ? 'Generating...' : 'Generate Developer Walkthrough';
  };

  const parseJsonResponse = async (response) => {
    const contentType = String(response.headers.get('content-type') || '').toLowerCase();
    if (contentType.includes('application/json')) {
      return response.json();
    }

    const text = await response.text().catch(() => '');
    if (response.redirected) {
      throw new Error('Your session expired or requires sign-in. Refresh the page and try again.');
    }
    if (text.trim().startsWith('<!DOCTYPE') || text.trim().startsWith('<html')) {
      throw new Error('The server returned a page instead of an API response. Refresh the page and try again.');
    }
    throw new Error(`Unexpected API response format (${contentType || 'unknown'}).`);
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
    const renderTable = () => {
      if (!table) return;
      const columnCount = Math.max(table.headers.length, ...table.rows.map((row) => row.length), 1);
      const renderCells = (cells, tag) => Array.from({ length: columnCount }, (_, index) => (
        `<${tag}>${inline(cells[index] || '')}</${tag}>`
      )).join('');
      html.push(
        '<div class="briefing-markdown-table-wrap"><table class="briefing-markdown-table">'
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

  const setPrdReviewLoading = (isLoading) => {
    if (!prdReviewButton) return;
    prdReviewButton.disabled = isLoading;
    prdReviewButton.textContent = isLoading ? 'Generating Review...' : 'Generate AI PRD Review';
  };

  const renderPrdReview = (payload) => {
    if (!prdReviewPanel) return;
    const review = payload.review || {};
    const prd = payload.prd || {};
    const language = payload.language === 'en' ? 'English' : 'Chinese';
    prdReviewPanel.hidden = false;
    prdReviewPanel.dataset.tone = 'success';
    prdReviewPanel.innerHTML = `
      <div class="briefing-review-meta">
        <div>
          <strong>${escapeHtml(payload.cached ? 'Cached AI PRD Review' : 'AI PRD Review')}</strong>
          <span>${escapeHtml(language)} · ${escapeHtml(prd.title || 'PRD')}</span>
        </div>
        <span>${escapeHtml(review.updated_at || '')}</span>
      </div>
      <div class="briefing-review-markdown">${renderMarkdown(review.result_markdown || '')}</div>
      <div class="briefing-review-actions">
        <button class="button button-secondary" type="button" data-prd-review-regenerate>Regenerate</button>
      </div>
    `;
    prdReviewPanel.querySelector('[data-prd-review-regenerate]')?.addEventListener('click', () => {
      generatePrdReview({ forceRefresh: true });
    });
  };

  const renderPrdReviewError = (message) => {
    if (!prdReviewPanel) return;
    prdReviewPanel.hidden = false;
    prdReviewPanel.dataset.tone = 'error';
    prdReviewPanel.innerHTML = `<p>${escapeHtml(message || 'Could not generate AI PRD Review right now.')}</p>`;
  };

  const generatePrdReview = async ({ forceRefresh = false } = {}) => {
    const formData = sessionForm ? new FormData(sessionForm) : new FormData();
    const pageRef = String(formData.get('page_ref') || '').trim();
    if (!isValidHttpUrl(pageRef)) {
      setStatus('Enter a valid Confluence page URL.', 'error');
      renderPrdReviewError('Enter a valid Confluence page URL.');
      return;
    }
    setPrdReviewLoading(true);
    if (prdReviewPanel) {
      prdReviewPanel.hidden = false;
      prdReviewPanel.dataset.tone = 'neutral';
      prdReviewPanel.innerHTML = '<div class="briefing-review-loading">Reading the PRD and generating an AI PRD Review with Codex...</div>';
    }
    try {
      const response = await fetch('/prd-briefing/api/review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          prd_url: pageRef,
          language: briefingLanguage?.value || 'zh',
          force_refresh: forceRefresh,
        }),
      });
      const payload = await parseJsonResponse(response);
      if (!response.ok) throw new Error(payload.message || 'Could not generate AI PRD Review right now.');
      renderPrdReview(payload);
    } catch (error) {
      const message = error.message || 'Could not generate AI PRD Review right now.';
      setStatus(message, 'error');
      renderPrdReviewError(message);
    } finally {
      setPrdReviewLoading(false);
    }
  };

  const renderReaderMode = () => {
    const enabled = Boolean(state.readerMode);
    document.body.classList.toggle('briefing-reader-mode', enabled);
    if (readerModeToggle) {
      readerModeToggle.textContent = enabled ? 'Exit Reader Mode' : 'Enter Reader Mode';
      readerModeToggle.setAttribute('aria-pressed', enabled ? 'true' : 'false');
    }
  };

  const renderNoImageMode = () => {
    const enabled = Boolean(state.noImageMode);
    document.body.classList.toggle('briefing-no-image-mode', enabled);
    if (noImageModeToggle) {
      noImageModeToggle.textContent = enabled ? 'Show Images' : 'No-image Mode';
      noImageModeToggle.setAttribute('aria-pressed', enabled ? 'true' : 'false');
    }
    if (enabled) {
      closeImageLightbox();
    }
  };

  const captureReadingAnchor = () => {
    if (!sectionDetailNode) return null;
    const viewportTop = 0;
    const viewportBottom = window.innerHeight || document.documentElement.clientHeight || 0;
    const viewportAnchorY = Math.round(viewportBottom * 0.42);
    const sections = Array.from(sectionDetailNode.querySelectorAll('[data-source-section-index]'));
    if (!sections.length) return null;
    let best = null;
    sections.forEach((node) => {
      const rect = node.getBoundingClientRect();
      const visibleTop = Math.max(rect.top, viewportTop);
      const visibleBottom = Math.min(rect.bottom, viewportBottom);
      const visibleHeight = Math.max(0, visibleBottom - visibleTop);
      const containsAnchor = rect.top <= viewportAnchorY && rect.bottom >= viewportAnchorY;
      const distance = containsAnchor
        ? 0
        : Math.min(Math.abs(rect.top - viewportAnchorY), Math.abs(rect.bottom - viewportAnchorY));
      if (
        !best
        || (containsAnchor && !best.containsAnchor)
        || (containsAnchor === best.containsAnchor && visibleHeight > best.visibleHeight)
        || (containsAnchor === best.containsAnchor && visibleHeight === best.visibleHeight && distance < best.distance)
      ) {
        best = {
          index: node.dataset.sourceSectionIndex,
          node,
          top: rect.top,
          anchorY: viewportAnchorY,
          containsAnchor,
          visibleHeight,
          distance,
        };
      }
    });
    if (best?.node) {
      const readableNodes = Array.from(best.node.querySelectorAll([
        '.briefing-source-heading',
        'h1',
        'h2',
        'h3',
        'h4',
        'h5',
        'h6',
        'p',
        'li',
        'td',
        'th',
        'blockquote',
        'pre',
      ].join(','))).filter((node) => {
        const text = (node.innerText || node.textContent || '').replace(/\s+/g, ' ').trim();
        const rect = node.getBoundingClientRect();
        return text.length > 0 && rect.width > 0 && rect.height > 0;
      });
      let readableAnchor = null;
      readableNodes.forEach((node) => {
        const rect = node.getBoundingClientRect();
        const containsAnchor = rect.top <= viewportAnchorY && rect.bottom >= viewportAnchorY;
        const distance = containsAnchor ? 0 : Math.min(Math.abs(rect.top - viewportAnchorY), Math.abs(rect.bottom - viewportAnchorY));
        if (!readableAnchor || distance < readableAnchor.distance) {
          readableAnchor = {
            node,
            top: rect.top,
            offset: clamp(viewportAnchorY - rect.top, 0, Math.max(0, rect.height)),
            distance,
          };
        }
      });
      if (readableAnchor) {
        best.readableNode = readableAnchor.node;
        best.readableTop = readableAnchor.top;
        best.readableOffset = readableAnchor.offset;
      }
      delete best.node;
    }
    return best;
  };

  const restoreReadingAnchor = (anchor) => {
    if (!anchor || !sectionDetailNode) return;
    const node = Array.from(sectionDetailNode.querySelectorAll('[data-source-section-index]'))
      .find((item) => String(item.dataset.sourceSectionIndex) === String(anchor.index));
    if (!node) return;
    if (anchor.readableNode && node.contains(anchor.readableNode)) {
      const readableRect = anchor.readableNode.getBoundingClientRect();
      if (readableRect.width > 0 && readableRect.height > 0) {
        const offset = clamp(Number(anchor.readableOffset) || 0, 0, readableRect.height);
        window.scrollBy({
          top: readableRect.top + offset - anchor.anchorY,
          left: 0,
          behavior: 'auto',
        });
        return;
      }
    }
    const rect = node.getBoundingClientRect();
    window.scrollBy({
      top: rect.top - anchor.top,
      left: 0,
      behavior: 'auto',
    });
  };

  const restoreReadingAnchorAfterLayout = (anchor) => {
    if (!anchor) return;
    window.requestAnimationFrame(() => {
      restoreReadingAnchor(anchor);
      window.setTimeout(() => restoreReadingAnchor(anchor), 120);
      window.setTimeout(() => restoreReadingAnchor(anchor), 320);
    });
  };

  const applyNoImageTogglePosition = (position) => {
    if (!noImageModeToggle || !position) return;
    const margin = 12;
    const rect = noImageModeToggle.getBoundingClientRect();
    const width = rect.width || 118;
    const height = rect.height || 44;
    const maxLeft = Math.max(margin, window.innerWidth - width - margin);
    const maxTop = Math.max(margin, window.innerHeight - height - margin);
    const left = clamp(Number(position.left) || margin, margin, maxLeft);
    const top = clamp(Number(position.top) || margin, margin, maxTop);
    noImageModeToggle.style.left = `${left}px`;
    noImageModeToggle.style.top = `${top}px`;
    noImageModeToggle.style.right = 'auto';
    noImageModeToggle.style.bottom = 'auto';
    noImageModeToggle.style.transform = 'none';
  };

  const saveNoImageTogglePosition = () => {
    if (!noImageModeToggle) return;
    const rect = noImageModeToggle.getBoundingClientRect();
    try {
      window.localStorage.setItem(NO_IMAGE_MODE_POSITION_STORAGE_KEY, JSON.stringify({
        left: Math.round(rect.left),
        top: Math.round(rect.top),
      }));
    } catch {}
  };

  const restoreNoImageTogglePosition = () => {
    if (!noImageModeToggle) return;
    try {
      const saved = JSON.parse(window.localStorage.getItem(NO_IMAGE_MODE_POSITION_STORAGE_KEY) || 'null');
      if (saved && Number.isFinite(Number(saved.left)) && Number.isFinite(Number(saved.top))) {
        applyNoImageTogglePosition(saved);
      }
    } catch {}
  };

  const setupNoImageToggleDrag = () => {
    if (!noImageModeToggle) return;
    let dragState = null;
    noImageModeToggle.addEventListener('pointerdown', (event) => {
      if (event.button !== 0) return;
      const rect = noImageModeToggle.getBoundingClientRect();
      dragState = {
        pointerId: event.pointerId,
        startX: event.clientX,
        startY: event.clientY,
        left: rect.left,
        top: rect.top,
        moved: false,
      };
      noImageModeToggle.setPointerCapture?.(event.pointerId);
      noImageModeToggle.classList.add('is-dragging');
    });
    noImageModeToggle.addEventListener('pointermove', (event) => {
      if (!dragState || event.pointerId !== dragState.pointerId) return;
      const deltaX = event.clientX - dragState.startX;
      const deltaY = event.clientY - dragState.startY;
      if (Math.abs(deltaX) + Math.abs(deltaY) > 4) {
        dragState.moved = true;
      }
      if (!dragState.moved) return;
      event.preventDefault();
      applyNoImageTogglePosition({
        left: dragState.left + deltaX,
        top: dragState.top + deltaY,
      });
    });
    const finishDrag = (event) => {
      if (!dragState || event.pointerId !== dragState.pointerId) return;
      noImageModeToggle.releasePointerCapture?.(event.pointerId);
      noImageModeToggle.classList.remove('is-dragging');
      if (dragState.moved) {
        noImageModeToggle.dataset.suppressClick = 'true';
        saveNoImageTogglePosition();
        window.setTimeout(() => {
          delete noImageModeToggle.dataset.suppressClick;
        }, 250);
      }
      dragState = null;
    };
    noImageModeToggle.addEventListener('pointerup', finishDrag);
    noImageModeToggle.addEventListener('pointercancel', finishDrag);
    window.addEventListener('resize', () => {
      if (!noImageModeToggle.style.left || !noImageModeToggle.style.top) return;
      const rect = noImageModeToggle.getBoundingClientRect();
      applyNoImageTogglePosition({ left: rect.left, top: rect.top });
      saveNoImageTogglePosition();
    });
  };

  const stopNarration = () => {
    if (state.currentAudio) {
      state.currentAudio.pause();
      state.currentAudio.currentTime = 0;
      state.currentAudio = null;
    }
    clearSourceHighlights();
    state.isNarrating = false;
    if (narrateButton) narrateButton.disabled = !state.sessionId;
  };

  const activeBlock = () => {
    if (!state.briefingBlocks.length) return null;
    return state.briefingBlocks[state.currentBlockIndex] || state.briefingBlocks[0] || null;
  };

  const activeSection = () => state.sections[state.currentSectionIndex] || state.sections[0] || null;

  const activeSectionIndexes = () => {
    const block = activeBlock();
    if (block) return (block.section_indexes || []).map((value) => Number(value)).filter(Number.isFinite);
    return [state.currentSectionIndex];
  };

  const clearSourceHighlights = () => {
    if (!sectionDetailNode) return;
    sectionDetailNode.querySelectorAll('.briefing-source-section.is-narrating-source').forEach((node) => {
      node.classList.remove('is-narrating-source');
    });
  };

  const highlightActiveSources = () => {
    if (!sectionDetailNode) return;
    clearSourceHighlights();
    const indexes = new Set(activeSectionIndexes());
    let first = null;
    sectionDetailNode.querySelectorAll('[data-source-section-index]').forEach((node) => {
      const index = Number(node.dataset.sourceSectionIndex || '-1');
      const active = indexes.has(index);
      node.classList.toggle('is-narrating-source', active);
      if (active && !first) first = node;
    });
    first?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };

  const closeImageLightbox = () => {
    if (!imageLightbox) return;
    if (typeof imageLightbox.close === 'function' && imageLightbox.open) {
      imageLightbox.close();
    }
  };

  const openImageLightbox = (src, alt) => {
    if (!imageLightbox || !imageLightboxMedia || !src) return;
    imageLightboxMedia.src = src;
    imageLightboxMedia.alt = alt || 'Enlarged PRD image preview';
    if (imageLightboxOpen) imageLightboxOpen.href = src;
    if (typeof imageLightbox.showModal === 'function') {
      imageLightbox.showModal();
    }
  };

  const addHorizontalHints = () => {
    if (!sectionDetailNode) return;
    sectionDetailNode.querySelectorAll('.briefing-scroll-hint').forEach((hint) => hint.remove());
    const wrappers = sectionDetailNode.querySelectorAll('.confluence-embedded-file-wrapper');
    wrappers.forEach((wrapper) => {
      wrapper.classList.add('briefing-horizontal-scroll');
      const hasOverflow = wrapper.scrollWidth > wrapper.clientWidth + 8;
      if (hasOverflow && !wrapper.previousElementSibling?.classList.contains('briefing-scroll-hint')) {
        const hint = document.createElement('div');
        hint.className = 'briefing-scroll-hint';
        hint.textContent = 'Scroll horizontally to view the full content';
        wrapper.parentNode?.insertBefore(hint, wrapper);
      }
      const syncState = () => {
        wrapper.classList.toggle('is-scrollable-right', wrapper.scrollLeft + wrapper.clientWidth < wrapper.scrollWidth - 8);
        wrapper.classList.toggle('is-scrollable-left', wrapper.scrollLeft > 8);
      };
      syncState();
      wrapper.addEventListener('scroll', syncState, { passive: true });
    });
  };

  const enhancePresentationTables = () => {
    if (!sectionDetailNode) return;
    const wrappers = sectionDetailNode.querySelectorAll('.table-wrap');
    wrappers.forEach((wrapper) => {
      const table = wrapper.querySelector('table');
      if (!table || table.dataset.briefingEnhanced === 'true') return;
      const rows = Array.from(table.querySelectorAll('tr'));
      if (rows.length < 2) return;
      const bodyRows = rows.filter((row) => row.querySelectorAll('td').length >= 2);
      if (!bodyRows.length) return;
      const screenshotLikeRows = bodyRows.filter((row) => {
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return false;
        const leftTextLength = (cells[0].innerText || '').trim().length;
        const rightImages = cells[1].querySelectorAll('img').length;
        return leftTextLength > 30 && rightImages > 0;
      });
      if (screenshotLikeRows.length < Math.max(1, Math.floor(bodyRows.length / 2))) return;

      table.dataset.briefingEnhanced = 'true';
      wrapper.classList.add('briefing-presentation-table');
      const cards = document.createElement('div');
      cards.className = 'briefing-presentation-cards';

      bodyRows.forEach((row, index) => {
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) return;
        const card = document.createElement('article');
        card.className = 'briefing-presentation-card';

        const textPane = document.createElement('div');
        textPane.className = 'briefing-presentation-copy';
        textPane.innerHTML = cells[0].innerHTML;

        const imagePane = document.createElement('div');
        imagePane.className = 'briefing-presentation-visual';
        imagePane.innerHTML = cells[1].innerHTML;

        const label = document.createElement('div');
        label.className = 'briefing-presentation-step';
        label.textContent = `Reference ${index + 1}`;

        card.appendChild(label);
        card.appendChild(textPane);
        card.appendChild(imagePane);
        cards.appendChild(card);
      });

      if (!cards.children.length) return;

      wrapper.insertAdjacentElement('afterend', cards);
      wrapper.classList.add('is-replaced');
    });
  };

  const classifyTableLayouts = () => {
    if (!sectionDetailNode) return;
    sectionDetailNode.querySelectorAll('.table-wrap table').forEach((table) => {
      const rows = Array.from(table.querySelectorAll('tr'));
      const maxColumns = rows.reduce((largest, row) => Math.max(largest, row.querySelectorAll('th, td').length), 0);
      const imageCells = Array.from(table.querySelectorAll('td, th')).filter((cell) => cell.querySelector('img'));
      const hasMedia = imageCells.length > 0;
      const hasDenseColumns = maxColumns >= 5;
      const hasMediaSplit = hasMedia && maxColumns === 2;

      table.classList.toggle('briefing-dense-table', hasDenseColumns);
      table.classList.toggle('briefing-media-table', hasMedia);
      table.classList.toggle('briefing-media-split-table', hasMediaSplit);
      if (hasDenseColumns || hasMedia) {
        table.closest('.table-wrap')?.classList.add('briefing-natural-table-wrap');
      }

      imageCells.forEach((cell) => {
        cell.classList.add('briefing-media-cell');
        const text = fastNodeText(cell);
        cell.classList.toggle('briefing-pure-media-cell', text.length === 0 || isDecorativeArrowText(text));
      });
    });
  };

  const hideDecorativeArrowArtifacts = () => {
    if (!sectionDetailNode) return;
    sectionDetailNode.querySelectorAll('.briefing-decorative-arrow-only, .briefing-decorative-marker-row').forEach((node) => {
      node.classList.remove('briefing-decorative-arrow-only');
      node.classList.remove('briefing-decorative-marker-row');
    });
    sectionDetailNode.querySelectorAll('.briefing-original-content tr').forEach((row) => {
      const cells = Array.from(row.children).filter((child) => child.matches?.('td, th'));
      if (!cells.length) return;
      const isMarkerOnlyRow = cells.every((cell) => {
        const hasMedia = Boolean(cell.querySelector('img, video, svg, canvas'));
        return !hasMedia && isDecorativeMarkerText(fastNodeText(cell));
      });
      if (isMarkerOnlyRow) {
        row.classList.add('briefing-decorative-marker-row');
      }
    });
    sectionDetailNode.querySelectorAll('.briefing-original-content p, .briefing-original-content li, .briefing-presentation-copy p, .briefing-presentation-copy li').forEach((node) => {
      const hasMedia = Boolean(node.querySelector('img, video, svg, canvas'));
      if (!hasMedia && isDecorativeMarkerText(fastNodeText(node))) {
        node.classList.add('briefing-decorative-arrow-only');
      }
    });
    sectionDetailNode.querySelectorAll('.briefing-original-content td, .briefing-original-content th').forEach((node) => {
      const hasMedia = Boolean(node.querySelector('img, video, svg, canvas'));
      if (!hasMedia && isDecorativeArrowText(fastNodeText(node))) {
        node.classList.add('briefing-decorative-arrow-only');
      }
    });
  };

  const classifySectionImages = () => {
    if (!sectionDetailNode) return;
    sectionDetailNode.querySelectorAll('img').forEach((image) => {
      const applyClass = () => {
        const inMediaArea = Boolean(image.closest('.briefing-media-cell, .briefing-presentation-visual'));
        const contextText = image.closest('.briefing-presentation-copy, td, th, p, li')?.textContent?.toLowerCase() || '';
        const src = `${image.currentSrc || image.src || ''}`.toLowerCase();
        const isSmallAsset = image.naturalWidth > 0 && image.naturalHeight > 0 && image.naturalWidth <= 180 && image.naturalHeight <= 180;
        const isIconLike =
          !inMediaArea
          && (image.closest('.briefing-presentation-copy')
          || isSmallAsset
          || /arrow|expand|collapse|up|down|icon/.test(src)
          || /expand|collapse|icon/.test(contextText));
        image.classList.toggle('briefing-inline-icon', Boolean(isIconLike));
      };

      if (image.complete) {
        applyClass();
      } else {
        image.addEventListener('load', applyClass, { once: true });
      }
    });
  };

  const playCurrentSection = async () => {
    if (!state.sessionId || state.isNarrating) return;
    state.isNarrating = true;
    clearWalkthroughStatus();
    if (narrateButton) {
      narrateButton.disabled = true;
      narrateButton.textContent = `Generating ${briefingLanguageLabel()} narration...`;
    }
    try {
      const response = await fetch(`/prd-briefing/api/session/${state.sessionId}/narrate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          briefing_block_id: activeBlock()?.block_id || null,
          section_index: state.currentSectionIndex,
          include_audio: true,
        }),
      });
      const payload = await parseJsonResponse(response);
      if (!response.ok) throw new Error(payload.message || 'Could not generate narration for this module.');
      if (payload.cached) {
        setWalkthroughStatus(`Cache hit. Preparing the ${briefingLanguageLabel()} narration for this module...`, 'neutral');
        await wait(CACHED_NARRATION_DELAY_MS);
      }
      if (payload.audio_url) {
        const audio = new Audio(payload.audio_url);
        state.currentAudio = audio;
        highlightActiveSources();
        audio.addEventListener('ended', () => {
          state.currentAudio = null;
          state.isNarrating = false;
          clearSourceHighlights();
          if (narrateButton) narrateButton.disabled = false;
        }, { once: true });
        audio.addEventListener('error', () => {
          state.currentAudio = null;
          state.isNarrating = false;
          clearSourceHighlights();
          if (narrateButton) narrateButton.disabled = false;
        }, { once: true });
        await audio.play().catch(() => {
          state.currentAudio = null;
          state.isNarrating = false;
          clearSourceHighlights();
          if (narrateButton) narrateButton.disabled = false;
        });
      } else {
        state.isNarrating = false;
        clearSourceHighlights();
        if (narrateButton) {
          narrateButton.disabled = false;
          narrateButton.textContent = `Start ${briefingLanguageLabel()} Narration`;
        }
        throw new Error('Server-side voice is unavailable right now. Browser speech fallback is disabled.');
      }
      setStatus(
        payload.audio_url
          ? (payload.cached ? `Cached ${briefingLanguageLabel()} narration is ready for this module.` : `${briefingLanguageLabel()} narration has been generated for this module.`)
          : 'Server-side voice is unavailable right now.',
        'success',
      );
      setWalkthroughStatus(
        payload.cached
          ? `Cached ${briefingLanguageLabel()} narration is playing. Related PRD source sections are highlighted.`
          : `${briefingLanguageLabel()} narration is playing. Related PRD source sections are highlighted.`,
        'success',
      );
    } catch (error) {
      state.isNarrating = false;
      clearSourceHighlights();
      if (narrateButton) {
        narrateButton.disabled = false;
        narrateButton.textContent = `Start ${briefingLanguageLabel()} Narration`;
      }
      const raw = error.message || 'Could not generate narration for this section.';
      const hasOpenAI = raw.includes('OpenAI');
      const friendly = raw.includes('429') || raw.includes('Too Many Requests')
        ? (hasOpenAI
            ? 'OpenAI is rate limited right now. Try this narration again later.'
            : 'The text model is rate limited right now. Try this narration again later.')
        : raw;
      setStatus(friendly, 'error');
      setWalkthroughStatus(friendly, 'error');
    }
  };

  const renderSections = () => {
    if (!sectionListNode || !sectionDetailNode) return;
    if (!state.sections.length) {
      clearWalkthroughStatus();
      sectionListNode.innerHTML = '<div class="empty-state"><p>PM briefing module navigation appears here after generation.</p></div>';
      sectionDetailNode.innerHTML = '<div class="empty-state"><p>Select a briefing module to view the merged walkthrough and original PRD content.</p></div>';
      narrateButton.disabled = true;
      narrateButton.textContent = `Start ${briefingLanguageLabel()} Narration`;
      return;
    }
    const blocks = state.briefingBlocks.length ? state.briefingBlocks : state.sections.map((section, index) => ({
      block_id: `section-${index}`,
      title: section.section_path,
      briefing_goal: 'Generate the walkthrough from the original PRD section.',
      merged_summary: section.briefing_summary || section.content || '',
      section_indexes: [index],
      source_refs: [{ section_index: index, section_path: section.section_path }],
      developer_focus: section.briefing_notes || [],
      walkthrough_cached: section.walkthrough_cached,
      walkthrough_audio_cached: section.walkthrough_audio_cached,
    }));
    sectionListNode.innerHTML = blocks.map((block, index) => `
      <button class="briefing-outline-item ${index === state.currentBlockIndex ? 'is-active' : ''}" type="button" data-block-index="${index}">
        <span>${index + 1}</span>
        <strong>${escapeHtml(block.title)}</strong>
        <small>${escapeHtml((block.section_indexes || []).length)} PRD section(s)</small>
        <div class="briefing-cache-pill-row">
          ${block.walkthrough_cached ? '<em class="briefing-cache-pill">Script cached</em>' : ''}
          ${block.walkthrough_audio_cached ? '<em class="briefing-cache-pill briefing-cache-pill-secondary">Audio cached</em>' : ''}
        </div>
      </button>
    `).join('');
    const block = blocks[state.currentBlockIndex] || blocks[0];
    const sourceIndexes = (block.section_indexes || []).map((value) => Number(value)).filter(Number.isFinite);
    const renderSourceSection = (sectionIndex) => {
      const section = state.sections[sectionIndex];
      if (!section) return '';
      const hasOriginalHtml = Boolean(section.html_content && section.html_content.trim());
      const images = !hasOriginalHtml
        ? (section.image_refs || []).map((src) => `<img src="${escapeHtml(src)}" alt="${escapeHtml(section.section_path)}">`).join('')
        : '';
      const rawHtml = String(section.html_content || '').trim();
      const contentMarkup = renderPlainSourceContent(section);
      const sourceNotice = rawHtml
        ? '<p class="briefing-source-render-note">PRD source rendered as text for browser performance.</p>'
        : '';
      return `
        <section class="briefing-source-section" data-source-section-index="${sectionIndex}">
          <div class="briefing-source-heading">
            <span>PRD ${sectionIndex + 1}</span>
            <strong>${escapeHtml(section.section_path)}</strong>
          </div>
          <div class="briefing-original-content">${sourceNotice}${contentMarkup || `<p>${escapeHtml(section.content || '')}</p>`}</div>
          ${images ? `<div class="briefing-image-grid">${images}</div>` : ''}
        </section>
      `;
    };
    const sourceMarkup = (sourceIndexes.length ? sourceIndexes : [state.currentSectionIndex])
      .map(renderSourceSection)
      .join('');
    sectionDetailNode.innerHTML = `
      <div class="briefing-section-heading">
        <h3>${escapeHtml(block.title)}</h3>
        <span class="briefing-section-meta">Briefing module ${state.currentBlockIndex + 1} / ${blocks.length}</span>
      </div>
      <article class="briefing-block-summary">
        <p class="briefing-overview-kicker">PM Briefing Goal</p>
        <p>${escapeHtml(block.briefing_goal || '')}</p>
        <p>${escapeHtml(block.merged_summary || '')}</p>
        ${(block.developer_focus || []).length ? `
          <ul>${(block.developer_focus || []).slice(0, 4).map((item) => `<li>${escapeHtml(item)}</li>`).join('')}</ul>
        ` : ''}
      </article>
      <div class="briefing-source-stack">${sourceMarkup}</div>
    `;
    enhancePresentationTables();
    classifyTableLayouts();
    addHorizontalHints();
    classifySectionImages();
    hideDecorativeArrowArtifacts();
    sectionDetailNode.querySelectorAll('img').forEach((image) => {
      image.setAttribute('tabindex', '0');
      image.setAttribute('role', 'button');
      image.setAttribute('aria-label', `${block.title} image preview`);
      const openPreview = () => openImageLightbox(image.currentSrc || image.src, image.alt || block.title);
      image.addEventListener('click', openPreview);
      image.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          openPreview();
        }
      });
    });
    sectionListNode.querySelectorAll('[data-block-index]').forEach((button) => {
      button.addEventListener('click', () => {
        stopNarration();
        state.currentBlockIndex = Number(button.dataset.blockIndex || 0);
        const selectedBlock = blocks[state.currentBlockIndex] || null;
        state.currentSectionIndex = Number((selectedBlock?.section_indexes || [0])[0] || 0);
        renderSections();
        if (selectedBlock?.walkthrough_cached) {
          const detail = selectedBlock.walkthrough_audio_cached
            ? `This module already has cached script and audio. Clicking "Start ${briefingLanguageLabel()} Narration" will not call text or voice generation again.`
            : `This module already has a cached script. Clicking "Start ${briefingLanguageLabel()} Narration" will not call the text model again.`;
          setWalkthroughStatus(detail, 'success');
        } else {
          clearWalkthroughStatus();
        }
      });
    });
    narrateButton.disabled = state.isNarrating;
    if (!state.isNarrating) narrateButton.textContent = `Start ${briefingLanguageLabel()} Narration`;
  };

  const renderMessages = (messages = []) => {
    if (!chatLogNode) return;
    state.messages = messages;
    if (!messages.length) {
      chatLogNode.innerHTML = '<div class="empty-state"><p>Generate a walkthrough before asking your first follow-up question.</p></div>';
      return;
    }
    chatLogNode.innerHTML = messages.map((message) => {
      if (message.role === 'user') {
        return `<article class="chat-bubble chat-bubble-user"><strong>You</strong><p>${escapeHtml(message.body)}</p></article>`;
      }
      const citations = (() => {
        try {
          return JSON.parse(message.citations_json || '[]');
        } catch {
          return [];
        }
      })();
      const citationMarkup = citations.length ? `
        <div class="citation-list">
          ${citations.map((citation) => `
            <a href="${escapeHtml(citation.source_url)}" target="_blank" rel="noreferrer">
              ${escapeHtml(citation.title)} · ${escapeHtml(citation.section_path)}
            </a>
          `).join('')}
        </div>
      ` : '';
      const audioMarkup = message.audio_url ? `<audio controls src="${escapeHtml(message.audio_url)}"></audio>` : '';
      return `
        <article class="chat-bubble chat-bubble-assistant">
          <div class="chat-bubble-head">
            <strong>Briefing Assistant</strong>
            <span class="briefing-pill">${escapeHtml(message.groundedness || 'Answer')}</span>
          </div>
          <p>${escapeHtml(message.body).replaceAll('\n', '<br>')}</p>
          ${citationMarkup}
          ${audioMarkup}
        </article>
      `;
    }).join('');
  };

  const applySessionPayload = (payload) => {
    stopNarration();
    state.sessionId = payload.session.session_id;
    state.sections = payload.sections || [];
    state.briefingBlocks = payload.briefing_blocks || [];
    state.briefingLanguage = payload.session?.audience === 'developer_en' ? 'en' : 'zh';
    state.currentSectionIndex = 0;
    state.currentBlockIndex = 0;
    setStatus(`Generated the ${briefingLanguageLabel()} developer walkthrough for "${payload.session.title}".`, 'success');
    renderSections();
    renderMessages(payload.messages || []);
  };

  if (sessionForm) {
    sessionForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      const formData = new FormData(sessionForm);
      const pageRef = String(formData.get('page_ref') || '').trim();
      if (!isValidHttpUrl(pageRef)) {
        setStatus('Enter a valid Confluence page URL.', 'error');
        return;
      }
      setSessionSubmitLoading(true);
      const language = briefingLanguage?.value || 'zh';
      setStatus(`Reading the Confluence PRD and generating a ${language === 'en' ? 'English' : 'Chinese'} developer walkthrough...`);
      try {
        const response = await fetch('/prd-briefing/api/session', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            page_ref: pageRef,
            mode: formData.get('mode'),
            language,
          }),
        });
        const payload = await parseJsonResponse(response);
        if (!response.ok) throw new Error(payload.message || 'Could not generate the PRD walkthrough right now.');
        setSessionSubmitLoading(false);
        setStatus('PRD loaded. Rendering the page...');
        await wait(0);
        try {
          applySessionPayload(payload);
        } catch (renderError) {
          console.error(renderError);
          setStatus('PRD loaded, but the page could not render the walkthrough safely. Please try again or use another PRD link.', 'error');
        }
      } catch (error) {
        setStatus(error.message || 'Could not generate the PRD walkthrough right now.', 'error');
      } finally {
        setSessionSubmitLoading(false);
      }
    });
  }

  if (prdReviewButton) {
    prdReviewButton.addEventListener('click', () => {
      generatePrdReview();
    });
  }

  if (chatForm) {
    chatForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      if (!state.sessionId) {
        setStatus('Generate a walkthrough first.', 'error');
        return;
      }
      const formData = new FormData(chatForm);
      const question = String(formData.get('question') || '').trim();
      if (!question) return;
      if (chatSubmitButton) {
        chatSubmitButton.disabled = true;
        chatSubmitButton.textContent = 'Answering...';
      }
      try {
        const response = await fetch(`/prd-briefing/api/session/${state.sessionId}/answer`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question }),
        });
        const payload = await parseJsonResponse(response);
        if (!response.ok) throw new Error(payload.message || 'Could not answer this question right now.');
        const userBubble = { role: 'user', body: question };
        const assistantBubble = {
          role: 'assistant',
          body: payload.answer_text,
          groundedness: payload.groundedness,
          citations_json: JSON.stringify(payload.citations || []),
          audio_url: payload.audio_url,
        };
        renderMessages([...(state.messages || []), userBubble, assistantBubble]);
        chatForm.reset();
      } catch (error) {
        setStatus(error.message || 'Could not answer this question right now.', 'error');
      } finally {
        if (chatSubmitButton) {
          chatSubmitButton.disabled = false;
          chatSubmitButton.textContent = 'Submit Question';
        }
      }
    });
  }

  quickQuestionButtons.forEach((button) => {
    button.addEventListener('click', () => {
      const textarea = chatForm?.querySelector('textarea[name="question"]');
      if (!textarea) return;
      textarea.value = button.dataset.quickQuestion || '';
      textarea.focus();
    });
  });

  if (narrateButton) {
    narrateButton.addEventListener('click', async () => {
      if (!state.sessionId) return;
      await playCurrentSection();
    });
  }

  if (readerModeToggle) {
    readerModeToggle.addEventListener('click', () => {
      state.readerMode = !state.readerMode;
      try {
        window.localStorage.setItem(READER_MODE_STORAGE_KEY, state.readerMode ? '1' : '0');
      } catch {}
      renderReaderMode();
      if (state.readerMode) {
        document.querySelector('.briefing-primary-panel')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    });
  }

  if (noImageModeToggle) {
    noImageModeToggle.addEventListener('click', (event) => {
      if (noImageModeToggle.dataset.suppressClick === 'true') {
        event.preventDefault();
        return;
      }
      const readingAnchor = captureReadingAnchor();
      state.noImageMode = !state.noImageMode;
      try {
        window.localStorage.setItem(NO_IMAGE_MODE_STORAGE_KEY, state.noImageMode ? '1' : '0');
      } catch {}
      renderNoImageMode();
      restoreReadingAnchorAfterLayout(readingAnchor);
    });
  }

  if (imageLightboxClose) {
    imageLightboxClose.addEventListener('click', () => {
      closeImageLightbox();
    });
  }

  if (imageLightbox) {
    imageLightbox.addEventListener('click', (event) => {
      if (event.target === imageLightbox) {
        closeImageLightbox();
      }
    });
    imageLightbox.addEventListener('close', () => {
      if (imageLightboxMedia) {
        imageLightboxMedia.removeAttribute('src');
      }
      if (imageLightboxOpen) {
        imageLightboxOpen.setAttribute('href', '#');
      }
    });
  }

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      if (imageLightbox?.open) {
        closeImageLightbox();
        return;
      }
      if (state.readerMode) {
        state.readerMode = false;
        try {
          window.localStorage.setItem(READER_MODE_STORAGE_KEY, '0');
        } catch {}
        renderReaderMode();
      }
    }
  });

  try {
    state.readerMode = window.localStorage.getItem(READER_MODE_STORAGE_KEY) === '1';
  } catch {
    state.readerMode = false;
  }
  try {
    state.noImageMode = window.localStorage.getItem(NO_IMAGE_MODE_STORAGE_KEY) === '1';
  } catch {
    state.noImageMode = false;
  }
  renderReaderMode();
  renderNoImageMode();
  restoreNoImageTogglePosition();
  setupNoImageToggleDrag();
})();
