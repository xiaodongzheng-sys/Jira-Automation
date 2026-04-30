(() => {
  const sessionForm = document.querySelector('[data-briefing-session-form]');
  const statusNode = document.querySelector('[data-briefing-status]');
  const presenterView = document.querySelector('[data-presenter-view]');
  const sessionSubmitButton = sessionForm?.querySelector('button[type="submit"]');
  const briefingLanguage = document.querySelector('[data-briefing-language]');
  const prdReviewButton = document.querySelector('[data-prd-review-generate]');
  const prdReviewPanel = document.querySelector('[data-prd-review-panel]');
  const imageLightbox = document.querySelector('[data-image-lightbox]');
  const imageLightboxMedia = document.querySelector('[data-image-lightbox-media]');
  const imageLightboxClose = document.querySelector('[data-image-lightbox-close]');
  const imageLightboxOpen = document.querySelector('[data-image-lightbox-open]');

  /**
   * @typedef {Object} PresentationTimestamp
   * @property {string} sentence
   * @property {number} start
   * @property {number} end
   *
   * @typedef {Object} PresentationChunk
   * @property {string} id
   * @property {string} title
   * @property {string} content
   * @property {string=} audioUrl
   * @property {number=} duration
   * @property {PresentationTimestamp[]=} timestamps
   * @property {string[]=} imageUrls
   * @property {'draft'|'editing'|'audio-loading'|'ready'|'playing'|'audio-failed'=} audioStatus
   */

  const PREFETCH_WINDOW_SIZE = 2;

  let state = {
    sessionId: null,
    sessionTitle: '',
    chunks: [],
    currentIndex: 0,
    activeSentenceIndex: -1,
    isGenerating: false,
    continueRequired: false,
    currentAudio: null,
    currentAudioIndex: -1,
  };

  const pendingAudio = new Map();
  const audioVersions = new Map();
  let prefetchTail = Promise.resolve();

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const isValidHttpUrl = (value) => /^https?:\/\/\S+/i.test(String(value || '').trim());

  const setStatus = (message, tone = 'neutral') => {
    if (!statusNode) return;
    statusNode.innerHTML = `<p>${escapeHtml(message)}</p>`;
    statusNode.dataset.tone = tone;
  };

  const setSessionSubmitLoading = (isLoading) => {
    if (!sessionSubmitButton) return;
    sessionSubmitButton.disabled = isLoading;
    sessionSubmitButton.textContent = isLoading ? '生成中...' : '生成宣讲';
  };

  const parseJsonResponse = async (response) => {
    const contentType = String(response.headers.get('content-type') || '').toLowerCase();
    if (contentType.includes('application/json')) {
      return response.json();
    }
    const text = await response.text().catch(() => '');
    if (response.redirected || text.trim().startsWith('<!DOCTYPE') || text.trim().startsWith('<html')) {
      throw new Error('Your session expired or requires sign-in. Refresh the page and try again.');
    }
    throw new Error(`Unexpected API response format (${contentType || 'unknown'}).`);
  };

  const splitSentences = (text) => {
    const normalized = String(text || '').replace(/\s+/g, ' ').trim();
    if (!normalized) return [];
    const matches = normalized.match(/[^。！？!?；;]+[。！？!?；;]?/g) || [normalized];
    return matches.map((item) => item.trim()).filter(Boolean);
  };

  const estimateTimestamps = (content, duration = 30) => {
    const sentences = splitSentences(content);
    const weights = sentences.map((sentence) => Math.max(1, sentence.replace(/\s+/g, '').length));
    const total = weights.reduce((sum, value) => sum + value, 0) || 1;
    let cursor = 0;
    return sentences.map((sentence, index) => {
      const end = index === sentences.length - 1
        ? duration
        : Math.min(duration, cursor + (duration * weights[index] / total));
      const item = { sentence, start: Number(cursor.toFixed(2)), end: Number(Math.max(end, cursor + 0.2).toFixed(2)) };
      cursor = end;
      return item;
    });
  };

  const sanitizeChunk = (chunk, index) => ({
    id: String(chunk.id || `chunk-${index + 1}`),
    title: String(chunk.title || `宣讲段落 ${index + 1}`),
    content: String(chunk.content || '').trim(),
    audioUrl: chunk.audioUrl || '',
    duration: Number(chunk.duration || 0),
    timestamps: Array.isArray(chunk.timestamps) ? chunk.timestamps : [],
    imageUrls: Array.isArray(chunk.imageUrls) ? chunk.imageUrls.filter(Boolean) : [],
    audioStatus: chunk.audioStatus || (chunk.audioUrl ? 'ready' : 'draft'),
    errorMessage: '',
  });

  const activeChunk = () => state.chunks[state.currentIndex] || null;

  const statusLabel = (chunk) => {
    const status = chunk?.audioStatus || 'draft';
    if (status === 'ready') return 'Audio ready';
    if (status === 'playing') return 'Playing';
    if (status === 'audio-loading') return 'Generating audio';
    if (status === 'audio-failed') return 'Manual read';
    if (status === 'editing') return 'Editing';
    return 'Draft';
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

  const stopCurrentAudio = ({ reset = false } = {}) => {
    if (!state.currentAudio) return;
    state.currentAudio.pause();
    if (reset) state.currentAudio.currentTime = 0;
    const chunk = state.chunks[state.currentAudioIndex];
    if (chunk && chunk.audioStatus === 'playing') {
      chunk.audioStatus = 'ready';
    }
    state.currentAudio = null;
    state.currentAudioIndex = -1;
  };

  const abortAudioRequest = (index) => {
    const pending = pendingAudio.get(index);
    if (pending?.controller) {
      pending.controller.abort();
    }
    pendingAudio.delete(index);
  };

  const applyAudioPayload = (index, payload, version) => {
    const chunk = state.chunks[index];
    if (!chunk || audioVersions.get(index) !== version) return;
    const generated = payload.chunk || payload;
    chunk.audioUrl = generated.audioUrl || '';
    chunk.duration = Number(generated.duration || 0);
    chunk.timestamps = Array.isArray(generated.timestamps) ? generated.timestamps : [];
    chunk.imageUrls = Array.isArray(generated.imageUrls) && generated.imageUrls.length ? generated.imageUrls : chunk.imageUrls;
    chunk.audioStatus = chunk.audioUrl ? 'ready' : 'audio-failed';
    chunk.errorMessage = chunk.audioUrl ? '' : 'No audio URL returned.';
    if (index === state.currentIndex) {
      setStatus(chunk.audioUrl ? `第 ${index + 1} 段音频已就绪，可以播放。` : `第 ${index + 1} 段音频不可用，可手动朗读。`, chunk.audioUrl ? 'success' : 'error');
    }
    renderPresenterView();
  };

  const enqueueAudio = (index) => {
    const chunk = state.chunks[index];
    if (!chunk || !state.sessionId) return Promise.resolve();
    if (chunk.audioStatus === 'ready' || chunk.audioStatus === 'playing' || chunk.audioStatus === 'audio-loading') {
      return pendingAudio.get(index)?.promise || Promise.resolve();
    }

    const version = (audioVersions.get(index) || 0) + 1;
    audioVersions.set(index, version);
    const controller = new AbortController();
    chunk.audioStatus = 'audio-loading';
    chunk.errorMessage = '';
    renderPresenterView();

    const task = async () => {
      try {
        const response = await fetch('/prd-briefing/api/generate-audio', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            session_id: state.sessionId,
            chunk: {
              id: chunk.id,
              title: chunk.title,
              content: chunk.content,
              imageUrls: chunk.imageUrls || [],
            },
          }),
          signal: controller.signal,
        });
        const payload = await parseJsonResponse(response);
        if (!response.ok) throw new Error(payload.message || 'Could not generate audio for this chunk.');
        applyAudioPayload(index, payload, version);
      } catch (error) {
        if (controller.signal.aborted || audioVersions.get(index) !== version) return;
        const current = state.chunks[index];
        if (current) {
          current.audioStatus = 'audio-failed';
          current.errorMessage = error.message || 'TTS failed. You can manually read this chunk.';
        }
        if (index === state.currentIndex) {
          setStatus(current?.errorMessage || 'TTS failed. You can manually read this chunk.', 'error');
        }
        renderPresenterView();
      } finally {
        pendingAudio.delete(index);
      }
    };

    const promise = prefetchTail.then(task, task);
    prefetchTail = promise.catch(() => {});
    pendingAudio.set(index, { controller, promise });
    return promise;
  };

  const ensurePrefetchWindow = (anchorIndex = state.currentIndex) => {
    const last = Math.min(state.chunks.length - 1, anchorIndex + PREFETCH_WINDOW_SIZE);
    for (let index = anchorIndex; index <= last; index += 1) {
      enqueueAudio(index);
    }
  };

  const syncSentenceHighlight = () => {
    const chunk = activeChunk();
    const audio = state.currentAudio;
    if (!chunk || !audio) return;
    const timestamps = chunk.timestamps?.length ? chunk.timestamps : estimateTimestamps(chunk.content, chunk.duration || 30);
    const current = audio.currentTime;
    const activeIndex = timestamps.findIndex((item) => current >= Number(item.start || 0) && current < Number(item.end || 0));
    if (activeIndex === state.activeSentenceIndex) return;
    state.activeSentenceIndex = activeIndex;
    presenterView?.querySelectorAll('[data-sentence-index]').forEach((node) => {
      node.classList.toggle('is-active', Number(node.dataset.sentenceIndex || '-1') === activeIndex);
    });
  };

  const playCurrentChunk = async () => {
    const chunk = activeChunk();
    if (!chunk) return;
    if (chunk.audioStatus === 'audio-failed') {
      setStatus('这段音频生成失败，可以手动朗读后继续下一段。', 'error');
      return;
    }
    if (chunk.audioStatus !== 'ready') {
      setStatus(`正在生成第 ${state.currentIndex + 1} 段音频，请稍等。`, 'neutral');
      await enqueueAudio(state.currentIndex);
      if (activeChunk()?.audioStatus !== 'ready') return;
    }

    stopCurrentAudio();
    const audio = new Audio(chunk.audioUrl);
    state.currentAudio = audio;
    state.currentAudioIndex = state.currentIndex;
    state.continueRequired = false;
    chunk.audioStatus = 'playing';
    renderPresenterView();
    ensurePrefetchWindow(state.currentIndex + 1);

    audio.addEventListener('timeupdate', syncSentenceHighlight);
    audio.addEventListener('ended', () => {
      const endedChunk = state.chunks[state.currentIndex];
      if (endedChunk) endedChunk.audioStatus = 'ready';
      state.currentAudio = null;
      state.currentAudioIndex = -1;
      state.activeSentenceIndex = (endedChunk?.timestamps || []).length - 1;
      state.continueRequired = true;
      renderPresenterView();
    }, { once: true });
    audio.addEventListener('error', () => {
      const failedChunk = state.chunks[state.currentIndex];
      if (failedChunk) {
        failedChunk.audioStatus = 'audio-failed';
        failedChunk.errorMessage = 'Audio playback failed. You can manually read this chunk.';
      }
      state.currentAudio = null;
      state.currentAudioIndex = -1;
      renderPresenterView();
    }, { once: true });

    try {
      await audio.play();
    } catch {
      if (chunk.audioStatus === 'playing') chunk.audioStatus = 'ready';
      state.currentAudio = null;
      state.currentAudioIndex = -1;
      renderPresenterView();
    }
  };

  const pauseCurrentChunk = () => {
    const chunk = activeChunk();
    if (!chunk || chunk.audioStatus !== 'playing') return;
    stopCurrentAudio();
    chunk.audioStatus = 'ready';
    renderPresenterView();
  };

  const continueToNextChunk = async () => {
    if (state.currentIndex >= state.chunks.length - 1) {
      state.continueRequired = false;
      renderPresenterView();
      return;
    }
    state.currentIndex += 1;
    state.activeSentenceIndex = -1;
    state.continueRequired = false;
    renderPresenterView();
    ensurePrefetchWindow(state.currentIndex);
    await playCurrentChunk();
  };

  const goToChunk = (index) => {
    if (index < 0 || index >= state.chunks.length) return;
    stopCurrentAudio({ reset: true });
    state.currentIndex = index;
    state.activeSentenceIndex = -1;
    state.continueRequired = false;
    renderPresenterView();
    ensurePrefetchWindow(index);
  };

  const enterEditMode = (index) => {
    const chunk = state.chunks[index];
    if (!chunk) return;
    if (state.currentAudioIndex === index) {
      stopCurrentAudio();
    }
    abortAudioRequest(index);
    audioVersions.set(index, (audioVersions.get(index) || 0) + 1);
    chunk.audioStatus = 'editing';
    state.currentIndex = index;
    state.continueRequired = false;
    renderPresenterView();
  };

  const saveEdit = (index) => {
    const chunk = state.chunks[index];
    const textarea = presenterView?.querySelector(`[data-edit-content="${index}"]`);
    if (!chunk || !textarea) return;
    const nextContent = String(textarea.value || '').trim();
    if (!nextContent) return;
    abortAudioRequest(index);
    audioVersions.set(index, (audioVersions.get(index) || 0) + 1);
    chunk.content = nextContent;
    chunk.audioUrl = '';
    chunk.duration = 0;
    chunk.timestamps = [];
    chunk.audioStatus = 'draft';
    chunk.errorMessage = '';
    state.activeSentenceIndex = -1;
    renderPresenterView();
    enqueueAudio(index);
  };

  const cancelEdit = (index) => {
    const chunk = state.chunks[index];
    if (!chunk) return;
    chunk.audioStatus = chunk.audioUrl ? 'ready' : 'draft';
    renderPresenterView();
  };

  const manualAdvance = () => {
    const chunk = activeChunk();
    if (!chunk) return;
    state.activeSentenceIndex = (chunk.timestamps || estimateTimestamps(chunk.content)).length - 1;
    state.continueRequired = true;
    renderPresenterView();
  };

  const renderPresenterView = () => {
    if (!presenterView) return;
    if (!state.chunks.length) {
      presenterView.innerHTML = '<div class="empty-state"><p>生成宣讲后，研发视角大纲和播放器会显示在这里。</p></div>';
      return;
    }

    const chunk = activeChunk();
    const timestamps = chunk.timestamps?.length ? chunk.timestamps : estimateTimestamps(chunk.content, chunk.duration || 30);
    const images = (chunk.imageUrls || []).map((src) => `
      <button class="briefing-presenter-image" type="button" data-preview-image="${escapeHtml(src)}">
        <img src="${escapeHtml(src)}" alt="${escapeHtml(chunk.title)}">
      </button>
    `).join('');
    const isPlaying = chunk.audioStatus === 'playing';
    const isEditing = chunk.audioStatus === 'editing';
    const canPlay = chunk.audioStatus === 'ready';
    const isLoading = chunk.audioStatus === 'audio-loading';
    const hasFailed = chunk.audioStatus === 'audio-failed';

    presenterView.innerHTML = `
      <div class="briefing-presenter-layout">
        <aside class="briefing-presenter-outline" aria-label="Briefing outline">
          ${state.chunks.map((item, index) => `
            <button class="briefing-presenter-outline-item ${index === state.currentIndex ? 'is-active' : ''}" type="button" data-go-chunk="${index}">
              <span>${index + 1}</span>
              <strong>${escapeHtml(item.title)}</strong>
              <small data-status="${escapeHtml(item.audioStatus || 'draft')}">${escapeHtml(statusLabel(item))}</small>
            </button>
          `).join('')}
        </aside>
        <article class="briefing-presenter-stage">
          <div class="briefing-presenter-stage-head">
            <div>
              <p class="briefing-overview-kicker">Chunk ${state.currentIndex + 1} / ${state.chunks.length}</p>
              <h3>${escapeHtml(chunk.title)}</h3>
            </div>
            <span class="briefing-presenter-status" data-status="${escapeHtml(chunk.audioStatus || 'draft')}">${escapeHtml(statusLabel(chunk))}</span>
          </div>
          ${images ? `<div class="briefing-presenter-images">${images}</div>` : ''}
          ${isEditing ? `
            <div class="briefing-presenter-editor">
              <textarea data-edit-content="${state.currentIndex}" rows="8">${escapeHtml(chunk.content)}</textarea>
              <div class="button-row">
                <button class="button" type="button" data-save-edit="${state.currentIndex}">保存并重新生成音频</button>
                <button class="button button-secondary" type="button" data-cancel-edit="${state.currentIndex}">Cancel</button>
              </div>
            </div>
          ` : `
            <div class="briefing-presenter-script" data-edit-chunk="${state.currentIndex}" title="Double click to edit this chunk">
              ${timestamps.map((item, index) => `
                <span class="briefing-presenter-sentence ${index === state.activeSentenceIndex ? 'is-active' : ''}" data-sentence-index="${index}">
                  ${escapeHtml(item.sentence)}
                </span>
              `).join('')}
            </div>
          `}
          ${chunk.errorMessage ? `<p class="briefing-presenter-error">${escapeHtml(chunk.errorMessage)}</p>` : ''}
          <div class="briefing-presenter-controls">
            <button class="button" type="button" data-play-current ${canPlay ? '' : 'disabled'}>${isPlaying ? '播放中' : '播放/继续'}</button>
            <button class="button button-secondary" type="button" data-pause-current ${isPlaying ? '' : 'disabled'}>暂停</button>
            <button class="button button-secondary" type="button" data-next-current ${state.currentIndex >= state.chunks.length - 1 ? 'disabled' : ''}>下一段</button>
            ${isLoading ? '<span class="briefing-presenter-loading">正在生成本段音频...</span>' : ''}
            ${hasFailed ? '<button class="button button-secondary" type="button" data-manual-advance>手动朗读并继续</button>' : ''}
          </div>
          ${state.continueRequired ? `
            <div class="briefing-presenter-continue">
              <button class="button" type="button" data-continue-next>
                ${state.currentIndex >= state.chunks.length - 1 ? '宣讲已结束' : '开发/测试提问结束，继续下一段'}
              </button>
            </div>
          ` : ''}
        </article>
      </div>
    `;

    presenterView.querySelectorAll('[data-go-chunk]').forEach((button) => {
      button.addEventListener('click', () => goToChunk(Number(button.dataset.goChunk || 0)));
    });
    presenterView.querySelector('[data-play-current]')?.addEventListener('click', playCurrentChunk);
    presenterView.querySelector('[data-pause-current]')?.addEventListener('click', pauseCurrentChunk);
    presenterView.querySelector('[data-next-current]')?.addEventListener('click', () => goToChunk(state.currentIndex + 1));
    presenterView.querySelector('[data-continue-next]')?.addEventListener('click', continueToNextChunk);
    presenterView.querySelector('[data-manual-advance]')?.addEventListener('click', manualAdvance);
    presenterView.querySelector('[data-edit-chunk]')?.addEventListener('dblclick', () => enterEditMode(state.currentIndex));
    presenterView.querySelector('[data-save-edit]')?.addEventListener('click', (event) => saveEdit(Number(event.currentTarget.dataset.saveEdit || state.currentIndex)));
    presenterView.querySelector('[data-cancel-edit]')?.addEventListener('click', (event) => cancelEdit(Number(event.currentTarget.dataset.cancelEdit || state.currentIndex)));
    presenterView.querySelectorAll('[data-preview-image]').forEach((button) => {
      button.addEventListener('click', () => openImageLightbox(button.dataset.previewImage, chunk.title));
    });
  };

  const generatePresentation = async () => {
    const formData = sessionForm ? new FormData(sessionForm) : new FormData();
    const pageRef = String(formData.get('page_ref') || '').trim();
    if (!isValidHttpUrl(pageRef)) {
      setStatus('Enter a valid Confluence page URL.', 'error');
      return;
    }

    stopCurrentAudio({ reset: true });
    pendingAudio.forEach((item) => item.controller?.abort());
    pendingAudio.clear();
    audioVersions.clear();
    state = {
      sessionId: null,
      sessionTitle: '',
      chunks: [],
      currentIndex: 0,
      activeSentenceIndex: -1,
      isGenerating: true,
      continueRequired: false,
      currentAudio: null,
      currentAudioIndex: -1,
    };
    renderPresenterView();
    setSessionSubmitLoading(true);
    setStatus('正在读取 PRD 并生成宣讲大纲，预计需要 30-40 秒。');

    try {
      const response = await fetch('/prd-briefing/api/process-prd', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ page_ref: pageRef }),
      });
      const payload = await parseJsonResponse(response);
      if (!response.ok) throw new Error(payload.message || 'Could not process this PRD.');
      state.sessionId = payload.session?.session_id || null;
      state.sessionTitle = payload.session?.title || 'PRD';
      state.chunks = (payload.chunks || []).map(sanitizeChunk).filter((chunk) => chunk.content);
      state.isGenerating = false;
      state.currentIndex = 0;
      setStatus(`已生成 "${state.sessionTitle}" 的宣讲大纲。正在优先生成开场音频。`, 'success');
      renderPresenterView();
      enqueueAudio(0).then(() => ensurePrefetchWindow(1));
    } catch (error) {
      state.isGenerating = false;
      const message = error.message || 'Could not process this PRD.';
      setStatus(message, 'error');
      if (presenterView) {
        presenterView.innerHTML = `<div class="empty-state"><p>${escapeHtml(message)}</p></div>`;
      }
    } finally {
      setSessionSubmitLoading(false);
    }
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
    imageLightboxMedia.alt = alt || 'PRD image preview';
    if (imageLightboxOpen) imageLightboxOpen.href = src;
    if (typeof imageLightbox.showModal === 'function') {
      imageLightbox.showModal();
    }
  };

  if (sessionForm) {
    sessionForm.addEventListener('submit', (event) => {
      event.preventDefault();
      generatePresentation();
    });
  }

  if (prdReviewButton) {
    prdReviewButton.addEventListener('click', () => {
      generatePrdReview();
    });
  }

  if (imageLightboxClose) {
    imageLightboxClose.addEventListener('click', closeImageLightbox);
  }

  if (imageLightbox) {
    imageLightbox.addEventListener('click', (event) => {
      if (event.target === imageLightbox) closeImageLightbox();
    });
    imageLightbox.addEventListener('close', () => {
      imageLightboxMedia?.removeAttribute('src');
      imageLightboxOpen?.setAttribute('href', '#');
    });
  }

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && imageLightbox?.open) {
      closeImageLightbox();
    }
  });

  renderPresenterView();
})();
