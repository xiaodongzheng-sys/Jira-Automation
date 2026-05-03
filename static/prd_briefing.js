(() => {
  const sessionForm = document.querySelector('[data-briefing-session-form]');
  const statusNode = document.querySelector('[data-briefing-status]');
  const presenterView = document.querySelector('[data-presenter-view]');
  const sessionSubmitButton = sessionForm?.querySelector('button[type="submit"]');
  const pageRefInput = document.querySelector('[data-briefing-page-ref]');
  const briefingLanguage = document.querySelector('[data-briefing-language]');
  const imageLightbox = document.querySelector('[data-image-lightbox]');
  const imageLightboxMedia = document.querySelector('[data-image-lightbox-media]');
  const imageLightboxClose = document.querySelector('[data-image-lightbox-close]');
  const imageLightboxOpen = document.querySelector('[data-image-lightbox-open]');
  const theaterToggle = document.querySelector('[data-theater-toggle]');

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
   * @property {{type:'image'|'table'|'none',content:string}=} media
   * @property {'draft'|'editing'|'audio-loading'|'ready'|'playing'|'audio-failed'=} audioStatus
   */

  const PREFETCH_WINDOW_SIZE = 2;
  const FORM_STORAGE_KEY = 'prd-briefing:last-form:v1';

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
    theaterMode: false,
    panicPaused: false,
    shareSoundReminderShown: false,
    manualPauseContext: null,
    audioContextUnlocked: false,
  };

  const pendingAudio = new Map();
  const audioVersions = new Map();
  let prefetchTail = Promise.resolve();
  let panicFeedbackTimer = null;
  let shareSoundToastTimer = null;
  let qaAudioContext = null;

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const isValidHttpUrl = (value) => /^https?:\/\/\S+/i.test(String(value || '').trim());

  const readSavedForm = () => {
    try {
      const parsed = JSON.parse(window.localStorage.getItem(FORM_STORAGE_KEY) || '{}');
      return parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
      return {};
    }
  };

  const saveFormDefaults = () => {
    try {
      window.localStorage.setItem(FORM_STORAGE_KEY, JSON.stringify({
        page_ref: String(pageRefInput?.value || '').trim(),
        language: briefingLanguage?.value === 'en' ? 'en' : 'zh',
      }));
    } catch {
      // Local persistence is optional; generation should continue without it.
    }
  };

  const restoreFormDefaults = () => {
    const saved = readSavedForm();
    if (pageRefInput && saved.page_ref) pageRefInput.value = String(saved.page_ref);
    if (briefingLanguage) briefingLanguage.value = saved.language === 'en' ? 'en' : 'zh';
  };

  const setStatus = (message, tone = 'neutral') => {
    if (!statusNode) return;
    statusNode.innerHTML = `<p>${escapeHtml(message)}</p>`;
    statusNode.dataset.tone = tone;
  };

  const setSessionSubmitLoading = (isLoading) => {
    if (!sessionSubmitButton) return;
    sessionSubmitButton.disabled = isLoading;
    sessionSubmitButton.textContent = isLoading ? 'Generating...' : 'Generate Briefing';
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

  const sanitizeMedia = (media) => {
    if (!media || typeof media !== 'object') return { type: 'none', content: '' };
    const type = String(media.type || 'none').toLowerCase();
    if ((type === 'image' || type === 'table') && media.content) {
      return { type, content: String(media.content || '') };
    }
    return { type: 'none', content: '' };
  };

  const sanitizeChunk = (chunk, index) => ({
    id: String(chunk.id || `chunk-${index + 1}`),
    title: String(chunk.title || `Briefing chunk ${index + 1}`),
    content: String(chunk.content || '').trim(),
    audioUrl: chunk.audioUrl || '',
    duration: Number(chunk.duration || 0),
    timestamps: Array.isArray(chunk.timestamps) ? chunk.timestamps : [],
    imageUrls: Array.isArray(chunk.imageUrls) ? chunk.imageUrls.filter(Boolean) : [],
    media: sanitizeMedia(chunk.media),
    cacheKey: String(chunk.cacheKey || ''),
    audioStatus: chunk.audioStatus || (chunk.audioUrl ? 'ready' : 'draft'),
    errorMessage: '',
  });

  const activeChunk = () => state.chunks[state.currentIndex] || null;

  const isEditableTarget = (target) => {
    const element = target instanceof Element ? target : null;
    if (!element) return false;
    const tagName = element.tagName.toLowerCase();
    return tagName === 'input'
      || tagName === 'textarea'
      || tagName === 'select'
      || element.isContentEditable
      || Boolean(element.closest('[data-edit-content]'));
  };

  const statusLabel = (chunk) => {
    const status = chunk?.audioStatus || 'draft';
    if (status === 'ready') return 'Audio ready';
    if (status === 'playing') return 'Playing';
    if (status === 'audio-loading') return 'Generating audio';
    if (status === 'audio-failed') return 'Manual read';
    if (status === 'editing') return 'Editing';
    return 'Draft';
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

  const showShareSoundReminder = () => {
    if (state.shareSoundReminderShown) return;
    state.shareSoundReminderShown = true;
    const toast = document.createElement('div');
    toast.className = 'briefing-share-sound-toast';
    toast.textContent = 'Make sure Zoom screen sharing has Share Sound enabled.';
    document.body.appendChild(toast);
    window.clearTimeout(shareSoundToastTimer);
    shareSoundToastTimer = window.setTimeout(() => {
      toast.classList.add('is-hiding');
      window.setTimeout(() => toast.remove(), 180);
    }, 5200);
  };

  const unlockQuestionCueAudio = async () => {
    const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
    if (!AudioContextCtor) return;
    if (!qaAudioContext) qaAudioContext = new AudioContextCtor();
    try {
      if (qaAudioContext.state === 'suspended') {
        await qaAudioContext.resume();
      }
      state.audioContextUnlocked = qaAudioContext.state === 'running';
    } catch {
      state.audioContextUnlocked = false;
    }
  };

  const playQuestionCue = () => {
    if (!state.audioContextUnlocked || !qaAudioContext || qaAudioContext.state !== 'running') return;
    try {
      const now = qaAudioContext.currentTime;
      const gain = qaAudioContext.createGain();
      gain.gain.setValueAtTime(0.0001, now);
      gain.gain.exponentialRampToValueAtTime(0.14, now + 0.015);
      gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.42);
      gain.connect(qaAudioContext.destination);

      [0, 0.16].forEach((offset, index) => {
        const oscillator = qaAudioContext.createOscillator();
        oscillator.type = 'sine';
        oscillator.frequency.setValueAtTime(index === 0 ? 880 : 1174.66, now + offset);
        oscillator.connect(gain);
        oscillator.start(now + offset);
        oscillator.stop(now + offset + 0.18);
      });
    } catch {
      // The cue is helpful but not critical; never block the briefing on it.
    }
  };

  const setTheaterMode = (enabled) => {
    state.theaterMode = Boolean(enabled);
    document.body.classList.toggle('briefing-theater-mode', state.theaterMode);
    if (theaterToggle) {
      theaterToggle.textContent = state.theaterMode ? 'Exit Presentation Mode' : 'Start Presentation Mode';
    }
    if (state.theaterMode) {
      showShareSoundReminder();
      unlockQuestionCueAudio();
    }
    renderPresenterView();
  };

  const flashPanicFeedback = () => {
    state.panicPaused = true;
    document.body.classList.add('briefing-panic-active');
    window.clearTimeout(panicFeedbackTimer);
    panicFeedbackTimer = window.setTimeout(() => {
      document.body.classList.remove('briefing-panic-active');
      renderPresenterView();
    }, 900);
  };

  const clearPanicFeedback = () => {
    state.panicPaused = false;
    document.body.classList.remove('briefing-panic-active');
    window.clearTimeout(panicFeedbackTimer);
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
    chunk.media = generated.media ? sanitizeMedia(generated.media) : sanitizeMedia(chunk.media);
    chunk.cacheKey = generated.cacheKey || chunk.cacheKey || '';
    chunk.audioStatus = chunk.audioUrl ? 'ready' : 'audio-failed';
    chunk.errorMessage = chunk.audioUrl ? '' : 'No audio URL returned.';
    if (index === state.currentIndex) {
      setStatus(chunk.audioUrl ? `Chunk ${index + 1} audio is ready to play.` : `Chunk ${index + 1} audio is unavailable. You can read it manually.`, chunk.audioUrl ? 'success' : 'error');
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
              media: chunk.media || { type: 'none', content: '' },
              cacheKey: chunk.cacheKey || '',
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

  const resolveManualRewindPoint = (chunk, audio) => {
    const timestamps = chunk.timestamps?.length ? chunk.timestamps : estimateTimestamps(chunk.content, chunk.duration || 30);
    const current = Number(audio?.currentTime || 0);
    let sentenceIndex = Number(state.activeSentenceIndex);
    if (sentenceIndex < 0) {
      sentenceIndex = timestamps.findIndex((item) => current >= Number(item.start || 0) && current < Number(item.end || 0));
    }
    const sentenceStart = sentenceIndex >= 0 ? Number(timestamps[sentenceIndex]?.start) : Number.NaN;
    return {
      sentenceIndex,
      rewindTo: Number.isFinite(sentenceStart) ? Math.max(0, sentenceStart) : Math.max(0, current - 3),
    };
  };

  const playCurrentChunk = async () => {
    const chunk = activeChunk();
    if (!chunk) return;
    showShareSoundReminder();
    await unlockQuestionCueAudio();
    if (chunk.audioStatus === 'audio-failed') {
      setStatus('Audio generation failed for this chunk. Read it manually, then continue to the next chunk.', 'error');
      return;
    }
    if (chunk.audioStatus !== 'ready') {
      setStatus(`Generating audio for chunk ${state.currentIndex + 1}. Please wait.`, 'neutral');
      await enqueueAudio(state.currentIndex);
      if (activeChunk()?.audioStatus !== 'ready') return;
    }

    stopCurrentAudio();
    const audio = new Audio(chunk.audioUrl);
    const manualContext = state.manualPauseContext?.index === state.currentIndex ? state.manualPauseContext : null;
    if (manualContext) {
      const rewindTo = Math.max(0, Number(manualContext.rewindTo || 0));
      try {
        audio.currentTime = rewindTo;
        state.activeSentenceIndex = Number(manualContext.sentenceIndex ?? state.activeSentenceIndex);
      } catch {
        audio.addEventListener('loadedmetadata', () => {
          audio.currentTime = Math.min(Math.max(0, rewindTo), Number(audio.duration || rewindTo));
        }, { once: true });
      }
    }
    state.manualPauseContext = null;
    clearPanicFeedback();
    state.currentAudio = audio;
    state.currentAudioIndex = state.currentIndex;
    state.continueRequired = false;
    chunk.audioStatus = 'playing';
    renderPresenterView();
    ensurePrefetchWindow(state.currentIndex + 1);

    const playingIndex = state.currentIndex;
    audio.addEventListener('timeupdate', syncSentenceHighlight);
    audio.addEventListener('ended', () => {
      const endedChunk = state.chunks[playingIndex];
      if (endedChunk) endedChunk.audioStatus = 'ready';
      state.currentAudio = null;
      state.currentAudioIndex = -1;
      state.activeSentenceIndex = (endedChunk?.timestamps || []).length - 1;
      state.continueRequired = true;
      playQuestionCue();
      renderPresenterView();
    }, { once: true });
    audio.addEventListener('error', () => {
      const failedChunk = state.chunks[playingIndex];
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

  const pauseCurrentChunk = ({ panic = false } = {}) => {
    const chunk = activeChunk();
    if (!chunk || chunk.audioStatus !== 'playing' || !state.currentAudio) return;
    const rewind = resolveManualRewindPoint(chunk, state.currentAudio);
    state.manualPauseContext = {
      index: state.currentIndex,
      sentenceIndex: rewind.sentenceIndex,
      rewindTo: rewind.rewindTo,
    };
    stopCurrentAudio();
    chunk.audioStatus = 'ready';
    if (panic) flashPanicFeedback();
    renderPresenterView();
  };

  const toggleCurrentChunkPlayback = () => {
    const chunk = activeChunk();
    if (!chunk) return;
    if (chunk.audioStatus === 'playing') {
      pauseCurrentChunk({ panic: true });
      return;
    }
    if (state.manualPauseContext?.index === state.currentIndex || chunk.audioStatus === 'ready') {
      playCurrentChunk();
    }
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
    state.manualPauseContext = null;
    clearPanicFeedback();
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
    state.manualPauseContext = null;
    clearPanicFeedback();
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
    state.manualPauseContext = null;
    clearPanicFeedback();
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
    state.manualPauseContext = null;
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
      presenterView.innerHTML = '<div class="empty-state"><p>The engineering-focused outline and player will appear here after the briefing is generated.</p></div>';
      return;
    }

    const chunk = activeChunk();
    const timestamps = chunk.timestamps?.length ? chunk.timestamps : estimateTimestamps(chunk.content, chunk.duration || 30);
    const progressPercent = state.chunks.length
      ? Math.min(100, Math.max(0, ((state.currentIndex + Math.max(0, state.activeSentenceIndex + 1) / Math.max(1, timestamps.length)) / state.chunks.length) * 100))
      : 0;
    const media = sanitizeMedia(chunk.media);
    const mediaHtml = media.type === 'image' ? `
      <button class="briefing-presenter-media briefing-presenter-media-image" type="button" data-preview-image="${escapeHtml(media.content)}">
        <img src="${escapeHtml(media.content)}" alt="${escapeHtml(chunk.title)}">
      </button>
    ` : media.type === 'table' ? `
      <div class="briefing-presenter-media briefing-presenter-media-table">
        ${media.content}
      </div>
    ` : '';
    const images = mediaHtml ? '' : (chunk.imageUrls || []).map((src) => `
      <button class="briefing-presenter-image" type="button" data-preview-image="${escapeHtml(src)}">
        <img src="${escapeHtml(src)}" alt="${escapeHtml(chunk.title)}">
      </button>
    `).join('');
    const hasMedia = Boolean(mediaHtml || images);
    const isPlaying = chunk.audioStatus === 'playing';
    const isEditing = chunk.audioStatus === 'editing';
    const canPlay = chunk.audioStatus === 'ready';
    const isLoading = chunk.audioStatus === 'audio-loading';
    const hasFailed = chunk.audioStatus === 'audio-failed';

    presenterView.innerHTML = `
      <div class="briefing-presenter-layout ${state.theaterMode ? 'is-theater' : ''} ${hasMedia ? 'has-media has-images' : 'has-no-media has-no-images'} media-${escapeHtml(media.type)}">
        ${state.theaterMode ? `
          <div class="briefing-theater-chrome">
            <span>Chapter ${state.currentIndex + 1} / ${state.chunks.length}</span>
            <button class="button button-secondary" type="button" data-theater-exit>Exit Presentation Mode</button>
          </div>
        ` : ''}
        ${state.theaterMode ? '' : `<aside class="briefing-presenter-outline" aria-label="Briefing outline">
          ${state.chunks.map((item, index) => `
            <button class="briefing-presenter-outline-item ${index === state.currentIndex ? 'is-active' : ''}" type="button" data-go-chunk="${index}">
              <span>${index + 1}</span>
              <strong>${escapeHtml(item.title)}</strong>
              <small data-status="${escapeHtml(item.audioStatus || 'draft')}">${escapeHtml(statusLabel(item))}</small>
            </button>
          `).join('')}
        </aside>`}
        <article class="briefing-presenter-stage">
          <div class="briefing-presenter-stage-head">
            <div>
              <p class="briefing-overview-kicker">Chunk ${state.currentIndex + 1} / ${state.chunks.length}</p>
              <h3>${escapeHtml(chunk.title)}</h3>
            </div>
            <span class="briefing-presenter-status" data-status="${escapeHtml(chunk.audioStatus || 'draft')}">${escapeHtml(statusLabel(chunk))}</span>
          </div>
          ${mediaHtml ? `<div class="briefing-presenter-media-pane">${mediaHtml}</div>` : ''}
          ${images ? `<div class="briefing-presenter-images">${images}</div>` : ''}
          ${isEditing ? `
            <div class="briefing-presenter-editor">
              <textarea data-edit-content="${state.currentIndex}" rows="8">${escapeHtml(chunk.content)}</textarea>
              <div class="button-row">
                <button class="button" type="button" data-save-edit="${state.currentIndex}">Save and Regenerate Audio</button>
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
            <button class="button" type="button" data-play-current ${canPlay ? '' : 'disabled'}>${isPlaying ? 'Playing' : 'Play / Continue'}</button>
            <button class="button button-secondary" type="button" data-pause-current ${isPlaying ? '' : 'disabled'}>Pause</button>
            <button class="button button-secondary" type="button" data-next-current ${state.currentIndex >= state.chunks.length - 1 ? 'disabled' : ''}>Next Chunk</button>
            ${isLoading ? '<span class="briefing-presenter-loading">Generating audio for this chunk...</span>' : ''}
            ${hasFailed ? '<button class="button button-secondary" type="button" data-manual-advance>Read Manually and Continue</button>' : ''}
          </div>
          ${state.continueRequired ? `
            <div class="briefing-presenter-continue">
              <button class="button" type="button" data-continue-next>
                ${state.currentIndex >= state.chunks.length - 1 ? 'Briefing Complete' : 'Q&A Finished, Continue to Next Chunk'}
              </button>
            </div>
          ` : ''}
        </article>
        ${state.panicPaused ? '<div class="briefing-panic-watermark">Q&A in Progress</div>' : ''}
        ${state.theaterMode ? `
          <div class="briefing-theater-progress" aria-hidden="true">
            <span style="width: ${progressPercent.toFixed(1)}%"></span>
          </div>
        ` : ''}
      </div>
    `;

    presenterView.querySelectorAll('[data-go-chunk]').forEach((button) => {
      button.addEventListener('click', () => goToChunk(Number(button.dataset.goChunk || 0)));
    });
    presenterView.querySelector('[data-play-current]')?.addEventListener('click', playCurrentChunk);
    presenterView.querySelector('[data-pause-current]')?.addEventListener('click', pauseCurrentChunk);
    presenterView.querySelector('[data-next-current]')?.addEventListener('click', () => goToChunk(state.currentIndex + 1));
    presenterView.querySelector('[data-continue-next]')?.addEventListener('click', continueToNextChunk);
    presenterView.querySelector('[data-theater-exit]')?.addEventListener('click', () => setTheaterMode(false));
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
    const language = briefingLanguage?.value === 'en' ? 'en' : 'zh';
    saveFormDefaults();
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
      theaterMode: state.theaterMode,
      panicPaused: false,
      shareSoundReminderShown: state.shareSoundReminderShown,
      manualPauseContext: null,
      audioContextUnlocked: state.audioContextUnlocked,
    };
    clearPanicFeedback();
    renderPresenterView();
    setSessionSubmitLoading(true);
    setStatus('Reading the PRD and generating the briefing outline. This usually takes 30-40 seconds.');

    try {
      const response = await fetch('/prd-briefing/api/process-prd', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ page_ref: pageRef, language }),
      });
      const payload = await parseJsonResponse(response);
      if (!response.ok) throw new Error(payload.message || 'Could not process this PRD.');
      state.sessionId = payload.session?.session_id || null;
      state.sessionTitle = payload.session?.title || 'PRD';
      state.chunks = (payload.chunks || []).map(sanitizeChunk).filter((chunk) => chunk.content);
      state.isGenerating = false;
      state.currentIndex = 0;
      setStatus(`Generated the briefing outline for "${state.sessionTitle}". Generating the opening audio first.`, 'success');
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

  restoreFormDefaults();
  pageRefInput?.addEventListener('input', saveFormDefaults);
  briefingLanguage?.addEventListener('change', saveFormDefaults);

  if (sessionForm) {
    sessionForm.addEventListener('submit', (event) => {
      event.preventDefault();
      generatePresentation();
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

  if (theaterToggle) {
    theaterToggle.addEventListener('click', () => setTheaterMode(!state.theaterMode));
  }

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && imageLightbox?.open) {
      closeImageLightbox();
      return;
    }
    if (event.key === 'Escape' && state.theaterMode) {
      setTheaterMode(false);
      return;
    }
    if (event.code === 'Space' && !isEditableTarget(event.target)) {
      const chunk = activeChunk();
      if (!chunk || (!state.currentAudio && chunk.audioStatus !== 'ready' && state.manualPauseContext?.index !== state.currentIndex)) return;
      event.preventDefault();
      toggleCurrentChunkPlayback();
    }
  });

  renderPresenterView();
})();
