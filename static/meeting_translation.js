(() => {
  const root = document.querySelector('[data-meeting-translation-root]');
  if (!root) return;

  const languageLabels = {
    en: 'English',
    id: 'Bahasa Indonesia',
    zh: 'Mandarin',
  };

  const state = {
    sessionId: '',
    events: null,
    translatedLine: null,
    originalLine: null,
  };

  const nodes = {
    form: root.querySelector('[data-translation-form]'),
    language: root.querySelector('[data-translation-language]'),
    start: root.querySelector('[data-translation-start]'),
    stop: root.querySelector('[data-translation-stop]'),
    status: root.querySelector('[data-translation-status]'),
    translatedTitle: root.querySelector('[data-translated-title]'),
    translatedTranscript: root.querySelector('[data-translated-transcript]'),
    originalTranscript: root.querySelector('[data-original-transcript]'),
  };

  const api = async (url, options = {}) => {
    const response = await fetch(url, {
      headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
      ...options,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.message || 'Request failed.');
    }
    return payload;
  };

  const selectedLanguage = () => {
    const value = String(nodes.language?.value || 'en').trim().toLowerCase();
    return ['en', 'id', 'zh'].includes(value) ? value : 'en';
  };

  const setStatus = (status, message) => {
    const safeStatus = String(status || 'idle').trim().toLowerCase() || 'idle';
    if (nodes.status) {
      nodes.status.dataset.state = safeStatus;
      nodes.status.textContent = message || safeStatus.charAt(0).toUpperCase() + safeStatus.slice(1);
    }
  };

  const setRunning = (running) => {
    if (nodes.start) nodes.start.disabled = running;
    if (nodes.stop) nodes.stop.disabled = !running;
    if (nodes.language) nodes.language.disabled = running;
  };

  const createLine = (container) => {
    const line = document.createElement('p');
    line.className = 'meeting-translation-line is-partial';
    container.appendChild(line);
    container.scrollTop = container.scrollHeight;
    return line;
  };

  const appendDelta = (kind, delta) => {
    const value = String(delta || '');
    if (!value) return;
    const container = kind === 'translated' ? nodes.translatedTranscript : nodes.originalTranscript;
    if (!container) return;
    const key = kind === 'translated' ? 'translatedLine' : 'originalLine';
    if (!state[key]) state[key] = createLine(container);
    state[key].textContent += value;
    container.scrollTop = container.scrollHeight;
    if (/[.!?。！？]\s*$/.test(state[key].textContent) || value.includes('\n')) {
      state[key].classList.remove('is-partial');
      state[key] = null;
    }
  };

  const resetTranscript = () => {
    if (nodes.translatedTranscript) nodes.translatedTranscript.innerHTML = '';
    if (nodes.originalTranscript) nodes.originalTranscript.innerHTML = '';
    state.translatedLine = null;
    state.originalLine = null;
  };

  const closeEvents = () => {
    if (state.events) {
      state.events.close();
      state.events = null;
    }
  };

  const handleEvent = (event) => {
    if (event.type === 'status') {
      setStatus(event.status, event.message || event.error);
      if (['stopped', 'error'].includes(String(event.status || '').toLowerCase())) {
        setRunning(false);
        closeEvents();
      }
      return;
    }
    if (event.type === 'snapshot') {
      setStatus(event.status, event.message);
      return;
    }
    if (event.type === 'translated_delta') {
      appendDelta('translated', event.delta);
      return;
    }
    if (event.type === 'original_delta') {
      appendDelta('original', event.delta);
    }
  };

  const openEvents = (sessionId) => {
    closeEvents();
    const source = new EventSource(`/api/meeting-translation/sessions/${encodeURIComponent(sessionId)}/events`);
    source.onmessage = (message) => {
      try {
        handleEvent(JSON.parse(message.data));
      } catch (_error) {
        // Ignore malformed keepalive chunks.
      }
    };
    source.onerror = () => {
      if (state.sessionId) setStatus('error', 'Translation event stream disconnected.');
      setRunning(false);
      closeEvents();
    };
    state.events = source;
  };

  nodes.language?.addEventListener('change', () => {
    const language = selectedLanguage();
    if (nodes.translatedTitle) nodes.translatedTitle.textContent = languageLabels[language] || 'Translated';
  });

  nodes.form?.addEventListener('submit', async (event) => {
    event.preventDefault();
    const targetLanguage = selectedLanguage();
    resetTranscript();
    setRunning(true);
    setStatus('connecting', 'Connecting...');
    if (nodes.translatedTitle) nodes.translatedTitle.textContent = languageLabels[targetLanguage] || 'Translated';
    try {
      const payload = await api('/api/meeting-translation/start', {
        method: 'POST',
        body: JSON.stringify({ target_language: targetLanguage }),
      });
      state.sessionId = payload.session?.session_id || '';
      if (!state.sessionId) throw new Error('Meeting Translation did not return a session id.');
      setStatus(payload.session?.status || 'connecting', payload.session?.message || 'Connecting...');
      openEvents(state.sessionId);
    } catch (error) {
      state.sessionId = '';
      setRunning(false);
      setStatus('error', error.message || 'Could not start Meeting Translation.');
    }
  });

  nodes.stop?.addEventListener('click', async () => {
    const sessionId = state.sessionId;
    if (!sessionId) return;
    setStatus('stopping', 'Stopping...');
    nodes.stop.disabled = true;
    try {
      await api(`/api/meeting-translation/sessions/${encodeURIComponent(sessionId)}/stop`, { method: 'POST' });
    } catch (error) {
      setStatus('error', error.message || 'Could not stop Meeting Translation.');
    } finally {
      state.sessionId = '';
      setRunning(false);
      closeEvents();
    }
  });
})();
