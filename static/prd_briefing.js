(() => {
  const sessionForm = document.querySelector('[data-briefing-session-form]');
  const kbForm = document.querySelector('[data-kb-upload-form]');
  const chatForm = document.querySelector('[data-chat-form]');
  const statusNode = document.querySelector('[data-briefing-status]');
  const walkthroughStatusNode = document.querySelector('[data-walkthrough-status]');
  const sessionOverviewNode = document.querySelector('[data-session-overview]');
  const sectionListNode = document.querySelector('[data-section-list]');
  const sectionDetailNode = document.querySelector('[data-section-detail]');
  const chatLogNode = document.querySelector('[data-chat-log]');
  const kbListNode = document.querySelector('[data-kb-list]');
  const narrateButton = document.querySelector('[data-play-section]');
  const readerModeToggle = document.querySelector('[data-reader-mode-toggle]');
  const recordQuestionButton = document.querySelector('[data-record-question]');
  const quickQuestionButtons = document.querySelectorAll('[data-quick-question]');
  const imageLightbox = document.querySelector('[data-image-lightbox]');
  const imageLightboxMedia = document.querySelector('[data-image-lightbox-media]');
  const imageLightboxClose = document.querySelector('[data-image-lightbox-close]');
  const imageLightboxOpen = document.querySelector('[data-image-lightbox-open]');

  let state = {
    sessionId: null,
    sections: [],
    currentSectionIndex: 0,
    mediaRecorder: null,
    chunks: [],
    messages: [],
    isNarrating: false,
    currentAudio: null,
    readerMode: false,
  };

  const READER_MODE_STORAGE_KEY = 'prd_briefing_reader_mode';

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

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
    walkthroughStatusNode.innerHTML = '<p>这里会显示当前讲解生成状态。</p>';
    delete walkthroughStatusNode.dataset.tone;
  };

  const renderKbSources = (sources) => {
    if (!kbListNode) return;
    if (!Array.isArray(sources) || !sources.length) {
      kbListNode.innerHTML = '<p class="help-text">还没有上传知识库文件。</p>';
      return;
    }
    kbListNode.innerHTML = sources.map((source) => `
      <article class="briefing-chip-card">
        <strong>${escapeHtml(source.title)}</strong>
        <span>${escapeHtml(source.source_type)} · updated ${escapeHtml(source.updated_at)}</span>
      </article>
    `).join('');
  };

  const renderSessionOverview = (overview) => {
    if (!sessionOverviewNode) return;
    if (!overview || !overview.overview) {
      sessionOverviewNode.innerHTML = '<div class="empty-state"><p>生成完成后，这里会出现“3 分钟看懂这个需求”。</p></div>';
      return;
    }
    const backgroundGoal = escapeHtml(overview.background_goal || '');
    const implementationOverview = escapeHtml(overview.implementation_overview || overview.overview || '');
    sessionOverviewNode.innerHTML = `
      <section class="briefing-overview-hero briefing-overview-hero-split">
        <article class="briefing-overview-card briefing-overview-summary">
          <p class="briefing-overview-kicker">业务背景和主要目的</p>
          <p>${backgroundGoal || implementationOverview}</p>
        </article>
        <article class="briefing-overview-card briefing-overview-summary">
          <p class="briefing-overview-kicker">需求概览和开发注意点</p>
          <p>${implementationOverview}</p>
        </article>
      </section>
    `;
  };

  const renderReaderMode = () => {
    const enabled = Boolean(state.readerMode);
    document.body.classList.toggle('briefing-reader-mode', enabled);
    if (readerModeToggle) {
      readerModeToggle.textContent = enabled ? '退出阅读模式' : '进入阅读模式';
      readerModeToggle.setAttribute('aria-pressed', enabled ? 'true' : 'false');
    }
  };

  const stopNarration = () => {
    if (state.currentAudio) {
      state.currentAudio.pause();
      state.currentAudio.currentTime = 0;
      state.currentAudio = null;
    }
    state.isNarrating = false;
    if (narrateButton) narrateButton.disabled = !state.sessionId;
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
    imageLightboxMedia.alt = alt || '放大的 PRD 图片预览';
    if (imageLightboxOpen) imageLightboxOpen.href = src;
    if (typeof imageLightbox.showModal === 'function') {
      imageLightbox.showModal();
    }
  };

  const addHorizontalHints = () => {
    if (!sectionDetailNode) return;
    const wrappers = sectionDetailNode.querySelectorAll('.table-wrap, .confluence-embedded-file-wrapper');
    wrappers.forEach((wrapper) => {
      wrapper.classList.add('briefing-horizontal-scroll');
      const hasOverflow = wrapper.scrollWidth > wrapper.clientWidth + 8;
      if (hasOverflow && !wrapper.previousElementSibling?.classList.contains('briefing-scroll-hint')) {
        const hint = document.createElement('div');
        hint.className = 'briefing-scroll-hint';
        hint.textContent = '可左右滚动查看完整内容';
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

      table.classList.toggle('briefing-dense-table', hasDenseColumns);
      table.classList.toggle('briefing-media-table', hasMedia);
      if (hasDenseColumns || hasMedia) {
        table.closest('.table-wrap')?.classList.add('briefing-natural-table-wrap');
      }

      imageCells.forEach((cell) => {
        cell.classList.add('briefing-media-cell');
      });
    });
  };

  const classifySectionImages = () => {
    if (!sectionDetailNode) return;
    sectionDetailNode.querySelectorAll('img').forEach((image) => {
      const applyClass = () => {
        const ratio = image.naturalHeight ? image.naturalWidth / image.naturalHeight : 0;
        const contextText = image.closest('.briefing-presentation-copy, td, th, p, li')?.textContent?.toLowerCase() || '';
        const src = `${image.currentSrc || image.src || ''}`.toLowerCase();
        const isIconLike =
          image.closest('.briefing-presentation-copy')
          || (ratio > 0.78 && ratio < 1.22)
          || /arrow|expand|collapse|up|down|icon/.test(src)
          || /expand|collapse|icon/.test(contextText);
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
      narrateButton.textContent = '正在生成中文讲解…';
    }
    try {
      const response = await fetch(`/prd-briefing/api/session/${state.sessionId}/narrate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ section_index: state.currentSectionIndex, include_audio: true }),
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload.message || '当前 section 无法生成讲解。');
      if (payload.audio_url) {
        const audio = new Audio(payload.audio_url);
        state.currentAudio = audio;
        audio.addEventListener('ended', () => {
          state.currentAudio = null;
          state.isNarrating = false;
          if (narrateButton) narrateButton.disabled = false;
        }, { once: true });
        audio.addEventListener('error', () => {
          state.currentAudio = null;
          state.isNarrating = false;
          if (narrateButton) narrateButton.disabled = false;
        }, { once: true });
        await audio.play().catch(() => {
          state.currentAudio = null;
          state.isNarrating = false;
          if (narrateButton) narrateButton.disabled = false;
        });
      } else {
        state.isNarrating = false;
        if (narrateButton) {
          narrateButton.disabled = false;
          narrateButton.textContent = '开始中文讲解这一节';
        }
        throw new Error('服务端语音当前不可用，浏览器机械音兜底已关闭。');
      }
      setStatus(
        payload.audio_url
          ? '当前 section 的中文讲解已经生成。'
          : '服务端语音当前不可用。',
        'success',
      );
      setWalkthroughStatus('当前 section 的中文讲解已经生成并开始播放。', 'success');
    } catch (error) {
      state.isNarrating = false;
      if (narrateButton) {
        narrateButton.disabled = false;
        narrateButton.textContent = '开始中文讲解这一节';
      }
      const raw = error.message || '当前 section 无法生成讲解。';
      const hasOpenAI = raw.includes('OpenAI');
      const friendly = raw.includes('429') || raw.includes('Too Many Requests')
        ? (hasOpenAI
            ? 'OpenAI 当前触发限流，暂时无法生成这一节的讲解，请稍后再试。'
            : '当前文本模型触发限流，暂时无法生成这一节的讲解，请稍后再试。')
        : raw;
      setStatus(friendly, 'error');
      setWalkthroughStatus(friendly, 'error');
    }
  };

  const renderSections = () => {
    if (!sectionListNode || !sectionDetailNode) return;
    if (!state.sections.length) {
      clearWalkthroughStatus();
      sectionListNode.innerHTML = '<div class="empty-state"><p>生成完成后，这里会出现 PRD section 导航。</p></div>';
      sectionDetailNode.innerHTML = '<div class="empty-state"><p>请选择一个 section 查看英文原文和中文开发讲解。</p></div>';
      narrateButton.disabled = true;
      narrateButton.textContent = '开始中文讲解这一节';
      return;
    }
    sectionListNode.innerHTML = state.sections.map((section, index) => `
      <button class="briefing-outline-item ${index === state.currentSectionIndex ? 'is-active' : ''}" type="button" data-section-index="${index}">
        <span>${index + 1}</span>
        <strong>${escapeHtml(section.section_path)}</strong>
      </button>
    `).join('');
    const section = state.sections[state.currentSectionIndex];
    const hasOriginalHtml = Boolean(section.html_content && section.html_content.trim());
    const images = !hasOriginalHtml
      ? (section.image_refs || []).map((src) => `<img src="${escapeHtml(src)}" alt="${escapeHtml(section.section_path)}">`).join('')
      : '';
    const contentMarkup = section.html_content && section.html_content.trim()
      ? section.html_content
      : (section.content || '')
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => `<p>${escapeHtml(line)}</p>`)
        .join('');
    sectionDetailNode.innerHTML = `
      <div class="briefing-section-heading">
        <h3>${escapeHtml(section.section_path)}</h3>
      </div>
      <div class="briefing-original-content">${contentMarkup || `<p>${escapeHtml(section.content || '')}</p>`}</div>
      ${images ? `<div class="briefing-image-grid">${images}</div>` : ''}
    `;
    enhancePresentationTables();
    classifyTableLayouts();
    addHorizontalHints();
    classifySectionImages();
    sectionDetailNode.querySelectorAll('img').forEach((image) => {
      image.setAttribute('tabindex', '0');
      image.setAttribute('role', 'button');
      image.setAttribute('aria-label', `${section.section_path} 图片预览`);
      const openPreview = () => openImageLightbox(image.currentSrc || image.src, image.alt || section.section_path);
      image.addEventListener('click', openPreview);
      image.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          openPreview();
        }
      });
    });
    sectionListNode.querySelectorAll('[data-section-index]').forEach((button) => {
      button.addEventListener('click', () => {
        stopNarration();
        clearWalkthroughStatus();
        state.currentSectionIndex = Number(button.dataset.sectionIndex || 0);
        renderSections();
      });
    });
    narrateButton.disabled = state.isNarrating;
    if (!state.isNarrating) narrateButton.textContent = '开始中文讲解这一节';
  };

  const renderMessages = (messages = []) => {
    if (!chatLogNode) return;
    state.messages = messages;
    if (!messages.length) {
      chatLogNode.innerHTML = '<div class="empty-state"><p>先生成中文开发讲解，再继续追问你的第一个问题。</p></div>';
      return;
    }
    chatLogNode.innerHTML = messages.map((message) => {
      if (message.role === 'user') {
        return `<article class="chat-bubble chat-bubble-user"><strong>你</strong><p>${escapeHtml(message.body)}</p></article>`;
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
            <strong>讲解助手</strong>
            <span class="briefing-pill">${escapeHtml(message.groundedness || '回答')}</span>
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
    state.currentSectionIndex = 0;
    setStatus(`已生成《${payload.session.title}》的中文开发讲解。`, 'success');
    renderSessionOverview(payload.session_overview || null);
    renderSections();
    renderKbSources(payload.kb_sources || []);
    renderMessages(payload.messages || []);
  };

  if (sessionForm) {
    sessionForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      const formData = new FormData(sessionForm);
      setStatus('正在读取 Confluence PRD，并生成中文开发讲解…');
      try {
        const response = await fetch('/prd-briefing/api/session', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            page_ref: formData.get('page_ref'),
            mode: formData.get('mode'),
          }),
        });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.message || '当前无法生成 PRD 讲解。');
        applySessionPayload(payload);
      } catch (error) {
        setStatus(error.message || '当前无法生成 PRD 讲解。', 'error');
      }
    });
  }

  if (kbForm) {
    kbForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      const formData = new FormData(kbForm);
      try {
        const response = await fetch('/prd-briefing/api/kb/upload', { method: 'POST', body: formData });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.message || '知识库文件上传失败。');
        const sessionId = state.sessionId;
        if (sessionId) {
          const sessionResponse = await fetch(`/prd-briefing/api/session/${sessionId}`);
          const sessionPayload = await sessionResponse.json();
          applySessionPayload(sessionPayload);
        } else {
          kbListNode.insertAdjacentHTML('afterbegin', `<article class="briefing-chip-card"><strong>${escapeHtml(payload.title)}</strong><span>${escapeHtml(payload.chunk_count)} chunks</span></article>`);
        }
      } catch (error) {
        setStatus(error.message || '知识库文件上传失败。', 'error');
      }
    });
  }

  if (chatForm) {
    chatForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      if (!state.sessionId) {
        setStatus('请先生成中文开发讲解。', 'error');
        return;
      }
      const formData = new FormData(chatForm);
      const question = String(formData.get('question') || '').trim();
      if (!question) return;
      try {
        const response = await fetch(`/prd-briefing/api/session/${state.sessionId}/answer`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question }),
        });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.message || '当前无法回答这个问题。');
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
        setStatus(error.message || '当前无法回答这个问题。', 'error');
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

  const startRecording = async () => {
    if (!navigator.mediaDevices?.getUserMedia) {
      setStatus('当前浏览器不支持麦克风录音。', 'error');
      return;
    }
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const chunks = [];
    const recorder = new MediaRecorder(stream);
    recorder.ondataavailable = (event) => {
      if (event.data.size > 0) chunks.push(event.data);
    };
    recorder.onstop = async () => {
      const blob = new Blob(chunks, { type: 'audio/webm' });
      const formData = new FormData();
      formData.append('audio', blob, 'question.webm');
      try {
        const response = await fetch('/prd-briefing/api/transcribe', { method: 'POST', body: formData });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.message || '录音转文字失败。');
        const textarea = chatForm?.querySelector('textarea[name="question"]');
        if (textarea) textarea.value = payload.text || '';
        setStatus('录音已转成文字，请检查后再提交问题。', 'success');
      } catch (error) {
        setStatus(error.message || '录音转文字失败。', 'error');
      }
      stream.getTracks().forEach((track) => track.stop());
      if (recordQuestionButton) recordQuestionButton.textContent = '开始录音';
      state.mediaRecorder = null;
      state.chunks = [];
    };
    recorder.start();
    state.mediaRecorder = recorder;
    recordQuestionButton.textContent = '停止录音';
  };

  if (recordQuestionButton) {
    recordQuestionButton.addEventListener('click', async () => {
      if (state.mediaRecorder && state.mediaRecorder.state === 'recording') {
        state.mediaRecorder.stop();
        return;
      }
      try {
        await startRecording();
      } catch (error) {
        setStatus(error.message || '当前无法开始录音。', 'error');
      }
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
  renderReaderMode();
})();
