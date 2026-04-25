(() => {
  const sessionForm = document.querySelector('[data-briefing-session-form]');
  const chatForm = document.querySelector('[data-chat-form]');
  const statusNode = document.querySelector('[data-briefing-status]');
  const walkthroughStatusNode = document.querySelector('[data-walkthrough-status]');
  const sessionOverviewNode = document.querySelector('[data-session-overview]');
  const sectionListNode = document.querySelector('[data-section-list]');
  const sectionDetailNode = document.querySelector('[data-section-detail]');
  const chatLogNode = document.querySelector('[data-chat-log]');
  const narrateButton = document.querySelector('[data-play-section]');
  const readerModeToggle = document.querySelector('[data-reader-mode-toggle]');
  const quickQuestionButtons = document.querySelectorAll('[data-quick-question]');
  const imageLightbox = document.querySelector('[data-image-lightbox]');
  const imageLightboxMedia = document.querySelector('[data-image-lightbox-media]');
  const imageLightboxClose = document.querySelector('[data-image-lightbox-close]');
  const imageLightboxOpen = document.querySelector('[data-image-lightbox-open]');
  const sessionSubmitButton = sessionForm?.querySelector('button[type="submit"]');
  const chatSubmitButton = chatForm?.querySelector('button[type="submit"]');
  const CACHED_NARRATION_DELAY_MS = 0;

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
  };

  const READER_MODE_STORAGE_KEY = 'prd_briefing_reader_mode';

  const isValidHttpUrl = (value) => /^https?:\/\/\S+/i.test(String(value || '').trim());

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

  const wait = (durationMs) => new Promise((resolve) => {
    window.setTimeout(resolve, durationMs);
  });

  const parseJsonResponse = async (response) => {
    const contentType = String(response.headers.get('content-type') || '').toLowerCase();
    if (contentType.includes('application/json')) {
      return response.json();
    }

    const text = await response.text().catch(() => '');
    if (response.redirected) {
      throw new Error('当前会话已失效或需要重新登录，请刷新页面后重试。');
    }
    if (text.trim().startsWith('<!DOCTYPE') || text.trim().startsWith('<html')) {
      throw new Error('服务端返回了页面而不是接口结果。请刷新页面后重试；如果还不行，通常是登录态失效或服务短暂异常。');
    }
    throw new Error(`接口返回格式异常（${contentType || 'unknown'}）。`);
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
    imageLightboxMedia.alt = alt || '放大的 PRD 图片预览';
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
      const hasMediaSplit = hasMedia && maxColumns === 2;

      table.classList.toggle('briefing-dense-table', hasDenseColumns);
      table.classList.toggle('briefing-media-table', hasMedia);
      table.classList.toggle('briefing-media-split-table', hasMediaSplit);
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
      narrateButton.textContent = '正在生成中文讲解…';
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
      if (!response.ok) throw new Error(payload.message || '当前模块无法生成讲解。');
      if (payload.cached) {
        setWalkthroughStatus('命中缓存，正在整理当前模块的中文讲解并准备播放…', 'neutral');
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
          narrateButton.textContent = '开始中文讲解这个模块';
        }
        throw new Error('服务端语音当前不可用，浏览器机械音兜底已关闭。');
      }
      setStatus(
        payload.audio_url
          ? (payload.cached ? '已命中缓存并准备好当前模块的中文讲解。' : '当前模块的中文讲解已经生成。')
          : '服务端语音当前不可用。',
        'success',
      );
      setWalkthroughStatus(
        payload.cached
          ? '已命中缓存，当前模块的中文讲解已经准备好并开始播放，相关 PRD 原文已高亮。'
          : '当前模块的中文讲解已经生成并开始播放，相关 PRD 原文已高亮。',
        'success',
      );
    } catch (error) {
      state.isNarrating = false;
      clearSourceHighlights();
      if (narrateButton) {
        narrateButton.disabled = false;
        narrateButton.textContent = '开始中文讲解这个模块';
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
      sectionListNode.innerHTML = '<div class="empty-state"><p>生成完成后，这里会出现 PM briefing 模块导航。</p></div>';
      sectionDetailNode.innerHTML = '<div class="empty-state"><p>请选择一个 briefing 模块查看合并讲解和 PRD 原文。</p></div>';
      narrateButton.disabled = true;
      narrateButton.textContent = '开始中文讲解这个模块';
      return;
    }
    const blocks = state.briefingBlocks.length ? state.briefingBlocks : state.sections.map((section, index) => ({
      block_id: `section-${index}`,
      title: section.section_path,
      briefing_goal: '按原 PRD section 生成讲解。',
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
        <small>${escapeHtml((block.section_indexes || []).length)} 个 PRD section</small>
        <div class="briefing-cache-pill-row">
          ${block.walkthrough_cached ? '<em class="briefing-cache-pill">文案已缓存</em>' : ''}
          ${block.walkthrough_audio_cached ? '<em class="briefing-cache-pill briefing-cache-pill-secondary">音频已缓存</em>' : ''}
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
      const contentMarkup = section.html_content && section.html_content.trim()
        ? section.html_content
        : (section.content || '')
          .split('\n')
          .map((line) => line.trim())
          .filter(Boolean)
          .map((line) => `<p>${escapeHtml(line)}</p>`)
          .join('');
      return `
        <section class="briefing-source-section" data-source-section-index="${sectionIndex}">
          <div class="briefing-source-heading">
            <span>PRD ${sectionIndex + 1}</span>
            <strong>${escapeHtml(section.section_path)}</strong>
          </div>
          <div class="briefing-original-content">${contentMarkup || `<p>${escapeHtml(section.content || '')}</p>`}</div>
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
        <span class="briefing-section-meta">第 ${state.currentBlockIndex + 1} / ${blocks.length} 个 briefing 模块</span>
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
    sectionDetailNode.querySelectorAll('img').forEach((image) => {
      image.setAttribute('tabindex', '0');
      image.setAttribute('role', 'button');
      image.setAttribute('aria-label', `${block.title} 图片预览`);
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
            ? '这个模块的文案和音频都已命中缓存，点击“开始中文讲解这个模块”时不会重新调用文本模型或语音生成。'
            : '这个模块的文案已命中缓存，点击“开始中文讲解这个模块”时不会重新调用文本模型。';
          setWalkthroughStatus(detail, 'success');
        } else {
          clearWalkthroughStatus();
        }
      });
    });
    narrateButton.disabled = state.isNarrating;
    if (!state.isNarrating) narrateButton.textContent = '开始中文讲解这个模块';
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
    state.briefingBlocks = payload.briefing_blocks || [];
    state.currentSectionIndex = 0;
    state.currentBlockIndex = 0;
    setStatus(`已生成《${payload.session.title}》的中文开发讲解。`, 'success');
    renderSessionOverview(payload.session_overview || null);
    renderSections();
    renderMessages(payload.messages || []);
  };

  if (sessionForm) {
    sessionForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      const formData = new FormData(sessionForm);
      const pageRef = String(formData.get('page_ref') || '').trim();
      if (!isValidHttpUrl(pageRef)) {
        setStatus('请输入有效的 Confluence 页面链接。', 'error');
        return;
      }
      if (sessionSubmitButton) {
        sessionSubmitButton.disabled = true;
        sessionSubmitButton.textContent = '正在生成…';
      }
      setStatus('正在读取 Confluence PRD，并生成中文开发讲解…');
      try {
        const response = await fetch('/prd-briefing/api/session', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            page_ref: pageRef,
            mode: formData.get('mode'),
          }),
        });
        const payload = await parseJsonResponse(response);
        if (!response.ok) throw new Error(payload.message || '当前无法生成 PRD 讲解。');
        applySessionPayload(payload);
      } catch (error) {
        setStatus(error.message || '当前无法生成 PRD 讲解。', 'error');
      } finally {
        if (sessionSubmitButton) {
          sessionSubmitButton.disabled = false;
          sessionSubmitButton.textContent = '生成中文开发讲解';
        }
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
      if (chatSubmitButton) {
        chatSubmitButton.disabled = true;
        chatSubmitButton.textContent = '正在回答…';
      }
      try {
        const response = await fetch(`/prd-briefing/api/session/${state.sessionId}/answer`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ question }),
        });
        const payload = await parseJsonResponse(response);
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
      } finally {
        if (chatSubmitButton) {
          chatSubmitButton.disabled = false;
          chatSubmitButton.textContent = '提交开发问题';
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
