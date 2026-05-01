(() => {
  const root = document.querySelector('[data-source-code-qa-root]');
  if (!root) return;

  const configUrl = root.dataset.configUrl;
  const saveUrl = root.dataset.saveUrl;
  const syncUrl = root.dataset.syncUrl;
  const queryUrl = root.dataset.queryUrl;
  const attachmentUrl = root.dataset.attachmentUrl;
  const runtimeEvidenceUrl = root.dataset.runtimeEvidenceUrl;
  const jobsUrlTemplate = root.dataset.jobsUrl || '/api/jobs/__JOB_ID__';
  const feedbackUrl = root.dataset.feedbackUrl;
  const sessionsUrl = root.dataset.sessionsUrl;
  const modelAvailabilityUrl = root.dataset.modelAvailabilityUrl;
  const canManage = root.dataset.canManage === 'true';
  const options = JSON.parse(root.dataset.options || '{}');

  const pmTeam = document.querySelector('[data-source-pm-team]');
  const country = document.querySelector('[data-source-country]');
  const answerMode = document.querySelector('[data-source-answer-mode]');
  const queryMode = document.querySelector('[data-source-query-mode]');
  const queryModeHelp = document.querySelector('[data-source-query-mode-help]');
  const llmProvider = document.querySelector('[data-source-llm-provider]');
  const countryWrap = document.querySelector('[data-source-country-wrap]');
  const countryHelp = document.querySelector('[data-source-country-help]');
  const configStatus = document.querySelector('[data-source-config-status]');
  const adminStatus = document.querySelector('[data-source-admin-status]');
  const reposInput = document.querySelector('[data-source-repos-input]');
  const saveButton = document.querySelector('[data-source-save-config]');
  const syncButton = document.querySelector('[data-source-sync]');
  const questionInput = document.querySelector('[data-source-question]');
  const chatComposer = document.querySelector('.source-qa-chat-composer');
  const queryButton = document.querySelector('[data-source-query]');
  const queryStatus = document.querySelector('[data-source-query-status]');
  const attachmentInput = document.querySelector('[data-source-attachment-input]');
  const attachmentUploadButton = document.querySelector('[data-source-attachment-upload]');
  const attachmentsList = document.querySelector('[data-source-attachments]');
  const repoStatus = document.querySelector('[data-source-repo-status]');
  const summary = document.querySelector('[data-source-summary]');
  const results = document.querySelector('[data-source-results]');
  const llmAnswer = document.querySelector('[data-source-llm-answer]');
  const activeMode = document.querySelector('[data-source-active-mode]');
  const activeCache = document.querySelector('[data-source-active-cache]');
  const activeUsage = document.querySelector('[data-source-active-usage]');
  const fallbackNotice = document.querySelector('[data-source-fallback-notice]');
  const liveAnswer = document.querySelector('[data-source-live-answer]');
  const feedback = document.querySelector('[data-source-feedback]');
  const feedbackReasons = document.querySelector('[data-source-feedback-reasons]');
  const feedbackStatus = document.querySelector('[data-source-feedback-status]');
  const copyAnswerButton = document.querySelector('[data-source-copy-answer]');
  const evidenceSummary = document.querySelector('[data-source-evidence-summary]');
  const debugTrace = document.querySelector('[data-source-debug-trace]');
  const indexHealth = document.querySelector('[data-source-index-health]');
  const modelAvailability = document.querySelector('[data-source-model-availability]');
  const modelAvailabilityStatus = document.querySelector('[data-source-model-availability-status]');
  const saveModelAvailabilityButton = document.querySelector('[data-source-save-model-availability]');
  const runtimeEvidencePmTeam = document.querySelector('[data-source-runtime-pm-team]');
  const runtimeEvidenceCountry = document.querySelector('[data-source-runtime-country]');
  const runtimeEvidenceSourceType = document.querySelector('[data-source-runtime-source-type]');
  const runtimeEvidenceInput = document.querySelector('[data-source-runtime-evidence-input]');
  const runtimeEvidenceUploadButton = document.querySelector('[data-source-runtime-evidence-upload]');
  const runtimeEvidenceList = document.querySelector('[data-source-runtime-evidence-list]');
  const runtimeEvidenceStatus = document.querySelector('[data-source-runtime-evidence-status]');
  const newSessionButton = document.querySelector('[data-source-new-session]');
  const viewTabs = Array.from(document.querySelectorAll('[data-source-view-tab]'));
  const viewPanels = Array.from(document.querySelectorAll('[data-source-view-panel]'));
  const sessionList = document.querySelector('[data-source-session-list]');
  const sessionTitle = document.querySelector('[data-source-session-title]');
  const sessionContext = document.querySelector('[data-source-session-context]');
  const sessionProvider = document.querySelector('[data-source-session-provider]');
  const sessionScope = document.querySelector('[data-source-session-scope]');
  const sessionMessages = document.querySelector('[data-source-session-messages]');
  let config = { mappings: {} };
  let gitAuthReady = false;
  let llmReady = false;
  let llmProviders = {};
  let modelAvailabilityPayload = {};
  let llmPolicy = {};
  let indexHealthPayload = {};
  let configLoadState = 'idle';
  let lastPayload = null;
  let conversationContext = null;
  let sourceSessions = [];
  let activeSession = null;
  let activeSessionId = '';
  let activeQueryProgress = null;
  let sessionHistoryExpanded = false;
  let liveAssistantMessage = null;
  let pendingUserMessage = null;
  let pendingAttachments = [];
  let activeAttachmentUploads = 0;
  let nextAttachmentUploadToken = 0;
  let attachmentPreview = null;
  let activeQueryControl = null;
  let pendingFeedbackRating = '';
  let notificationPermissionAsked = (() => {
    try {
      return window.localStorage.getItem('source-code-qa:notification-permission-asked:v1') === '1';
    } catch (_error) {
      return false;
    }
  })();
  const preferenceKey = 'source-code-qa:last-query-config:v1';
  const notificationPreferenceKey = 'source-code-qa:notification-permission-asked:v1';

  const setSourceView = (view) => {
    const nextView = view === 'admin' && canManage ? 'admin' : 'chat';
    viewTabs.forEach((tab) => {
      const active = tab.dataset.sourceViewTab === nextView;
      tab.classList.toggle('is-active', active);
      tab.setAttribute('aria-selected', active ? 'true' : 'false');
    });
    viewPanels.forEach((panel) => {
      const active = panel.dataset.sourceViewPanel === nextView;
      panel.classList.toggle('is-active', active);
      panel.hidden = !active;
    });
    if (nextView === 'admin') {
      renderSelectedConfig();
      loadRuntimeEvidence();
    }
  };

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const readJson = async (response) => {
    const contentType = response.headers.get('content-type') || '';
    if (!contentType.includes('application/json')) {
      const text = await response.text();
      const httpStatus = response.status ? `HTTP ${response.status}` : 'non-JSON response';
      const looksHtml = text.includes('<!DOCTYPE') || contentType.includes('text/html');
      const error = new Error(looksHtml
        ? `${httpStatus}: the portal returned an HTML error/timeout page instead of JSON. Please retry; if it repeats, check server logs with the request time.`
        : `${httpStatus}: ${text.slice(0, 180)}`);
      error.transientPortalHtml = looksHtml;
      error.httpStatus = response.status || 0;
      throw error;
    }
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.message || 'Request failed.');
    }
    return payload;
  };
  const sleep = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms));
  const jobStatusUrl = (jobId) => jobsUrlTemplate.replace('__JOB_ID__', encodeURIComponent(jobId));
  const jobEventsUrl = (jobId) => `${jobStatusUrl(jobId)}/events`;
  const sourceQaJobErrorMessage = (payloadOrError) => {
    const category = String(payloadOrError?.error_category || '').toLowerCase();
    const rawMessage = String(payloadOrError?.message || payloadOrError?.error || '').trim();
    if (category === 'local_agent_offline') {
      return 'Mac local-agent 当前不可用。请确认 host stack 在线后点击 Reconnect。';
    }
    if (category === 'gateway_disconnected') {
      return '网关连接中断，但后台任务可能仍在运行。点击 Reconnect 恢复状态。';
    }
    if (category === 'job_running') {
      return '后台仍在分析代码，连接刚才中断了。点击 Reconnect 恢复状态。';
    }
    if (category === 'job_queued') {
      return '任务已进入队列，后台会按用户公平轮转调度。';
    }
    if (category === 'job_stalled') {
      return '后台任务暂时没有进展，可能仍在 Codex 推理。可以 Reconnect 或 Retry。';
    }
    if (category === 'job_not_found') {
      return '这个后台任务状态已经找不到了，请重新提交问题。';
    }
    if (category === 'codex_timeout_or_rate_limit') {
      return 'Codex 推理超时或被限流。可以重试，或者先缩小问题范围。';
    }
    if (/failed to fetch|load failed|networkerror|internet connection appears to be offline/i.test(rawMessage)) {
      return '浏览器和后台状态接口断开了。后台任务可能仍在运行，点击 Reconnect 恢复状态。';
    }
    return rawMessage || 'Source Code Q&A failed.';
  };
  const isTransientJobStatusError = (error) => {
    const message = String(error?.message || '').toLowerCase();
    return Boolean(error?.transientPortalHtml)
      || message.includes('html error/timeout page')
      || message.includes('non-json response')
      || message.includes('failed to fetch')
      || message.includes('load failed')
      || message.includes('networkerror')
      || message.includes('network request failed')
      || message.includes('internet connection appears to be offline');
  };
  const apiFetchJson = async (url, options = {}, retryOptions = {}) => {
    const attempts = Number(retryOptions.attempts || 1);
    let lastError = null;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try {
        return await fetch(url, {
          ...options,
          headers: {
            Accept: 'application/json',
            ...(options.headers || {}),
          },
        }).then(readJson);
      } catch (error) {
        lastError = error;
        if (!isTransientJobStatusError(error) || attempt >= attempts - 1) {
          throw error;
        }
        await sleep(Number(retryOptions.delayMs || 500) + (attempt * Number(retryOptions.backoffMs || 350)));
      }
    }
    throw lastError || new Error('Request failed.');
  };
  const readJobStatus = async (jobId) => {
    let lastError = null;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      try {
        return await apiFetchJson(jobStatusUrl(jobId), { method: 'GET' });
      } catch (error) {
        lastError = error;
        if (!isTransientJobStatusError(error)) {
          throw error;
        }
        await sleep(Math.min(1800, 450 + (attempt * 150)));
      }
    }
    throw new Error(lastError?.message
      ? `Job status connection interrupted. Last error: ${lastError.message}`
      : 'Job status connection interrupted.');
  };

  const allCountryValue = () => options.all_country || 'All';
  const sharedCodeTeams = ['AF', 'GRC'];
  const countrySpecificTeams = ['CRMS'];
  const runtimeCountries = () => (
    Array.isArray(options.countries) && options.countries.length
      ? options.countries.map((item) => String(item || '').trim()).filter(Boolean)
      : ['SG', 'ID', 'PH']
  );
  const isCountrySpecificTeam = (team = pmTeam?.value) => countrySpecificTeams.includes(String(team || '').trim());
  const isSharedCodeTeam = (team = pmTeam?.value) => sharedCodeTeams.includes(String(team || '').trim());
  const runtimeCapabilities = () => options.runtime_capabilities || {};
  const countryCapability = (team, countryCode) => {
    const teamCapabilities = runtimeCapabilities()?.[team] || {};
    return teamCapabilities?.[countryCode] || { hasConfig: false, hasDB: false };
  };
  const capabilityLabel = (team, countryCode) => {
    if (countryCode === allCountryValue()) return 'Common code only';
    const capability = countryCapability(team, countryCode);
    if (capability.hasConfig && capability.hasDB) return 'Apollo + DB';
    if (capability.hasConfig) return 'Apollo only';
    if (capability.hasDB) return 'DB only';
    return 'No runtime evidence';
  };
  const runtimeContextHelp = () => {
    const team = pmTeam?.value || 'AF';
    const selectedCountry = currentCountry();
    if (isCountrySpecificTeam(team)) {
      return `国家独立代码，必须选择具体国家；运行上下文：${team}:${selectedCountry} (${capabilityLabel(team, selectedCountry)}).`;
    }
    if (selectedCountry === allCountryValue()) {
      return '仅查通用代码，不加载国家运行时数据。';
    }
    if (isSharedCodeTeam(team)) {
      return `代码范围：${team}:${allCountryValue()}；运行上下文：${team}:${selectedCountry} (${capabilityLabel(team, selectedCountry)}).`;
    }
    return `运行上下文：${team}:${selectedCountry} (${capabilityLabel(team, selectedCountry)}).`;
  };
  const currentCountry = () => country.value || (isCountrySpecificTeam() ? runtimeCountries()[0] || 'SG' : allCountryValue());
  const currentRepositoryCountry = () => (pmTeam.value === 'CRMS' ? currentCountry() : allCountryValue());
  const currentKey = () => `${pmTeam.value}:${currentRepositoryCountry()}`;
  const currentRepoCount = () => (config.mappings?.[currentKey()] || []).length;
  const defaultLlmProvider = () => {
    if (!llmProvider) return 'codex_cli_bridge';
    const enabledPreferred = Array.from(llmProvider.options || []).find((option) => !option.disabled && option.value === 'codex_cli_bridge');
    if (enabledPreferred) return enabledPreferred.value;
    const firstEnabled = Array.from(llmProvider.options || []).find((option) => !option.disabled);
    return firstEnabled?.value || 'codex_cli_bridge';
  };
  const providerOptionIsEnabled = (value) => {
    if (!llmProvider) return false;
    return Array.from(llmProvider.options || []).some((option) => option.value === value && !option.disabled);
  };
  const selectedLlmProvider = () => (providerOptionIsEnabled(llmProvider?.value) ? llmProvider.value : defaultLlmProvider());
  const selectedProviderReady = () => {
    const provider = selectedLlmProvider();
    if (llmProviders[provider]) return Boolean(llmProviders[provider].ready);
    return provider === (llmPolicy.provider?.provider || 'codex_cli_bridge') ? llmReady : false;
  };
  const formatElapsed = (startedAt) => {
    const seconds = Math.max(0, (performance.now() - startedAt) / 1000);
    return seconds < 10 ? `${seconds.toFixed(1)}s` : `${Math.round(seconds)}s`;
  };
  const formatDuration = (seconds) => {
    const value = Math.max(0, Number(seconds || 0));
    if (value < 60) return `${Math.round(value)} 秒`;
    return `${Math.max(1, Math.round(value / 60))} 分钟`;
  };
  const formatEtaRange = (range) => {
    if (!Array.isArray(range) || range.length < 2) return '';
    const lower = Number(range[0] || 0);
    const upper = Number(range[1] || 0);
    if (upper <= 0) return '即将开始';
    return `预计等待约 ${formatDuration(lower)}-${formatDuration(upper)}`;
  };
  const notificationSupported = () => 'Notification' in window;
  const requestNotificationPermission = async () => {
    if (!notificationSupported() || Notification.permission !== 'default') return;
    if (notificationPermissionAsked) return;
    notificationPermissionAsked = true;
    try {
      window.localStorage.setItem(notificationPreferenceKey, '1');
    } catch (_error) {
      // Ignore storage failures; permission itself is the source of truth.
    }
    try {
      await Notification.requestPermission();
    } catch (_error) {
      // Notification permission prompts can be blocked by browser policy.
    }
  };
  const notifyJobFinished = (title, body) => {
    if (!notificationSupported() || Notification.permission !== 'granted') return;
    try {
      new Notification(title, { body });
    } catch (_error) {
      // Notifications are best-effort and must not block the chat flow.
    }
  };
  const formatAttachmentSize = (bytes) => {
    const size = Number(bytes || 0);
    if (size >= 1024 * 1024) return `${(size / 1024 / 1024).toFixed(1)} MB`;
    if (size >= 1024) return `${Math.round(size / 1024)} KB`;
    return `${size} B`;
  };
  const isImageAttachment = (item) => item?.kind === 'image' || String(item?.mime_type || '').startsWith('image/');
  const attachmentPreviewUrl = (item) => {
    if (!attachmentUrl || !item?.id || !activeSessionId) return '';
    return `${attachmentUrl.replace(/\/$/, '')}/${encodeURIComponent(item.id)}?session_id=${encodeURIComponent(activeSessionId)}`;
  };
  const renderAttachmentChip = (item, options = {}) => {
    const filename = item.filename || 'attachment';
    const kind = item.uploading ? 'image' : (item.kind || item.mime_type || 'file');
    const isPreviewable = isImageAttachment(item) && !item.uploading && item.id && activeSessionId;
    const stateClass = [
      item.uploading ? 'is-uploading' : '',
      isPreviewable ? 'is-previewable' : '',
    ].filter(Boolean).join(' ');
    const previewAttrs = isPreviewable
      ? ` role="button" tabindex="0" data-source-preview-attachment="${escapeHtml(item.id)}" title="Preview ${escapeHtml(filename)}"`
      : '';
    const removeButton = options.removable
      ? `<button type="button" aria-label="Remove ${escapeHtml(filename)}" data-source-remove-attachment="${escapeHtml(item.id || item.temp_id || '')}">x</button>`
      : '';
    return `
      <span class="source-qa-attachment-chip ${stateClass}"${previewAttrs}>
        <strong>${escapeHtml(filename)}</strong>
        <small>${item.uploading ? 'Uploading...' : `${escapeHtml(kind)} · ${escapeHtml(formatAttachmentSize(item.size))}`}</small>
        ${removeButton}
      </span>
    `;
  };
  const renderAttachmentChips = (items = []) => {
    if (!items.length) return '';
    return `
      <div class="source-qa-message-attachments">
        ${items.map((item) => renderAttachmentChip(item)).join('')}
      </div>
    `;
  };
  const renderPendingAttachments = () => {
    if (!attachmentsList) return;
    if (!pendingAttachments.length) {
      attachmentsList.hidden = true;
      attachmentsList.innerHTML = '';
      return;
    }
    attachmentsList.hidden = false;
    attachmentsList.innerHTML = pendingAttachments.map((item) => renderAttachmentChip(item, { removable: true })).join('');
  };
  const clearPendingAttachments = () => {
    pendingAttachments = [];
    if (attachmentInput) attachmentInput.value = '';
    renderPendingAttachments();
  };
  const updateAttachmentUploadState = () => {
    const uploading = activeAttachmentUploads > 0;
    if (attachmentUploadButton) attachmentUploadButton.disabled = uploading;
    if (queryButton && !activeQueryControl) queryButton.disabled = uploading;
  };
  const ensureAttachmentPreview = () => {
    if (attachmentPreview) return attachmentPreview;
    const container = document.createElement('div');
    container.className = 'source-qa-attachment-preview';
    container.hidden = true;
    container.innerHTML = `
      <div class="source-qa-attachment-preview-backdrop" data-source-preview-close></div>
      <div class="source-qa-attachment-preview-panel" role="dialog" aria-modal="true" aria-label="Image preview">
        <button type="button" class="source-qa-attachment-preview-close" data-source-preview-close aria-label="Close preview">x</button>
        <img alt="">
        <p></p>
      </div>
    `;
    document.body.appendChild(container);
    container.addEventListener('click', (event) => {
      if (event.target.closest('[data-source-preview-close]')) {
        container.hidden = true;
        const image = container.querySelector('img');
        if (image) image.removeAttribute('src');
      }
    });
    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape' && !container.hidden) {
        container.hidden = true;
        const image = container.querySelector('img');
        if (image) image.removeAttribute('src');
      }
    });
    attachmentPreview = container;
    return container;
  };
  const findAttachmentById = (attachmentId) => {
    const wanted = String(attachmentId || '');
    if (!wanted) return null;
    const pending = pendingAttachments.find((item) => item.id === wanted);
    if (pending) return pending;
    const messages = Array.isArray(activeSession?.messages) ? activeSession.messages : [];
    for (const message of messages) {
      const items = Array.isArray(message.attachments)
        ? message.attachments
        : (Array.isArray(message.payload?.attachments) ? message.payload.attachments : []);
      const found = items.find((item) => item.id === wanted);
      if (found) return found;
    }
    if (pendingUserMessage?.attachments) {
      return pendingUserMessage.attachments.find((item) => item.id === wanted) || null;
    }
    return null;
  };
  const openAttachmentPreview = (attachmentId) => {
    const item = findAttachmentById(attachmentId);
    const url = attachmentPreviewUrl(item);
    if (!item || !url || !isImageAttachment(item)) return;
    const preview = ensureAttachmentPreview();
    const image = preview.querySelector('img');
    const caption = preview.querySelector('p');
    if (image) {
      image.alt = item.filename || 'Attachment preview';
      image.src = url;
    }
    if (caption) caption.textContent = item.filename || 'Attachment preview';
    preview.hidden = false;
  };
  const handleAttachmentPreviewEvent = (event) => {
    const target = event.target.closest('[data-source-preview-attachment]');
    if (!target) return;
    if (event.type === 'keydown' && !['Enter', ' '].includes(event.key)) return;
    event.preventDefault();
    openAttachmentPreview(target.dataset.sourcePreviewAttachment || '');
  };
  const uploadSourceAttachment = async (file, sessionId) => {
    if (!attachmentUrl) throw new Error('Attachment API is not configured.');
    const form = new FormData();
    form.append('session_id', sessionId);
    form.append('file', file);
    const payload = await fetch(attachmentUrl, {
      method: 'POST',
      headers: { Accept: 'application/json' },
      body: form,
    }).then(readJson);
    return payload.attachment;
  };
  const addAttachmentFiles = async (files) => {
    const selectedFiles = Array.from(files || []);
    if (!selectedFiles.length) return 0;
    if (pendingAttachments.length + selectedFiles.length > 5) {
      if (queryStatus) queryStatus.textContent = 'At most 5 attachments are supported per question.';
      return 0;
    }
    const existingImages = pendingAttachments.filter((item) => item.kind === 'image').length;
    const nextImages = selectedFiles.filter((file) => String(file.type || '').startsWith('image/')).length;
    if (existingImages + nextImages > 3) {
      if (queryStatus) queryStatus.textContent = 'At most 3 image attachments are supported per question.';
      return 0;
    }
    const session = await ensureActiveSession({ preserveLive: true, preservePending: true });
    if (!session?.id) throw new Error('Could not create a chat session for attachments.');
    let uploadedCount = 0;
    try {
      for (const file of selectedFiles) {
        if (file.size > 10 * 1024 * 1024) {
          throw new Error(`${file.name} is larger than 10MB.`);
        }
        const tempId = `uploading-${Date.now()}-${nextAttachmentUploadToken += 1}`;
        pendingAttachments.push({
          id: tempId,
          temp_id: tempId,
          filename: file.name || 'image.png',
          mime_type: file.type || 'image/png',
          kind: String(file.type || '').startsWith('image/') ? 'image' : 'file',
          size: file.size,
          uploading: true,
        });
        activeAttachmentUploads += 1;
        updateAttachmentUploadState();
        renderPendingAttachments();
        if (queryStatus) queryStatus.textContent = `Uploading ${file.name}...`;
        try {
          const uploaded = await uploadSourceAttachment(file, session.id);
          pendingAttachments = pendingAttachments.map((item) => (item.id === tempId ? uploaded : item));
          uploadedCount += 1;
          renderPendingAttachments();
        } catch (error) {
          pendingAttachments = pendingAttachments.filter((item) => item.id !== tempId);
          renderPendingAttachments();
          throw error;
        } finally {
          activeAttachmentUploads = Math.max(0, activeAttachmentUploads - 1);
          updateAttachmentUploadState();
        }
      }
      if (queryStatus) queryStatus.textContent = 'Attachment uploaded.';
      return uploadedCount;
    } finally {
      if (attachmentInput) attachmentInput.value = '';
      updateAttachmentUploadState();
    }
  };
  const clipboardImageExtension = (mimeType) => {
    const normalized = String(mimeType || '').toLowerCase();
    if (normalized.includes('jpeg') || normalized.includes('jpg')) return 'jpg';
    if (normalized.includes('webp')) return 'webp';
    if (normalized.includes('gif')) return 'gif';
    return 'png';
  };
  const nameClipboardImage = (file, index = 0) => {
    if (file?.name) return file;
    const extension = clipboardImageExtension(file?.type);
    const filename = index === 0 ? `image.${extension}` : `image-${index + 1}.${extension}`;
    try {
      return new File([file], filename, {
        type: file?.type || `image/${extension}`,
        lastModified: file?.lastModified || Date.now(),
      });
    } catch (error) {
      return file;
    }
  };
  const imageFilesFromClipboard = (clipboardData) => {
    if (!clipboardData) return [];
    const pastedFiles = Array.from(clipboardData.files || [])
      .filter((file) => String(file.type || '').startsWith('image/'));
    if (pastedFiles.length) {
      return pastedFiles.map((file, index) => nameClipboardImage(file, index));
    }
    return Array.from(clipboardData.items || [])
      .filter((item) => item.kind === 'file' && String(item.type || '').startsWith('image/'))
      .map((item) => item.getAsFile())
      .filter(Boolean)
      .map((file, index) => nameClipboardImage(file, index));
  };
  const handleAttachmentPaste = async (event) => {
    const pastedImages = imageFilesFromClipboard(event.clipboardData);
    if (!pastedImages.length) return;
    event.preventDefault();
    try {
      const uploadedCount = await addAttachmentFiles(pastedImages);
      if (uploadedCount > 0 && queryStatus) {
        queryStatus.textContent = uploadedCount === 1
          ? 'Pasted image uploaded.'
          : `${uploadedCount} pasted images uploaded.`;
      }
    } catch (error) {
      if (queryStatus) queryStatus.textContent = error.message || 'Pasted image upload failed.';
    }
  };

  const stopQueryProgress = () => {
    if (activeQueryProgress) {
      window.clearInterval(activeQueryProgress);
      activeQueryProgress = null;
    }
  };

  const startQueryProgress = (message = 'Submitting query to server...') => {
    stopQueryProgress();
    const startedAt = performance.now();
    let currentMessage = message;
    const setMessage = (nextMessage) => {
      currentMessage = nextMessage || currentMessage;
      if (queryStatus) {
        queryStatus.textContent = `${currentMessage} elapsed ${formatElapsed(startedAt)}`;
      }
    };
    const update = () => {
      if (queryStatus) {
        queryStatus.textContent = `${currentMessage} elapsed ${formatElapsed(startedAt)}`;
      }
    };
    update();
    activeQueryProgress = window.setInterval(update, 500);
    return { startedAt, setMessage };
  };

  const buildFeedbackReplayContext = (payload) => ({
    trace_id: payload.trace_id || '',
    answer_mode: payload.answer_mode || '',
    query_mode: payload.query_mode || '',
    llm_budget_mode: payload.llm_budget_mode || payload.llm_requested_budget_mode || '',
    llm_provider: payload.llm_provider || '',
    llm_model: payload.llm_model || '',
    llm_route: payload.llm_route || {},
    codex_candidate_paths: (payload.llm_route?.candidate_paths || []).slice(0, 30),
    llm_finish_reason: payload.llm_finish_reason || '',
    summary: payload.summary || '',
    rendered_answer: payload.llm_answer || '',
    citations: payload.citations || [],
    answer_contract: payload.answer_contract || {},
    evidence_pack: payload.evidence_pack || {},
    tool_trace: (payload.tool_trace || []).slice(0, 30),
    matches_snapshot: (payload.matches || []).slice(0, 10).map((match) => ({
      repo: match.repo,
      path: match.path,
      line_start: match.line_start,
      line_end: match.line_end,
      retrieval: match.retrieval,
      trace_stage: match.trace_stage,
      score: match.score,
      snippet: match.snippet,
    })),
  });

  const selectHasValue = (select, value) => {
    if (!select || value == null) return false;
    return Array.from(select.options).some((option) => option.value === String(value));
  };

  const loadLastQueryConfig = () => {
    try {
      return JSON.parse(window.localStorage.getItem(preferenceKey) || '{}');
    } catch (_error) {
      return {};
    }
  };

  const restoreLastQueryConfig = () => {
    const saved = loadLastQueryConfig();
    if (selectHasValue(pmTeam, saved.pm_team)) {
      pmTeam.value = saved.pm_team;
    }
    updateCountryVisibility();
    if (selectHasValue(country, saved.country)) {
      country.value = saved.country;
      updateCountryVisibility();
    }
    if (selectHasValue(answerMode, saved.answer_mode)) {
      answerMode.value = saved.answer_mode;
    }
    if (selectHasValue(queryMode, saved.query_mode)) {
      queryMode.value = saved.query_mode;
    } else if (selectHasValue(queryMode, 'fast')) {
      queryMode.value = 'fast';
    }
    updateQueryModeHelp();
    if (providerOptionIsEnabled(saved.llm_provider)) {
      llmProvider.value = saved.llm_provider;
    } else if (providerOptionIsEnabled('codex_cli_bridge')) {
      llmProvider.value = 'codex_cli_bridge';
    }
  };

  const rememberLastQueryConfig = (selectedAnswerMode, selectedProvider) => {
    try {
      window.localStorage.setItem(preferenceKey, JSON.stringify({
        pm_team: pmTeam.value,
        country: currentCountry(),
        answer_mode: selectedAnswerMode,
        query_mode: queryMode?.value || 'fast',
        llm_provider: selectedProvider,
      }));
    } catch (_error) {
      // Local storage can be blocked in private/browser-managed contexts.
    }
  };

  const updateQueryModeHelp = () => {
    if (!queryModeHelp) return;
    if ((queryMode?.value || 'fast') === 'deep') {
      queryModeHelp.textContent = '更完整，允许 Codex 深挖和修复，可能需要 2-6 分钟。';
    } else {
      queryModeHelp.textContent = '目标：不排队时 1 分钟内返回第一版答案；复杂问题会标注缺口。';
    }
  };

  const providerLabel = (value) => {
    const option = Array.from(llmProvider?.options || []).find((item) => item.value === value);
    return option ? option.textContent.replace(/\s*\(Unavailable\)\s*/i, '').trim() : (value || 'Codex');
  };

  const formatSessionTime = (value) => {
    if (!value) return '';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  };

  const renderSessionList = () => {
    if (!sessionList) return;
    if (!sourceSessions.length) {
      sessionList.innerHTML = '<div class="source-qa-empty">No chats yet.</div>';
      return;
    }
    const visibleSessions = sessionHistoryExpanded ? sourceSessions : sourceSessions.slice(0, 5);
    sessionList.innerHTML = visibleSessions.map((item) => {
      const activeClass = item.id === activeSessionId ? ' is-active' : '';
      const scope = [item.pm_team || '', item.country || 'All', providerLabel(item.llm_provider)].filter(Boolean).join(' · ');
      const pending = pendingJobFromSession(item);
      const stateLabel = pending?.jobId ? ' · queued/running' : '';
      return `
        <div class="source-qa-session-row${activeClass}">
          <button class="source-qa-session-item" type="button" data-source-session-id="${escapeHtml(item.id)}">
            <span>${escapeHtml(item.title || 'New Source Code Chat')}</span>
            <small>${escapeHtml(scope)} · ${escapeHtml(formatSessionTime(item.updated_at))}${escapeHtml(stateLabel)}</small>
          </button>
          <button class="source-qa-session-archive" type="button" data-source-session-archive="${escapeHtml(item.id)}" aria-label="Archive chat">Archive</button>
        </div>
      `;
    }).join('') + (sourceSessions.length > 5 ? `
      <button class="source-qa-session-more" type="button" data-source-session-more>
        ${sessionHistoryExpanded ? 'Show less' : `Show more (${sourceSessions.length - 5})`}
      </button>
    ` : '');
    sessionList.querySelectorAll('[data-source-session-id]').forEach((button) => {
      button.addEventListener('click', () => loadSession(button.dataset.sourceSessionId || ''));
    });
    sessionList.querySelectorAll('[data-source-session-archive]').forEach((button) => {
      button.addEventListener('click', (event) => {
        event.stopPropagation();
        archiveSession(button.dataset.sourceSessionArchive || '');
      });
    });
    const moreButton = sessionList.querySelector('[data-source-session-more]');
    if (moreButton) {
      moreButton.addEventListener('click', () => {
        sessionHistoryExpanded = !sessionHistoryExpanded;
        renderSessionList();
      });
    }
  };

  const citationPattern = /\[((?:S\d+|[\w./-]+\.(?:java|xml|kt|groovy|md|sql|yml|yaml|properties|json|ts|tsx|js):\d+(?:-\d+)?)(?:\]\s*\[)?(?:[^\]]*)?)\]/g;
  const renderAnswerText = (text) => {
    const paragraphs = String(text || '')
      .split(/\n{2,}/)
      .map((paragraph) => paragraph.trim())
      .filter(Boolean);
    if (!paragraphs.length) return '<p>Answer completed.</p>';
    return paragraphs.map((paragraph) => {
      const lines = paragraph.split('\n').map((line) => line.trim()).filter(Boolean);
      if (lines.length && lines.every((line) => line.startsWith('- '))) {
        return `<ul class="source-qa-answer-list">${lines.map((line) => renderAnswerLine(line.slice(2), 'li')).join('')}</ul>`;
      }
      return renderAnswerLine(paragraph, 'p');
    }).join('');
  };
  const renderAnswerLine = (text, tagName) => {
    const citations = [];
    const cleanText = String(text || '').replace(citationPattern, (_match, citationText) => {
      citationText.split('] [').forEach((item) => {
        const value = item.trim();
        if (value) citations.push(value);
      });
      return '';
    }).replace(/\s{2,}/g, ' ').trim();
    const citationHtml = citations.length
      ? `<span class="source-qa-inline-citations">${citations.map((item) => `<span>${escapeHtml(item)}</span>`).join('')}</span>`
      : '';
    return `<${tagName}>${escapeHtml(cleanText)}${citationHtml}</${tagName}>`;
  };

  const renderSessionMessages = (session) => {
    if (!sessionMessages) return;
    const messages = [...(session?.messages || [])];
    if (pendingUserMessage?.text) {
      const alreadyPersisted = messages.some((message) =>
        message.role === 'user' && String(message.text || '').trim() === pendingUserMessage.text
      );
      if (!alreadyPersisted) {
        messages.push({
          role: 'user',
          text: pendingUserMessage.text,
          created_at: pendingUserMessage.created_at || '',
          attachments: pendingUserMessage.attachments || [],
          pending: true,
        });
      }
    }
    if (liveAssistantMessage?.text) {
      messages.push({
        role: 'assistant',
        text: liveAssistantMessage.text,
        title: liveAssistantMessage.title || 'Codex Live',
        created_at: liveAssistantMessage.created_at || '',
        live: true,
        stopped: Boolean(liveAssistantMessage.stopped),
        job_id: liveAssistantMessage.job_id || '',
        retry_question: liveAssistantMessage.retry_question || '',
        payload: {
          llm_provider: selectedLlmProvider(),
          llm_model: liveAssistantMessage.meta || 'streaming CLI output',
        },
      });
    }
    if (!messages.length) {
      sessionMessages.innerHTML = '<div class="source-qa-empty">Start a chat to build a reusable investigation context.</div>';
      return;
    }
    sessionMessages.innerHTML = messages.slice(-20).map((message) => {
      const payload = message.payload || {};
      const meta = message.role === 'assistant'
        ? [payload.llm_provider ? providerLabel(payload.llm_provider) : '', payload.llm_model || '', payload.trace_id ? `trace ${payload.trace_id}` : ''].filter(Boolean).join(' · ')
        : formatSessionTime(message.created_at);
      const text = message.role === 'assistant'
        ? (message.text || payload.llm_answer || payload.structured_answer?.direct_answer || payload.summary || 'Answer completed.')
        : message.text;
      const attachmentItems = Array.isArray(message.attachments) ? message.attachments : (Array.isArray(payload.attachments) ? payload.attachments : []);
      const citations = (payload.structured_answer?.citations || payload.matches || [])
        .slice(0, 4)
        .map((item) => typeof item === 'string' ? item : item.path)
        .filter(Boolean);
      const liveActions = message.live && message.job_id ? `
        <div class="source-qa-live-actions">
          <button class="button button-secondary" type="button" data-source-reconnect-job="${escapeHtml(message.job_id)}" data-source-retry-question="${escapeHtml(message.retry_question || '')}">Reconnect</button>
          ${message.retry_question ? `<button class="button button-secondary" type="button" data-source-retry-question="${escapeHtml(message.retry_question)}">Retry query</button>` : ''}
        </div>
      ` : '';
      return `
        <article class="source-qa-message source-qa-message-${escapeHtml(message.role || 'assistant')}${message.live ? ' is-live' : ''}">
          <div class="source-qa-message-head">
            <strong>
              ${message.live ? `${escapeHtml(message.title || 'Codex Live')} <em>${message.stopped ? 'Stopped' : 'Running - not final answer'}</em>` : (message.role === 'user' ? 'You' : 'Assistant')}
            </strong>
            <span>${escapeHtml(meta)}</span>
          </div>
          <div class="source-qa-message-body">
            ${message.live ? `<pre>${escapeHtml(text)}</pre>` : (message.role === 'assistant' ? renderReadableAnswerBody(payload, text) : `<p>${escapeHtml(text)}</p>`)}
            ${renderAttachmentChips(attachmentItems)}
            ${liveActions}
          </div>
          ${citations.length ? `<div class="source-qa-message-citations">${citations.map((item) => `<span>${escapeHtml(item)}</span>`).join('')}</div>` : ''}
        </article>
      `;
    }).join('');
    sessionMessages.scrollTop = sessionMessages.scrollHeight;
  };

  const renderOptimisticUserMessage = (question, attachments = []) => {
    if (!sessionMessages) return;
    const text = String(question || '').trim();
    if (!text) return;
    pendingUserMessage = {
      text,
      created_at: new Date().toISOString(),
      attachments: attachments.map((item) => ({ ...item })),
    };
    renderSessionMessages(activeSession);
  };

  const pendingJobFromSession = (session) => {
    const messages = Array.isArray(session?.messages) ? session.messages : [];
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      const message = messages[index];
      const jobId = String(message?.pending_job_id || '').trim();
      if (message?.pending && jobId) {
        return {
          jobId,
          question: String(message.text || '').trim(),
          attachments: Array.isArray(message.attachments) ? message.attachments : [],
        };
      }
    }
    return null;
  };

  const applyActiveSession = (session) => {
    activeSession = session || null;
    activeSessionId = session?.id || '';
    if (sessionTitle) sessionTitle.textContent = session?.title || 'New Source Code Chat';
    if (sessionContext) {
      sessionContext.hidden = true;
      sessionContext.textContent = '';
    }
    if (sessionProvider) sessionProvider.textContent = providerLabel(session?.llm_provider || selectedLlmProvider());
    if (sessionScope) sessionScope.textContent = [session?.pm_team || pmTeam.value, session?.country || currentCountry()].filter(Boolean).join(' · ');
    if (session?.last_context && typeof session.last_context === 'object') {
      conversationContext = session.last_context;
    } else if (!session) {
      conversationContext = null;
    }
    renderSessionMessages(session);
    renderSessionList();
  };

  const loadSessions = async () => {
    if (!sessionsUrl) return;
    try {
      const payload = await fetch(sessionsUrl).then(readJson);
      sourceSessions = payload.sessions || [];
      if (activeSessionId && sourceSessions.some((item) => item.id === activeSessionId)) {
        renderSessionList();
        return;
      }
      if (!activeSessionId && sourceSessions.length) {
        await loadSession(sourceSessions[0].id);
        return;
      }
      renderSessionList();
    } catch (_error) {
      if (sessionList) sessionList.innerHTML = '<div class="source-qa-empty">Chat history could not be loaded.</div>';
    }
  };

  const loadSession = async (sessionId, options = {}) => {
    if (!sessionsUrl || !sessionId) return null;
    try {
      const payload = await fetch(`${sessionsUrl}/${encodeURIComponent(sessionId)}`).then(readJson);
      if (!options.preserveLive) liveAssistantMessage = null;
      if (!options.preservePending) pendingUserMessage = null;
      applyActiveSession(payload.session || null);
      if (!options.skipJobResume) {
        resumePendingJobFromSession(payload.session || null);
      }
      return payload.session || null;
    } catch (error) {
      if (queryStatus) queryStatus.textContent = error.message || 'Session could not be loaded.';
      return null;
    }
  };

  const archiveSession = async (sessionId) => {
    if (!sessionsUrl || !sessionId) return;
    try {
      await apiFetchJson(`${sessionsUrl}/${encodeURIComponent(sessionId)}/archive`, { method: 'POST' });
      sourceSessions = sourceSessions.filter((item) => item.id !== sessionId);
      if (activeSessionId === sessionId) {
        liveAssistantMessage = null;
        pendingUserMessage = null;
        activeSessionId = '';
        activeSession = null;
        conversationContext = null;
        if (sourceSessions.length) {
          await loadSession(sourceSessions[0].id);
        } else {
          applyActiveSession(null);
        }
      } else {
        renderSessionList();
      }
    } catch (error) {
      if (queryStatus) queryStatus.textContent = error.message || 'Chat could not be archived.';
    }
  };

  const createSession = async (options = {}) => {
    if (!sessionsUrl) return null;
    const payload = await fetch(sessionsUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        pm_team: pmTeam.value,
        country: currentCountry(),
        llm_provider: selectedLlmProvider(),
      }),
    }).then(readJson);
    const session = payload.session || null;
    if (session) {
      sourceSessions = [session, ...sourceSessions.filter((item) => item.id !== session.id)].slice(0, 30);
      conversationContext = null;
      if (!options.preserveLive) liveAssistantMessage = null;
      if (!options.preservePending) pendingUserMessage = null;
      applyActiveSession(session);
    }
    return session;
  };

  const ensureActiveSession = async (options = {}) => {
    if (activeSessionId) return activeSession;
    return createSession(options);
  };

  const updateCountryVisibility = () => {
    if (!country) return;
    const team = pmTeam?.value || 'AF';
    const countries = runtimeCountries();
    const allValue = allCountryValue();
    const previous = country.value || '';
    const allowedCountries = isCountrySpecificTeam(team) ? countries : [allValue, ...countries];
    const nextValue = allowedCountries.includes(previous)
      ? previous
      : (isCountrySpecificTeam(team) ? countries[0] || 'SG' : allValue);
    country.innerHTML = '';
    allowedCountries.forEach((countryCode) => {
      const option = document.createElement('option');
      option.value = countryCode;
      option.textContent = countryCode === allValue
        ? `${allValue} · ${capabilityLabel(team, countryCode)}`
        : `${countryCode} · ${capabilityLabel(team, countryCode)}`;
      country.appendChild(option);
    });
    country.value = nextValue;
    country.disabled = false;
    countryWrap?.classList.remove('source-qa-country-disabled');
    if (countryHelp) countryHelp.textContent = runtimeContextHelp();
  };

  const updateQueryButtonState = (running = Boolean(activeQueryControl && !activeQueryControl.stopped)) => {
    if (!queryButton) return;
    queryButton.textContent = running ? 'Stop' : 'Send';
    queryButton.classList.toggle('is-stopping', running);
    queryButton.setAttribute('aria-label', running ? 'Stop current Codex run' : 'Send question');
    queryButton.disabled = !running && activeAttachmentUploads > 0;
  };

  const stopActiveQuery = () => {
    if (!activeQueryControl || activeQueryControl.stopped) return;
    activeQueryControl.stopped = true;
    stopQueryProgress();
    renderLiveAnswer('Stopped by user. This run is no longer updating in the chat.', { title: 'Codex Live', meta: 'stopped', stopped: true });
    updateQueryButtonState(false);
    if (queryStatus) queryStatus.textContent = 'Stopped.';
  };

  const updateAnswerModeState = () => {
    const answerModeValue = answerMode?.value || 'auto';
    const providerReady = selectedProviderReady();
    const providerLabel = llmProvider?.selectedOptions?.[0]?.textContent || llmPolicy.provider?.provider || 'LLM';
    updateQueryButtonState();
    if (queryStatus) {
      queryStatus.textContent = providerReady
        ? `Ready. Smart Answer will search first and call ${providerLabel}.`
        : `${providerLabel} is unavailable.`;
    }
  };

  const renderStatus = (items = []) => {
    if (!repoStatus) return;
    if (!items.length) {
      repoStatus.innerHTML = '<div class="source-qa-empty">No repositories configured for this selection.</div>';
      return;
    }
    repoStatus.innerHTML = items.map((item) => `
      <div class="source-qa-repo-card source-qa-repo-${escapeHtml(item.state)}">
        <strong>${escapeHtml(item.display_name)}</strong>
        <span>${escapeHtml(item.state)} · ${escapeHtml(item.message)}</span>
        <code>${escapeHtml(item.path || item.url)}</code>
      </div>
    `).join('');
  };

  const compactNumber = (value) => {
    const number = Number(value || 0);
    return Number.isFinite(number) ? number.toLocaleString() : '0';
  };

  const renderIndexHealth = (health = {}) => {
    if (!indexHealth) return;
    const totals = health.totals || {};
    const keyHealth = health.keys?.[currentKey()] || {};
    const staleRepos = (keyHealth.freshness?.stale_repos || []).slice(0, 4);
    const state = health.status || 'unknown';
    indexHealth.innerHTML = `
      <div class="source-qa-health-head">
        <strong>Index ${escapeHtml(state)}</strong>
        <span>${compactNumber(totals.ready)} / ${compactNumber(totals.repos)} repos ready</span>
      </div>
      <div class="source-qa-health-metrics">
        <span>${compactNumber(totals.files)} files</span>
        <span>${compactNumber(totals.lines)} lines</span>
        <span>${compactNumber(totals.definitions)} definitions</span>
        <span>${compactNumber(totals.semantic_chunks)} chunks</span>
      </div>
      <p>${escapeHtml(keyHealth.freshness?.warning || (health.newest_indexed_at ? `Newest index: ${health.newest_indexed_at}` : 'No synced index yet.'))}</p>
      ${staleRepos.length ? `<ul>${staleRepos.map((repo) => `<li>${escapeHtml(repo)}</li>`).join('')}</ul>` : ''}
    `;
  };

  const renderModelAvailability = () => {
    if (!modelAvailability) return;
    modelAvailability.querySelectorAll('input[type="checkbox"]').forEach((input) => {
      input.checked = Boolean(modelAvailabilityPayload[input.value]);
    });
  };

  const updateLlmProviderOptions = (providerOptions = []) => {
    if (!llmProvider || !Array.isArray(providerOptions) || !providerOptions.length) return;
    const previousValue = llmProvider.value;
    llmProvider.innerHTML = providerOptions.map((provider) => `
      <option value="${escapeHtml(provider.value)}" ${provider.disabled ? 'disabled' : ''}>${escapeHtml(provider.label)}</option>
    `).join('');
    if (providerOptionIsEnabled(previousValue)) {
      llmProvider.value = previousValue;
    } else {
      llmProvider.value = defaultLlmProvider();
    }
    updateAnswerModeState();
    if (sessionProvider) sessionProvider.textContent = providerLabel(selectedLlmProvider());
  };

  const renderSelectedConfig = () => {
    if (configLoadState !== 'loaded') {
      const loading = configLoadState === 'loading' || configLoadState === 'idle';
      if (configStatus) {
        configStatus.textContent = loading
          ? `Loading repository configuration for ${currentKey()}...`
          : 'Repository config could not be loaded. Refresh the page or try again.';
      }
      if (reposInput) {
        reposInput.value = loading ? 'Loading repository mapping...' : '';
        reposInput.disabled = loading;
      }
      if (saveButton) saveButton.disabled = true;
      if (syncButton) syncButton.disabled = true;
      if (repoStatus) {
        repoStatus.innerHTML = `<div class="source-qa-empty">${loading ? 'Repository status is loading.' : 'Repository status is unavailable until config loads.'}</div>`;
      }
      return;
    }
    const entries = config.mappings?.[currentKey()] || [];
    const count = entries.length;
    const runtimeScopeNote = runtimeContextHelp();
    configStatus.textContent = count
      ? `${count} repositories configured for ${currentKey()}. ${runtimeScopeNote}`
      : `No repositories configured for ${currentKey()} yet. ${runtimeScopeNote}`;
    if (reposInput) {
      reposInput.value = entries.map((entry) => `${entry.display_name} | ${entry.url}`).join('\n');
      reposInput.disabled = false;
    }
    if (saveButton) saveButton.disabled = false;
    if (syncButton) syncButton.disabled = !gitAuthReady;
    renderStatus(entries.map((entry) => ({
      ...entry,
      state: 'configured',
      message: 'Configured. Sync status will update after refresh.',
      path: entry.url,
    })));
    renderIndexHealth(indexHealthPayload);
  };

  const parseRepoLines = () => {
    return String(reposInput?.value || '')
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => {
        const parts = line.split('|').map((part) => part.trim()).filter(Boolean);
        if (parts.length >= 2) {
          return { display_name: parts[0], url: parts.slice(1).join('|').trim() };
        }
        return { display_name: '', url: line };
      });
  };

  const loadConfig = async () => {
    configLoadState = 'loading';
    renderSelectedConfig();
    try {
      const payload = await apiFetchJson(configUrl, {}, { attempts: 5 });
      config = payload.config || { mappings: {} };
      configLoadState = 'loaded';
      gitAuthReady = Boolean(payload.git_auth_ready);
      llmReady = Boolean(payload.llm_ready);
      llmProviders = payload.llm_providers || {};
      modelAvailabilityPayload = payload.model_availability || {};
      llmPolicy = payload.llm_policy || {};
      indexHealthPayload = payload.index_health || {};
      if (payload.options) {
        options.countries = payload.options.countries || options.countries;
        options.all_country = payload.options.all_country || options.all_country;
        options.runtime_capabilities = payload.options.runtime_capabilities || options.runtime_capabilities || {};
      }
      updateCountryVisibility();
      updateLlmProviderOptions(payload.options?.llm_providers || []);
      renderModelAvailability();
      if (!gitAuthReady && adminStatus) {
        adminStatus.textContent = 'Set SOURCE_CODE_QA_GITLAB_TOKEN on the server before running Sync / Refresh.';
      } else if (adminStatus && llmPolicy.provider) {
        const provider = llmPolicy.provider.provider || payload.llm_provider || 'llm';
        const routerVersion = llmPolicy.router?.version ? ` · router v${llmPolicy.router.version}` : '';
        adminStatus.textContent = `LLM provider: ${provider}${routerVersion}.`;
      }
      updateAnswerModeState();
      renderSelectedConfig();
      renderIndexHealth(indexHealthPayload);
    } catch (error) {
      configLoadState = 'error';
      configStatus.textContent = error.message || 'Repository config could not be loaded.';
      if (adminStatus) adminStatus.textContent = error.message || 'Repository config could not be loaded.';
      renderSelectedConfig();
    }
  };

  const saveConfig = async () => {
    if (!canManage) return;
    if (configLoadState !== 'loaded') {
      adminStatus.textContent = 'Repository configuration is still loading. Please wait before saving.';
      return;
    }
    adminStatus.textContent = 'Saving repository mapping...';
    try {
      const payload = await apiFetchJson(saveUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pm_team: pmTeam.value,
          country: currentRepositoryCountry(),
          repositories: parseRepoLines(),
        }),
      }, { attempts: 3 });
      config = payload.config || config;
      adminStatus.textContent = `Saved ${payload.repositories?.length || 0} repositories for ${payload.key}.`;
      renderSelectedConfig();
    } catch (error) {
      adminStatus.textContent = error.message || 'Save failed.';
    }
  };

  const collectModelAvailability = () => {
    const availability = {};
    modelAvailability?.querySelectorAll('input[type="checkbox"]').forEach((input) => {
      availability[input.value] = input.checked;
    });
    return availability;
  };

  const saveModelAvailability = async () => {
    if (!canManage || !modelAvailabilityUrl) return;
    if (modelAvailabilityStatus) modelAvailabilityStatus.textContent = 'Saving model availability...';
    try {
      const payload = await apiFetchJson(modelAvailabilityUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ availability: collectModelAvailability() }),
      }, { attempts: 3 });
      modelAvailabilityPayload = payload.model_availability || {};
      updateLlmProviderOptions(payload.options?.llm_providers || []);
      renderModelAvailability();
      if (modelAvailabilityStatus) modelAvailabilityStatus.textContent = 'Model availability saved.';
    } catch (error) {
      if (modelAvailabilityStatus) modelAvailabilityStatus.textContent = error.message || 'Save failed.';
    }
  };

  const runtimeEvidenceScope = () => ({
    pm_team: runtimeEvidencePmTeam?.value || 'AF',
    country: runtimeEvidenceCountry?.value || 'SG',
  });

  const runtimeEvidenceTypeLabel = (sourceType) => ({
    apollo: 'Apollo UAT reference',
    db: 'DB',
    other: 'Other',
  }[sourceType] || sourceType || 'Runtime');

  const renderRuntimeEvidence = (items = []) => {
    if (!runtimeEvidenceList) return;
    if (!items.length) {
      runtimeEvidenceList.innerHTML = '<div class="source-qa-empty">No runtime evidence loaded for this scope.</div>';
      return;
    }
    runtimeEvidenceList.innerHTML = items.map((item) => `
      <div class="source-qa-runtime-item">
        <div>
          <strong>${escapeHtml(item.filename || 'runtime evidence')}</strong>
          <small>${escapeHtml(runtimeEvidenceTypeLabel(item.source_type))} · ${escapeHtml(item.pm_team || '')}:${escapeHtml(item.country || '')} · ${escapeHtml(formatAttachmentSize(item.size))}</small>
        </div>
        <div class="source-qa-runtime-meta">
          <span>${escapeHtml((item.created_at || '').replace('T', ' ').replace('Z', ''))}</span>
          <button class="button button-secondary" type="button" data-source-runtime-delete="${escapeHtml(item.id || '')}">Delete</button>
        </div>
      </div>
    `).join('');
    runtimeEvidenceList.querySelectorAll('[data-source-runtime-delete]').forEach((button) => {
      button.addEventListener('click', () => deleteRuntimeEvidence(button.dataset.sourceRuntimeDelete || ''));
    });
  };

  const loadRuntimeEvidence = async () => {
    if (!canManage || !runtimeEvidenceUrl || !runtimeEvidenceList) return;
    const scope = runtimeEvidenceScope();
    if (runtimeEvidenceStatus) runtimeEvidenceStatus.textContent = `Loading runtime evidence for ${scope.pm_team}:${scope.country}...`;
    try {
      const url = new URL(runtimeEvidenceUrl, window.location.origin);
      url.searchParams.set('pm_team', scope.pm_team);
      url.searchParams.set('country', scope.country);
      const payload = await apiFetchJson(url.toString(), {}, { attempts: 3 });
      renderRuntimeEvidence(payload.evidence || []);
      if (runtimeEvidenceStatus) runtimeEvidenceStatus.textContent = `Loaded ${(payload.evidence || []).length} runtime evidence file(s) for ${scope.pm_team}:${scope.country}.`;
    } catch (error) {
      renderRuntimeEvidence([]);
      if (runtimeEvidenceStatus) runtimeEvidenceStatus.textContent = error.message || 'Runtime evidence could not be loaded.';
    }
  };

  const uploadRuntimeEvidence = async (file) => {
    if (!canManage || !runtimeEvidenceUrl || !file) return;
    const scope = runtimeEvidenceScope();
    const formData = new FormData();
    formData.append('pm_team', scope.pm_team);
    formData.append('country', scope.country);
    formData.append('source_type', runtimeEvidenceSourceType?.value || 'other');
    formData.append('file', file);
    if (runtimeEvidenceStatus) runtimeEvidenceStatus.textContent = `Uploading ${file.name || 'runtime evidence'}...`;
    try {
      const payload = await apiFetchJson(runtimeEvidenceUrl, {
        method: 'POST',
        body: formData,
      }, { attempts: 3 });
      renderRuntimeEvidence(payload.items || (payload.evidence ? [payload.evidence] : []));
      if (runtimeEvidenceStatus) runtimeEvidenceStatus.textContent = `Uploaded ${payload.evidence?.filename || file.name || 'runtime evidence'} for ${scope.pm_team}:${scope.country}.`;
    } catch (error) {
      if (runtimeEvidenceStatus) runtimeEvidenceStatus.textContent = error.message || 'Upload failed.';
    }
  };

  const deleteRuntimeEvidence = async (evidenceId) => {
    if (!canManage || !runtimeEvidenceUrl || !evidenceId) return;
    const scope = runtimeEvidenceScope();
    const url = new URL(`${runtimeEvidenceUrl}/${encodeURIComponent(evidenceId)}`, window.location.origin);
    url.searchParams.set('pm_team', scope.pm_team);
    url.searchParams.set('country', scope.country);
    if (runtimeEvidenceStatus) runtimeEvidenceStatus.textContent = 'Deleting runtime evidence...';
    try {
      const payload = await apiFetchJson(url.toString(), { method: 'DELETE' }, { attempts: 3 });
      renderRuntimeEvidence(payload.evidence || []);
      if (runtimeEvidenceStatus) runtimeEvidenceStatus.textContent = payload.deleted ? 'Runtime evidence deleted.' : 'Runtime evidence was already removed.';
    } catch (error) {
      if (runtimeEvidenceStatus) runtimeEvidenceStatus.textContent = error.message || 'Delete failed.';
    }
  };

  const pollSyncJob = async (jobId) => {
    while (jobId) {
      const payload = await readJobStatus(jobId);
      adminStatus.textContent = payload.message || 'Syncing repositories...';
      if (payload.state === 'completed') {
        const result = (payload.results || [])[0] || {};
        renderStatus(result.repo_status || result.results || []);
        adminStatus.textContent = result.status === 'ok'
          ? 'Sync completed.'
          : (payload.notice?.summary || 'Sync completed with issues. Check status cards.');
        return result;
      }
      if (payload.state === 'failed') {
        throw new Error(payload.error || payload.message || 'Sync failed.');
      }
      await sleep(900);
    }
    return {};
  };

  const syncRepos = async () => {
    if (!canManage) return;
    if (configLoadState !== 'loaded') {
      adminStatus.textContent = 'Repository configuration is still loading. Please wait before syncing.';
      return;
    }
    if (!gitAuthReady) {
      adminStatus.textContent = 'SOURCE_CODE_QA_GITLAB_TOKEN is missing on the server.';
      return;
    }
    const syncScope = currentKey();
    adminStatus.textContent = `Syncing ${syncScope}. This can take a minute on first clone...`;
    if (syncButton) syncButton.disabled = true;
    try {
      const payload = await apiFetchJson(syncUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ pm_team: pmTeam.value, country: currentRepositoryCountry() }),
      }, { attempts: 5, delayMs: 700 });
      if (payload.status === 'queued' && payload.job_id) {
        await pollSyncJob(payload.job_id);
        await loadConfig();
        return;
      }
      renderStatus(payload.repo_status || payload.results || []);
      await loadConfig();
      adminStatus.textContent = payload.status === 'ok'
        ? 'Sync completed.'
        : (payload.message || 'Sync completed with issues. Check status cards.');
    } catch (error) {
      adminStatus.textContent = error.message || 'Sync failed.';
    } finally {
      if (syncButton) syncButton.disabled = false;
    }
  };

  const renderMatches = (matches = [], options = {}) => {
    if (!results) return;
    if (!matches.length) {
      results.innerHTML = '<div class="source-qa-empty">No confident code match for this question.</div>';
      return;
    }
    const body = matches.map((match, index) => `
      <article class="source-qa-match">
        <div class="source-qa-match-head">
          <strong>[S${index + 1}] ${escapeHtml(match.repo)} · ${escapeHtml(match.path)}</strong>
          <span>Lines ${escapeHtml(match.line_start)}-${escapeHtml(match.line_end)} · score ${escapeHtml(match.score)} · ${escapeHtml(match.retrieval || 'file_scan')}</span>
        </div>
        <p>${escapeHtml(match.reason)}</p>
        <pre><code>${escapeHtml(match.snippet)}</code></pre>
      </article>
    `).join('');
    if (options.compact) {
      const openAttr = options.open ? ' open' : '';
      const summary = options.open
        ? `Showing ${matches.length} code references because the answer needs evidence review`
        : `Show ${matches.length} code references used for this answer`;
      results.innerHTML = `
        <details class="source-qa-evidence"${openAttr}>
          <summary>${escapeHtml(summary)}</summary>
          ${body}
        </details>
      `;
      return;
    }
    results.innerHTML = body;
  };

  const buildConversationContext = (payload, questionOverride = '') => ({
    key: currentKey(),
    pm_team: pmTeam.value,
    country: currentCountry(),
    question: questionOverride || questionInput.value,
    trace_id: payload?.trace_id || '',
    summary: payload?.summary || '',
    answer: payload?.llm_answer || '',
    rendered_answer: payload?.llm_answer || '',
    attachments: (payload?.attachments || []).slice(0, 5),
    llm_provider: payload?.llm_provider || '',
    llm_model: payload?.llm_model || '',
    llm_route: payload?.llm_route || {},
    codex_cli_summary: payload?.codex_cli_summary || {},
    codex_citation_validation: payload?.answer_claim_check?.codex_citation_validation || {},
    codex_candidate_paths: (payload?.llm_route?.candidate_paths || []).slice(0, 30),
    repo_scope: Array.from(new Set((payload?.matches || []).map((match) => match.repo).filter(Boolean))).slice(0, 8),
    matches: (payload?.matches || []).slice(0, 8).map((match) => ({
      path: match.path,
      snippet: match.snippet,
      repo: match.repo,
      reason: match.reason,
      retrieval: match.retrieval,
      trace_stage: match.trace_stage,
      line_start: match.line_start,
      line_end: match.line_end,
      score: match.score,
    })),
    matches_snapshot: (payload?.matches || []).slice(0, 10).map((match) => ({
      path: match.path,
      repo: match.repo,
      reason: match.reason,
      retrieval: match.retrieval,
      trace_stage: match.trace_stage,
      line_start: match.line_start,
      line_end: match.line_end,
      score: match.score,
    })),
    trace_paths: (payload?.trace_paths || []).slice(0, 5),
    structured_answer: payload?.structured_answer || {},
    answer_contract: payload?.answer_contract || {},
    evidence_pack: payload?.evidence_pack || {},
    answer_quality: payload?.answer_quality || {},
  });

  const shouldOpenEvidence = (payload) => {
    const quality = payload?.answer_quality || {};
    const contract = payload?.answer_contract || {};
    return quality.status === 'needs_more_trace'
      || quality.confidence === 'low'
      || contract.status === 'blocked_missing_source'
      || Boolean((contract.missing_links || []).length);
  };

  const renderDebugTrace = (payload) => {
    if (!debugTrace) return;
    if (!payload || payload.status !== 'ok') {
      debugTrace.hidden = true;
      debugTrace.innerHTML = '';
      return;
    }
    const quality = payload.answer_quality || {};
    const queryComponents = (payload.query_plan?.components || [])
      .map((component) => `<li>${escapeHtml(component.name)}: ${escapeHtml((component.terms || []).slice(0, 8).join(', '))}</li>`)
      .join('');
    const tracePaths = (payload.trace_paths || [])
      .slice(0, 5)
      .map((path) => `<li>${escapeHtml(path.repo)}: ${escapeHtml((path.edges || []).map((edge) => `${edge.edge_kind}:${edge.to_name || edge.to_file}`).join(' -> '))}</li>`)
      .join('');
    const repoEdges = (payload.repo_graph?.edges || [])
      .slice(0, 8)
      .map((edge) => {
        const confidence = Number.isFinite(Number(edge.confidence)) ? ` · ${(Number(edge.confidence) * 100).toFixed(0)}%` : '';
        const reason = edge.match_reason ? ` · ${edge.match_reason}` : '';
        return `<li>${escapeHtml(edge.from_repo)} -> ${escapeHtml(edge.to_repo)} · ${escapeHtml(edge.edge_kind)}${escapeHtml(confidence)}${escapeHtml(reason)} · ${escapeHtml(edge.evidence)}</li>`;
      })
      .join('');
    const contract = payload.answer_contract || {};
    const confirmedSources = (contract.confirmed_sources || [])
      .slice(0, 6)
      .map((item) => `<li>${escapeHtml(item)}</li>`)
      .join('');
    const missingLinks = (contract.missing_links || [])
      .slice(0, 6)
      .map((item) => `<li>${escapeHtml(item)}</li>`)
      .join('');
    const judge = payload.answer_judge || {};
    const judgeIssues = (judge.issues || [])
      .slice(0, 5)
      .map((item) => `<li>${escapeHtml(item)}</li>`)
      .join('');
    const toolTrace = (payload.tool_trace || [])
      .slice(0, 8)
      .map((step) => {
        const label = [
          step.phase || 'trace',
          step.tool || 'tool',
          step.step || '',
        ].filter(Boolean).join(' · ');
        const counts = [
          Number.isFinite(Number(step.matches_found)) ? `found ${step.matches_found}` : '',
          Number.isFinite(Number(step.matches_added)) ? `added ${step.matches_added}` : '',
        ].filter(Boolean).join(', ');
        const terms = (step.terms || []).slice(0, 5).join(', ');
        return `<li>${escapeHtml(label)}${counts ? ` (${escapeHtml(counts)})` : ''}${terms ? `: ${escapeHtml(terms)}` : ''}</li>`;
      })
      .join('');
    debugTrace.hidden = false;
    debugTrace.innerHTML = `
      <details class="source-qa-evidence">
        <summary>Why this answer?</summary>
        <div class="source-qa-debug-grid">
          <section>
            <strong>Quality</strong>
            <p>${escapeHtml(quality.status || 'unknown')} · ${escapeHtml(quality.confidence || 'unknown')} · missing: ${escapeHtml((quality.missing || []).join(', ') || 'none')}</p>
          </section>
          <section>
            <strong>Query plan</strong>
            <ul>${queryComponents || '<li>No decomposition terms.</li>'}</ul>
          </section>
          <section>
            <strong>Trace paths</strong>
            <ul>${tracePaths || '<li>No trace path found.</li>'}</ul>
          </section>
          <section>
            <strong>Repo graph</strong>
            <ul>${repoEdges || '<li>No cross-repo edge found.</li>'}</ul>
          </section>
          <section>
            <strong>Evidence boundary</strong>
            <ul>${confirmedSources || missingLinks || '<li>No answer contract boundary.</li>'}</ul>
          </section>
          <section>
            <strong>Missing links</strong>
            <ul>${missingLinks || '<li>No missing link reported.</li>'}</ul>
          </section>
          <section>
            <strong>Answer judge</strong>
            <p>${escapeHtml(judge.mode || 'deterministic_evidence_judge')} · ${escapeHtml(judge.status || 'unknown')} · checked: ${escapeHtml(String(judge.checked_items ?? 'n/a'))}</p>
            <ul>${judgeIssues || '<li>No judge issue reported.</li>'}</ul>
          </section>
          <section>
            <strong>Investigation tools</strong>
            <ul>${toolTrace || '<li>No planner tool step recorded.</li>'}</ul>
          </section>
        </div>
      </details>
    `;
  };

  const renderEvidenceSummary = (payload) => {
    if (!evidenceSummary) return;
    if (!payload || payload.status !== 'ok') {
      evidenceSummary.hidden = true;
      evidenceSummary.innerHTML = '';
      return;
    }
    const pack = payload.evidence_pack || {};
    const quality = payload.answer_quality || {};
    const contract = payload.answer_contract || {};
    const confirmed = [
      ...(pack.confirmed_facts || []),
      ...(contract.confirmed_sources || []),
    ].filter(Boolean).slice(0, 5);
    const inferred = [
      ...(pack.inferred_facts || []),
      ...(contract.data_carriers || []),
      ...(contract.field_population || []),
    ].filter(Boolean).slice(0, 5);
    const missing = [
      ...(pack.missing_facts || []),
      ...(pack.evidence_limits || []),
      ...(contract.missing_links || []),
      ...(quality.missing || []),
    ].filter(Boolean).slice(0, 5);
    const policies = (quality.policies || [])
      .slice(0, 4)
      .map((policy) => `${policy.name}: ${policy.status}`);
    const freshness = payload.index_freshness?.status
      ? `Index: ${payload.index_freshness.status}`
      : '';
    const confidence = quality.confidence ? `Confidence: ${quality.confidence}` : '';
    const meta = [quality.status ? `Quality: ${quality.status}` : '', confidence, freshness, ...policies]
      .filter(Boolean)
      .join(' · ');
    if (!confirmed.length && !inferred.length && !missing.length && !meta) {
      evidenceSummary.hidden = true;
      evidenceSummary.innerHTML = '';
      return;
    }
    const list = (title, items, emptyText) => `
      <section>
        <strong>${escapeHtml(title)}</strong>
        <ul>${items.length ? items.map((item) => `<li>${escapeHtml(item)}</li>`).join('') : `<li>${escapeHtml(emptyText)}</li>`}</ul>
      </section>
    `;
    evidenceSummary.hidden = false;
    evidenceSummary.innerHTML = `
      <div class="source-qa-evidence-card">
        <div class="source-qa-evidence-head">
          <strong>Evidence Boundary</strong>
          <span>${escapeHtml(meta || 'Evidence summary')}</span>
        </div>
        <div class="source-qa-evidence-grid">
          ${list('Confirmed', confirmed, 'No confirmed source facts extracted yet.')}
          ${list('Inferred', inferred, 'No inferred carrier or call-chain facts extracted.')}
          ${list('Missing', missing, 'No missing evidence reported.')}
        </div>
      </div>
    `;
  };

  const renderInlineCitationList = (citations = []) => {
    const values = (citations || [])
      .map((item) => normalizeCitation(item))
      .filter(Boolean)
      .slice(0, 6);
    if (!values.length) return '';
    return `<span class="source-qa-citations" aria-label="Citations">${values.map((item) => `<span>${escapeHtml(item)}</span>`).join('')}</span>`;
  };

  const normalizeCitation = (value) => {
    const raw = String(value || '').trim();
    if (!raw) return '';
    const match = raw.match(/S?\s*(\d+)/i);
    return match ? `S${match[1]}` : raw.replace(/^\[|\]$/g, '');
  };

  const stripCitationTags = (text) => String(text || '')
    .replace(/\s*\[S\d+\]/gi, '')
    .replace(/\s{2,}/g, ' ')
    .trim();

  const extractCitationTags = (text) => {
    const tags = [];
    String(text || '').replace(/\[S(\d+)\]/gi, (_match, id) => {
      const tag = `S${id}`;
      if (!tags.includes(tag)) tags.push(tag);
      return '';
    });
    return tags;
  };

  const isCitationOnlyText = (text) => {
    const normalized = String(text || '').trim();
    return Boolean(normalized) && /^(?:\[?S\d+\]?\s*)+$/i.test(normalized);
  };

  const cleanDirectAnswer = (directAnswer, fallbackAnswer, hasStructuredSections) => {
    let text = String(directAnswer || fallbackAnswer || '').trim();
    if (!text) return '';
    if (hasStructuredSections) {
      text = text.split(/\n\s*(?:[-*]\s+|Missing evidence:)/i)[0].trim();
      text = text.split(/\s+-\s+(?=(?:The|If|Missing evidence|Specific details|Specific API|Confirmed|Missing)\b)/i)[0].trim();
    }
    return stripCitationTags(text);
  };

  const normalizedClaimsForDisplay = (claims = []) => {
    const seen = new Set();
    const items = [];
    for (const claim of claims || []) {
      const rawText = typeof claim === 'string' ? claim : claim?.text;
      if (isCitationOnlyText(rawText)) continue;
      const text = stripCitationTags(rawText);
      if (!text || seen.has(text.toLowerCase())) continue;
      const citationTags = [
        ...(Array.isArray(claim?.citations) ? claim.citations : []),
        ...extractCitationTags(rawText),
      ];
      items.push({ text, citations: Array.from(new Set(citationTags.map(normalizeCitation).filter(Boolean))) });
      seen.add(text.toLowerCase());
      if (items.length >= 8) break;
    }
    return items;
  };

  const renderRawModelAnswer = (payload, answer, directAnswer, hasStructuredSections) => {
    if (payload?.deadline_hit && payload?.fallback_used) return '';
    const rawAnswer = String(payload?.llm_answer || answer || '').trim();
    if (!hasStructuredSections || !rawAnswer) return '';
    const normalizedDirect = String(directAnswer || '').trim();
    const shouldShow = rawAnswer.length > normalizedDirect.length + 80 || /\n\s*(?:Conclusion|Evidence|Source-code Evidence|Missing|Next Checks|Confidence)\b/i.test(rawAnswer);
    if (!shouldShow) return '';
    const provider = String(payload?.llm_provider || '').toLowerCase();
    const label = provider.includes('codex') ? 'Raw Codex Answer' : 'Raw Model Answer';
    return `
      <details class="source-qa-raw-answer">
        <summary>${escapeHtml(label)}</summary>
        <pre>${escapeHtml(rawAnswer)}</pre>
      </details>
    `;
  };

  const renderReadableAnswerBody = (payload, answer) => {
    const structured = payload?.structured_answer || {};
    const claims = normalizedClaimsForDisplay(Array.isArray(structured.claims) ? structured.claims : []);
    const missing = (Array.isArray(structured.missing_evidence) ? structured.missing_evidence : [])
      .map((item) => String(item || '').trim())
      .filter(Boolean)
      .slice(0, 6);
    const confidence = String(structured.confidence || payload?.answer_contract?.confidence || '').trim();
    const directAnswer = cleanDirectAnswer(structured.direct_answer, answer, Boolean(claims.length || missing.length));
    if (directAnswer || claims.length || missing.length) {
      return `
        <section class="source-qa-answer-section source-qa-answer-direct">
          <strong>Answer</strong>
          ${directAnswer ? `<p>${escapeHtml(directAnswer)}</p>` : '<p>No direct answer returned.</p>'}
        </section>
        ${claims.length ? `
          <section class="source-qa-answer-section">
            <strong>Evidence</strong>
            <div class="source-qa-claim-list">
              ${claims.map((claim) => `
                <div class="source-qa-claim-item">
                  <p>${escapeHtml(claim.text)}</p>
                  ${renderInlineCitationList(claim.citations || [])}
                </div>
              `).join('')}
            </div>
          </section>
        ` : ''}
        ${missing.length ? `
          <section class="source-qa-answer-section source-qa-answer-missing">
            <strong>Missing Evidence</strong>
            <ul>${missing.slice(0, 6).map((item) => `<li>${escapeHtml(item)}</li>`).join('')}</ul>
          </section>
        ` : ''}
        ${confidence ? `
          <section class="source-qa-answer-section">
            <strong>Confidence</strong>
            <p>${escapeHtml(confidence)}</p>
          </section>
        ` : ''}
        ${renderRawModelAnswer(payload, answer, directAnswer, Boolean(claims.length || missing.length))}
      `;
    }
    const paragraphs = answer.split(/\n{2,}/).map((part) => part.trim()).filter(Boolean);
    return paragraphs.map((part) => `<p>${escapeHtml(part)}</p>`).join('');
  };

  const renderAnswerQualityBanner = (payload) => {
    if (payload?.deadline_hit && payload?.fallback_used) {
      return `
        <section class="source-qa-answer-quality source-qa-answer-quality-fast">
          <strong>Fast mode reached the 1-minute limit</strong>
          <span>Fast 已给出基于检索证据的第一版判断；完整关系链路建议用 Deep 模式继续确认。</span>
          <button type="button" class="button button-secondary source-qa-deep-continue" data-source-deep-continue>
            用 Deep 模式继续验证
          </button>
        </section>
      `;
    }
    const quality = payload?.answer_quality || {};
    const contract = payload?.answer_contract || {};
    const judge = payload?.answer_judge || {};
    const validation = payload?.answer_claim_check?.codex_citation_validation || {};
    const issues = [
      ...(quality.missing || []),
      ...(contract.missing_links || []),
      ...(judge.issues || []),
      ...(validation.issues || []),
    ].map((item) => String(item || '').trim()).filter(Boolean);
    const status = String(quality.status || contract.status || '').toLowerCase();
    const confidence = String(contract.confidence || quality.confidence || '').toLowerCase();
    const judgeStatus = String(judge.status || '').toLowerCase();
    const validationStatus = String(validation.status || '').toLowerCase();
    const show = status === 'needs_more_trace'
      || confidence === 'low'
      || ['repair', 'warn', 'insufficient_evidence'].includes(judgeStatus)
      || (validationStatus && !['ok', 'skipped'].includes(validationStatus))
      || contract.status === 'blocked_missing_source'
      || contract.status === 'unreliable_llm_answer';
    if (!show) return '';
    const label = confidence === 'low' || contract.status === 'unreliable_llm_answer'
      ? 'Low confidence'
      : 'Needs more evidence';
    return `
      <section class="source-qa-answer-quality">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(status || contract.status || judgeStatus || validationStatus || 'review needed')}</span>
        ${issues.length ? `<ul>${issues.slice(0, 4).map((item) => `<li>${escapeHtml(item)}</li>`).join('')}</ul>` : ''}
      </section>
    `;
  };

  const renderLlmAnswer = (payload) => {
    if (!llmAnswer) return;
    const answer = String(payload?.llm_answer || '').trim();
    if (!answer) {
      llmAnswer.hidden = true;
      llmAnswer.innerHTML = '';
      return;
    }
    const usage = payload?.llm_usage || {};
    const meta = [
      payload?.llm_budget_mode ? `budget: ${payload.llm_budget_mode}` : '',
      payload?.llm_provider ? `provider: ${payload.llm_provider}` : '',
      payload?.llm_model ? `model: ${payload.llm_model}` : '',
      payload?.llm_thinking_budget !== undefined ? `thinking: ${payload.llm_thinking_budget}` : '',
      payload?.llm_route?.mode === 'auto' ? `route: ${payload.llm_route.reason}` : '',
      payload?.llm_cached ? 'cache hit' : 'live call',
      usage?.promptTokenCount || usage?.prompt_tokens ? `prompt: ${usage.promptTokenCount || usage.prompt_tokens}` : '',
      usage?.candidatesTokenCount || usage?.completion_tokens ? `output: ${usage.candidatesTokenCount || usage.completion_tokens}` : '',
      usage?.totalTokenCount || usage?.total_tokens ? `total: ${usage.totalTokenCount || usage.total_tokens}` : '',
    ].filter(Boolean).join(' · ');
    const confidence = payload?.structured_answer?.confidence || payload?.answer_contract?.confidence || payload?.answer_quality?.confidence;
    llmAnswer.hidden = false;
    llmAnswer.innerHTML = `
      <div class="source-qa-llm-card">
        <div class="source-qa-llm-head">
          <div>
            <strong>Answer</strong>
            ${confidence ? `<em>${escapeHtml(confidence)} confidence</em>` : ''}
          </div>
          <span>${escapeHtml(meta)}</span>
        </div>
        ${renderAnswerQualityBanner(payload)}
        <div class="source-qa-answer-body">
          ${renderReadableAnswerBody(payload, answer)}
        </div>
      </div>
    `;
  };

  const renderPendingQuery = (question, answerModeLabel) => {
    const trimmedQuestion = String(question || '').trim();
    if (summary) {
      summary.textContent = trimmedQuestion
        ? `Running current question: ${trimmedQuestion.slice(0, 180)}${trimmedQuestion.length > 180 ? '...' : ''}`
        : 'Running current question.';
    }
    if (queryStatus && notificationSupported()) {
      queryStatus.textContent = '请保持当前标签页开启以接收通知。';
    }
    if (results) {
      results.hidden = true;
      results.innerHTML = '';
    }
    if (activeMode) activeMode.textContent = answerModeLabel || 'auto';
    renderUsageBadges({});
    renderFallbackNotice({});
    renderLlmAnswer({});
    if (selectedLlmProvider() === 'codex_cli_bridge') {
      renderLiveAnswer('Codex is preparing the read-only investigation...', { pending: true });
    } else {
      renderLiveAnswer('');
    }
    renderEvidenceSummary(null);
    renderDebugTrace(null);
    if (feedback) feedback.hidden = true;
    if (feedbackReasons) feedbackReasons.hidden = true;
    if (feedbackStatus) feedbackStatus.textContent = '';
  };

  const renderLiveAnswer = (message, options = {}) => {
    const text = String(message || '').trim();
    if (!text) {
      liveAssistantMessage = null;
      if (liveAnswer) {
        liveAnswer.hidden = true;
        liveAnswer.innerHTML = '';
      }
      renderSessionMessages(activeSession);
      return;
    }
    liveAssistantMessage = {
      text,
      title: options.title || 'Codex Live',
      meta: options.meta || 'read-only investigation',
      created_at: new Date().toISOString(),
      stopped: Boolean(options.stopped),
      job_id: options.jobId || '',
      retry_question: options.retryQuestion || '',
    };
    if (liveAnswer) {
      liveAnswer.hidden = true;
      liveAnswer.innerHTML = '';
    }
    renderSessionMessages(activeSession);
  };

  const countAssistantMessages = (session) =>
    (session?.messages || []).filter((message) => message.role === 'assistant').length;

  const finalAnswerTextFromPayload = (payload) => {
    const structuredAnswer = payload?.structured_answer?.direct_answer;
    return String(
      structuredAnswer ||
      payload?.llm_answer ||
      payload?.summary ||
      ''
    ).trim();
  };

  const finalizeLiveAnswer = async (payload, previousAssistantCount, provider) => {
    if (provider !== 'codex_cli_bridge') {
      renderLiveAnswer('');
      return;
    }
    let refreshedSession = activeSession;
    if (activeSessionId && countAssistantMessages(refreshedSession) <= previousAssistantCount) {
      await sleep(300);
      refreshedSession = await loadSession(activeSessionId, { preserveLive: true, preservePending: true });
    }
    if (countAssistantMessages(refreshedSession) > previousAssistantCount) {
      pendingUserMessage = null;
      renderLiveAnswer('');
      return;
    }
    const fallbackText = finalAnswerTextFromPayload(payload);
    renderLiveAnswer(
      fallbackText || 'Codex completed, but the saved chat has not refreshed yet. Please keep this page open while the session catches up.',
      { title: 'Assistant', meta: fallbackText ? 'final answer fallback' : 'session refresh pending', stopped: true }
    );
  };

  const renderFallbackNotice = (payload) => {
    if (!fallbackNotice) return;
    const notice = payload?.fallback_notice;
    const freshnessWarning = payload?.index_freshness?.warning;
    if (!notice?.message) {
      if (freshnessWarning) {
        fallbackNotice.hidden = false;
        fallbackNotice.textContent = `Index freshness: ${freshnessWarning}`;
        return;
      }
      fallbackNotice.hidden = true;
      fallbackNotice.textContent = '';
      return;
    }
    fallbackNotice.hidden = false;
    fallbackNotice.textContent = `${notice.title || 'Fallback'}: ${notice.message}`;
  };

  const renderUsageBadges = (payload) => {
    const llmAnswered = ['gemini_flash', 'auto'].includes(payload?.answer_mode) && Boolean(payload?.llm_answer);
    if (activeCache) {
      if (llmAnswered) {
        activeCache.hidden = false;
        const cacheMeta = payload?.cache_metadata || {};
        const cachedAt = cacheMeta.cached_at ? ` · ${formatSessionTime(cacheMeta.cached_at)}` : '';
        const modeMeta = payload?.query_mode || cacheMeta.query_mode ? ` · ${payload?.query_mode || cacheMeta.query_mode}` : '';
        activeCache.textContent = payload?.llm_cached ? `cache hit${cachedAt}${modeMeta}` : `live LLM${modeMeta}`;
      } else {
        activeCache.hidden = true;
        activeCache.textContent = 'live';
      }
    }
    if (activeUsage) {
      const usage = payload?.llm_usage || {};
      const total = usage?.totalTokenCount || usage?.total_tokens;
      if (llmAnswered && total) {
        activeUsage.hidden = false;
        activeUsage.textContent = `${total} tokens`;
      } else {
        activeUsage.hidden = true;
        activeUsage.textContent = 'tokens';
      }
    }
  };

  const applyQueryJobStatus = (payload, progress) => {
    const etaText = payload.queued_position
      ? `排队第 ${payload.queued_position} 位${payload.eta_seconds_range?.length ? `，${formatEtaRange(payload.eta_seconds_range)}` : ''}`
      : '';
    const stageLabels = {
      queued: '排队中',
      auto_sync: '同步检查',
      evidence_pack: '构建证据',
      llm_generation: 'Codex 推理',
      codex_session_lock: '等待 Codex 会话',
      codex_deep_investigation: '补充深挖',
      completed: '质量校验完成',
    };
    const modeText = payload.query_mode === 'fast' ? '快速模式' : (payload.query_mode === 'deep' ? '深度模式' : '');
    const stageLabel = stageLabels[payload.stage] || '';
    const progressText = payload.total
      ? `${stageLabel ? `${stageLabel}: ` : ''}${payload.message || 'Processing source-code question.'} (${payload.current || 0}/${payload.total})`
      : `${stageLabel ? `${stageLabel}: ` : ''}${payload.message || 'Processing source-code question.'}`;
    const stalledText = payload.stalled_retryable ? ' 后台超过 3 分钟无新进展，可重连或重试。' : '';
    progress?.setMessage([modeText, progressText, etaText, stalledText].filter(Boolean).join(' '));
    if (payload.stage === 'codex_stream' && payload.message) {
      renderLiveAnswer(payload.message, { title: 'Codex Live', meta: 'streaming CLI output', jobId: payload.job_id });
    }
  };

  const watchQueryJobEvents = (jobId, progress, control) => new Promise((resolve, reject) => {
    if (!window.EventSource) {
      reject(new Error('EventSource is not supported by this browser.'));
      return;
    }
    const source = new EventSource(jobEventsUrl(jobId));
    let settled = false;
    const close = () => {
      source.close();
    };
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      close();
      callback(value);
    };
    const handlePayload = (event) => {
      if (control?.stopped) {
        const error = new Error('Stopped by user.');
        error.stoppedByUser = true;
        finish(reject, error);
        return;
      }
      try {
        const payload = JSON.parse(event.data || '{}');
        applyQueryJobStatus(payload, progress);
        if (payload.state === 'completed') {
          finish(resolve, (payload.results || [])[0] || {});
        } else if (payload.state === 'failed') {
          const error = new Error(sourceQaJobErrorMessage(payload));
          error.jobPayload = payload;
          finish(reject, error);
        }
      } catch (error) {
        finish(reject, error);
      }
    };
    source.addEventListener('message', handlePayload);
    source.addEventListener('completed', handlePayload);
    source.addEventListener('failed', handlePayload);
    source.onerror = () => {
      if (settled) return;
      close();
      const error = new Error('SSE connection interrupted.');
      error.gatewayDisconnected = true;
      reject(error);
    };
  });

  const pollQueryJob = async (jobId, progress, control) => {
    while (jobId) {
      if (control?.stopped) {
        const error = new Error('Stopped by user.');
        error.stoppedByUser = true;
        throw error;
      }
      const payload = await readJobStatus(jobId);
      if (control?.stopped) {
        const error = new Error('Stopped by user.');
        error.stoppedByUser = true;
        throw error;
      }
      applyQueryJobStatus(payload, progress);
      if (payload.state === 'completed') {
        return (payload.results || [])[0] || {};
      }
      if (payload.state === 'failed') {
        const error = new Error(sourceQaJobErrorMessage(payload));
        error.jobPayload = payload;
        throw error;
      }
      await sleep(700);
    }
    return {};
  };

  const runQueryJob = async (jobId, progress, control) => {
    try {
      return await watchQueryJobEvents(jobId, progress, control);
    } catch (error) {
      if (error?.stoppedByUser) throw error;
      progress?.setMessage('Live status stream disconnected; falling back to status polling...');
      return pollQueryJob(jobId, progress, control);
    }
  };

  const resumeQueryJob = async (jobId, question = '') => {
    const normalizedJobId = String(jobId || '').trim();
    if (!normalizedJobId || (activeQueryControl && !activeQueryControl.stopped)) return;
    const progress = startQueryProgress('Reconnecting to Source Code Q&A job...');
    const queryControl = { stopped: false, jobId: normalizedJobId };
    activeQueryControl = queryControl;
    updateQueryButtonState(true);
    renderLiveAnswer('Reconnecting to the background Source Code Q&A job...', {
      title: 'Codex Live',
      meta: 'reconnecting',
      jobId: normalizedJobId,
      retryQuestion: question,
    });
    try {
      const payload = await runQueryJob(normalizedJobId, progress, queryControl);
      if (queryControl.stopped) return;
      lastPayload = payload;
      if (activeSessionId) {
        await loadSession(activeSessionId, { preserveLive: true, preservePending: true, skipJobResume: true });
      }
      if (payload.session) {
        activeSession = payload.session;
        activeSessionId = payload.session.id || activeSessionId;
        sourceSessions = [payload.session, ...sourceSessions.filter((item) => item.id !== payload.session.id)].slice(0, 30);
        applyActiveSession(payload.session);
      } else if (activeSessionId) {
        await loadSession(activeSessionId, { skipJobResume: true });
      }
      conversationContext = buildConversationContext(payload, question || conversationContext?.question || '');
      if (queryStatus) queryStatus.textContent = `Reconnected and completed in ${formatElapsed(progress.startedAt)}.`;
      notifyJobFinished('Source Code Q&A completed', (question || payload.summary || 'Your code question is ready.').slice(0, 180));
      renderUsageBadges(payload);
      renderFallbackNotice(payload);
      renderStatus(payload.repo_status || []);
      await finalizeLiveAnswer(payload, 0, selectedLlmProvider());
      if (selectedLlmProvider() === 'codex_cli_bridge') {
        renderLlmAnswer({});
      } else {
        renderLlmAnswer(payload);
      }
    } catch (error) {
      if (error?.stoppedByUser) {
        if (queryStatus) queryStatus.textContent = `Stopped after ${formatElapsed(progress.startedAt)}.`;
        return;
      }
      const message = sourceQaJobErrorMessage(error.jobPayload || error);
      if (queryStatus) queryStatus.textContent = `${message} elapsed ${formatElapsed(progress.startedAt)}`;
      notifyJobFinished('Source Code Q&A failed', (question || message).slice(0, 180));
      renderLiveAnswer(message, {
        title: 'Codex Live',
        meta: 'status disconnected',
        stopped: true,
        jobId: normalizedJobId,
        retryQuestion: question,
      });
    } finally {
      stopQueryProgress();
      if (activeQueryControl === queryControl) {
        activeQueryControl = null;
        updateQueryButtonState(false);
      }
    }
  };

  const resumePendingJobFromSession = (session) => {
    if (activeQueryControl && !activeQueryControl.stopped) return;
    const pending = pendingJobFromSession(session);
    if (!pending?.jobId) return;
    renderLiveAnswer('A previous Source Code Q&A job is still pending. Reconnecting to status...', {
      title: 'Codex Live',
      meta: 'pending job',
      jobId: pending.jobId,
      retryQuestion: pending.question,
    });
    resumeQueryJob(pending.jobId, pending.question);
  };

  const queryCode = async () => {
    if (activeQueryControl && !activeQueryControl.stopped) {
      stopActiveQuery();
      return;
    }
    const selectedAnswerMode = answerMode?.value || 'auto';
    const selectedQueryMode = queryMode?.value || 'fast';
    const selectedProvider = selectedLlmProvider();
    const effectiveAnswerMode = selectedAnswerMode;
    if (activeMode) activeMode.textContent = `${effectiveAnswerMode} · ${selectedQueryMode}`;
    const progress = startQueryProgress(selectedQueryMode === 'fast' ? 'Submitting fast query to server...' : 'Submitting deep query to server...');
    const submittedQuestion = String(questionInput?.value || '').trim();
    if (!submittedQuestion) {
      stopQueryProgress();
      if (queryStatus) queryStatus.textContent = 'Question is empty.';
      return;
    }
    if (activeAttachmentUploads > 0 || pendingAttachments.some((item) => item.uploading)) {
      stopQueryProgress();
      if (queryStatus) queryStatus.textContent = 'Please wait for image upload to finish.';
      updateQueryButtonState(false);
      return;
    }
    requestNotificationPermission();
    const queryControl = { stopped: false, jobId: '', question: submittedQuestion };
    activeQueryControl = queryControl;
    updateQueryButtonState(true);
    if (questionInput) questionInput.value = '';
    renderPendingQuery(submittedQuestion, effectiveAnswerMode);
    try {
      const session = await ensureActiveSession({ preserveLive: true, preservePending: true });
      if (queryControl.stopped) return;
      const previousAssistantCount = countAssistantMessages(session || activeSession);
      const attachmentsForQuestion = pendingAttachments.map((item) => ({ ...item }));
      renderOptimisticUserMessage(submittedQuestion, attachmentsForQuestion);
      clearPendingAttachments();
      const initialPayload = await apiFetchJson(queryUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_id: session?.id || '',
          pm_team: pmTeam.value,
          country: currentCountry(),
          question: submittedQuestion,
          answer_mode: effectiveAnswerMode,
          query_mode: selectedQueryMode,
          llm_provider: selectedProvider,
          llm_budget_mode: selectedQueryMode === 'fast' ? 'fast' : 'auto',
          attachment_ids: attachmentsForQuestion.map((item) => item.id).filter(Boolean),
          conversation_context: conversationContext,
          async: true,
        }),
      }, { attempts: 3 });
      if (queryControl.stopped) return;
      if (initialPayload.job_id) {
        queryControl.jobId = initialPayload.job_id;
        applyQueryJobStatus(initialPayload, progress);
      }
      const payload = initialPayload.status === 'queued' && initialPayload.job_id
        ? await runQueryJob(initialPayload.job_id, progress, queryControl)
        : initialPayload;
      if (queryControl.stopped) return;
      lastPayload = payload;
      conversationContext = buildConversationContext(payload, submittedQuestion);
      if (payload.session) {
        activeSession = payload.session;
        activeSessionId = payload.session.id || activeSessionId;
        sourceSessions = [payload.session, ...sourceSessions.filter((item) => item.id !== payload.session.id)].slice(0, 30);
        pendingUserMessage = null;
        applyActiveSession(payload.session);
      } else if (activeSessionId) {
        pendingUserMessage = null;
        await loadSession(activeSessionId, { preserveLive: true });
      }
      rememberLastQueryConfig(effectiveAnswerMode, selectedProvider);
      summary.textContent = payload.summary || 'Search completed.';
      if (payload.llm_retryable_error?.retryable) {
        queryStatus.textContent = `LLM quota/rate limit hit; retry is still enabled. Code search completed in ${formatElapsed(progress.startedAt)}.`;
      } else {
        queryStatus.textContent = payload.status === 'ok'
          ? `Search completed in ${formatElapsed(progress.startedAt)}.`
          : `${payload.status} after ${formatElapsed(progress.startedAt)}.`;
      }
      notifyJobFinished('Source Code Q&A completed', submittedQuestion.slice(0, 180));
      if (activeMode) activeMode.textContent = `${payload.answer_mode || effectiveAnswerMode} · ${payload.query_mode || selectedQueryMode}`;
      renderUsageBadges(payload);
      renderFallbackNotice(payload);
      renderStatus(payload.repo_status || []);
      await finalizeLiveAnswer(payload, previousAssistantCount, selectedProvider);
      if (selectedProvider === 'codex_cli_bridge') {
        renderLlmAnswer({});
      } else {
        renderLlmAnswer(payload);
      }
      renderEvidenceSummary(null);
      renderDebugTrace(null);
      if (results) {
        results.hidden = true;
        results.innerHTML = '';
      }
      if (feedback) {
        feedback.hidden = payload.status !== 'ok';
      }
      if (feedbackReasons) feedbackReasons.hidden = true;
      if (feedbackStatus) {
        feedbackStatus.textContent = '';
      }
    } catch (error) {
      if (error?.stoppedByUser) {
        if (queryStatus) queryStatus.textContent = `Stopped after ${formatElapsed(progress.startedAt)}.`;
        return;
      }
      const message = sourceQaJobErrorMessage(error.jobPayload || error);
      if (queryStatus) queryStatus.textContent = `${message} elapsed ${formatElapsed(progress.startedAt)}`;
      notifyJobFinished('Source Code Q&A failed', submittedQuestion.slice(0, 180) || message);
      renderUsageBadges({});
      renderFallbackNotice({});
      if (selectedProvider === 'codex_cli_bridge') {
        renderLiveAnswer(message, {
          title: 'Codex Live',
          meta: 'error',
          stopped: true,
          jobId: queryControl.jobId,
          retryQuestion: submittedQuestion,
        });
      } else {
        renderLiveAnswer('');
      }
      renderLlmAnswer({});
      renderEvidenceSummary(null);
      renderDebugTrace(null);
      if (feedback) feedback.hidden = true;
      if (feedbackReasons) feedbackReasons.hidden = true;
    } finally {
      stopQueryProgress();
      if (activeQueryControl === queryControl) {
        activeQueryControl = null;
        updateQueryButtonState(false);
      }
    }
  };

  const latestUserQuestion = () => {
    const messages = Array.isArray(activeSession?.messages) ? activeSession.messages : [];
    for (let index = messages.length - 1; index >= 0; index -= 1) {
      const message = messages[index] || {};
      if (message.role === 'user' && String(message.content || '').trim()) {
        return String(message.content || '').trim();
      }
    }
    return String(conversationContext?.question || lastPayload?.original_question || questionInput?.value || '').trim();
  };

  const continueWithDeepMode = () => {
    const question = latestUserQuestion();
    if (!question) {
      if (queryStatus) queryStatus.textContent = 'No question found for deep verification.';
      return;
    }
    if (selectHasValue(queryMode, 'deep')) {
      queryMode.value = 'deep';
      updateQueryModeHelp();
    }
    if (questionInput) questionInput.value = question;
    queryCode();
  };

  const feedbackReasonLabels = {
    deprecated_class: '引用了已废弃类',
    opposite_logic: '逻辑与实际表现相反',
    off_topic: '答非所问',
    missing_key_flow: '缺少关键链路',
    wrong_scope: '国家/团队范围错了',
  };

  const sendFeedback = async (rating, reason = '') => {
    if (!lastPayload || !feedbackUrl) return;
    if (feedbackStatus) feedbackStatus.textContent = 'Saving feedback...';
    try {
      await fetch(feedbackUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          rating,
          reason,
          comment: reason ? feedbackReasonLabels[reason] || reason : '',
          pm_team: pmTeam.value,
          country: currentCountry(),
          question: lastPayload.original_question || conversationContext?.question || questionInput.value,
          trace_id: lastPayload.trace_id || '',
          answer_mode: lastPayload.answer_mode || '',
          query_mode: lastPayload.query_mode || '',
          llm_budget_mode: lastPayload.llm_budget_mode || lastPayload.llm_requested_budget_mode || '',
          top_paths: (lastPayload.matches || []).slice(0, 5).map((match) => match.path),
          answer_quality: lastPayload.answer_quality || {},
          replay_context: buildFeedbackReplayContext(lastPayload),
        }),
      }).then(readJson);
      if (feedbackStatus) feedbackStatus.textContent = 'Feedback saved.';
      pendingFeedbackRating = '';
      if (feedbackReasons) feedbackReasons.hidden = true;
    } catch (error) {
      if (feedbackStatus) feedbackStatus.textContent = error.message || 'Feedback failed.';
    }
  };

  const copyAnswerWithCitations = async () => {
    if (!lastPayload) return;
    const answer = finalAnswerTextFromPayload(lastPayload) || lastPayload.summary || '';
    const citations = (lastPayload.citations || lastPayload.matches || [])
      .slice(0, 8)
      .map((item) => {
        if (typeof item === 'string') return item;
        return [item.repo, item.path, item.line_start ? `L${item.line_start}` : ''].filter(Boolean).join(':');
      })
      .filter(Boolean);
    const text = `${answer}${citations.length ? `\n\nReferences:\n${citations.map((item) => `- ${item}`).join('\n')}` : ''}`;
    try {
      await navigator.clipboard.writeText(text);
      if (feedbackStatus) feedbackStatus.textContent = 'Answer copied with citations.';
    } catch (_error) {
      if (feedbackStatus) feedbackStatus.textContent = 'Copy failed. Please copy from the answer manually.';
    }
  };

  pmTeam.addEventListener('change', () => {
    updateCountryVisibility();
    conversationContext = null;
    if (sessionScope) sessionScope.textContent = [pmTeam.value, currentCountry()].filter(Boolean).join(' · ');
    renderSelectedConfig();
  });
  country.addEventListener('change', () => {
    conversationContext = null;
    if (countryHelp) countryHelp.textContent = runtimeContextHelp();
    if (sessionScope) sessionScope.textContent = [pmTeam.value, currentCountry()].filter(Boolean).join(' · ');
    renderSelectedConfig();
  });
  answerMode?.addEventListener('change', updateAnswerModeState);
  queryMode?.addEventListener('change', updateQueryModeHelp);
  llmProvider?.addEventListener('change', () => {
    updateAnswerModeState();
    if (sessionProvider) sessionProvider.textContent = providerLabel(selectedLlmProvider());
  });
  newSessionButton?.addEventListener('click', async () => {
    if (queryStatus) queryStatus.textContent = 'Starting a new chat...';
    try {
      await createSession();
      if (questionInput) questionInput.value = '';
      clearPendingAttachments();
      if (summary) summary.textContent = 'No question asked yet.';
      if (results) results.innerHTML = '<div class="source-qa-empty">Ask a question to generate an answer. Retrieval details stay in the background unless Codex is unavailable.</div>';
      renderUsageBadges({});
      renderFallbackNotice({});
      renderLlmAnswer({});
      renderLiveAnswer('');
      renderEvidenceSummary(null);
      renderDebugTrace(null);
      if (feedback) feedback.hidden = true;
      if (feedbackReasons) feedbackReasons.hidden = true;
      if (queryStatus) queryStatus.textContent = 'New chat ready.';
    } catch (error) {
      if (queryStatus) queryStatus.textContent = error.message || 'Could not start a new chat.';
    }
  });
  saveButton?.addEventListener('click', saveConfig);
  saveModelAvailabilityButton?.addEventListener('click', saveModelAvailability);
  runtimeEvidencePmTeam?.addEventListener('change', loadRuntimeEvidence);
  runtimeEvidenceCountry?.addEventListener('change', loadRuntimeEvidence);
  runtimeEvidenceUploadButton?.addEventListener('click', () => runtimeEvidenceInput?.click());
  runtimeEvidenceInput?.addEventListener('change', async () => {
    const file = runtimeEvidenceInput.files?.[0];
    if (runtimeEvidenceInput) runtimeEvidenceInput.value = '';
    if (file) await uploadRuntimeEvidence(file);
  });
  syncButton?.addEventListener('click', syncRepos);
  queryButton?.addEventListener('click', queryCode);
  (chatComposer || questionInput)?.addEventListener('paste', handleAttachmentPaste);
  attachmentUploadButton?.addEventListener('click', () => attachmentInput?.click());
  attachmentInput?.addEventListener('change', async () => {
    try {
      await addAttachmentFiles(attachmentInput.files);
    } catch (error) {
      if (queryStatus) queryStatus.textContent = error.message || 'Attachment upload failed.';
      if (attachmentInput) attachmentInput.value = '';
    }
  });
  attachmentsList?.addEventListener('click', (event) => {
    const button = event.target.closest('[data-source-remove-attachment]');
    if (button) {
      const attachmentId = button.dataset.sourceRemoveAttachment || '';
      pendingAttachments = pendingAttachments.filter((item) => item.id !== attachmentId);
      renderPendingAttachments();
      updateAttachmentUploadState();
      return;
    }
    handleAttachmentPreviewEvent(event);
  });
  attachmentsList?.addEventListener('keydown', handleAttachmentPreviewEvent);
  sessionMessages?.addEventListener('click', (event) => {
    const deepContinueButton = event.target.closest('[data-source-deep-continue]');
    if (deepContinueButton) {
      continueWithDeepMode();
      return;
    }
    const reconnectButton = event.target.closest('[data-source-reconnect-job]');
    if (reconnectButton) {
      const jobId = reconnectButton.dataset.sourceReconnectJob || '';
      const retryQuestion = reconnectButton.dataset.sourceRetryQuestion || '';
      resumeQueryJob(jobId, retryQuestion);
      return;
    }
    const retryButton = event.target.closest('[data-source-retry-question]');
    if (retryButton) {
      if (questionInput) questionInput.value = retryButton.dataset.sourceRetryQuestion || '';
      queryCode();
      return;
    }
    handleAttachmentPreviewEvent(event);
  });
  llmAnswer?.addEventListener('click', (event) => {
    const deepContinueButton = event.target.closest('[data-source-deep-continue]');
    if (!deepContinueButton) return;
    continueWithDeepMode();
  });
  sessionMessages?.addEventListener('keydown', handleAttachmentPreviewEvent);
  viewTabs.forEach((tab) => {
    tab.addEventListener('click', () => setSourceView(tab.dataset.sourceViewTab));
  });
  window.addEventListener('portal:tab-activated', (event) => {
    if (event.detail?.tabName === 'admin') {
      renderSelectedConfig();
      loadRuntimeEvidence();
    }
  });
  document.querySelectorAll('[data-source-feedback-rating]').forEach((button) => {
    button.addEventListener('click', () => {
      const rating = button.dataset.sourceFeedbackRating || '';
      if (rating === 'incorrect') {
        pendingFeedbackRating = rating;
        if (feedbackReasons) feedbackReasons.hidden = false;
        if (feedbackStatus) feedbackStatus.textContent = '请选择一个原因标签；也可以直接点其它反馈按钮。';
        return;
      }
      sendFeedback(rating);
    });
  });
  document.querySelectorAll('[data-source-feedback-reason]').forEach((button) => {
    button.addEventListener('click', () => {
      sendFeedback(pendingFeedbackRating || 'incorrect', button.dataset.sourceFeedbackReason || '');
    });
  });
  copyAnswerButton?.addEventListener('click', copyAnswerWithCitations);

  setSourceView('chat');
  restoreLastQueryConfig();
  applyActiveSession(null);
  updateAnswerModeState();
  loadConfig();
  loadSessions();
})();
