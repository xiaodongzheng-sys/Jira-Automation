(() => {
  const root = document.querySelector('[data-meeting-recorder-root]');
  if (!root) return;
  const UPCOMING_MEETING_DISPLAY_LIMIT = 3;

  const state = {
    activeRecordId: '',
    selectedRecordId: root.dataset.selectedRecordId || '',
    initialSelectionPending: Boolean(root.dataset.selectedRecordId),
    diagnostics: null,
    signalCheckToken: 0,
    signalCheckTimer: null,
  };

  const nodes = {
    diagnostic: root.querySelector('[data-meeting-recorder-diagnostic]'),
    upcoming: root.querySelector('[data-meeting-upcoming]'),
    calendarStatus: root.querySelector('[data-meeting-calendar-status]'),
    refresh: root.querySelector('[data-meeting-refresh]'),
    startForm: root.querySelector('[data-meeting-start-form]'),
    recordingStatus: root.querySelector('[data-meeting-recording-status]'),
    transcriptLanguage: root.querySelector('[data-meeting-transcript-language]'),
    records: root.querySelector('[data-meeting-records]'),
    recordsRefresh: root.querySelector('[data-meeting-records-refresh]'),
    recordDate: root.querySelector('[data-meeting-record-date]'),
    recordDateToggle: root.querySelector('[data-meeting-record-date-toggle]'),
    recordCalendar: root.querySelector('[data-meeting-record-calendar]'),
    detail: root.querySelector('[data-meeting-record-detail]'),
  };

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const downloadUrl = (url) => {
    if (!url) return '';
    const separator = String(url).includes('?') ? '&' : '?';
    return `${url}${separator}download=1`;
  };

  const filenameFromUrl = (url, fallback = 'meeting.wav') => {
    try {
      const pathname = new URL(url, window.location.origin).pathname;
      return decodeURIComponent(pathname.split('/').filter(Boolean).pop() || fallback);
    } catch (_error) {
      return fallback;
    }
  };

  const filenameFromDisposition = (header) => {
    const value = String(header || '');
    const utf8Match = value.match(/filename\*=UTF-8''([^;]+)/i);
    if (utf8Match) return decodeURIComponent(utf8Match[1]);
    const quotedMatch = value.match(/filename="([^"]+)"/i);
    if (quotedMatch) return quotedMatch[1];
    const plainMatch = value.match(/filename=([^;]+)/i);
    return plainMatch ? plainMatch[1].trim() : '';
  };

  const selectedTranscriptLanguage = () => {
    const value = String(nodes.transcriptLanguage?.value || 'zh').trim().toLowerCase();
    return ['zh', 'en', 'mixed'].includes(value) ? value : 'zh';
  };

  const transcriptLanguageOptionsHtml = (selected = 'zh') => {
    const safeSelected = ['zh', 'en', 'mixed'].includes(String(selected || '').toLowerCase()) ? String(selected).toLowerCase() : 'zh';
    return [
      ['zh', 'Chinese'],
      ['en', 'English'],
      ['mixed', 'Mixed'],
    ].map(([value, label]) => `<option value="${value}" ${value === safeSelected ? 'selected' : ''}>${escapeHtml(label)}</option>`).join('');
  };

  const api = async (url, options = {}) => {
    let response;
    try {
      response = await fetch(url, {
        headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
        ...options,
      });
    } catch (error) {
      const networkError = new Error('Connection interrupted. Refreshing status...');
      networkError.isNetworkError = true;
      networkError.cause = error;
      throw networkError;
    }
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = new Error(payload.message || 'Request failed.');
      error.payload = payload;
      throw error;
    }
    return payload;
  };

  const delay = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms));

  const isNetworkError = (error) => Boolean(error?.isNetworkError);

  const refreshRecordState = async (recordId) => {
    try {
      await loadRecord(recordId);
      await loadRecords();
      return true;
    } catch (_error) {
      return false;
    }
  };

  const meetingProcessStatusText = (payload) => {
    const message = String(payload?.progress?.message || payload?.message || '').trim();
    const stateLabel = statusLabel(payload?.state || payload?.status || '');
    return message || (stateLabel ? `${stateLabel}...` : 'Processing...');
  };

  const processStatusTarget = () => ({
    set textContent(value) {
      if (nodes.recordingStatus) nodes.recordingStatus.textContent = value;
    },
  });

  const monitorAutoProcessJob = async (recordId, payload) => {
    const autoProcessError = String(payload?.auto_process_error || '').trim();
    if (autoProcessError) {
      if (nodes.recordingStatus) nodes.recordingStatus.textContent = `Meeting processing was not queued: ${autoProcessError}`;
      return;
    }
    const jobId = String(payload?.job_id || '').trim();
    if (!recordId || !jobId) return;
    if (nodes.recordingStatus) nodes.recordingStatus.textContent = 'Transcribing audio and generating meeting minutes...';
    try {
      await pollMeetingProcessJob(recordId, jobId, processStatusTarget());
    } catch (error) {
      if (nodes.recordingStatus) nodes.recordingStatus.textContent = error.message || 'Meeting processing failed.';
    }
  };

  const platformLabel = (platform) => {
    if (platform === 'google_meet') return 'Google Meet';
    if (platform === 'zoom') return 'Zoom';
    return 'Meeting';
  };

  const platformFromLink = (link) => {
    const value = String(link || '').toLowerCase();
    if (value.includes('meet.google.com')) return 'google_meet';
    if (value.includes('zoom.us')) return 'zoom';
    return '';
  };

  const isScreenCaptureKitStartupFailure = (error) => {
    const message = String(error?.message || error || '');
    return /ScreenCaptureKit|Screen Recording|System Audio Recording|Microphone|TCC|helper|permission|declined TCCs|not authorized|unavailable/i.test(message);
  };

  const statusLabel = (status) => {
    const normalized = String(status || '').trim();
    if (!normalized) return 'Unknown';
    return normalized.charAt(0).toUpperCase() + normalized.slice(1);
  };

  const formatDateTime = (value) => {
    if (!value) return '';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'Asia/Singapore',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hourCycle: 'h23',
    }).formatToParts(date).reduce((acc, part) => {
      acc[part.type] = part.value;
      return acc;
    }, {});
    return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second} SGT`;
  };

  const localDateValue = (value = new Date()) => {
    const date = value instanceof Date ? value : new Date(value || '');
    if (Number.isNaN(date.getTime())) return '';
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  };

  const parseLocalDateValue = (value) => {
    const match = String(value || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!match) return new Date();
    return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  };

  const calendarTitle = (date) => new Intl.DateTimeFormat(undefined, {
    month: 'long',
    year: 'numeric',
  }).format(date);

  const renderRecordCalendar = (viewDate = parseLocalDateValue(nodes.recordDate?.value)) => {
    if (!nodes.recordCalendar) return;
    const selectedValue = nodes.recordDate?.value || localDateValue();
    const selected = parseLocalDateValue(selectedValue);
    const firstDay = new Date(viewDate.getFullYear(), viewDate.getMonth(), 1);
    const gridStart = new Date(firstDay);
    gridStart.setDate(firstDay.getDate() - firstDay.getDay());
    const days = [];
    for (let index = 0; index < 42; index += 1) {
      const date = new Date(gridStart);
      date.setDate(gridStart.getDate() + index);
      const value = localDateValue(date);
      const classes = [
        'meeting-record-calendar-day',
        date.getMonth() === viewDate.getMonth() ? '' : 'is-muted',
        value === selectedValue ? 'is-selected' : '',
      ].filter(Boolean).join(' ');
      days.push(`<button class="${classes}" type="button" data-meeting-calendar-day="${value}">${date.getDate()}</button>`);
    }
    nodes.recordCalendar.dataset.viewMonth = localDateValue(firstDay);
    nodes.recordCalendar.innerHTML = `
      <div class="meeting-record-calendar-head">
        <button class="meeting-record-calendar-nav" type="button" data-meeting-calendar-prev aria-label="Previous month">&lsaquo;</button>
        <strong>${escapeHtml(calendarTitle(viewDate))}</strong>
        <button class="meeting-record-calendar-nav" type="button" data-meeting-calendar-next aria-label="Next month">&rsaquo;</button>
      </div>
      <div class="meeting-record-calendar-grid">
        ${['S', 'M', 'T', 'W', 'T', 'F', 'S'].map((day) => `<span class="meeting-record-calendar-weekday">${day}</span>`).join('')}
        ${days.join('')}
      </div>
    `;
    const selectedButton = nodes.recordCalendar.querySelector(`[data-meeting-calendar-day="${localDateValue(selected)}"]`);
    selectedButton?.setAttribute('aria-current', 'date');
  };

  const hideRecordCalendar = () => {
    if (nodes.recordCalendar) nodes.recordCalendar.hidden = true;
  };

  const toggleRecordCalendar = () => {
    if (!nodes.recordCalendar) return;
    if (nodes.recordCalendar.hidden) {
      renderRecordCalendar(parseLocalDateValue(nodes.recordDate?.value));
      nodes.recordCalendar.hidden = false;
    } else {
      hideRecordCalendar();
    }
  };

  const recordDateValue = (record) => localDateValue(record?.recording_started_at || record?.created_at || '');

  const selectRecordDate = (record) => {
    const value = recordDateValue(record);
    if (!value || !nodes.recordDate) return;
    nodes.recordDate.value = value;
    renderRecordCalendar(parseLocalDateValue(value));
  };

  const durationLabel = (start, end) => {
    const startDate = new Date(start || '');
    const endDate = new Date(end || '');
    if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime()) || endDate <= startDate) return '';
    const seconds = Math.round((endDate - startDate) / 1000);
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    const remaining = seconds % 60;
    return remaining ? `${minutes}m ${remaining}s` : `${minutes}m`;
  };

  const renderInlineMarkdown = (value) => escapeHtml(value)
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/`([^`]+)`/g, '<code>$1</code>');

  const renderMarkdown = (markdown) => {
    const cleaned = String(markdown || '').trim();
    if (!cleaned) return '<p class="empty-state">Minutes are not generated yet.</p>';
    const lines = cleaned.split(/\r?\n/);
    const html = [];
    let listDepth = 0;
    const closeLists = (targetDepth = 0) => {
      while (listDepth > targetDepth) {
        html.push('</ul>');
        listDepth -= 1;
      }
    };
    lines.forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed) {
        closeLists();
        return;
      }
      const heading = trimmed.match(/^(?:#{1,6}\s+|\*\*)([^*#].*?)(?:\*\*)?$/);
      if (heading && !trimmed.startsWith('- ') && !trimmed.startsWith('* ')) {
        closeLists();
        html.push(`<h4>${renderInlineMarkdown(heading[1].trim())}</h4>`);
        return;
      }
      const bullet = line.match(/^(\s*)[-*]\s+(.+)$/);
      if (bullet) {
        const depth = bullet[1].replace(/\t/g, '  ').length >= 2 ? 2 : 1;
        while (listDepth < depth) {
          html.push('<ul>');
          listDepth += 1;
        }
        closeLists(depth);
        html.push(`<li>${renderInlineMarkdown(bullet[2])}</li>`);
        return;
      }
      closeLists();
      html.push(`<p>${renderInlineMarkdown(trimmed)}</p>`);
    });
    closeLists();
    return html.join('');
  };

  const renderTranscriptQuality = (transcript) => {
    const quality = transcript?.quality || {};
    const segments = Array.isArray(transcript?.segments) ? transcript.segments : [];
    if (!quality.possible_incomplete && !segments.length) return '';
    const warnings = Array.isArray(quality.warnings) ? quality.warnings : [];
    const segmentTags = segments
      .filter((segment) => segment.quality === 'low_audio' || segment.possible_missed_speech)
      .slice(0, 8)
      .map((segment) => `low audio ${formatTimestamp(segment.start_seconds || 0)}-${formatTimestamp(segment.end_seconds || 0)} language=${segment.language || 'auto'}`);
    return `
      <div class="meeting-transcript-quality ${quality.possible_incomplete ? 'is-warning' : ''}">
        <strong>${quality.possible_incomplete ? 'Transcript may be incomplete' : 'Transcript quality'}</strong>
        <span>${escapeHtml([
          quality.language ? `language=${quality.language}` : '',
          quality.low_audio_segment_count ? `${quality.low_audio_segment_count} low-audio segment(s)` : '',
          quality.no_audio_segment_count ? `${quality.no_audio_segment_count} no-audio segment(s)` : '',
          quality.repetitive_chunk_count ? `${quality.repetitive_chunk_count} repeated chunk(s)` : '',
        ].filter(Boolean).join(' · ') || 'No quality warnings')}</span>
        ${warnings.length || segmentTags.length ? `<ul>${[...warnings, ...segmentTags].map((item) => `<li>${escapeHtml(item)}</li>`).join('')}</ul>` : ''}
      </div>
    `;
  };

  const renderTranscript = (transcript) => {
    const qualityMarkup = renderTranscriptQuality(transcript);
    const chunks = Array.isArray(transcript?.chunks) ? transcript.chunks.filter((chunk) => String(chunk?.text || '').trim()) : [];
    const ownerSpeechChunks = Array.isArray(transcript?.owner_speech_candidates)
      ? transcript.owner_speech_candidates.filter((chunk) => String(chunk?.text || '').trim())
      : [];
    const ownerSpeechMarkup = ownerSpeechChunks.length ? `
      <div class="meeting-transcript-quality">
        <strong>Me candidate from local microphone</strong>
        <span>Candidate only; not diarized speaker proof.</span>
      </div>
      <div class="meeting-transcript-list">
        ${ownerSpeechChunks.map((chunk) => `
          <div class="meeting-transcript-row">
            <time>${escapeHtml(formatTimestamp(chunk.start_seconds || 0))}</time>
            <p><strong>Me candidate:</strong> ${escapeHtml(chunk.text || '')}</p>
          </div>
        `).join('')}
      </div>
    ` : (transcript?.owner_speech_status === 'failed' && transcript?.owner_speech_warning ? `
      <div class="meeting-transcript-quality">
        <strong>Me candidate unavailable</strong>
        <span>${escapeHtml(transcript.owner_speech_warning)}</span>
      </div>
    ` : '');
    if (chunks.length) {
      return `
        ${qualityMarkup}
        <div class="meeting-transcript-list">
          ${chunks.map((chunk) => `
            <div class="meeting-transcript-row">
              <time>${escapeHtml(formatTimestamp(chunk.start_seconds || 0))}</time>
              <p>${escapeHtml(chunk.text || '')}</p>
            </div>
          `).join('')}
        </div>
        ${ownerSpeechMarkup}
      `;
    }
    const text = String(transcript?.text || '').trim();
    if (!text) return `${qualityMarkup}<p class="empty-state">Transcript is not generated yet.</p>${ownerSpeechMarkup}`;
    return `
      ${qualityMarkup}
      <div class="meeting-transcript-list">
        ${text.split(/\n+/).filter(Boolean).map((line) => `
          <div class="meeting-transcript-row">
            <time>--:--</time>
            <p>${escapeHtml(line)}</p>
          </div>
        `).join('')}
      </div>
      ${ownerSpeechMarkup}
    `;
  };

  const updateRecordSelection = () => {
    nodes.records?.querySelectorAll('[data-record-id]').forEach((button) => {
      button.classList.toggle('is-active', button.dataset.recordId === state.selectedRecordId);
    });
  };

  const renderDiagnostics = (payload) => {
    const recorderReady = Boolean(payload.ffmpeg_configured && payload.whisper_cpp_configured && payload.whisper_model_exists);
    const audioReady = Boolean(payload.system_audio_configured);
    const audioLabel = payload.audio_capture_label || 'Audio input unknown';
    const audioClass = audioReady ? 'is-ready' : 'is-warning';
    const devices = Array.isArray(payload.audio_devices) ? payload.audio_devices : [];
    return `
      <div class="meeting-diagnostic-head">
        <span class="meeting-diagnostic-kicker">Recorder status</span>
        <span class="meeting-diagnostic-pill ${recorderReady ? 'is-ready' : 'is-warning'}">${escapeHtml(recorderReady ? 'Ready' : 'Needs setup')}</span>
      </div>
      <div class="meeting-diagnostic-grid">
        <div class="meeting-diagnostic-item">
          <span class="meeting-diagnostic-dot ${audioClass}"></span>
          <div>
            <strong>Recorder input</strong>
            <span>${escapeHtml(audioLabel)} · ${escapeHtml(payload.audio_input || 'not configured')}</span>
          </div>
        </div>
        <div class="meeting-diagnostic-item">
          <span class="meeting-diagnostic-dot ${payload.whisper_cpp_configured && payload.whisper_model_exists ? 'is-ready' : 'is-warning'}"></span>
          <div>
            <strong>Transcription</strong>
            <span>${escapeHtml(payload.whisper_cpp_configured && payload.whisper_model_exists ? 'whisper.cpp ready' : 'whisper.cpp not ready')}</span>
          </div>
        </div>
        <div class="meeting-diagnostic-item is-wide">
          <span class="meeting-diagnostic-dot ${audioClass}"></span>
          <div>
            <strong>Meet/Zoom setup</strong>
            <span>${escapeHtml(payload.meeting_audio_setup_note || 'Meetings use ScreenCaptureKit system audio + microphone; keep speaker and microphone on normal devices.')}</span>
          </div>
        </div>
      </div>
      ${payload.audio_capture_warning ? `<p class="meeting-audio-warning">${escapeHtml(payload.audio_capture_warning)}</p>` : ''}
      <details class="meeting-device-list">
        <summary>${escapeHtml(devices.length ? `${devices.length} audio devices` : 'No audio devices detected')}</summary>
        <div>${devices.map((device) => `<span>${escapeHtml(device)}</span>`).join('')}</div>
      </details>
    `;
  };

  const setRecordingState = (record) => {
    state.activeRecordId = record?.record_id || '';
    if (nodes.recordingStatus) {
      nodes.recordingStatus.textContent = state.activeRecordId
        ? `Recording: ${record.title || 'Untitled meeting'}`
        : 'No active recording.';
    }
  };

  const clearSignalChecks = () => {
    state.signalCheckToken += 1;
    if (state.signalCheckTimer) {
      window.clearTimeout(state.signalCheckTimer);
      state.signalCheckTimer = null;
    }
  };

  const scheduleSignalCheck = (recordId, delayMs, remainingChecks, token) => {
    if (!recordId || remainingChecks <= 0 || token !== state.signalCheckToken) return;
    state.signalCheckTimer = window.setTimeout(async () => {
      if (token !== state.signalCheckToken || state.activeRecordId !== recordId) return;
      try {
        const payload = await api(`/api/meeting-recorder/records/${encodeURIComponent(recordId)}/signal-check`, { method: 'POST' });
        const record = payload.record || {};
        const health = record.recording_health || {};
        if (record.status === 'failed' || health.status === 'failed') {
          clearSignalChecks();
          setRecordingState(null);
          if (nodes.recordingStatus) {
            nodes.recordingStatus.textContent = health.warning || record.error || 'Recorder audio stopped. Start a new recording.';
          }
          await loadRecords();
          await loadRecord(recordId);
          return;
        }
        if (nodes.recordingStatus && state.activeRecordId === recordId) {
          nodes.recordingStatus.textContent = `Recording: ${record.title || 'Untitled meeting'}`;
        }
        scheduleSignalCheck(recordId, 3000, remainingChecks - 1, token);
      } catch (_error) {
        scheduleSignalCheck(recordId, 3000, remainingChecks - 1, token);
      }
    }, delayMs);
  };

  const startSignalChecks = (record) => {
    clearSignalChecks();
    const recordId = record?.record_id || '';
    if (!recordId || record?.status !== 'recording') return;
    const token = state.signalCheckToken;
    scheduleSignalCheck(recordId, 4000, 4, token);
  };

  const loadDiagnostics = async () => {
    try {
      const payload = await api('/api/meeting-recorder/diagnostics');
      state.diagnostics = payload;
      nodes.diagnostic.innerHTML = renderDiagnostics(payload);
      if (nodes.recordingStatus && payload.audio_capture_warning && !state.activeRecordId) {
        nodes.recordingStatus.textContent = payload.audio_capture_warning;
      }
    } catch (error) {
      nodes.diagnostic.textContent = error.message;
    }
  };

  const startRecording = async (meeting) => {
    clearSignalChecks();
    const meetingLink = String(meeting?.meeting_link || '').trim();
    let payload;
    try {
      payload = await api('/api/meeting-recorder/start', {
        method: 'POST',
        body: JSON.stringify({
          title: meeting?.title || 'Meeting',
          platform: platformFromLink(meetingLink) || meeting?.platform || 'unknown',
          meeting_link: meetingLink,
          recording_mode: meeting?.recording_mode || 'audio_only',
          transcript_language: meeting?.transcript_language || selectedTranscriptLanguage(),
          calendar_event_id: meeting?.calendar_event_id || '',
          scheduled_start: meeting?.scheduled_start || '',
          scheduled_end: meeting?.scheduled_end || '',
          attendees: meeting?.attendees || [],
        }),
      });
    } catch (error) {
      if (!meetingLink && isScreenCaptureKitStartupFailure(error)) {
        throw new Error(
          `${error.message || 'ScreenCaptureKit helper could not start.'} ` +
          'Grant Screen & System Audio Recording and Microphone permissions, then start recording again.'
        );
      }
      throw error;
    }
    setRecordingState(payload.record);
    selectRecordDate(payload.record);
    await loadRecords();
    await loadRecord(payload.record.record_id);
    startSignalChecks(payload.record);
  };

  const stopRecording = async (recordId) => {
    clearSignalChecks();
    const payload = await api(`/api/meeting-recorder/records/${encodeURIComponent(recordId)}/stop`, { method: 'POST' });
    setRecordingState(null);
    await loadRecords();
    await loadRecord(payload.record.record_id);
    await monitorAutoProcessJob(payload.record.record_id, payload);
  };

  const loadUpcoming = async () => {
    if (!nodes.upcoming) return;
    nodes.calendarStatus.textContent = 'Loading upcoming calendar meetings…';
    try {
      const payload = await api('/api/meeting-recorder/calendar/upcoming');
      const meetings = (Array.isArray(payload.meetings) ? payload.meetings : []).slice(0, UPCOMING_MEETING_DISPLAY_LIMIT);
      nodes.calendarStatus.textContent = meetings.length ? `${meetings.length} upcoming meeting(s).` : 'No upcoming calendar meetings found.';
      const defaultTranscriptLanguage = selectedTranscriptLanguage();
      nodes.upcoming.innerHTML = meetings.map((meeting, index) => `
        <article class="meeting-list-item">
          <div>
            <strong>${escapeHtml(meeting.title || 'Untitled meeting')}</strong>
            <span>${escapeHtml(platformLabel(meeting.platform))} · ${escapeHtml(meeting.start || '')}</span>
          </div>
          <div class="meeting-list-actions">
            <select aria-label="Transcript language for ${escapeHtml(meeting.title || 'calendar meeting')}" data-meeting-row-transcript-language="${index}">
              ${transcriptLanguageOptionsHtml(defaultTranscriptLanguage)}
            </select>
            <button class="button button-secondary" type="button" data-meeting-start-index="${index}">Start</button>
          </div>
        </article>
      `).join('');
      nodes.upcoming.querySelectorAll('[data-meeting-start-index]').forEach((button) => {
        button.addEventListener('click', async () => {
          const meeting = meetings[Number(button.dataset.meetingStartIndex) || 0];
          const rowLanguage = nodes.upcoming.querySelector(`[data-meeting-row-transcript-language="${button.dataset.meetingStartIndex}"]`)?.value || 'zh';
          button.disabled = true;
          try {
            await startRecording({
              title: meeting.title,
              platform: meeting.platform,
              meeting_link: meeting.meeting_link,
              recording_mode: 'audio_only',
              transcript_language: rowLanguage,
              calendar_event_id: meeting.calendar_event_id,
              scheduled_start: meeting.start,
              scheduled_end: meeting.end,
              attendees: meeting.attendees || [],
            });
          } catch (error) {
            nodes.calendarStatus.textContent = error.message;
          } finally {
            button.disabled = false;
          }
        });
      });
    } catch (error) {
      nodes.calendarStatus.textContent = error.message;
      nodes.upcoming.innerHTML = '';
    }
  };

  const loadRecords = async ({ restoreActive = false } = {}) => {
    if (!nodes.records) return;
    const payload = await api('/api/meeting-recorder/records');
    const serverRecords = Array.isArray(payload.records) ? payload.records : [];
    if (restoreActive) {
      const activeRecord = serverRecords.find((record) => String(record?.status || '').trim().toLowerCase() === 'recording');
      if (activeRecord) {
        setRecordingState(activeRecord);
        state.selectedRecordId = activeRecord.record_id || '';
        state.initialSelectionPending = Boolean(state.selectedRecordId);
        selectRecordDate(activeRecord);
        startSignalChecks(activeRecord);
      }
    }
    const selectedDate = nodes.recordDate?.value || localDateValue();
    const records = serverRecords.filter((record) => recordDateValue(record) === selectedDate);
    if (!records.length) {
      nodes.records.innerHTML = `<p class="empty-state">No meeting recordings on ${escapeHtml(selectedDate)}.</p>`;
      if (state.selectedRecordId && !state.initialSelectionPending) {
        state.selectedRecordId = '';
        updateRecordSelection();
      }
      if (nodes.detail && !state.initialSelectionPending) {
        nodes.detail.innerHTML = '<p class="empty-state">Select a recorded meeting to view audio, transcript, and minutes.</p>';
      }
      return;
    }
    nodes.records.innerHTML = records.map((record) => `
      <button class="meeting-record-row ${record.record_id === state.selectedRecordId ? 'is-active' : ''}" type="button" data-record-id="${escapeHtml(record.record_id)}">
        <span class="meeting-record-main">
          <strong>${escapeHtml(record.title || 'Untitled meeting')}</strong>
          <span>${escapeHtml(formatDateTime(record.recording_started_at || record.created_at) || platformLabel(record.platform))}</span>
        </span>
        <span class="meeting-record-meta">
          <span>${escapeHtml(platformLabel(record.platform))}</span>
          <span>${escapeHtml(durationLabel(record.recording_started_at, record.recording_stopped_at) || statusLabel(record.status))}</span>
          <span>${escapeHtml(record.minutes_status === 'completed' ? 'Minutes ready' : statusLabel(record.status))}</span>
        </span>
      </button>
    `).join('');
    nodes.records.querySelectorAll('[data-record-id]').forEach((button) => {
      button.addEventListener('click', () => {
        const recordId = button.dataset.recordId || '';
        loadRecord(recordId);
      });
    });
    if (state.selectedRecordId && !records.some((record) => record.record_id === state.selectedRecordId)) {
      state.selectedRecordId = '';
      if (nodes.detail && !state.initialSelectionPending) {
        nodes.detail.innerHTML = '<p class="empty-state">Select a recorded meeting to view audio, transcript, and minutes.</p>';
      }
    }
    if (state.initialSelectionPending && state.selectedRecordId && records.some((record) => record.record_id === state.selectedRecordId)) {
      state.initialSelectionPending = false;
      await loadRecord(state.selectedRecordId);
    }
  };

  const loadRecord = async (recordId) => {
    if (!recordId || !nodes.detail) return;
    const payload = await api(`/api/meeting-recorder/records/${encodeURIComponent(recordId)}`);
    const record = payload.record || {};
    const transcript = record.transcript || {};
    const minutes = record.minutes || {};
    const media = record.media || {};
    const transcriptChunks = Array.isArray(transcript.chunks) ? transcript.chunks.filter((chunk) => String(chunk?.text || '').trim()) : [];
    const transcriptLineCount = transcriptChunks.length || String(transcript.text || '').split(/\n+/).filter((line) => line.trim()).length;
    const isRecording = record.status === 'recording';
    const recordingUrl = isRecording ? '' : (media.audio_url || '');
    const recordingDownloadUrl = downloadUrl(recordingUrl);
    const transcriptUrl = transcript.asset_url || '';
    const transcriptDownloadUrl = downloadUrl(transcriptUrl);
    const recordDiagnostics = record.diagnostics_snapshot || {};
    const audioLabel = recordDiagnostics.audio_capture_label || state.diagnostics?.audio_capture_label || '';
    const audioInputLabel = recordDiagnostics.audio_input || '';
    const audioSummary = [audioLabel, audioInputLabel ? `input: ${audioInputLabel}` : ''].filter(Boolean).join(' · ');
    const audioPreflight = record.audio_preflight || {};
    const recordingHealth = record.recording_health || {};
    const isFailed = record.status === 'failed';
    const canProcess = ['recorded', 'failed', 'completed', 'processing'].includes(record.status);
    const canDownloadAudio = Boolean(recordingUrl) && !isFailed;
    const warningText = audioPreflight.warning || recordingHealth.warning || '';
    const showWarningText = warningText && warningText !== record.error;
    state.selectedRecordId = recordId;
    updateRecordSelection();
    nodes.detail.innerHTML = `
      <div class="meeting-detail-header">
        <div class="meeting-detail-title">
          <p class="eyebrow">${escapeHtml(platformLabel(record.platform))}</p>
          <h2>${escapeHtml(record.title || 'Untitled meeting')}</h2>
          <div class="meeting-detail-meta">
            ${formatDateTime(record.recording_started_at || record.created_at) ? `<span>${escapeHtml(formatDateTime(record.recording_started_at || record.created_at))}</span>` : ''}
            ${durationLabel(record.recording_started_at, record.recording_stopped_at) ? `<span>${escapeHtml(durationLabel(record.recording_started_at, record.recording_stopped_at))}</span>` : ''}
            <span>${escapeHtml(statusLabel(record.status))}</span>
            ${record.transcript_language_label ? `<span>${escapeHtml(record.transcript_language_label)} transcript</span>` : ''}
          </div>
        </div>
        <div class="meeting-detail-actions">
          <span class="badge badge-${escapeHtml(record.status || 'scheduled')}">${escapeHtml(statusLabel(record.status))}</span>
          ${record.status === 'recording' ? `<button class="button" type="button" data-record-stop="${escapeHtml(record.record_id)}">Stop</button>` : ''}
          ${canProcess ? `<button class="button" type="button" data-record-process="${escapeHtml(record.record_id)}">${record.status === 'processing' ? 'Check processing' : 'Process'}</button>` : ''}
          ${minutes.markdown ? `<button class="button button-secondary" type="button" data-record-email="${escapeHtml(record.record_id)}">Send Email</button>` : ''}
          <button class="button button-danger" type="button" data-record-delete="${escapeHtml(record.record_id)}">Delete</button>
        </div>
      </div>
      ${record.error ? `<div class="inline-status inline-status-error">${escapeHtml(record.error)}</div>` : ''}
      ${showWarningText ? `
        <div class="inline-status inline-status-error">
          ${escapeHtml(warningText)}
        </div>
      ` : ''}
      <section class="meeting-output">
        <div class="meeting-output-head">
          <h3>Minutes</h3>
          ${minutes.markdown ? '<span>Ready</span>' : '<span>Pending</span>'}
        </div>
        <div class="meeting-markdown">${renderMarkdown(minutes.markdown || '')}</div>
      </section>
      <section class="meeting-output">
        <div class="meeting-output-head">
          <h3>Audio Recording</h3>
          ${audioSummary ? `<span>${escapeHtml(audioSummary)}</span>` : ''}
        </div>
        ${isRecording ? `
          <p class="empty-state">Audio download will be available after stopping the recording.</p>
        ` : canDownloadAudio ? `
          <div class="button-row meeting-media-actions">
            <button class="button" type="button" data-record-download-asset="${escapeHtml(recordingDownloadUrl)}" data-download-filename="${escapeHtml(filenameFromUrl(recordingUrl, 'meeting.wav'))}" data-download-status-selector="[data-media-download-status]">Download audio file</button>
            <span class="inline-status" data-media-download-status>Downloads the meeting audio.</span>
          </div>
        ` : '<p class="empty-state">Audio is not available yet.</p>'}
      </section>
      <section class="meeting-output">
        <details class="meeting-transcript-panel">
          <summary>
            <span>
              <strong>Transcript</strong>
              <small>${escapeHtml(transcriptLineCount ? `${transcriptLineCount} segment(s)` : 'Not generated yet')}</small>
            </span>
            <span class="meeting-transcript-toggle">Open</span>
          </summary>
          <div class="meeting-transcript-tools">
            ${transcriptUrl ? `<button class="button button-secondary" type="button" data-record-download-asset="${escapeHtml(transcriptDownloadUrl)}" data-download-filename="${escapeHtml(filenameFromUrl(transcriptUrl, 'meeting-transcript.txt'))}" data-download-status-selector="[data-transcript-download-status]">Download transcript</button>` : ''}
          </div>
        ${transcriptUrl ? '<div class="inline-status" data-transcript-download-status hidden></div>' : ''}
          <div class="meeting-transcript-scroll">
            ${renderTranscript(transcript)}
          </div>
        </details>
      </section>
    `;
    bindDetailActions(record.record_id);
  };

  const pollMeetingProcessJob = async (recordId, jobId, button) => {
    const deadline = Date.now() + (60 * 60 * 1000);
    while (Date.now() < deadline) {
      let payload;
      try {
        payload = await api(`/api/meeting-recorder/process-jobs/${encodeURIComponent(jobId)}`);
      } catch (error) {
        if (!isNetworkError(error)) throw error;
        button.textContent = error.message;
        await delay(2000);
        await refreshRecordState(recordId);
        await delay(1500);
        continue;
      }
      const stateValue = String(payload.state || '').toLowerCase();
      button.textContent = meetingProcessStatusText(payload);
      if (stateValue === 'completed') {
        await loadRecord(recordId);
        await loadRecords();
        return;
      }
      if (stateValue === 'failed') {
        throw new Error(payload.error || payload.message || 'Meeting processing failed.');
      }
      await delay(1500);
    }
    throw new Error('Meeting processing is still running. Refresh this record later to check progress.');
  };

  const bindDetailActions = (recordId) => {
    nodes.detail.querySelectorAll('[data-record-stop]').forEach((button) => {
      button.addEventListener('click', async () => {
        button.disabled = true;
        await stopRecording(recordId);
      });
    });
    nodes.detail.querySelectorAll('[data-record-process]').forEach((button) => {
      button.addEventListener('click', async () => {
        button.disabled = true;
        button.textContent = 'Queueing...';
        try {
          const payload = await api(`/api/meeting-recorder/records/${encodeURIComponent(recordId)}/process`, { method: 'POST' });
          const jobId = String(payload.job_id || '').trim();
          if (!jobId) {
            await loadRecord(recordId);
            await loadRecords();
            return;
          }
          await pollMeetingProcessJob(recordId, jobId, button);
        } catch (error) {
          if (isNetworkError(error)) {
            button.textContent = error.message;
            const refreshed = await refreshRecordState(recordId);
            if (!refreshed) {
              button.disabled = false;
            }
            return;
          }
          button.textContent = error.message;
        }
      });
    });
    nodes.detail.querySelectorAll('[data-record-email]').forEach((button) => {
      button.addEventListener('click', async () => {
        button.disabled = true;
        try {
          await api(`/api/meeting-recorder/records/${encodeURIComponent(recordId)}/send-email`, {
            method: 'POST',
            body: JSON.stringify({}),
          });
          button.textContent = 'Email sent';
        } catch (error) {
          button.textContent = error.message;
        }
      });
    });
    nodes.detail.querySelectorAll('[data-record-delete]').forEach((button) => {
      button.addEventListener('click', async () => {
        button.disabled = true;
        await api(`/api/meeting-recorder/records/${encodeURIComponent(recordId)}`, { method: 'DELETE' });
        nodes.detail.innerHTML = '<p class="empty-state">Select a recorded meeting to view audio, transcript, and minutes.</p>';
        await loadRecords();
      });
    });
    nodes.detail.querySelectorAll('[data-record-download-asset]').forEach((button) => {
      button.addEventListener('click', async () => {
        const status = nodes.detail.querySelector(button.dataset.downloadStatusSelector || '[data-media-download-status]');
        const originalText = button.textContent;
        button.disabled = true;
        button.textContent = 'Preparing download...';
        if (status) {
          status.hidden = false;
          status.textContent = 'Checking file...';
          status.classList.remove('inline-status-error');
        }
        try {
          const response = await fetch(button.dataset.recordDownloadAsset || '', { credentials: 'same-origin' });
          const contentType = response.headers.get('Content-Type') || '';
          if (!response.ok) throw new Error(`Download failed with HTTP ${response.status}.`);
          if (contentType.includes('text/html')) {
            throw new Error('Download returned an HTML page instead of the requested file. Refresh the page and sign in again, then retry.');
          }
          const blob = await response.blob();
          if (!blob.size) throw new Error('Downloaded file is empty.');
          const filename = filenameFromDisposition(response.headers.get('Content-Disposition'))
            || button.dataset.downloadFilename
            || 'meeting-download';
          const objectUrl = URL.createObjectURL(blob);
          const link = document.createElement('a');
          link.href = objectUrl;
          link.download = filename;
          document.body.appendChild(link);
          link.click();
          link.remove();
          window.setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
          if (status) status.textContent = `Download started: ${filename}`;
        } catch (error) {
          if (status) {
            status.textContent = error.message || 'Could not download audio.';
            status.classList.add('inline-status-error');
          }
        } finally {
          button.disabled = false;
          button.textContent = originalText;
        }
      });
    });
  };

  const formatTimestamp = (seconds) => {
    const safe = Math.max(0, Number(seconds) || 0);
    const mins = Math.floor(safe / 60);
    const secs = Math.floor(safe % 60);
    return `${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
  };

  nodes.refresh?.addEventListener('click', loadUpcoming);
  nodes.recordsRefresh?.addEventListener('click', loadRecords);
  if (nodes.recordDate) {
    nodes.recordDate.value = localDateValue();
    nodes.recordDate.addEventListener('change', () => {
      state.initialSelectionPending = false;
      state.selectedRecordId = '';
      renderRecordCalendar(parseLocalDateValue(nodes.recordDate.value));
      if (nodes.detail) {
        nodes.detail.innerHTML = '<p class="empty-state">Select a recorded meeting to view audio, transcript, and minutes.</p>';
      }
      loadRecords();
    });
  }
  nodes.recordDateToggle?.addEventListener('click', (event) => {
    event.preventDefault();
    toggleRecordCalendar();
  });
  nodes.recordCalendar?.addEventListener('click', (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const viewDate = parseLocalDateValue(nodes.recordCalendar.dataset.viewMonth || nodes.recordDate?.value);
    if (target.matches('[data-meeting-calendar-prev]')) {
      renderRecordCalendar(new Date(viewDate.getFullYear(), viewDate.getMonth() - 1, 1));
      return;
    }
    if (target.matches('[data-meeting-calendar-next]')) {
      renderRecordCalendar(new Date(viewDate.getFullYear(), viewDate.getMonth() + 1, 1));
      return;
    }
    const value = target.dataset.meetingCalendarDay;
    if (value && nodes.recordDate) {
      nodes.recordDate.value = value;
      hideRecordCalendar();
      nodes.recordDate.dispatchEvent(new Event('change', { bubbles: true }));
    }
  });
  document.addEventListener('click', (event) => {
    if (!nodes.recordCalendar || nodes.recordCalendar.hidden) return;
    const target = event.target;
    if (target instanceof Node && (
      nodes.recordCalendar.contains(target)
      || nodes.recordDateToggle?.contains(target)
      || nodes.recordDate?.contains(target)
    )) {
      return;
    }
    hideRecordCalendar();
  });
  nodes.startForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    const submitButton = nodes.startForm.querySelector('button[type="submit"]');
    const originalText = submitButton?.textContent || 'Start Recording';
    if (submitButton?.disabled) return;
    const data = new FormData(nodes.startForm);
    if (submitButton) {
      submitButton.disabled = true;
      submitButton.textContent = 'Starting...';
    }
    if (nodes.recordingStatus) {
      nodes.recordingStatus.textContent = 'Checking microphone/audio input...';
    }
    try {
      await startRecording({
        title: data.get('title') || 'Untitled meeting',
        meeting_link: '',
        recording_mode: 'audio_only',
        transcript_language: data.get('transcript_language') || selectedTranscriptLanguage(),
      });
    } catch (error) {
      nodes.recordingStatus.textContent = error.message;
    } finally {
      if (submitButton) {
        submitButton.disabled = false;
        submitButton.textContent = originalText;
      }
    }
  });

  loadDiagnostics();
  loadUpcoming();
  loadRecords({ restoreActive: true });
})();
