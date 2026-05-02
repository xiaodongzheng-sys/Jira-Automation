(() => {
  const root = document.querySelector('[data-meeting-recorder-root]');
  if (!root) return;

  const state = {
    activeRecordId: '',
    selectedRecordId: root.dataset.selectedRecordId || '',
    initialSelectionPending: Boolean(root.dataset.selectedRecordId),
    diagnostics: null,
  };

  const nodes = {
    diagnostic: root.querySelector('[data-meeting-recorder-diagnostic]'),
    upcoming: root.querySelector('[data-meeting-upcoming]'),
    calendarStatus: root.querySelector('[data-meeting-calendar-status]'),
    refresh: root.querySelector('[data-meeting-refresh]'),
    startForm: root.querySelector('[data-meeting-start-form]'),
    stopCurrent: root.querySelector('[data-meeting-stop-current]'),
    recordingStatus: root.querySelector('[data-meeting-recording-status]'),
    records: root.querySelector('[data-meeting-records]'),
    recordsRefresh: root.querySelector('[data-meeting-records-refresh]'),
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

  const filenameFromUrl = (url, fallback = 'meeting-recording.mp4') => {
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

  const api = async (url, options = {}) => {
    const response = await fetch(url, {
      headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
      ...options,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.message || 'Request failed.');
    return payload;
  };

  const telemetry = (event, data = {}) => {
    fetch('/api/meeting-recorder/reminder-telemetry', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        event,
        page_path: window.location.pathname,
        ...data,
      }),
    }).catch(() => {});
  };

  const platformLabel = (platform) => {
    if (platform === 'google_meet') return 'Google Meet';
    if (platform === 'zoom') return 'Zoom';
    return 'Meeting';
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
    return new Intl.DateTimeFormat(undefined, {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    }).format(date);
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

  const stripScreenEvidenceSection = (markdown) => String(markdown || '')
    .replace(/(?:^|\n)(?:#{1,6}\s*)?Screen Evidence\s*\n[\s\S]*?(?=\n(?:#{1,6}\s*\S|\*\*[^*\n]+\*\*)|\s*$)/gi, '\n')
    .replace(/(?:^|\n)\*\*Screen Evidence\*\*\s*\n[\s\S]*?(?=\n\*\*[^*\n]+\*\*|\s*$)/gi, '\n')
    .trim();

  const renderInlineMarkdown = (value) => escapeHtml(value)
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/`([^`]+)`/g, '<code>$1</code>');

  const renderMarkdown = (markdown) => {
    const cleaned = stripScreenEvidenceSection(markdown);
    if (!cleaned) return '<p class="empty-state">Minutes are not generated yet.</p>';
    const lines = cleaned.split(/\r?\n/);
    const html = [];
    let listOpen = false;
    const closeList = () => {
      if (listOpen) {
        html.push('</ul>');
        listOpen = false;
      }
    };
    lines.forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed) {
        closeList();
        return;
      }
      const heading = trimmed.match(/^(?:#{1,6}\s+|\*\*)([^*#].*?)(?:\*\*)?$/);
      if (heading && !trimmed.startsWith('- ') && !trimmed.startsWith('* ')) {
        closeList();
        html.push(`<h4>${renderInlineMarkdown(heading[1].trim())}</h4>`);
        return;
      }
      const bullet = trimmed.match(/^[-*]\s+(.+)$/);
      if (bullet) {
        if (!listOpen) {
          html.push('<ul>');
          listOpen = true;
        }
        html.push(`<li>${renderInlineMarkdown(bullet[1])}</li>`);
        return;
      }
      closeList();
      html.push(`<p>${renderInlineMarkdown(trimmed)}</p>`);
    });
    closeList();
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
        ].filter(Boolean).join(' · ') || 'No quality warnings')}</span>
        ${warnings.length || segmentTags.length ? `<ul>${[...warnings, ...segmentTags].map((item) => `<li>${escapeHtml(item)}</li>`).join('')}</ul>` : ''}
      </div>
    `;
  };

  const renderTranscript = (transcript) => {
    const qualityMarkup = renderTranscriptQuality(transcript);
    const chunks = Array.isArray(transcript?.chunks) ? transcript.chunks.filter((chunk) => String(chunk?.text || '').trim()) : [];
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
      `;
    }
    const text = String(transcript?.text || '').trim();
    if (!text) return `${qualityMarkup}<p class="empty-state">Transcript is not generated yet.</p>`;
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
    const videoConfig = [
      payload.video_input || 'not configured',
      payload.video_max_width && payload.video_max_height ? `${payload.video_max_width}x${payload.video_max_height}` : '',
      payload.video_fps ? `${payload.video_fps}fps` : '',
    ].filter(Boolean).join(' · ');
    return `
      <div class="meeting-diagnostic-head">
        <span class="meeting-diagnostic-kicker">Recorder status</span>
        <span class="meeting-diagnostic-pill ${recorderReady ? 'is-ready' : 'is-warning'}">${escapeHtml(recorderReady ? 'Ready' : 'Needs setup')}</span>
      </div>
      <div class="meeting-diagnostic-grid">
        <div class="meeting-diagnostic-item">
          <span class="meeting-diagnostic-dot ${payload.ffmpeg_configured ? 'is-ready' : 'is-warning'}"></span>
          <div>
            <strong>Screen capture</strong>
            <span>${escapeHtml(videoConfig)}</span>
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
            <strong>Meeting audio</strong>
            <span>${escapeHtml(audioLabel)} · ${escapeHtml(payload.audio_input || 'not configured')}</span>
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
    if (nodes.stopCurrent) nodes.stopCurrent.disabled = !state.activeRecordId;
    if (nodes.recordingStatus) {
      nodes.recordingStatus.textContent = state.activeRecordId
        ? `Recording: ${record.title || 'Untitled meeting'}`
        : 'No active recording.';
    }
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
    const payload = await api('/api/meeting-recorder/start', {
      method: 'POST',
      body: JSON.stringify(meeting),
    });
    setRecordingState(payload.record);
    await loadRecords();
  };

  const stopRecording = async (recordId) => {
    const payload = await api(`/api/meeting-recorder/records/${encodeURIComponent(recordId)}/stop`, { method: 'POST' });
    setRecordingState(null);
    await loadRecords();
    await loadRecord(payload.record.record_id);
  };

  const loadUpcoming = async () => {
    if (!nodes.upcoming) return;
    nodes.calendarStatus.textContent = 'Loading upcoming Meet and Zoom meetings…';
    try {
      const payload = await api('/api/meeting-recorder/calendar/upcoming');
      const meetings = Array.isArray(payload.meetings) ? payload.meetings : [];
      nodes.calendarStatus.textContent = meetings.length ? `${meetings.length} upcoming meeting(s).` : 'No upcoming Meet or Zoom meetings found.';
      nodes.upcoming.innerHTML = meetings.map((meeting, index) => `
        <article class="meeting-list-item">
          <div>
            <strong>${escapeHtml(meeting.title || 'Untitled meeting')}</strong>
            <span>${escapeHtml(platformLabel(meeting.platform))} · ${escapeHtml(meeting.start || '')}</span>
          </div>
          <button class="button button-secondary" type="button" data-meeting-start-index="${index}">Start</button>
        </article>
      `).join('');
      nodes.upcoming.querySelectorAll('[data-meeting-start-index]').forEach((button) => {
        button.addEventListener('click', async () => {
          const meeting = meetings[Number(button.dataset.meetingStartIndex) || 0];
          button.disabled = true;
          try {
            await startRecording({
              title: meeting.title,
              platform: meeting.platform,
              meeting_link: meeting.meeting_link,
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

  const loadRecords = async () => {
    if (!nodes.records) return;
    const payload = await api('/api/meeting-recorder/records');
    const records = Array.isArray(payload.records) ? payload.records : [];
    if (!records.length) {
      nodes.records.innerHTML = '<p class="empty-state">No meeting recordings yet.</p>';
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
      button.addEventListener('click', () => loadRecord(button.dataset.recordId || ''));
    });
    if (state.initialSelectionPending && state.selectedRecordId) {
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
    const videoUrl = media.playback_video_url || media.video_url || '';
    const videoDownloadUrl = downloadUrl(videoUrl);
    const usingPlaybackCopy = Boolean(media.playback_video_url);
    const visualEvidence = Array.isArray(record.visual_evidence) ? record.visual_evidence : [];
    const audioLabel = state.diagnostics?.audio_capture_label || '';
    const audioPreflight = record.audio_preflight || {};
    const recordingHealth = record.recording_health || {};
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
          </div>
        </div>
        <div class="meeting-detail-actions">
          <span class="badge badge-${escapeHtml(record.status || 'scheduled')}">${escapeHtml(statusLabel(record.status))}</span>
          ${record.status === 'recording' ? `<button class="button" type="button" data-record-stop="${escapeHtml(record.record_id)}">Stop</button>` : ''}
          ${record.status === 'recorded' || record.status === 'failed' ? `<button class="button" type="button" data-record-process="${escapeHtml(record.record_id)}">Process</button>` : ''}
          ${minutes.markdown ? `<button class="button button-secondary" type="button" data-record-email="${escapeHtml(record.record_id)}">Send Email</button>` : ''}
          <button class="button button-danger" type="button" data-record-delete="${escapeHtml(record.record_id)}">Delete</button>
        </div>
      </div>
      ${record.error ? `<div class="inline-status inline-status-error">${escapeHtml(record.error)}</div>` : ''}
      ${audioPreflight.warning || recordingHealth.warning ? `
        <div class="inline-status inline-status-error">
          ${escapeHtml(audioPreflight.warning || recordingHealth.warning)}
        </div>
      ` : ''}
      <section class="meeting-output">
        <h3>Minutes</h3>
        <div class="meeting-markdown">${renderMarkdown(minutes.markdown || '')}</div>
      </section>
      <section class="meeting-output">
        <div class="meeting-output-head">
          <h3>Screen Recording</h3>
          ${audioLabel ? `<span>${escapeHtml(audioLabel)}</span>` : ''}
        </div>
        ${videoUrl ? `
          <div class="button-row meeting-video-actions">
            <button class="button" type="button" data-record-download-video="${escapeHtml(videoDownloadUrl)}" data-download-filename="${escapeHtml(filenameFromUrl(videoUrl))}">Download video file</button>
            <span class="inline-status" data-video-download-status>${usingPlaybackCopy ? 'Downloads the browser-compatible playback copy.' : 'Downloads the original recording for local playback.'}</span>
          </div>
        ` : '<p class="empty-state">Video is not available yet.</p>'}
        ${videoUrl ? `
          <div class="button-row meeting-video-actions">
            <button class="button button-secondary" type="button" data-record-repair-video="${escapeHtml(record.record_id)}">
              ${usingPlaybackCopy ? 'Rebuild downloadable copy' : 'Build downloadable playback copy'}
            </button>
          </div>
        ` : ''}
        ${visualEvidence.length ? `
          <div class="meeting-snapshots">
            ${visualEvidence.map((item) => `
              <a href="${escapeHtml(item.image_url || '#')}" target="_blank" rel="noreferrer">
                <span>${escapeHtml(formatTimestamp(item.timestamp_seconds || 0))}</span>
                <small>${escapeHtml((item.summary || 'Video snapshot').replace(/^Screen keyframe/i, 'Video snapshot'))}</small>
              </a>
            `).join('')}
          </div>
        ` : ''}
      </section>
      <section class="meeting-output">
        <h3>Transcript</h3>
        ${renderTranscript(transcript)}
      </section>
    `;
    bindDetailActions(record.record_id);
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
        button.textContent = 'Processing…';
        try {
          await api(`/api/meeting-recorder/records/${encodeURIComponent(recordId)}/process`, { method: 'POST' });
          await loadRecord(recordId);
          await loadRecords();
        } catch (error) {
          button.textContent = error.message;
        }
      });
    });
    nodes.detail.querySelectorAll('[data-record-repair-video]').forEach((button) => {
      button.addEventListener('click', async () => {
        button.disabled = true;
        const originalText = button.textContent;
        button.textContent = 'Repairing…';
        try {
          await api(`/api/meeting-recorder/records/${encodeURIComponent(recordId)}/repair-video`, { method: 'POST' });
          await loadRecord(recordId);
          await loadRecords();
        } catch (error) {
          button.disabled = false;
          button.textContent = error.message || originalText;
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
        nodes.detail.innerHTML = '<p class="empty-state">Select a recorded meeting to view transcript, minutes, and video.</p>';
        await loadRecords();
      });
    });
    nodes.detail.querySelectorAll('[data-record-download-video]').forEach((button) => {
      button.addEventListener('click', async () => {
        const status = nodes.detail.querySelector('[data-video-download-status]');
        const originalText = button.textContent;
        button.disabled = true;
        button.textContent = 'Preparing download...';
        if (status) {
          status.textContent = 'Checking video file...';
          status.classList.remove('inline-status-error');
        }
        try {
          const response = await fetch(button.dataset.recordDownloadVideo || '', { credentials: 'same-origin' });
          const contentType = response.headers.get('Content-Type') || '';
          if (!response.ok) throw new Error(`Download failed with HTTP ${response.status}.`);
          if (contentType.includes('text/html')) {
            throw new Error('Download returned an HTML page instead of a video file. Refresh the page and sign in again, then retry.');
          }
          const blob = await response.blob();
          if (!blob.size) throw new Error('Downloaded video file is empty.');
          const filename = filenameFromDisposition(response.headers.get('Content-Disposition'))
            || button.dataset.downloadFilename
            || 'meeting-recording.mp4';
          const objectUrl = URL.createObjectURL(blob);
          const link = document.createElement('a');
          link.href = objectUrl;
          link.download = filename;
          document.body.appendChild(link);
          link.click();
          link.remove();
          window.setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
          if (status) status.textContent = `Download started: ${filename}`;
          telemetry('video_download_started', { outcome: 'ok', reason: recordId || '' });
        } catch (error) {
          if (status) {
            status.textContent = error.message || 'Could not download video.';
            status.classList.add('inline-status-error');
          }
          telemetry('video_download_failed', { outcome: 'error', reason: recordId || '', error_message: error.message || '' });
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
  nodes.stopCurrent?.addEventListener('click', () => {
    if (state.activeRecordId) stopRecording(state.activeRecordId);
  });
  nodes.startForm?.addEventListener('submit', async (event) => {
    event.preventDefault();
    const data = new FormData(nodes.startForm);
    try {
      await startRecording({
        title: data.get('title') || 'Untitled meeting',
        meeting_link: data.get('meeting_link') || '',
      });
    } catch (error) {
      nodes.recordingStatus.textContent = error.message;
    }
  });

  loadDiagnostics();
  loadUpcoming();
  loadRecords();
})();
