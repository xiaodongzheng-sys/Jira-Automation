(() => {
  const root = document.querySelector('[data-meeting-recorder-root]');
  if (!root) return;

  const state = {
    activeRecordId: '',
    selectedRecordId: root.dataset.selectedRecordId || '',
    initialSelectionPending: Boolean(root.dataset.selectedRecordId),
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

  const api = async (url, options = {}) => {
    const response = await fetch(url, {
      headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
      ...options,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.message || 'Request failed.');
    return payload;
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

  const renderTranscript = (transcript) => {
    const chunks = Array.isArray(transcript?.chunks) ? transcript.chunks.filter((chunk) => String(chunk?.text || '').trim()) : [];
    if (chunks.length) {
      return `
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
    if (!text) return '<p class="empty-state">Transcript is not generated yet.</p>';
    return `
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
      nodes.diagnostic.textContent = payload.ffmpeg_configured
        ? `Local recorder ready: ${payload.ffmpeg_path}`
        : 'ffmpeg is not configured. Install ffmpeg before recording.';
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
    const videoUrl = record.media?.video_url || '';
    const visualEvidence = Array.isArray(record.visual_evidence) ? record.visual_evidence : [];
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
      <section class="meeting-output">
        <h3>Minutes</h3>
        <div class="meeting-markdown">${renderMarkdown(minutes.markdown || '')}</div>
      </section>
      <section class="meeting-output">
        <h3>Screen Recording</h3>
        ${videoUrl ? `<video controls class="meeting-video" src="${escapeHtml(videoUrl)}"></video>` : '<p class="empty-state">Video is not available yet.</p>'}
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
