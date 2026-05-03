(() => {
  const POLL_FALLBACK_MS = 60000;
  const STORAGE_PREFIX = 'meeting-recorder-reminders:v1';
  let visibleMeeting = null;
  let polling = false;
  let lastPayload = null;
  let lastError = null;

  const nodes = {
    indicator: document.querySelector('[data-meeting-recorder-indicator]'),
    indicatorDot: document.querySelector('[data-meeting-recorder-indicator-dot]'),
    indicatorLabel: document.querySelector('[data-meeting-recorder-indicator-label]'),
    indicatorDetail: document.querySelector('[data-meeting-recorder-indicator-detail]'),
    indicatorPoll: document.querySelector('[data-meeting-recorder-indicator-poll]'),
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
    if (!response.ok) {
      const error = new Error(payload.message || 'Request failed.');
      error.payload = payload;
      throw error;
    }
    return payload;
  };

  const telemetry = (event, data = {}) => {
    fetch('/api/meeting-recorder/reminder-telemetry', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        event,
        page_path: window.location.pathname,
        suppressed_count: Object.keys(readSuppressed()).length,
        ...data,
      }),
    }).catch(() => {});
  };

  const storageDateKey = () => {
    const parts = new Intl.DateTimeFormat('en', {
      timeZone: 'Asia/Singapore',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).formatToParts(new Date());
    const lookup = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return `${STORAGE_PREFIX}:${lookup.year}-${lookup.month}-${lookup.day}`;
  };

  const readSuppressed = () => {
    try {
      const raw = window.localStorage.getItem(storageDateKey());
      const parsed = raw ? JSON.parse(raw) : {};
      return parsed && typeof parsed === 'object' ? parsed : {};
    } catch (_error) {
      return { '__storage_unavailable__': { reason: 'localStorage unavailable', at: new Date().toISOString() } };
    }
  };

  const markSuppressed = (key, reason) => {
    if (!key) return;
    const suppressed = readSuppressed();
    suppressed[key] = { reason, at: new Date().toISOString() };
    try {
      window.localStorage.setItem(storageDateKey(), JSON.stringify(suppressed));
    } catch (_error) {
      // Indicator debug will show storage unavailable through readSuppressed().
    }
    telemetry('suppressed', { reason, outcome: key });
  };

  const isSuppressed = (meeting) => Boolean(readSuppressed()[meeting?.suppression_key || '']);

  const sameEvent = (meeting, record) => {
    if (!meeting || !record) return false;
    const meetingEventId = String(meeting.calendar_event_id || '').trim();
    const recordEventId = String(record.calendar_event_id || '').trim();
    if (meetingEventId && recordEventId && meetingEventId === recordEventId) return true;
    const meetingLink = String(meeting.meeting_link || '').trim();
    const recordLink = String(record.meeting_link || '').trim();
    return Boolean(meetingLink && recordLink && meetingLink === recordLink);
  };

  const platformLabel = (platform) => {
    if (platform === 'google_meet') return 'Google Meet';
    if (platform === 'zoom') return 'Zoom';
    return 'Meeting';
  };

  const formatStart = (value) => {
    const date = new Date(value || '');
    if (Number.isNaN(date.getTime())) return value || '';
    return new Intl.DateTimeFormat(undefined, {
      hour: '2-digit',
      minute: '2-digit',
      month: 'short',
      day: 'numeric',
    }).format(date);
  };

  const timeCopy = (secondsUntilStart) => {
    const seconds = Number(secondsUntilStart) || 0;
    if (seconds >= 60) return `Starts in ${Math.ceil(seconds / 60)} min`;
    if (seconds >= 0) return 'Starting now';
    const lateMinutes = Math.ceil(Math.abs(seconds) / 60);
    return lateMinutes <= 1 ? 'Started 1 min ago' : `Started ${lateMinutes} min ago`;
  };

  const indicatorState = (label, state) => {
    if (!nodes.indicator) return;
    nodes.indicator.dataset.state = state;
    if (nodes.indicatorLabel) nodes.indicatorLabel.textContent = label;
  };

  const updateIndicator = (payload, error = null) => {
    lastPayload = payload || lastPayload;
    lastError = error;
    const suppressed = readSuppressed();
    const meetings = Array.isArray(payload?.meetings) ? payload.meetings : [];
    const nextMeeting = meetings.find((meeting) => !isSuppressed(meeting));
    if (error) {
      indicatorState('Reminder failed', 'error');
    } else if (payload?.active_recording) {
      indicatorState('Recording', 'recording');
    } else if (nextMeeting) {
      indicatorState('Meeting found', 'found');
    } else {
      indicatorState(payload?.calendar_connected === false ? 'Calendar not connected' : 'Watching calendar', 'watching');
    }
    if (!nodes.indicatorDetail) return;
    const debug = payload?.debug || {};
    nodes.indicatorDetail.innerHTML = `
      <dl class="meeting-recorder-indicator-debug">
        <div><dt>Last poll</dt><dd>${escapeHtml(debug.checked_at || new Date().toISOString())}</dd></div>
        <div><dt>Reason</dt><dd>${escapeHtml(debug.reason || (error ? 'api_error' : 'unknown'))}</dd></div>
        <div><dt>Calendar</dt><dd>${escapeHtml(String(Boolean(payload?.calendar_connected)))}</dd></div>
        <div><dt>Eligible</dt><dd>${escapeHtml(String(meetings.length))}</dd></div>
        <div><dt>Active</dt><dd>${escapeHtml(payload?.active_recording?.title || (payload?.active_recording ? 'Recording' : 'No'))}</dd></div>
        <div><dt>Suppressed</dt><dd>${escapeHtml(Object.entries(suppressed).map(([key, value]) => `${key}: ${value?.reason || 'set'}`).join(' | ') || 'None')}</dd></div>
        <div><dt>Audio</dt><dd>${escapeHtml(debug.diagnostics?.audio_capture_label || payload?.diagnostics?.audio_capture_label || 'Unknown')}</dd></div>
        <div><dt>Error</dt><dd>${escapeHtml(error?.message || debug.error_code || 'None')}</dd></div>
      </dl>
    `;
  };

  const removeReminder = () => {
    document.querySelector('[data-meeting-reminder-backdrop]')?.remove();
    visibleMeeting = null;
  };

  const renderReminder = (meeting, diagnostics) => {
    const audioLabel = diagnostics?.audio_capture_label || 'Audio status unknown';
    const audioReady = diagnostics?.system_audio_configured;
    removeReminder();
    visibleMeeting = meeting;
    telemetry('reminder_rendered', { meeting_count: 1, reason: meeting.suppression_key || '' });
    document.body.insertAdjacentHTML('beforeend', `
      <div class="meeting-reminder-backdrop" data-meeting-reminder-backdrop>
        <section class="meeting-reminder-card" role="dialog" aria-modal="true" aria-labelledby="meeting-reminder-title">
          <button class="notice-close" type="button" aria-label="Dismiss" data-meeting-reminder-dismiss>×</button>
          <p class="notice-eyebrow">Meeting Recorder</p>
          <h2 id="meeting-reminder-title">Record this meeting?</h2>
          <p class="meeting-reminder-title">${escapeHtml(meeting.title || 'Untitled meeting')}</p>
          <div class="meeting-reminder-meta">
            <span>${escapeHtml(platformLabel(meeting.platform))}</span>
            <span>${escapeHtml(formatStart(meeting.start))}</span>
            <span>${escapeHtml(timeCopy(meeting.seconds_until_start))}</span>
          </div>
          <div class="meeting-reminder-audio ${audioReady ? 'is-ready' : 'is-warning'}">
            ${escapeHtml(audioLabel)}
          </div>
          <div class="button-row meeting-reminder-actions">
            <button class="button" type="button" data-meeting-reminder-start>Start recording</button>
            <a class="button button-secondary" href="/meeting-recorder">Open Recorder</a>
            <button class="button button-secondary" type="button" data-meeting-reminder-dismiss>Dismiss</button>
          </div>
          <div class="inline-status" data-meeting-reminder-status></div>
        </section>
      </div>
    `);
    const backdrop = document.querySelector('[data-meeting-reminder-backdrop]');
    const status = backdrop.querySelector('[data-meeting-reminder-status]');
    backdrop.querySelectorAll('[data-meeting-reminder-dismiss]').forEach((node) => {
      node.addEventListener('click', () => {
        markSuppressed(meeting.suppression_key, 'dismissed');
        removeReminder();
        updateIndicator(lastPayload, lastError);
      });
    });
    backdrop.addEventListener('click', (event) => {
      if (event.target === backdrop) {
        markSuppressed(meeting.suppression_key, 'dismissed');
        removeReminder();
        updateIndicator(lastPayload, lastError);
      }
    });
    backdrop.querySelector('[data-meeting-reminder-start]')?.addEventListener('click', async (event) => {
      const button = event.currentTarget;
      button.disabled = true;
      status.textContent = 'Starting recording...';
      telemetry('start_clicked', { reason: meeting.suppression_key || '' });
      try {
        const payload = await api('/api/meeting-recorder/start', {
          method: 'POST',
          body: JSON.stringify({
            title: meeting.title,
            platform: meeting.platform,
            meeting_link: meeting.meeting_link,
            recording_mode: 'audio_only',
            calendar_event_id: meeting.calendar_event_id,
            scheduled_start: meeting.start,
            scheduled_end: meeting.end,
            attendees: meeting.attendees || [],
          }),
        });
        markSuppressed(meeting.suppression_key, 'started');
        telemetry('start_success', { active_recording: true, reason: meeting.suppression_key || '' });
        status.textContent = `Recording started: ${payload.record?.title || meeting.title || 'Meeting'}`;
        window.setTimeout(removeReminder, 1800);
        pollReminders('manual');
      } catch (error) {
        button.disabled = false;
        telemetry('start_failed', { error_message: error.message || '', error_category: error.payload?.debug?.error_category || '' });
        status.textContent = error.message || 'Could not start recording.';
        status.classList.add('inline-status-error');
      }
    });
  };

  const pollReminders = async (reason = 'interval') => {
    if (polling) return;
    polling = true;
    indicatorState('Checking', 'checking');
    telemetry('poll_started', { reason });
    try {
      const payload = await api('/api/meeting-recorder/reminders');
      const meetings = Array.isArray(payload.meetings) ? payload.meetings : [];
      updateIndicator(payload);
      telemetry('poll_success', {
        reason,
        outcome: payload.debug?.reason || '',
        meeting_count: meetings.length,
        active_recording: Boolean(payload.active_recording),
      });
      if (!payload.calendar_connected || payload.active_recording) {
        meetings.filter((meeting) => sameEvent(meeting, payload.active_recording)).forEach((meeting) => {
          markSuppressed(meeting.suppression_key, 'started');
        });
        if (payload.active_recording) removeReminder();
        return;
      }
      const nextMeeting = meetings.find((meeting) => !isSuppressed(meeting));
      if (!nextMeeting) {
        if (visibleMeeting && isSuppressed(visibleMeeting)) removeReminder();
        return;
      }
      if (!visibleMeeting || visibleMeeting.suppression_key !== nextMeeting.suppression_key) {
        renderReminder(nextMeeting, payload.diagnostics || {});
      }
    } catch (error) {
      updateIndicator(lastPayload || {}, error);
      telemetry('poll_failed', {
        reason,
        error_message: error.message || '',
        error_category: error.payload?.debug?.error_category || 'api_error',
      });
    } finally {
      polling = false;
    }
  };

  const startPolling = async () => {
    telemetry('script_loaded', { outcome: nodes.indicator ? 'indicator_present' : 'indicator_missing' });
    await pollReminders('initial');
    window.setInterval(() => pollReminders('interval'), POLL_FALLBACK_MS);
  };

  nodes.indicatorPoll?.addEventListener('click', () => pollReminders('manual'));
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') pollReminders('visible');
  });
  window.addEventListener('focus', () => pollReminders('focus'));

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', startPolling, { once: true });
  } else {
    startPolling();
  }
})();
