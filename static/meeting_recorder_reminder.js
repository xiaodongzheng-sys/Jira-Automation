(() => {
  const POLL_FALLBACK_MS = 60000;
  const STORAGE_PREFIX = 'meeting-recorder-reminders:v1';
  let visibleMeeting = null;
  let polling = false;

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
      return {};
    }
  };

  const markSuppressed = (key, reason) => {
    if (!key) return;
    const suppressed = readSuppressed();
    suppressed[key] = { reason, at: new Date().toISOString() };
    try {
      window.localStorage.setItem(storageDateKey(), JSON.stringify(suppressed));
    } catch (_error) {
      // localStorage can be unavailable in private browsing; reminders still work for the current page.
    }
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

  const removeReminder = () => {
    document.querySelector('[data-meeting-reminder-backdrop]')?.remove();
    visibleMeeting = null;
  };

  const renderReminder = (meeting, diagnostics) => {
    const audioLabel = diagnostics?.audio_capture_label || 'Audio status unknown';
    const audioReady = diagnostics?.system_audio_configured;
    removeReminder();
    visibleMeeting = meeting;
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
      });
    });
    backdrop.addEventListener('click', (event) => {
      if (event.target === backdrop) {
        markSuppressed(meeting.suppression_key, 'dismissed');
        removeReminder();
      }
    });
    backdrop.querySelector('[data-meeting-reminder-start]')?.addEventListener('click', async (event) => {
      const button = event.currentTarget;
      button.disabled = true;
      status.textContent = 'Starting recording...';
      try {
        const payload = await api('/api/meeting-recorder/start', {
          method: 'POST',
          body: JSON.stringify({
            title: meeting.title,
            platform: meeting.platform,
            meeting_link: meeting.meeting_link,
            calendar_event_id: meeting.calendar_event_id,
            scheduled_start: meeting.start,
            scheduled_end: meeting.end,
            attendees: meeting.attendees || [],
          }),
        });
        markSuppressed(meeting.suppression_key, 'started');
        status.textContent = `Recording started: ${payload.record?.title || meeting.title || 'Meeting'}`;
        window.setTimeout(removeReminder, 1800);
      } catch (error) {
        button.disabled = false;
        status.textContent = error.message || 'Could not start recording.';
        status.classList.add('inline-status-error');
      }
    });
  };

  const pollReminders = async () => {
    if (polling) return;
    polling = true;
    try {
      const payload = await api('/api/meeting-recorder/reminders');
      const meetings = Array.isArray(payload.meetings) ? payload.meetings : [];
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
    } catch (_error) {
      // Global reminders should not interrupt other portal workflows when the calendar/local-agent path is unavailable.
    } finally {
      polling = false;
    }
  };

  const startPolling = async () => {
    await pollReminders();
    window.setInterval(pollReminders, POLL_FALLBACK_MS);
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', startPolling, { once: true });
  } else {
    startPolling();
  }
})();
