(() => {
  const root = document.querySelector('[data-meeting-recorder-root]');
  if (!root) return;

  const state = {
    activeRecordId: '',
    selectedRecordId: root.dataset.selectedRecordId || '',
    initialSelectionPending: Boolean(root.dataset.selectedRecordId),
    diagnostics: null,
    browserRecording: null,
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

  const browserAudioMimeType = () => {
    const candidates = [
      'audio/webm;codecs=opus',
      'audio/webm',
      'audio/mp4',
      'audio/ogg;codecs=opus',
    ];
    return candidates.find((candidate) => window.MediaRecorder?.isTypeSupported?.(candidate)) || '';
  };

  const browserAudioSupportSnapshot = () => ({
    get_user_media_supported: Boolean(navigator.mediaDevices?.getUserMedia),
    get_display_media_supported: Boolean(navigator.mediaDevices?.getDisplayMedia),
    media_recorder_supported: Boolean(window.MediaRecorder),
    selected_mime_type: browserAudioMimeType(),
    protocol: window.location.protocol,
    host: window.location.host,
  });

  const isVirtualAudioInputLabel = (label) => {
    const normalized = String(label || '').toLowerCase();
    return [
      'blackhole',
      'meeting recorder',
      'aggregate',
      'multi-output',
      'soundflower',
      'loopback',
    ].some((needle) => normalized.includes(needle));
  };

  const audioInputDevices = async () => {
    if (!navigator.mediaDevices?.enumerateDevices) return [];
    try {
      return (await navigator.mediaDevices.enumerateDevices())
        .filter((device) => device.kind === 'audioinput');
    } catch (_error) {
      return [];
    }
  };

  const choosePreferredAudioInput = (devices, currentLabel = '') => {
    const inputs = Array.isArray(devices) ? devices.filter((device) => device.kind === 'audioinput') : [];
    const realInputs = inputs.filter((device) => !isVirtualAudioInputLabel(device.label));
    const currentLooksReal = currentLabel && !isVirtualAudioInputLabel(currentLabel);
    if (currentLooksReal) return null;
    const priority = [
      /macbook.*microphone/i,
      /built-?in.*microphone/i,
      /airpods|headset|usb|external/i,
      /microphone/i,
    ];
    for (const pattern of priority) {
      const match = realInputs.find((device) => pattern.test(device.label || ''));
      if (match) return match;
    }
    return realInputs[0] || null;
  };

  const browserAudioConstraints = (deviceId = '') => ({
    audio: {
      ...(deviceId ? { deviceId: { exact: deviceId } } : {}),
      echoCancellation: true,
      noiseSuppression: true,
      autoGainControl: true,
    },
  });

  const browserMeetingTabConstraints = () => ({
    video: true,
    audio: true,
  });

  const stopStreamTracks = (stream) => {
    stream?.getTracks?.().forEach((track) => track.stop());
  };

  const watchCaptureTrackEnds = (stream, capturePath) => {
    if (!stream?.getTracks) return () => {};
    const disposers = [];
    stream.getTracks().forEach((track) => {
      const handler = () => {
        const active = state.browserRecording;
        if (!active || active.capturePath !== capturePath) return;
        const reason = `${track.kind || 'media'} track ended`;
        active.captureEndedAt = new Date().toISOString();
        active.captureEndReason = reason;
        if (nodes.recordingStatus) {
          nodes.recordingStatus.textContent = 'Meeting tab audio sharing stopped. Stop this recording and start again.';
        }
        telemetry('browser_audio_capture_track_ended', {
          capture_path: capturePath,
          track_kind: track.kind || '',
          track_label: track.label || '',
          elapsed_ms: Math.max(0, Date.now() - new Date(active.startedAt).getTime()),
        });
      };
      track.addEventListener('ended', handler);
      disposers.push(() => track.removeEventListener('ended', handler));
    });
    return () => disposers.forEach((dispose) => dispose());
  };

  const closeAudioContext = (context) => {
    const closePromise = context?.close?.();
    if (closePromise?.catch) closePromise.catch(() => {});
  };

  const mixBrowserMeetingAudio = async (micStream, meetingStream) => {
    const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
    if (!AudioContextCtor) {
      throw new Error('This browser cannot mix meeting tab audio and microphone audio.');
    }
    const meetingAudioTracks = meetingStream.getAudioTracks();
    if (!meetingAudioTracks.length) {
      throw new Error('Chrome did not provide meeting tab audio. Select the meeting Chrome tab and enable tab audio sharing.');
    }
    const context = new AudioContextCtor();
    if (context.state === 'suspended') await context.resume();
    const destination = context.createMediaStreamDestination();
    const sources = [];
    const connectStream = (stream) => {
      const source = context.createMediaStreamSource(stream);
      source.connect(destination);
      sources.push(source);
    };
    connectStream(meetingStream);
    if (micStream.getAudioTracks().length) connectStream(micStream);
    return {
      stream: destination.stream,
      context,
      sources,
      meetingAudioTrackLabel: meetingAudioTracks.map((track) => track.label || '').filter(Boolean).join(' | '),
      meetingVideoTrackLabel: meetingStream.getVideoTracks().map((track) => track.label || '').filter(Boolean).join(' | '),
      meetingAudioTrackCount: meetingAudioTracks.length,
    };
  };

  const amplitudeToDb = (value) => {
    const amplitude = Math.max(0, Number(value) || 0);
    if (!amplitude) return -120;
    return Math.max(-120, 20 * Math.log10(amplitude));
  };

  const measureBrowserAudioSignal = async (stream, durationMs = 1200) => {
    const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
    if (!AudioContextCtor) return { status: 'unavailable', rms_db: null, peak_db: null };
    const context = new AudioContextCtor();
    try {
      if (context.state === 'suspended') await context.resume();
      const source = context.createMediaStreamSource(stream);
      const analyser = context.createAnalyser();
      analyser.fftSize = 2048;
      source.connect(analyser);
      const samples = new Float32Array(analyser.fftSize);
      let sampleCount = 0;
      let sumSquares = 0;
      let peak = 0;
      const started = performance.now();
      await new Promise((resolve) => {
        const collect = () => {
          analyser.getFloatTimeDomainData(samples);
          for (const sample of samples) {
            const abs = Math.abs(sample);
            peak = Math.max(peak, abs);
            sumSquares += sample * sample;
          }
          sampleCount += samples.length;
          if (performance.now() - started >= durationMs) {
            resolve();
            return;
          }
          window.requestAnimationFrame(collect);
        };
        collect();
      });
      source.disconnect();
      const rms = Math.sqrt(sumSquares / Math.max(1, sampleCount));
      return {
        status: 'ok',
        rms_db: amplitudeToDb(rms),
        peak_db: amplitudeToDb(peak),
      };
    } catch (error) {
      return {
        status: 'unavailable',
        rms_db: null,
        peak_db: null,
        warning: error.message || '',
      };
    } finally {
      const closePromise = context.close?.();
      if (closePromise?.catch) closePromise.catch(() => {});
    }
  };

  const blobToBase64 = (blob) => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || '');
      resolve(result.includes(',') ? result.split(',').pop() : result);
    };
    reader.onerror = () => reject(reader.error || new Error('Could not read browser audio.'));
    reader.readAsDataURL(blob);
  });

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
    return new Intl.DateTimeFormat(undefined, {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    }).format(date);
  };

  const localDateValue = (value = new Date()) => {
    const date = value instanceof Date ? value : new Date(value || '');
    if (Number.isNaN(date.getTime())) return '';
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  };

  const recordDateValue = (record) => localDateValue(record?.recording_started_at || record?.created_at || '');

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
          telemetry('recording_signal_check_failed', {
            outcome: 'error',
            reason: recordId,
            error_message: health.warning || record.error || '',
          });
          return;
        }
        if (nodes.recordingStatus && state.activeRecordId === recordId) {
          nodes.recordingStatus.textContent = `Recording: ${record.title || 'Untitled meeting'}`;
        }
        scheduleSignalCheck(recordId, 3000, remainingChecks - 1, token);
      } catch (error) {
        telemetry('recording_signal_check_error', {
          outcome: 'error',
          reason: recordId,
          error_message: error.message || '',
        });
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
    const support = browserAudioSupportSnapshot();
    telemetry('recording_start_decision', {
      capture_path: meetingLink ? 'screencapturekit_audio' : 'screencapturekit_f2f',
      meeting_link_present: Boolean(meetingLink),
      title_present: Boolean(String(meeting?.title || '').trim()),
      browser_fallback_enabled: false,
      ...support,
    });
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
        telemetry('screencapturekit_permission_required', {
          capture_path: 'screencapturekit_f2f',
          sck_error_message: error.message || '',
          ...support,
        });
        throw new Error(
          `${error.message || 'ScreenCaptureKit helper could not start.'} ` +
          'Grant Screen & System Audio Recording and Microphone permissions, then start recording again.'
        );
      }
      throw error;
    }
    setRecordingState(payload.record);
    await loadRecords();
    await loadRecord(payload.record.record_id);
    startSignalChecks(payload.record);
  };

  const stopRecording = async (recordId) => {
    if (recordId === 'browser-audio' && state.browserRecording) {
      await stopBrowserAudioRecording();
      return;
    }
    clearSignalChecks();
    const payload = await api(`/api/meeting-recorder/records/${encodeURIComponent(recordId)}/stop`, { method: 'POST' });
    setRecordingState(null);
    await loadRecords();
    await loadRecord(payload.record.record_id);
    await monitorAutoProcessJob(payload.record.record_id, payload);
  };

  const startBrowserAudioRecording = async (meeting) => {
    clearSignalChecks();
    const meetingLink = String(meeting?.meeting_link || '').trim();
    const isLinkedMeeting = Boolean(meetingLink);
    const capturePath = isLinkedMeeting ? 'browser_tab_audio_linked' : 'browser_audio_f2f';
    const captureLabel = isLinkedMeeting ? 'Browser meeting tab audio + microphone' : 'Browser microphone';
    const mimeType = browserAudioMimeType();
    telemetry('browser_audio_get_user_media_started', {
      capture_path: capturePath,
      selected_mime_type: mimeType,
    });
    let micStream;
    let meetingStream;
    let recordingStream;
    let mix = null;
    let devices = [];
    let preferredDevice = null;
    let activeTrackLabel = '';
    let preflight = { status: 'not_checked', rms_db: null, peak_db: null };
    let cleanupCaptureHandlers = () => {};
    try {
      micStream = await navigator.mediaDevices.getUserMedia(browserAudioConstraints());
      activeTrackLabel = micStream.getAudioTracks()[0]?.label || '';
      devices = await audioInputDevices();
      preferredDevice = choosePreferredAudioInput(devices, activeTrackLabel);
      if (preferredDevice?.deviceId) {
        stopStreamTracks(micStream);
        micStream = await navigator.mediaDevices.getUserMedia(browserAudioConstraints(preferredDevice.deviceId));
        activeTrackLabel = micStream.getAudioTracks()[0]?.label || preferredDevice.label || '';
      }
      if (isLinkedMeeting) {
        if (nodes.recordingStatus) nodes.recordingStatus.textContent = 'Select the meeting tab and share tab audio...';
        meetingStream = await navigator.mediaDevices.getDisplayMedia(browserMeetingTabConstraints());
        cleanupCaptureHandlers = watchCaptureTrackEnds(meetingStream, capturePath);
        mix = await mixBrowserMeetingAudio(micStream, meetingStream);
        recordingStream = mix.stream;
      } else {
        recordingStream = micStream;
      }
      preflight = await measureBrowserAudioSignal(recordingStream);
      telemetry('browser_audio_preflight_checked', {
        capture_path: capturePath,
        selected_mime_type: mimeType,
        active_track_label: activeTrackLabel,
        preferred_device_label: preferredDevice?.label || '',
        input_device_count: devices.length,
        preflight_rms_db: preflight.rms_db,
        preflight_peak_db: preflight.peak_db,
        meeting_audio_track_label: mix?.meetingAudioTrackLabel || '',
        meeting_video_track_label: mix?.meetingVideoTrackLabel || '',
        meeting_audio_track_count: mix?.meetingAudioTrackCount || 0,
        audio_input_labels: devices.map((device) => device.label || '').filter(Boolean).slice(0, 12).join(' | '),
      });
      if (!isLinkedMeeting && preflight.status === 'ok' && Number(preflight.peak_db) <= -80) {
        stopStreamTracks(recordingStream);
        telemetry('browser_audio_preflight_too_quiet', {
          capture_path: capturePath,
          selected_mime_type: mimeType,
          active_track_label: activeTrackLabel,
          preferred_device_label: preferredDevice?.label || '',
          input_device_count: devices.length,
          preflight_rms_db: preflight.rms_db,
          preflight_peak_db: preflight.peak_db,
        });
        throw new Error('Chrome is receiving silence from the selected microphone. Check macOS/Chrome microphone input, then start again.');
      }
    } catch (error) {
      cleanupCaptureHandlers();
      stopStreamTracks(recordingStream);
      stopStreamTracks(micStream);
      stopStreamTracks(meetingStream);
      closeAudioContext(mix?.context);
      telemetry('browser_audio_get_user_media_failed', {
        capture_path: capturePath,
        selected_mime_type: mimeType,
        active_track_label: activeTrackLabel,
        preferred_device_label: preferredDevice?.label || '',
        input_device_count: devices.length,
        preflight_rms_db: preflight.rms_db,
        preflight_peak_db: preflight.peak_db,
        meeting_audio_track_label: mix?.meetingAudioTrackLabel || '',
        meeting_video_track_label: mix?.meetingVideoTrackLabel || '',
        meeting_audio_track_count: mix?.meetingAudioTrackCount || 0,
        error_name: error.name || '',
        error_message: error.message || '',
      });
      throw error;
    }
    const recorder = new MediaRecorder(recordingStream, mimeType ? { mimeType } : undefined);
    const chunks = [];
    const startedAt = new Date().toISOString();
    recorder.addEventListener('dataavailable', (event) => {
      if (event.data?.size) chunks.push(event.data);
    });
    recorder.start(1000);
    telemetry('browser_audio_recording_started', {
      capture_path: capturePath,
      selected_mime_type: mimeType,
      recorder_mime_type: recorder.mimeType || '',
      active_track_label: activeTrackLabel,
      preferred_device_label: preferredDevice?.label || '',
      input_device_count: devices.length,
      preflight_rms_db: preflight.rms_db,
      preflight_peak_db: preflight.peak_db,
      meeting_audio_track_label: mix?.meetingAudioTrackLabel || '',
      meeting_video_track_label: mix?.meetingVideoTrackLabel || '',
      meeting_audio_track_count: mix?.meetingAudioTrackCount || 0,
    });
    state.browserRecording = {
      recorder,
      stream: recordingStream,
      sourceStreams: [micStream, meetingStream].filter(Boolean),
      audioContext: mix?.context || null,
      chunks,
      startedAt,
      title: meeting?.title || 'Untitled meeting',
      meetingLink,
      platform: meeting?.platform || '',
      transcriptLanguage: meeting?.transcript_language || selectedTranscriptLanguage(),
      mimeType: recorder.mimeType || mimeType || 'audio/webm',
      activeTrackLabel,
      preferredDeviceLabel: preferredDevice?.label || '',
      inputDeviceCount: devices.length,
      capturePath,
      captureLabel,
      meetingAudioTrackLabel: mix?.meetingAudioTrackLabel || '',
      meetingVideoTrackLabel: mix?.meetingVideoTrackLabel || '',
      meetingAudioTrackCount: mix?.meetingAudioTrackCount || 0,
      preflight,
      cleanupCaptureHandlers,
      captureEndedAt: '',
      captureEndReason: '',
    };
    state.activeRecordId = 'browser-audio';
    state.selectedRecordId = 'browser-audio';
    if (nodes.recordingStatus) {
      nodes.recordingStatus.textContent = `Recording: ${state.browserRecording.title}`;
    }
    nodes.detail.innerHTML = `<p class="empty-state">${escapeHtml(captureLabel)} recording is active. Audio will be saved after stopping.</p>`;
    await loadRecords();
  };

  const stopBrowserAudioRecording = async () => {
    const active = state.browserRecording;
    if (!active) return;
    if (nodes.recordingStatus) nodes.recordingStatus.textContent = `Finalizing ${active.captureLabel || 'browser audio'}...`;
    const stoppedAt = new Date().toISOString();
    telemetry('browser_audio_stop_started', {
      capture_path: active.capturePath || 'browser_audio_f2f',
      chunk_count: active.chunks.length,
      recorder_mime_type: active.mimeType || '',
      recorder_state: active.recorder.state || '',
      active_track_label: active.activeTrackLabel || '',
      preferred_device_label: active.preferredDeviceLabel || '',
      input_device_count: active.inputDeviceCount || 0,
      preflight_rms_db: active.preflight?.rms_db,
      preflight_peak_db: active.preflight?.peak_db,
      meeting_audio_track_label: active.meetingAudioTrackLabel || '',
      meeting_video_track_label: active.meetingVideoTrackLabel || '',
      meeting_audio_track_count: active.meetingAudioTrackCount || 0,
      capture_ended_at: active.captureEndedAt || '',
      capture_end_reason: active.captureEndReason || '',
    });
    const stopped = active.recorder.state === 'inactive'
      ? Promise.resolve('already_inactive')
      : new Promise((resolve) => {
        let resolved = false;
        let timeoutId = 0;
        const done = (outcome) => {
          if (resolved) return;
          resolved = true;
          window.clearTimeout(timeoutId);
          resolve(outcome);
        };
        timeoutId = window.setTimeout(() => done('timeout'), 3000);
        active.recorder.addEventListener('stop', () => done('stop_event'), { once: true });
        try {
          active.recorder.requestData?.();
        } catch (_error) {
          // Some browsers throw if requestData races with stop; stopping can still succeed.
        }
        try {
          active.recorder.stop();
        } catch (_error) {
          done('stop_error');
        }
      });
    const stopOutcome = await stopped;
    active.cleanupCaptureHandlers?.();
    stopStreamTracks(active.stream);
    (active.sourceStreams || []).forEach((sourceStream) => stopStreamTracks(sourceStream));
    closeAudioContext(active.audioContext);
    const blob = new Blob(active.chunks, { type: active.mimeType || 'audio/webm' });
    const elapsedMs = Math.max(0, new Date(stoppedAt).getTime() - new Date(active.startedAt).getTime());
    telemetry('browser_audio_stop_finished', {
      capture_path: active.capturePath || 'browser_audio_f2f',
      blob_size: blob.size,
      chunk_count: active.chunks.length,
      elapsed_ms: elapsedMs,
      recorder_mime_type: active.mimeType || '',
      recorder_state: active.recorder.state || '',
      stop_outcome: stopOutcome,
      active_track_label: active.activeTrackLabel || '',
      preferred_device_label: active.preferredDeviceLabel || '',
      input_device_count: active.inputDeviceCount || 0,
      preflight_rms_db: active.preflight?.rms_db,
      preflight_peak_db: active.preflight?.peak_db,
      meeting_audio_track_label: active.meetingAudioTrackLabel || '',
      meeting_video_track_label: active.meetingVideoTrackLabel || '',
      meeting_audio_track_count: active.meetingAudioTrackCount || 0,
      capture_ended_at: active.captureEndedAt || '',
      capture_end_reason: active.captureEndReason || '',
    });
    if (!blob.size) {
      state.browserRecording = null;
      setRecordingState(null);
      await loadRecords();
      throw new Error('Browser did not return any audio data. Refresh the page and start a new recording.');
    }
    if (nodes.recordingStatus) nodes.recordingStatus.textContent = 'Preparing browser audio...';
    telemetry('browser_audio_upload_started', {
      capture_path: active.capturePath || 'browser_audio_f2f',
      blob_size: blob.size,
      chunk_count: active.chunks.length,
      elapsed_ms: elapsedMs,
      recorder_mime_type: active.mimeType || '',
      active_track_label: active.activeTrackLabel || '',
      preferred_device_label: active.preferredDeviceLabel || '',
      input_device_count: active.inputDeviceCount || 0,
      preflight_rms_db: active.preflight?.rms_db,
      preflight_peak_db: active.preflight?.peak_db,
      meeting_audio_track_label: active.meetingAudioTrackLabel || '',
      meeting_video_track_label: active.meetingVideoTrackLabel || '',
      meeting_audio_track_count: active.meetingAudioTrackCount || 0,
    });
    const audioBase64 = await blobToBase64(blob);
    if (nodes.recordingStatus) nodes.recordingStatus.textContent = 'Uploading browser audio...';
    let payload;
    try {
      payload = await api('/api/meeting-recorder/browser-audio', {
        method: 'POST',
        body: JSON.stringify({
          title: active.title,
          meeting_link: active.meetingLink,
          platform: active.platform,
          recording_started_at: active.startedAt,
          recording_stopped_at: stoppedAt,
          mime_type: blob.type || active.mimeType || 'audio/webm',
          audio_base64: audioBase64,
          browser_audio_device_label: [
            active.meetingAudioTrackLabel ? `tab: ${active.meetingAudioTrackLabel}` : '',
            active.activeTrackLabel ? `mic: ${active.activeTrackLabel}` : '',
          ].filter(Boolean).join(' | ') || active.activeTrackLabel || '',
          browser_audio_capture_source: active.capturePath || 'browser_audio_f2f',
          transcript_language: active.transcriptLanguage || 'mixed',
          browser_audio_preflight: active.preflight || {},
        }),
      });
    } catch (error) {
      telemetry('browser_audio_upload_failed', {
        capture_path: active.capturePath || 'browser_audio_f2f',
        blob_size: blob.size,
        chunk_count: active.chunks.length,
        elapsed_ms: elapsedMs,
        recorder_mime_type: active.mimeType || '',
        active_track_label: active.activeTrackLabel || '',
        preferred_device_label: active.preferredDeviceLabel || '',
        input_device_count: active.inputDeviceCount || 0,
        preflight_rms_db: active.preflight?.rms_db,
        preflight_peak_db: active.preflight?.peak_db,
        meeting_audio_track_label: active.meetingAudioTrackLabel || '',
        meeting_video_track_label: active.meetingVideoTrackLabel || '',
        meeting_audio_track_count: active.meetingAudioTrackCount || 0,
        error_message: error.message || '',
      });
      throw error;
    }
    telemetry('browser_audio_upload_succeeded', {
      capture_path: active.capturePath || 'browser_audio_f2f',
      blob_size: blob.size,
      chunk_count: active.chunks.length,
      elapsed_ms: elapsedMs,
      recorder_mime_type: active.mimeType || '',
      active_track_label: active.activeTrackLabel || '',
      preferred_device_label: active.preferredDeviceLabel || '',
      input_device_count: active.inputDeviceCount || 0,
      preflight_rms_db: active.preflight?.rms_db,
      preflight_peak_db: active.preflight?.peak_db,
      meeting_audio_track_label: active.meetingAudioTrackLabel || '',
      meeting_video_track_label: active.meetingVideoTrackLabel || '',
      meeting_audio_track_count: active.meetingAudioTrackCount || 0,
      record_id: payload.record?.record_id || '',
    });
    state.browserRecording = null;
    setRecordingState(null);
    await loadRecords();
    await loadRecord(payload.record.record_id);
    await monitorAutoProcessJob(payload.record.record_id, payload);
  };

  const loadUpcoming = async () => {
    if (!nodes.upcoming) return;
    nodes.calendarStatus.textContent = 'Loading upcoming Meet and Zoom meetings…';
    try {
      const payload = await api('/api/meeting-recorder/calendar/upcoming');
      const meetings = Array.isArray(payload.meetings) ? payload.meetings : [];
      nodes.calendarStatus.textContent = meetings.length ? `${meetings.length} upcoming meeting(s).` : 'No upcoming Meet or Zoom meetings found.';
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

  const loadRecords = async () => {
    if (!nodes.records) return;
    const payload = await api('/api/meeting-recorder/records');
    const serverRecords = Array.isArray(payload.records) ? payload.records : [];
    const selectedDate = nodes.recordDate?.value || localDateValue();
    const activeBrowserRecord = state.browserRecording ? {
      record_id: 'browser-audio',
      title: state.browserRecording.title || 'Untitled meeting',
      platform: 'f2f',
      status: 'recording',
      recording_started_at: state.browserRecording.startedAt,
      recording_stopped_at: '',
      created_at: state.browserRecording.startedAt,
      minutes_status: 'pending',
    } : null;
    const allRecords = activeBrowserRecord ? [activeBrowserRecord, ...serverRecords] : serverRecords;
    const records = allRecords.filter((record) => recordDateValue(record) === selectedDate);
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
        if (recordId === 'browser-audio' && state.browserRecording) {
          state.selectedRecordId = 'browser-audio';
          updateRecordSelection();
          nodes.detail.innerHTML = '<p class="empty-state">Browser microphone recording is active. Audio will be saved after stopping.</p>';
          return;
        }
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
          telemetry('recording_download_started', { outcome: 'ok', reason: recordId || '' });
        } catch (error) {
          if (status) {
            status.textContent = error.message || 'Could not download audio.';
            status.classList.add('inline-status-error');
          }
          telemetry('recording_download_failed', { outcome: 'error', reason: recordId || '', error_message: error.message || '' });
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
      if (nodes.detail) {
        nodes.detail.innerHTML = '<p class="empty-state">Select a recorded meeting to view audio, transcript, and minutes.</p>';
      }
      loadRecords();
    });
  }
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
  loadRecords();
})();
