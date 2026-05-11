(() => {
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

  const sourceQaJobErrorMessage = (payloadOrError) => {
    const category = String(payloadOrError?.error_category || '').toLowerCase();
    const rawMessage = String(payloadOrError?.message || payloadOrError?.error || '').trim();
    if (category === 'local_agent_offline') {
      return 'Mac local-agent is unavailable. Confirm the host stack is online, then click Reconnect.';
    }
    if (category === 'gateway_disconnected') {
      return 'The gateway connection was interrupted, but the background job may still be running. Click Reconnect to restore status.';
    }
    if (category === 'job_running') {
      return 'The background job is still analyzing code and the connection was interrupted. Click Reconnect to restore status.';
    }
    if (category === 'job_queued') {
      return 'The job is queued and will be scheduled fairly across users.';
    }
    if (category === 'job_stalled') {
      return 'The background job has no recent progress and may still be running Codex. You can Reconnect or Retry.';
    }
    if (category === 'job_not_found') {
      return 'This background job can no longer be found. Please submit the question again.';
    }
    if (category === 'codex_timeout_or_rate_limit') {
      return 'Codex timed out or was rate-limited. Retry, or narrow the question scope first.';
    }
    if (/failed to fetch|load failed|networkerror|internet connection appears to be offline/i.test(rawMessage)) {
      return 'The browser lost connection to the background status API. The job may still be running; click Reconnect to restore status.';
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

  const create = ({ jobsUrlTemplate = '/api/jobs/__JOB_ID__' } = {}) => {
    const jobStatusUrl = (jobId, template = jobsUrlTemplate) => template.replace('__JOB_ID__', encodeURIComponent(jobId));
    const jobEventsUrl = (jobId, template = jobsUrlTemplate) => `${jobStatusUrl(jobId, template)}/events`;
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
    const readJobStatus = async (jobId, template = jobsUrlTemplate) => {
      let lastError = null;
      for (let attempt = 0; attempt < 5; attempt += 1) {
        try {
          return await apiFetchJson(jobStatusUrl(jobId, template), { method: 'GET' });
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
    return {
      apiFetchJson,
      escapeHtml,
      isTransientJobStatusError,
      jobEventsUrl,
      jobStatusUrl,
      readJson,
      readJobStatus,
      sleep,
      sourceQaJobErrorMessage,
    };
  };

  window.SourceCodeQAApi = {
    create,
    escapeHtml,
    isTransientJobStatusError,
    readJson,
    sleep,
    sourceQaJobErrorMessage,
  };
})();
