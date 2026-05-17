(() => {
  const root = document.querySelector('[data-vpn-root]');
  if (!root) return;

  const list = root.querySelector('[data-vpn-list]');
  const form = root.querySelector('[data-vpn-form]');
  const statusNode = root.querySelector('[data-vpn-status]');
  const inlineStatus = root.querySelector('[data-vpn-inline-status]');
  const hostsDatalist = root.querySelector('[data-vpn-hosts]');
  const idInput = root.querySelector('[data-vpn-id]');

  let profiles = [];
  let currentVpnStatus = {};
  let lastRequestedProfileId = window.sessionStorage?.getItem('vpnConnectionLastProfileId') || '';
  let statusRetryTimer = 0;
  let statusPollTimer = 0;

  const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

  const setInlineStatus = (message, tone = 'neutral') => {
    if (!inlineStatus) return;
    inlineStatus.textContent = message || '';
    inlineStatus.dataset.tone = tone;
  };

  const isFetchInterrupted = (error) => (
    error instanceof TypeError && String(error.message || '').toLowerCase().includes('fetch')
  );

  const isTransientLocalAgentError = (error) => (
    isFetchInterrupted(error)
    || error?.transient === true
    || /mac local-agent is unavailable|endpoint is offline|err_ngrok_3200|context canceled/i.test(String(error?.message || ''))
  );

  const delay = (milliseconds) => new Promise((resolve) => {
    window.setTimeout(resolve, milliseconds);
  });

  const clearStatusRetry = () => {
    if (statusRetryTimer) {
      window.clearTimeout(statusRetryTimer);
      statusRetryTimer = 0;
    }
  };

  const scheduleStatusRetry = () => {
    clearStatusRetry();
    statusRetryTimer = window.setTimeout(() => {
      loadProfiles({ background: true });
    }, 10000);
  };

  const requestJson = async (url, options = {}) => {
    let response;
    try {
      response = await fetch(url, {
        headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
        ...options,
      });
    } catch (error) {
      error.transient = true;
      throw error;
    }
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload.status === 'error') {
      const error = new Error(payload.message || `Request failed with ${response.status}`);
      error.statusCode = response.status;
      error.transient = [502, 503, 504].includes(response.status) || isTransientLocalAgentError(error);
      throw error;
    }
    return payload;
  };

  const renderStatus = (vpnStatus) => {
    const state = vpnStatus?.state || (vpnStatus?.connected ? 'Connected' : 'Disconnected');
    statusNode.textContent = state ? `Cisco status: ${state}` : 'Cisco status unavailable';
    statusNode.dataset.connected = vpnStatus?.connected ? 'true' : 'false';
  };

  const renderHosts = (hosts) => {
    hostsDatalist.innerHTML = (hosts || []).map((host) => `<option value="${escapeHtml(host)}"></option>`).join('');
  };

  const activeProfileId = () => {
    if (!currentVpnStatus?.connected) return '';
    const newestConnectedProfileId = profiles
      .filter((profile) => profile.last_connected_at)
      .slice()
      .sort((left, right) => (
        Date.parse(right.last_connected_at || '') - Date.parse(left.last_connected_at || '')
      ))[0]?.id || '';
    if (newestConnectedProfileId) return newestConnectedProfileId;
    if (lastRequestedProfileId && profiles.some((profile) => profile.id === lastRequestedProfileId)) {
      return lastRequestedProfileId;
    }
    return '';
  };

  const renderProfiles = () => {
    if (!profiles.length) {
      list.innerHTML = '<div class="vpn-empty">No VPN profiles configured.</div>';
      return;
    }
    const connectedProfileId = activeProfileId();
    list.innerHTML = profiles.map((profile) => `
      <article class="vpn-profile" data-profile-id="${escapeHtml(profile.id)}" data-active="${profile.id === connectedProfileId ? 'true' : 'false'}">
        <div class="vpn-profile-main">
          <strong>${escapeHtml(profile.display_name)}</strong>
          <span>${escapeHtml(profile.vpn_host)}</span>
          <small>${escapeHtml(profile.username)}</small>
        </div>
        <div class="vpn-profile-actions">
          <button class="button button-secondary" type="button" data-vpn-edit="${escapeHtml(profile.id)}">Edit</button>
          ${profile.id === connectedProfileId
            ? `<button class="button button-secondary" type="button" data-vpn-disconnect-profile="${escapeHtml(profile.id)}">Disconnect</button>`
            : `<button class="button" type="button" data-vpn-connect="${escapeHtml(profile.id)}">Connect</button>`}
          <button class="button button-danger" type="button" data-vpn-delete="${escapeHtml(profile.id)}">Delete</button>
        </div>
      </article>
    `).join('');
  };

  const requiresSecondPassword = (profile) => {
    const value = `${profile?.display_name || ''} ${profile?.vpn_host || ''}`.toLowerCase();
    return value.includes('seabank ph');
  };

  const applyPayload = (payload) => {
    profiles = Array.isArray(payload.profiles) ? payload.profiles : profiles;
    currentVpnStatus = payload.vpn_status || payload.status || {};
    renderStatus(currentVpnStatus);
    renderHosts(payload.hosts || []);
    renderProfiles();
  };

  const loadProfiles = async ({ background = false } = {}) => {
    if (!background) {
      setInlineStatus('Loading VPN profiles...');
    }
    try {
      applyPayload(await requestJson(root.dataset.profilesUrl));
      clearStatusRetry();
      if (!background) {
        setInlineStatus('');
      }
    } catch (error) {
      if (isTransientLocalAgentError(error)) {
        setInlineStatus('Temporary local-agent tunnel issue. Retrying Cisco status...');
        await refreshProfilesAfterTransientLoad();
        return;
      }
      setInlineStatus(error.message, 'error');
    }
  };

  const refreshProfilesAfterTransientLoad = async () => {
    for (const waitMs of [1200, 2500, 5000]) {
      await delay(waitMs);
      try {
        applyPayload(await requestJson(root.dataset.profilesUrl));
        clearStatusRetry();
        setInlineStatus('');
        return;
      } catch (error) {
        if (!isTransientLocalAgentError(error)) {
          setInlineStatus(error.message, 'error');
          return;
        }
      }
    }
    setInlineStatus('Temporary local-agent tunnel issue. Still retrying Cisco status automatically.', 'error');
    scheduleStatusRetry();
  };

  const refreshProfilesAfterInterruptedConnect = async () => {
    for (const waitMs of [1200, 2500, 5000, 8000]) {
      await delay(waitMs);
      try {
        applyPayload(await requestJson(root.dataset.profilesUrl));
        if (statusNode?.dataset.connected === 'true') {
          setInlineStatus('VPN connected.', 'success');
        } else {
          setInlineStatus('Connection request was interrupted. Cisco status has been refreshed.', 'error');
        }
        return;
      } catch (error) {
        if (!isTransientLocalAgentError(error)) {
          setInlineStatus(error.message, 'error');
          return;
        }
      }
    }
    setInlineStatus('Connection response was interrupted while Cisco changed network. Click Refresh to check the latest status.', 'error');
  };

  const resetForm = () => {
    form.reset();
    idInput.value = '';
    setInlineStatus('');
  };

  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const data = Object.fromEntries(new FormData(form).entries());
    setInlineStatus('Saving profile...');
    try {
      applyPayload(await requestJson(root.dataset.saveUrl, {
        method: 'POST',
        body: JSON.stringify(data),
      }));
      resetForm();
      setInlineStatus('Profile saved.', 'success');
    } catch (error) {
      setInlineStatus(error.message, 'error');
    }
  });

  root.querySelector('[data-vpn-refresh]')?.addEventListener('click', loadProfiles);
  root.querySelector('[data-vpn-reset]')?.addEventListener('click', resetForm);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
      loadProfiles({ background: true });
    }
  });
  statusPollTimer = window.setInterval(() => {
    if (document.visibilityState === 'visible') {
      loadProfiles({ background: true });
    }
  }, 30000);

  list.addEventListener('click', async (event) => {
    const editId = event.target.closest('[data-vpn-edit]')?.dataset.vpnEdit;
    const connectId = event.target.closest('[data-vpn-connect]')?.dataset.vpnConnect;
    const disconnectId = event.target.closest('[data-vpn-disconnect-profile]')?.dataset.vpnDisconnectProfile;
    const deleteId = event.target.closest('[data-vpn-delete]')?.dataset.vpnDelete;
    if (editId) {
      const profile = profiles.find((item) => item.id === editId);
      if (!profile) return;
      form.elements.id.value = profile.id || '';
      form.elements.display_name.value = profile.display_name || '';
      form.elements.vpn_host.value = profile.vpn_host || '';
      form.elements.username.value = profile.username || '';
      form.elements.password.value = '';
      setInlineStatus('Editing profile. Leave password blank to keep it.');
      return;
    }
    if (connectId) {
      const profile = profiles.find((item) => item.id === connectId);
      if (!profile) return;
      const requestBody = {};
      if (requiresSecondPassword(profile)) {
        const secondPassword = window.prompt('Enter Cisco second password for Seabank PH VPN');
        if (secondPassword === null) {
          setInlineStatus('VPN connection cancelled.', 'neutral');
          return;
        }
        if (!secondPassword.trim()) {
          setInlineStatus('Cisco second password is required for Seabank PH VPN.', 'error');
          return;
        }
        requestBody.second_password = secondPassword;
      }
      setInlineStatus('Connecting VPN... approve MFA if prompted.');
      lastRequestedProfileId = connectId;
      window.sessionStorage?.setItem('vpnConnectionLastProfileId', connectId);
      try {
        const url = `${root.dataset.profilesUrl}/${encodeURIComponent(connectId)}/connect`;
        const payload = await requestJson(url, { method: 'POST', body: JSON.stringify(requestBody) });
        const connectStatus = payload.vpn_status || payload.status || {};
        if (Array.isArray(payload.profiles)) {
          applyPayload(payload);
        } else {
          await loadProfiles();
          renderStatus(connectStatus);
        }
        if (connectStatus.connected) {
          setInlineStatus('VPN connected.', 'success');
        } else {
          setInlineStatus(connectStatus.message || 'VPN did not connect.', 'error');
        }
      } catch (error) {
        if (isTransientLocalAgentError(error)) {
          setInlineStatus('Connection response was interrupted while Cisco changed network. Refreshing Cisco status...');
          await refreshProfilesAfterInterruptedConnect();
          return;
        }
        setInlineStatus(error.message, 'error');
      }
      return;
    }
    if (disconnectId) {
      setInlineStatus('Disconnecting VPN...');
      try {
        applyPayload(await requestJson(root.dataset.disconnectUrl, { method: 'POST', body: '{}' }));
        setInlineStatus('VPN disconnected.', 'success');
      } catch (error) {
        setInlineStatus(error.message, 'error');
      }
      return;
    }
    if (deleteId) {
      setInlineStatus('Deleting profile...');
      try {
        const url = `${root.dataset.profilesUrl}/${encodeURIComponent(deleteId)}`;
        applyPayload(await requestJson(url, { method: 'DELETE' }));
        setInlineStatus('Profile deleted.', 'success');
      } catch (error) {
        setInlineStatus(error.message, 'error');
      }
    }
  });

  loadProfiles();
})();
