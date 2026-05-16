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

  const requestJson = async (url, options = {}) => {
    const response = await fetch(url, {
      headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
      ...options,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || payload.status === 'error') {
      throw new Error(payload.message || `Request failed with ${response.status}`);
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

  const renderProfiles = () => {
    if (!profiles.length) {
      list.innerHTML = '<div class="vpn-empty">No VPN profiles configured.</div>';
      return;
    }
    list.innerHTML = profiles.map((profile) => `
      <article class="vpn-profile" data-profile-id="${escapeHtml(profile.id)}">
        <div class="vpn-profile-main">
          <strong>${escapeHtml(profile.display_name)}</strong>
          <span>${escapeHtml(profile.vpn_host)}</span>
          <small>${escapeHtml(profile.username)}</small>
        </div>
        <div class="vpn-profile-actions">
          <button class="button button-secondary" type="button" data-vpn-edit="${escapeHtml(profile.id)}">Edit</button>
          <button class="button" type="button" data-vpn-connect="${escapeHtml(profile.id)}">Connect</button>
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
    renderStatus(payload.vpn_status || payload.status || {});
    renderHosts(payload.hosts || []);
    renderProfiles();
  };

  const loadProfiles = async () => {
    setInlineStatus('Loading VPN profiles...');
    try {
      applyPayload(await requestJson(root.dataset.profilesUrl));
      setInlineStatus('');
    } catch (error) {
      setInlineStatus(error.message, 'error');
    }
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
  root.querySelector('[data-vpn-disconnect]')?.addEventListener('click', async () => {
    setInlineStatus('Disconnecting VPN...');
    try {
      applyPayload(await requestJson(root.dataset.disconnectUrl, { method: 'POST', body: '{}' }));
      setInlineStatus('VPN disconnected.', 'success');
    } catch (error) {
      setInlineStatus(error.message, 'error');
    }
  });

  list.addEventListener('click', async (event) => {
    const editId = event.target.closest('[data-vpn-edit]')?.dataset.vpnEdit;
    const connectId = event.target.closest('[data-vpn-connect]')?.dataset.vpnConnect;
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
      try {
        const url = `${root.dataset.profilesUrl}/${encodeURIComponent(connectId)}/connect`;
        const payload = await requestJson(url, { method: 'POST', body: JSON.stringify(requestBody) });
        const connectStatus = payload.vpn_status || payload.status || {};
        await loadProfiles();
        renderStatus(connectStatus);
        if (connectStatus.connected) {
          setInlineStatus('VPN connected.', 'success');
        } else {
          setInlineStatus(connectStatus.message || 'VPN did not connect.', 'error');
        }
      } catch (error) {
        if (isFetchInterrupted(error)) {
          await loadProfiles();
          if (statusNode?.dataset.connected === 'true') {
            setInlineStatus('VPN connected.', 'success');
          } else {
            setInlineStatus('Connection request was interrupted. Cisco status has been refreshed.', 'error');
          }
          return;
        }
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
