(() => {
  const root = document.querySelector("[data-business-insights]");
  if (!root) return;

  const tabs = [...root.querySelectorAll("[data-business-insights-tab]")];
  const panels = [...root.querySelectorAll("[data-business-insights-panel]")];

  const setActiveDomain = (domain) => {
    tabs.forEach((tab) => {
      const active = tab.dataset.businessInsightsTab === domain;
      tab.classList.toggle("is-active", active);
      tab.setAttribute("aria-selected", active ? "true" : "false");
    });
    panels.forEach((panel) => {
      const active = panel.dataset.businessInsightsPanel === domain;
      panel.classList.toggle("is-active", active);
      panel.hidden = !active;
    });
    const url = new URL(window.location.href);
    url.searchParams.set("domain", domain);
    window.history.replaceState({}, "", url);
  };

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => setActiveDomain(tab.dataset.businessInsightsTab));
  });

  // ---- Password gate for "Download Excel" / "Open Visualization" ----
  let downloadsUnlocked = root.dataset.downloadUnlocked === "true";
  const unlockUrl = root.dataset.downloadUnlockUrl || "/api/business-insights/download-unlock";

  const ensureUnlocked = async () => {
    if (downloadsUnlocked) return true;
    if (typeof window.portalDownloadUnlock !== "function") return false;
    const ok = await window.portalDownloadUnlock(unlockUrl);
    if (ok) downloadsUnlocked = true;
    return ok;
  };

  const isGatedDownloadLink = (el) =>
    !!el &&
    typeof el.matches === "function" &&
    el.matches(
      "[data-business-insights-download], [data-business-insights-visualization], a[href*='/artifacts/'], a[href*='/visualizations/']"
    );

  root.addEventListener("click", (event) => {
    const link = event.target.closest("a");
    if (!isGatedDownloadLink(link)) return;
    if (downloadsUnlocked) return; // already verified this session -> normal navigation
    event.preventDefault();
    const href = link.getAttribute("href");
    const openInNewTab = link.target === "_blank";
    ensureUnlocked().then((ok) => {
      if (!ok || !href) return;
      if (openInNewTab) {
        window.open(href, "_blank", "noopener");
      } else {
        window.location.href = href;
      }
    });
  });

  // ---- On-demand "Refresh data": run the Data Workbench generator + poll ----
  const POLL_MS = 4000;

  const statusEl = (button) => {
    const row = button.closest("[data-business-insights-report]");
    return row ? row.querySelector("[data-business-insights-status]") : null;
  };

  const setStatus = (button, message, tone) => {
    const el = statusEl(button);
    if (!el) return;
    el.textContent = message || "";
    el.dataset.tone = tone || "";
  };

  const applyCompletedReport = (button, report) => {
    if (!report) return;
    const row = button.closest("[data-business-insights-report]");
    const artifact = report.artifact;
    if (!row || !artifact || !artifact.url) return;
    const cell = row.querySelector(".business-insights-link-cell");
    if (!cell) return;
    let placeholder = cell.querySelector(".business-insights-placeholder");
    if (placeholder) placeholder.remove();
    let meta = cell.querySelector(".business-insights-meta");
    if (!meta) {
      meta = document.createElement("span");
      meta.className = "business-insights-meta";
      cell.insertBefore(meta, cell.querySelector(".business-insights-actions"));
    }
    meta.textContent = `${artifact.row_count} rows - ${artifact.created_at}`;
    // Point the existing Download/Visualization links at the fresh artifact,
    // or create them if this is the first successful generation.
    let actions = cell.querySelector(".business-insights-actions");
    let download = cell.querySelector('a[href*="/artifacts/"]');
    if (!download && actions) {
      download = document.createElement("a");
      download.className = "button button-secondary";
      download.textContent = "Download Excel";
      actions.insertBefore(download, actions.firstChild);
    }
    if (download) download.setAttribute("href", artifact.url);
    if (artifact.visualization_url) {
      let viz = cell.querySelector('a[href*="/visualizations/"]');
      if (!viz && actions) {
        viz = document.createElement("a");
        viz.className = "button button-secondary";
        viz.textContent = "Open Visualization";
        viz.target = "_blank";
        viz.rel = "noopener";
        download.insertAdjacentElement("afterend", viz);
      }
      if (viz) viz.setAttribute("href", artifact.visualization_url);
    }
  };

  const pollStatus = (button) => {
    fetch(button.dataset.statusUrl, { headers: { Accept: "application/json" } })
      .then((response) => response.json())
      .then((payload) => {
        const job = (payload && payload.job) || {};
        if (job.status === "running") {
          setStatus(button, job.progress ? `Refreshing… ${job.progress}` : "Refreshing… running Data Workbench query.", "running");
          window.setTimeout(() => pollStatus(button), POLL_MS);
          return;
        }
        if (job.status === "completed") {
          applyCompletedReport(button, payload.report);
          setStatus(button, "Data refreshed.", "good");
          button.disabled = false;
          return;
        }
        if (job.status === "failed") {
          setStatus(button, job.error || "Refresh failed.", "error");
          button.disabled = false;
          return;
        }
        // idle / unknown: stop and re-enable.
        button.disabled = false;
      })
      .catch(() => {
        setStatus(button, "Lost connection while checking refresh status. Reload to see the latest state.", "error");
        button.disabled = false;
      });
  };

  const startGenerate = (button) => {
    if (button.disabled) return;
    button.disabled = true;
    setStatus(button, "Starting refresh…", "running");
    fetch(button.dataset.generateUrl, { method: "POST", headers: { Accept: "application/json" } })
      .then((response) => response.json())
      .then((payload) => {
        if (!payload || payload.status !== "ok") {
          setStatus(button, (payload && payload.message) || "Could not start refresh.", "error");
          button.disabled = false;
          return;
        }
        pollStatus(button);
      })
      .catch(() => {
        setStatus(button, "Could not start refresh. Check your connection and retry.", "error");
        button.disabled = false;
      });
  };

  root.querySelectorAll("[data-business-insights-generate]").forEach((button) => {
    button.addEventListener("click", () => startGenerate(button));
    // If a refresh is already in flight (e.g. page reload), resume polling.
    if (button.dataset.statusUrl) {
      fetch(button.dataset.statusUrl, { headers: { Accept: "application/json" } })
        .then((response) => response.json())
        .then((payload) => {
          if (payload && payload.job && payload.job.status === "running") {
            button.disabled = true;
            setStatus(button, "Refreshing… running Data Workbench query.", "running");
            window.setTimeout(() => pollStatus(button), POLL_MS);
          }
        })
        .catch(() => {});
    }
  });

})();
