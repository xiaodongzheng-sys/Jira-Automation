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

  const showStatus = (container, message, tone = "") => {
    const status = container.querySelector("[data-business-insights-status]");
    if (!status) return;
    status.textContent = message;
    status.dataset.tone = tone;
  };

  root.querySelectorAll("[data-business-insights-sql]").forEach((button) => {
    button.addEventListener("click", async () => {
      const row = button.closest("[data-business-insights-report]");
      if (!row) return;
      const panel = row.querySelector("[data-business-insights-sql-panel]");
      const output = row.querySelector("[data-business-insights-sql-output]");
      try {
        button.disabled = true;
        showStatus(row, "Generating SQL...");
        const response = await fetch(button.dataset.sqlUrl, { headers: { Accept: "application/json" } });
        const payload = await response.json();
        if (!response.ok || payload.status !== "ok") {
          throw new Error(payload.message || "SQL generation failed.");
        }
        if (output) output.value = payload.sql || "";
        if (panel) panel.hidden = false;
        showStatus(row, "SQL ready. Run it in Data Workbench and upload the export.", "success");
      } catch (error) {
        showStatus(row, error.message || "SQL generation failed.", "error");
      } finally {
        button.disabled = false;
      }
    });
  });

  root.querySelectorAll("[data-business-insights-copy-sql]").forEach((button) => {
    button.addEventListener("click", async () => {
      const row = button.closest("[data-business-insights-report]");
      const output = row?.querySelector("[data-business-insights-sql-output]");
      if (!output?.value) return;
      await navigator.clipboard.writeText(output.value);
      showStatus(row, "SQL copied.", "success");
    });
  });

  root.querySelectorAll("[data-business-insights-upload]").forEach((form) => {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const row = form.closest("[data-business-insights-report]");
      const submit = form.querySelector('button[type="submit"]');
      try {
        if (submit) submit.disabled = true;
        showStatus(row, "Uploading export and building Excel...");
        const response = await fetch(form.action, {
          method: "POST",
          body: new FormData(form),
          headers: { Accept: "application/json" },
        });
        const payload = await response.json();
        if (!response.ok || payload.status !== "ok") {
          throw new Error(payload.message || "Upload failed.");
        }
        showStatus(row, "Excel generated. Reloading report link...", "success");
        window.location.reload();
      } catch (error) {
        showStatus(row, error.message || "Upload failed.", "error");
      } finally {
        if (submit) submit.disabled = false;
      }
    });
  });
})();
