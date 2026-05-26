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

})();
