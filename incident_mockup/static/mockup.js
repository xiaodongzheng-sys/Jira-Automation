document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll("[data-expand-toggle]").forEach((button) => {
    const panel = document.querySelector("[data-expand-panel]");
    if (!panel) return;
    button.addEventListener("click", () => {
      const hidden = panel.hasAttribute("hidden");
      if (hidden) {
        panel.removeAttribute("hidden");
        button.textContent = "⌃";
      } else {
        panel.setAttribute("hidden", "");
        button.textContent = "⌄";
      }
    });
  });

  document.querySelectorAll("[data-modal-open]").forEach((button) => {
    button.addEventListener("click", () => {
      const target = button.getAttribute("data-modal-open");
      const modal = document.querySelector(`[data-modal="${target}"]`);
      if (modal) modal.hidden = false;
    });
  });

  document.querySelectorAll("[data-modal-close]").forEach((button) => {
    button.addEventListener("click", () => {
      const modal = button.closest("[data-modal]");
      if (modal) modal.hidden = true;
    });
  });
});
