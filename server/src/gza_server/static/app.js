// Progressive enhancement only: every control below works without JS, this
// just spares the operator from scrolling a list of several hundred tags.
(() => {
  "use strict";

  /**
   * Narrow a multi-select to options matching what is typed.
   *
   * Selected options always stay visible. Hiding a selected option would make
   * the submitted filter invisible while still applying it, which reads as the
   * page ignoring the search box.
   */
  function wireOptionFilter(input) {
    const select = document.getElementById(input.dataset.filters);
    if (!select) return;

    const options = Array.from(select.options);
    input.addEventListener("input", () => {
      const needle = input.value.trim().toLowerCase();
      for (const option of options) {
        const matches = !needle || option.value.toLowerCase().includes(needle);
        option.hidden = !matches && !option.selected;
      }
    });
  }

  /**
   * Wire the select-all box and the live selection count.
   *
   * Selection is deliberately page-local: the checkboxes are plain form fields,
   * so what gets submitted is exactly what is visible and ticked. Carrying a
   * selection across pages would mean acting on rows that are no longer on
   * screen, which is what the filter-scoped bulk flow is for.
   */
  function wireSelection(form) {
    const rowBoxes = Array.from(form.querySelectorAll("input[data-select-row]"));
    if (!rowBoxes.length) return;

    const selectAll = form.querySelector("input[data-select-all]");
    const counter = form.querySelector("[data-selection-count]");

    function refresh() {
      const selected = rowBoxes.filter((box) => box.checked).length;
      if (selectAll) {
        selectAll.checked = selected === rowBoxes.length;
        selectAll.indeterminate = selected > 0 && selected < rowBoxes.length;
      }
      if (!counter) return;
      if (selected) {
        counter.textContent = `${selected} of ${rowBoxes.length} task${selected === 1 ? "" : "s"} on this page selected.`;
        counter.setAttribute("data-active", "");
      } else {
        counter.textContent = "No tasks selected. Tick rows to tag a subset of this page.";
        counter.removeAttribute("data-active");
      }
    }

    for (const box of rowBoxes) box.addEventListener("change", refresh);
    if (selectAll) {
      selectAll.addEventListener("change", () => {
        for (const box of rowBoxes) box.checked = selectAll.checked;
        refresh();
      });
    }
    refresh();
  }

  for (const input of document.querySelectorAll(".option-filter[data-filters]")) {
    wireOptionFilter(input);
  }
  for (const form of document.querySelectorAll(".selection-form")) {
    wireSelection(form);
  }
  // Follow a running task's log. The list carries the byte offset the server
  // page stopped at, so the stream resumes with no gap and no repeats.
  function wireLogFollow(list) {
    if (list.dataset.running !== "true") return;

    const taskId = list.dataset.taskId;
    const projectId = list.dataset.projectId;
    const stream = list.dataset.stream || "conversation";
    const offset = list.dataset.nextOffset || "0";
    const status = document.querySelector(".log-follow-status");

    const base = projectId
      ? `/api/projects/${encodeURIComponent(projectId)}/tasks/${encodeURIComponent(taskId)}`
      : `/api/tasks/${encodeURIComponent(taskId)}`;
    const source = new EventSource(
      `${base}/log/stream?stream=${encodeURIComponent(stream)}&offset=${encodeURIComponent(offset)}`
    );

    const atBottom = () =>
      window.innerHeight + window.scrollY >= document.body.offsetHeight - 40;

    function setStatus(text) {
      if (status) status.textContent = text;
    }

    source.addEventListener("entries", (event) => {
      const payload = JSON.parse(event.data);
      const stick = atBottom();
      for (const entry of payload.events) {
        const item = document.createElement("li");
        item.className = `log-event log-${entry.kind}${entry.is_error ? " log-error" : ""}`;

        const kind = document.createElement("span");
        kind.className = "log-kind";
        kind.textContent = entry.tool_name || entry.role || entry.kind;
        item.appendChild(kind);

        if (entry.title) {
          const title = document.createElement("span");
          title.className = "log-title";
          title.textContent = entry.title;
          item.appendChild(title);
        }
        if (entry.body && entry.body.trim()) {
          const body = document.createElement("pre");
          body.className = "log-body";
          body.textContent = entry.body;
          item.appendChild(body);
        }
        list.appendChild(item);
      }
      list.dataset.nextOffset = String(payload.next_offset);
      // Only auto-scroll a reader who was already at the bottom.
      if (stick) window.scrollTo(0, document.body.scrollHeight);
    });

    source.addEventListener("waiting", (event) => {
      setStatus(JSON.parse(event.data).message);
    });

    source.addEventListener("end", (event) => {
      const payload = JSON.parse(event.data);
      setStatus(
        payload.reason === "task_finished"
          ? `Task ${payload.status}. Reload for the full log.`
          : `Stopped following (${payload.reason}). Reload to continue.`
      );
      source.close();
    });

    source.onerror = () => setStatus("Lost connection to the log stream.");
  }

  for (const list of document.querySelectorAll(".log-events[data-running]")) {
    wireLogFollow(list);
  }
})();
