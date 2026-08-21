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

  for (const input of document.querySelectorAll(".option-filter[data-filters]")) {
    wireOptionFilter(input);
  }
})();
