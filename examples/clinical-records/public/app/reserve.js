// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Loaded as a classic script in <head>, before anything paints: it applies the
// heights the script-filled blocks settled at on the last visit, so a repeat
// visit does not move while its queries run. app.js saves the heights once the
// page is ready and releases them. Keyed by width, in 100px steps, because the
// heights depend on it. Storage that is missing or blocked just means no
// reservation, never a broken page.
(function () {
  var key = "clinical-records:heights:" + Math.round(window.innerWidth / 100);
  window.reservedHeightsKey = key;
  try {
    var saved = JSON.parse(localStorage.getItem(key) || "{}");
    var css = Object.keys(saved)
      .filter(function (id) {
        return /^[\w-]+$/.test(id) && typeof saved[id] === "number";
      })
      .map(function (id) {
        return "#" + id + "{min-height:" + Math.round(saved[id]) + "px}";
      })
      .join("");
    if (css) {
      var style = document.createElement("style");
      style.id = "reserved-heights";
      style.textContent = css;
      document.head.appendChild(style);
    }
  } catch (e) {
    // No storage: the first-visit reservations in index.html still apply.
  }
})();
