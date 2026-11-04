// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// "How this was computed": a disclosure under each scorecard section listing
// the requests that produced it, from the ledger.

import { el, fill } from "./dom.js";
import { describeRuns, groupByText, runsFor } from "./ledger.js";

function runTable(runs) {
  return el(
    "table",
    { class: "src-runs" },
    el(
      "thead",
      {},
      el(
        "tr",
        {},
        ["Sent as", "Givens", "Answer", "Time"].map((h) =>
          el("th", { scope: "col", text: h }),
        ),
      ),
    ),
    el(
      "tbody",
      {},
      runs.map((r) =>
        el(
          "tr",
          { class: r.refused ? "is-refused" : "" },
          el("td", { text: r.persona ?? "This page" }),
          el(
            "td",
            {},
            r.givens ? el("code", { text: JSON.stringify(r.givens) }) : "none",
          ),
          el("td", { text: r.outcome }),
          el("td", { class: "muted", text: `${r.ms} ms` }),
        ),
      ),
    ),
  );
}

function group(g) {
  return el(
    "div",
    { class: "src-group" },
    el(
      "div",
      { class: "src-kind" },
      el("span", {
        class: "chip",
        text: g.kind === "malloy" ? "Malloy" : "REST",
      }),
      g.kind === "malloy"
        ? el("span", {}, "run against ", el("code", { text: g.model }))
        : el("span", { text: "Publisher's governance export" }),
    ),
    el("pre", { class: "src-code" }, el("code", { text: g.text })),
    runTable(g.runs),
  );
}

/**
 * Fill a section's source panel from the ledger.
 * `lead` says in a sentence how the section turns these answers into what it shows.
 */
export function renderSource(slotId, section, lead) {
  const slot = document.getElementById(slotId);
  if (!slot) return;
  const runs = runsFor(section);
  fill(
    slot,
    el(
      "details",
      { class: "source", id: `${slotId}-details` },
      el(
        "summary",
        {},
        el("span", { class: "src-title", text: "How this was computed" }),
        el("span", { class: "src-count", text: describeRuns(runs) }),
      ),
      el("p", { class: "src-lead", text: lead }),
      groupByText(runs).map(group),
    ),
  );
}

/** Open a section's panel and bring it into view. */
export function openSource(slotId) {
  const details = document.getElementById(`${slotId}-details`);
  if (!details) return;
  details.open = true;
  details.scrollIntoView({ behavior: "smooth", block: "center" });
}
