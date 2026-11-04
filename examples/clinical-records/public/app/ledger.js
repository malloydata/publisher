// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Every request the scorecard makes, recorded as it runs, so each section can
// show exactly what produced its numbers: the Malloy text, the model, the
// identity it was sent with, and what came back. Recorded, not reconstructed:
// a panel can only show a query that actually ran. DOM-free, so the grouping
// is unit-tested.

const runs = [];
let sent = 0;

/** A number for a request as it is sent, so panels list runs in send order, not finish order. */
export const nextSeq = () => ++sent;

/** Record one request. `sections` are the parts of the page its answer feeds. */
export function recordRun(run) {
  runs.push(run);
}

/** The requests that fed one section, in the order they ran. */
export function runsFor(section, all = runs) {
  return all
    .filter((r) => r.sections.includes(section))
    .sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0));
}

/** Group runs that share a request text, keeping first-seen order. */
export function groupByText(list) {
  const groups = new Map();
  for (const r of list) {
    const key = `${r.kind}\u0000${r.text}`;
    if (!groups.has(key))
      groups.set(key, { kind: r.kind, model: r.model, text: r.text, runs: [] });
    groups.get(key).runs.push(r);
  }
  return [...groups.values()];
}

/** "30 Malloy queries", "1 governance request · 2 Malloy queries". */
export function describeRuns(list) {
  const malloy = list.filter((r) => r.kind === "malloy").length;
  const rest = list.length - malloy;
  const parts = [];
  if (rest) parts.push(`${rest} governance ${rest === 1 ? "request" : "requests"}`);
  if (malloy) parts.push(`${malloy} Malloy ${malloy === 1 ? "query" : "queries"}`);
  return parts.join(" · ") || "no requests";
}

/** Every request recorded so far. */
export const allRuns = () => runs.slice();
