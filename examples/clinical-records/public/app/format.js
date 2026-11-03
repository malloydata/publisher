// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Pure helpers: no DOM, no globals, so `node --test` covers them.

const intFmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

/** A count, with thousands separators; a dash for missing, never 0. */
export function count(n) {
  return n === null || n === undefined || Number.isNaN(Number(n))
    ? "–"
    : intFmt.format(Number(n));
}

/** "37 of 37", the form every headline verdict takes. */
export function ofTotal(n, total) {
  return `${count(n)} of ${count(total)}`;
}

/** Tally a published surface's fields by class. A field with several classes counts under each. */
export function classTally(fields) {
  const tally = {};
  for (const f of fields) {
    for (const c of f.classifications.length
      ? f.classifications
      : ["(none declared)"]) {
      tally[c] = (tally[c] ?? 0) + 1;
    }
  }
  return tally;
}

/** The four verdicts the page leads with, from the governance export. */
export function verdicts(published) {
  let fields = 0;
  let classified = 0;
  let gateGivens = 0;
  let secureGivens = 0;
  let gated = 0;
  let withinCeiling = 0;
  for (const s of published) {
    fields += s.fields.length;
    classified += s.fields.filter((f) => f.classifications.length > 0).length;
    const givens = [...s.authorize, ...s.accessFilter].flatMap((g) => g.givens);
    gateGivens += givens.length;
    secureGivens += givens.filter((g) => g.secure).length;
    if (s.authorize.length + s.accessFilter.length > 0) gated += 1;
    const carries = new Set(s.carries ?? []);
    if (
      s.carries &&
      s.fields.every((f) => f.classifications.every((c) => carries.has(c)))
    ) {
      withinCeiling += 1;
    }
  }
  return {
    fields,
    classified,
    gateGivens,
    secureGivens,
    surfaces: published.length,
    gated,
    withinCeiling,
  };
}

/**
 * What one access-grid cell means. `rows` is the encounter count the persona
 * read, or null when the server refused (403). `total` is every encounter.
 * `expect` is the policy's answer: "rows", "scoped", or undefined (refused).
 */
export function probeOutcome(rows, total, expect) {
  const actual = rows === null ? "refused" : rows < total ? "scoped" : "rows";
  const matches =
    (expect === undefined && actual === "refused") ||
    (expect === "rows" && actual === "rows") ||
    (expect === "scoped" && actual === "scoped");
  return { actual, matches };
}

/** Parse "#surface=research&cell=billing:biller" into an object; unknown keys dropped. */
export function readHash(hash, allowed) {
  const out = {};
  for (const part of hash.replace(/^#/, "").split("&")) {
    const [k, v] = part.split("=");
    if (k && v && allowed.includes(k)) out[k] = decodeURIComponent(v);
  }
  return out;
}

export function writeHash(state) {
  return (
    "#" +
    Object.entries(state)
      .filter(([, v]) => v)
      .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
      .join("&")
  );
}
