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

/** The classes a `#(classification)` note declares: one quoted class or a quoted list. */
export function declaredClasses(annotations) {
  for (const a of annotations ?? []) {
    const m = /^#\(classification\)\s*(.*)$/s.exec((a.value ?? a).trim());
    if (!m) continue;
    try {
      const v = JSON.parse(m[1]);
      return Array.isArray(v) ? v.map(String) : [String(v)];
    } catch {
      return [];
    }
  }
  return [];
}

/**
 * A source's fields from the model read's `sourceInfos` entry, joins
 * flattened to dotted paths, each with the classes declared on it. A field
 * with none declared inherits from what it reads; the package summary counts
 * it, and `get_context` reports the inherited set.
 */
export function declaredFields(sourceInfo, prefix = "", depth = 0) {
  const out = [];
  for (const f of sourceInfo?.schema?.fields ?? []) {
    if (f.kind === "join") {
      if (depth < 3)
        out.push(...declaredFields(f, `${prefix}${f.name}.`, depth + 1));
      continue;
    }
    if (f.kind !== "dimension" && f.kind !== "measure") continue;
    out.push({
      path: `${prefix}${f.name}`,
      kind: f.kind,
      classifications: declaredClasses(f.annotations),
    });
  }
  return out;
}

/** The givens a gate expression reads (`$NAME`). */
export function gateGivens(expression) {
  return [
    ...new Set([...expression.matchAll(/\$([A-Za-z_]\w*)/g)].map((m) => m[1])),
  ];
}

/**
 * The published domains, from the package's classification summary and the
 * model read of each domain's file: the classes it may serve and serves, and
 * its gates with whether each given they read is #(secure).
 */
export function domainsFrom(summary, modelReads) {
  return (summary?.sources ?? []).map((s) => {
    const read = modelReads[s.modelPath] ?? {};
    const api = (read.sources ?? []).find((x) => x.name === s.name) ?? {};
    const secure = new Set(
      (api.givens ?? []).filter((g) => g.secure).map((g) => g.name),
    );
    const gate = (exprs) =>
      (exprs ?? []).map((expression) => ({
        expression,
        givens: gateGivens(expression).map((name) => ({
          name,
          secure: secure.has(name),
        })),
      }));
    const info = (read.sourceInfos ?? [])
      .map((x) => (typeof x === "string" ? JSON.parse(x) : x))
      .find((x) => x.name === s.name);
    return {
      name: s.name,
      modelPath: s.modelPath,
      carries: s.carries,
      classCounts: s.classCounts ?? {},
      fieldCount: s.fields,
      unclassified: s.unclassified,
      authorize: gate(api.authorize),
      accessFilter: gate(api.accessFilter),
      fields: declaredFields(info),
    };
  });
}

/** The four verdicts the page leads with, from the published domains. */
export function verdicts(published) {
  let fields = 0;
  let classified = 0;
  let gateGivenCount = 0;
  let secureGivens = 0;
  let gated = 0;
  let withinCeiling = 0;
  for (const s of published) {
    fields += s.fieldCount;
    classified += s.fieldCount - s.unclassified;
    const givens = [...s.authorize, ...s.accessFilter].flatMap((g) => g.givens);
    gateGivenCount += givens.length;
    secureGivens += givens.filter((g) => g.secure).length;
    if (s.authorize.length + s.accessFilter.length > 0) gated += 1;
    const carries = new Set(s.carries ?? []);
    if (s.carries && Object.keys(s.classCounts).every((c) => carries.has(c))) {
      withinCeiling += 1;
    }
  }
  return {
    fields,
    classified,
    gateGivens: gateGivenCount,
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
