// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The entry point: reads the definitions in tiles.js, runs the queries, and
// hands the results to the renderers. Each section loads, fails and empties on
// its own, so one failed query never blanks the page.

import {
  CLASSES,
  CONTROLS,
  DEID,
  MODEL,
  PERSONAS,
  SURFACES,
  VIOLATIONS,
  probeQuery,
} from "./tiles.js";
import {
  classTally,
  count,
  ofTotal,
  probeOutcome,
  readHash,
  verdicts,
  writeHash,
} from "./format.js";
import { el, errorNote, fill } from "./dom.js";
import { ageHistogram } from "./histogram.js";
import { initExplore } from "./explore.js";

const $ = (id) => document.getElementById(id);
const state = readHash(location.hash, ["view", "as", "surface", "cell"]);
const setState = (patch) => {
  Object.assign(state, patch);
  history.replaceState(null, "", writeHash(state));
};

/* ── governance export ─────────────────────────────────────────────────── */

async function loadGovernance() {
  const { environment, package: pkg } = window.Publisher.context;
  const res = await fetch(
    `/api/v0/environments/${encodeURIComponent(environment)}/packages/${encodeURIComponent(pkg)}/governance`,
  );
  if (!res.ok) {
    const err = new Error(`governance export answered ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return res.json();
}

/* ── headline verdicts ─────────────────────────────────────────────────── */

function verdictCard(id, value, caption, ok) {
  const card = $(id);
  card.classList.toggle("is-pass", ok);
  card.classList.toggle("is-fail", !ok);
  fill(
    card,
    el("span", {
      class: "verdict-mark",
      "aria-hidden": "true",
      text: ok ? "✓" : "✕",
    }),
    el("span", { class: "verdict-value", text: value }),
    el("span", { class: "verdict-caption", text: caption }),
  );
}

function renderVerdicts(published) {
  const v = verdicts(published);
  verdictCard(
    "v-classified",
    ofTotal(v.classified, v.fields),
    "published fields carry a classification",
    v.classified === v.fields,
  );
  verdictCard(
    "v-secure",
    ofTotal(v.secureGivens, v.gateGivens),
    "gate attributes are set by the identity provider",
    v.secureGivens === v.gateGivens && v.gated === v.surfaces,
  );
  verdictCard(
    "v-ceiling",
    ofTotal(v.withinCeiling, v.surfaces),
    "domains serve only the classes they declare",
    v.withinCeiling === v.surfaces,
  );
  return v;
}

/* ── surface × class matrix, with the per-surface inventory ────────────── */

function renderMatrix(published) {
  const body = $("matrix-body");
  const rows = [];
  for (const def of SURFACES) {
    const s = published.find((p) => p.name === def.name);
    if (!s) continue;
    const tally = classTally(s.fields);
    const carries = new Set(s.carries ?? []);
    const open = state.surface === def.name;
    const toggle = el(
      "button",
      {
        class: "row-toggle",
        type: "button",
        "aria-expanded": open ? "true" : "false",
        "aria-controls": `inv-${def.name}`,
        onclick: () => {
          setState({ surface: state.surface === def.name ? "" : def.name });
          renderMatrix(published);
        },
      },
      el("span", { class: "surface-name", text: def.label }),
      el("span", { class: "surface-who", text: def.who }),
    );
    const cells = CLASSES.map((c) => {
      const n = tally[c.id] ?? 0;
      const allowed = carries.has(c.id);
      const cls =
        n > 0 && !allowed
          ? "cell is-violation"
          : allowed
            ? "cell is-allowed"
            : "cell is-closed";
      return el(
        "td",
        {
          class: cls,
          title: allowed
            ? "Declared in #(carries)"
            : "Not declared: the load refuses a field of this class",
        },
        n > 0 ? count(n) : allowed ? "0" : "—",
      );
    });
    rows.push(
      el(
        "tr",
        { class: open ? "is-open" : "" },
        el("th", { scope: "row" }, toggle),
        cells,
      ),
    );
    if (open)
      rows.push(
        el(
          "tr",
          { class: "inventory-row", id: `inv-${def.name}` },
          el("td", { colSpan: CLASSES.length + 1 }, inventory(s)),
        ),
      );
  }
  fill(body, rows);
}

function inventory(s) {
  const gates = [
    ...s.authorize.map((g) => ["Lock", g]),
    ...s.accessFilter.map((g) => ["Rows", g]),
  ];
  const gateList = el(
    "ul",
    { class: "gates" },
    gates.map(([kind, g]) =>
      el(
        "li",
        {},
        el("span", { class: "chip", text: kind }),
        el("code", { text: g.expression }),
        el("span", { class: "muted", text: " reads " }),
        g.givens.map((x) =>
          el("span", {
            class: x.secure ? "chip is-secure" : "chip is-insecure",
            text: `${x.name}${x.secure ? " · secure" : ""}`,
          }),
        ),
      ),
    ),
  );
  const fieldRows = s.fields.map((f) =>
    el(
      "tr",
      {},
      el("td", {}, el("code", { text: f.path })),
      el(
        "td",
        {},
        f.classifications.map((c) =>
          el("span", { class: `chip class-${c.replace(/\./g, "-")}`, text: c }),
        ),
      ),
      el(
        "td",
        { class: "muted" },
        f.inputs?.length
          ? `${f.inferred ? "Inferred from " : "Derived from "}${f.inputs.map((i) => i.field).join(", ")}`
          : f.kind === "measure"
            ? "Measure"
            : "Declared",
      ),
      el(
        "td",
        {},
        (f.controls ?? []).map((c) => el("span", { class: "chip", text: c })),
      ),
    ),
  );
  return el(
    "div",
    { class: "inventory" },
    el(
      "div",
      { class: "inv-meta" },
      el("div", {}, el("h4", { text: "Gates in force" }), gateList),
      el(
        "div",
        {},
        el("h4", { text: "Controls declared" }),
        el(
          "p",
          {},
          s.controls.map((c) => el("span", { class: "chip", text: c })),
        ),
      ),
    ),
    el(
      "table",
      { class: "fields" },
      el(
        "thead",
        {},
        el(
          "tr",
          {},
          ["Field", "Classes", "Lineage", "Controls"].map((h) =>
            el("th", { scope: "col", text: h }),
          ),
        ),
      ),
      el("tbody", {}, fieldRows),
    ),
  );
}

/* ── access grid: every surface × every persona, live ──────────────────── */

async function runProbe(surface, persona) {
  try {
    const [row] = await window.Publisher.query(MODEL, probeQuery(surface), {
      givens: persona.givens,
    });
    return { rows: Number(row?.encounters ?? 0) };
  } catch (err) {
    if (err?.status === 403) return { rows: null };
    return { error: err };
  }
}

/** Run tasks with at most `limit` in flight. */
async function pool(tasks, limit) {
  const results = new Array(tasks.length);
  let next = 0;
  const worker = async () => {
    while (next < tasks.length) {
      const i = next++;
      results[i] = await tasks[i]();
    }
  };
  await Promise.all(Array.from({ length: limit }, worker));
  return results;
}

async function renderAccess() {
  const head = $("access-head");
  fill(
    head,
    el(
      "tr",
      {},
      el("th", { scope: "col", text: "Domain" }),
      PERSONAS.map((p) => el("th", { scope: "col", text: p.label })),
    ),
  );
  const body = $("access-body");
  const cellEls = new Map();
  fill(
    body,
    SURFACES.map((s) =>
      el(
        "tr",
        {},
        el("th", { scope: "row", text: s.label }),
        PERSONAS.map((p) => {
          const td = el(
            "td",
            { class: "probe is-loading" },
            el("span", { class: "skeleton", text: "…" }),
          );
          cellEls.set(`${s.name}:${p.id}`, td);
          return td;
        }),
      ),
    ),
  );

  const keys = SURFACES.flatMap((s) => PERSONAS.map((p) => [s, p]));
  const results = await pool(
    keys.map(
      ([s, p]) =>
        () =>
          runProbe(s.name, p),
    ),
    4,
  );
  const total = Math.max(0, ...results.map((r) => r.rows ?? 0));
  let matched = 0;
  let decided = 0;
  keys.forEach(([s, p], i) => {
    const r = results[i];
    const td = cellEls.get(`${s.name}:${p.id}`);
    td.classList.remove("is-loading");
    td.dataset.surface = s.name;
    const key = `${s.name}:${p.id}`;
    const select = () => {
      setState({ cell: key });
      showProvenance(s, p, r, total);
    };
    if (r.error) {
      fill(td, el("span", { class: "is-error", text: "error" }));
      return;
    }
    decided += 1;
    const { actual, matches } = probeOutcome(r.rows, total, p.expect[s.name]);
    if (matches) matched += 1;
    td.classList.add(`is-${actual}`, matches ? "is-match" : "is-mismatch");
    fill(
      td,
      el(
        "button",
        {
          type: "button",
          class: "probe-btn",
          onclick: select,
          "aria-label": `${s.label} as ${p.label}`,
        },
        el("span", {
          class: "probe-outcome",
          text:
            actual === "refused"
              ? "Refused"
              : actual === "scoped"
                ? "Own org"
                : "All rows",
        }),
        el("span", {
          class: "probe-rows",
          text: r.rows === null ? "403" : count(r.rows),
        }),
        matches
          ? null
          : el("span", { class: "probe-flag", text: "not per policy" }),
      ),
    );
    if (state.cell === key) showProvenance(s, p, r, total);
  });
  verdictCard(
    "v-access",
    ofTotal(matched, keys.length),
    "access decisions match the policy's workforce table",
    matched === keys.length && decided === keys.length,
  );
}

function showProvenance(s, p, r, total) {
  fill(
    $("provenance"),
    el("h4", { text: `${s.label}, as ${p.label}` }),
    el(
      "dl",
      {},
      el("dt", { text: "Query" }),
      el("dd", {}, el("code", { text: probeQuery(s.name) })),
      el("dt", { text: "Givens sent" }),
      el("dd", {}, el("code", { text: JSON.stringify(p.givens) })),
      el("dt", { text: "Answer" }),
      el("dd", {
        text:
          r.rows === null
            ? "403, refused before the query compiled"
            : `${count(r.rows)} of ${count(total)} encounters`,
      }),
    ),
  );
}

/* ── de-identification, measured ───────────────────────────────────────── */

async function renderDeid() {
  const persona = (id) => PERSONAS.find((p) => p.id === id).givens;
  const side = async (key, histId, statsId, title) => {
    const def = DEID[key];
    try {
      const [ages, [prec]] = await Promise.all([
        window.Publisher.query(MODEL, def.ages, {
          givens: persona(def.persona),
        }),
        window.Publisher.query(MODEL, def.precision, {
          givens: persona(def.persona),
        }),
      ]);
      const unknown = ages
        .filter((r) => r.age === null)
        .reduce((a, r) => a + Number(r.patients), 0);
      fill(
        $(histId),
        ageHistogram(ages, {
          marker: key === "deid" ? 90 : undefined,
          label: `${title}: patients by age`,
        }),
      );
      fill(
        $(statsId),
        el(
          "div",
          { class: "stat" },
          el("span", { class: "stat-value", text: count(prec.oldest) }),
          el("span", { class: "stat-label", text: "oldest age served" }),
        ),
        el(
          "div",
          { class: "stat" },
          el("span", { class: "stat-value", text: count(prec.places) }),
          el("span", {
            class: "stat-label",
            text: key === "deid" ? "three-digit ZIP areas" : "ZIP codes",
          }),
        ),
        el(
          "div",
          { class: "stat" },
          el("span", { class: "stat-value", text: count(prec.dates) }),
          el("span", {
            class: "stat-label",
            text: key === "deid" ? "distinct years" : "distinct dates",
          }),
        ),
        unknown
          ? el("p", {
              class: "note",
              text: `${count(unknown)} patients with no birth date are not drawn.`,
            })
          : null,
      );
    } catch (err) {
      fill($(histId), errorNote(err));
    }
  };
  await Promise.all([
    side("lds", "hist-lds", "stats-lds", "Limited data set"),
    side("deid", "hist-deid", "stats-deid", "Research"),
  ]);
}

/* ── controls register ─────────────────────────────────────────────────── */

function renderControls(published, v, mismatched) {
  const declaring = (id) =>
    published
      .filter(
        (s) =>
          s.controls.includes(id) ||
          s.fields.some((f) => (f.controls ?? []).includes(id)),
      )
      .map((s) => s.name);
  const checks = {
    classified: v.classified === v.fields,
    gated: v.gated === v.surfaces,
    secure: v.secureGivens === v.gateGivens,
    ceiling: v.withinCeiling === v.surfaces,
    deid: true,
  };
  fill(
    $("controls-body"),
    CONTROLS.map((c) => {
      const probeFails =
        c.check === "probe" &&
        (c.surfaces ?? []).some((x) => mismatched.has(x));
      const status = probeFails
        ? "fail"
        : c.evidence === "review"
          ? "review"
          : c.check && checks[c.check] === false
            ? "fail"
            : "pass";
      return el(
        "tr",
        { class: `is-${status}` },
        el("th", { scope: "row" }, el("code", { text: c.id })),
        el("td", { text: c.text }),
        el(
          "td",
          {},
          el("span", {
            class: `chip evidence-${c.evidence}`,
            text:
              c.evidence === "model"
                ? "Model, enforced at load"
                : c.evidence === "server"
                  ? "Publisher"
                  : "Review",
          }),
        ),
        el(
          "td",
          { class: "muted" },
          c.note ?? (declaring(c.id).join(", ") || "Declared on the bases"),
        ),
        el(
          "td",
          {},
          el("span", {
            class: `status status-${status}`,
            text:
              status === "pass"
                ? "Holds"
                : status === "fail"
                  ? "Fails"
                  : "Needs review",
          }),
        ),
      );
    }),
  );
}

/* ── seeded violations ─────────────────────────────────────────────────── */

function renderViolations() {
  fill(
    $("violations"),
    VIOLATIONS.map((x) =>
      el(
        "li",
        { class: x.refused ? "is-refused" : "is-review" },
        el(
          "div",
          { class: "v-head" },
          el("span", {
            class: x.refused ? "status status-pass" : "status status-review",
            text: x.refused ? "Refused at load" : "Loads: review catches it",
          }),
          el("span", {
            class: "muted",
            text: `Variant ${x.id} · ${x.controls}`,
          }),
        ),
        el("p", { class: "v-what", text: x.what }),
        el(
          "p",
          {},
          x.refused
            ? el("code", { class: "refusal", text: x.reason })
            : x.reason,
        ),
      ),
    ),
  );
}

/* ── boot ──────────────────────────────────────────────────────────────── */

/** The governance export, loaded once and shared by both views. */
let governancePromise = null;
const governance = () => (governancePromise ??= loadGovernance());

let scorecardStarted = false;
let exploreStarted = false;

async function startScorecard() {
  if (scorecardStarted) return;
  scorecardStarted = true;
  renderViolations();
  const deid = renderDeid();
  let published = [];
  let v = null;
  try {
    const gov = await governance();
    published = gov.sources.filter((s) => s.published);
    v = renderVerdicts(published);
    renderMatrix(published);
  } catch (err) {
    fill(
      $("matrix-body"),
      el("tr", {}, el("td", { colSpan: CLASSES.length + 1 }, errorNote(err))),
    );
  }
  await renderAccess();
  // The surfaces whose row in the access grid differs from the policy.
  const mismatched = new Set(
    [...document.querySelectorAll("#access-body .is-mismatch")].map(
      (td) => td.dataset.surface,
    ),
  );
  if (v) renderControls(published, v, mismatched);
  await deid;
  rememberHeights();
  document.body.dataset.ready = "true";
}

// Save where each script-filled block settled, for reserve.js to apply on the
// next visit, then release this visit's reservation so the blocks can resize.
function rememberHeights() {
  const heights = {};
  for (const node of document.querySelectorAll("[data-reserve][id]")) {
    heights[node.id] = Math.round(node.getBoundingClientRect().height);
  }
  try {
    localStorage.setItem(window.reservedHeightsKey, JSON.stringify(heights));
  } catch {
    // No storage: the next visit reserves only what index.html does.
  }
  document.getElementById("reserved-heights")?.remove();
}

async function startExplore() {
  if (exploreStarted) return;
  exploreStarted = true;
  let gov = null;
  try {
    gov = await governance();
  } catch {
    // The view still works without classes on its column headers.
  }
  initExplore(gov, state.as || "clinician", (id) => setState({ as: id }));
}

function showView(view) {
  const explore = view === "explore";
  $("view-scorecard").hidden = explore;
  $("view-explore").hidden = !explore;
  for (const tab of document.querySelectorAll("[role=tab]")) {
    tab.setAttribute(
      "aria-selected",
      tab.dataset.view === (explore ? "explore" : "scorecard")
        ? "true"
        : "false",
    );
  }
  setState({ view: explore ? "explore" : "" });
  if (explore) startExplore();
  else startScorecard();
}

function main() {
  for (const tab of document.querySelectorAll("[role=tab]")) {
    tab.addEventListener("click", () => showView(tab.dataset.view));
  }
  showView(state.view === "explore" ? "explore" : "scorecard");
}

main();
