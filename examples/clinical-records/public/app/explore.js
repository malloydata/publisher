// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// "Explore as a role": pick the #(secure) attributes an identity provider
// would set, then see what that identity can discover, read the rows it may
// read, and watch what happens when an agent acting for it tries to get more.
// Every answer on this view comes from the server, asked as that identity.

import {
  ATTEMPTS,
  ATTRIBUTE_VALUES,
  MODEL,
  ORGANIZATIONS,
  PERSONAS,
  ROLE_VALUES,
  SAMPLES,
  SURFACES,
  sampleQuery,
} from "./tiles.js";
import { count } from "./format.js";
import { el, errorNote, fill } from "./dom.js";

const $ = (id) => document.getElementById(id);

/** The identity being explored: the three #(secure) givens. */
let identity = { ROLES: [], ORGANIZATIONS: [], ATTRIBUTES: [] };
let selectedSurface = "";
let classesByPath = new Map();
/** Bumped on every identity change, so a slow answer for an old identity is dropped. */
let generation = 0;

export function identityOfPreset(id) {
  const p = PERSONAS.find((x) => x.id === id);
  return p
    ? structuredClone(p.givens)
    : { ROLES: [], ORGANIZATIONS: [], ATTRIBUTES: [] };
}

/* ── identity editor ───────────────────────────────────────────────────── */

function checkboxGroup(given, values, labelOf, valueOf) {
  return el(
    "fieldset",
    { class: "given" },
    el(
      "legend",
      {},
      el("code", { text: given }),
      el("span", { class: "chip is-secure", text: "#(secure)" }),
    ),
    values.map((v) => {
      const value = valueOf(v);
      const id = `g-${given}-${value}`;
      return el(
        "label",
        { class: "check", for: id },
        el("input", {
          type: "checkbox",
          id,
          checked: identity[given].includes(value),
          onchange: (e) => {
            const set = new Set(identity[given]);
            if (e.target.checked) set.add(value);
            else set.delete(value);
            identity = { ...identity, [given]: [...set] };
            markPreset("");
            refresh();
          },
        }),
        el("span", { text: labelOf(v) }),
      );
    }),
  );
}

function renderEditor(onPreset) {
  fill(
    $("identity-presets"),
    PERSONAS.map((p) =>
      el("button", {
        type: "button",
        class: "preset",
        "data-preset": p.id,
        onclick: () => onPreset(p.id),
        text: p.label,
      }),
    ),
    el("button", {
      type: "button",
      class: "preset",
      "data-preset": "none",
      onclick: () => onPreset("none"),
      text: "No identity",
    }),
  );
  fill(
    $("identity-givens"),
    checkboxGroup(
      "ROLES",
      ROLE_VALUES,
      (v) => v.label,
      (v) => v.value,
    ),
    checkboxGroup(
      "ORGANIZATIONS",
      ORGANIZATIONS,
      (o) => o.name,
      (o) => o.id,
    ),
    checkboxGroup(
      "ATTRIBUTES",
      ATTRIBUTE_VALUES,
      (v) => v.label,
      (v) => v.value,
    ),
  );
}

function markPreset(id) {
  for (const b of document.querySelectorAll(".preset")) {
    b.setAttribute("aria-pressed", b.dataset.preset === id ? "true" : "false");
  }
}

/* ── discovery: what this identity can see exists ──────────────────────── */

async function discover() {
  const { environment, package: pkg } = window.Publisher.context;
  const res = await fetch(
    `/api/v0/environments/${encodeURIComponent(environment)}/packages/${encodeURIComponent(pkg)}/models/${MODEL}?givens=${encodeURIComponent(JSON.stringify(identity))}`,
  );
  if (!res.ok)
    throw Object.assign(new Error(`model read answered ${res.status}`), {
      status: res.status,
    });
  const compiled = await res.json();
  return new Set((compiled.sources ?? []).map((s) => s.name));
}

function renderDiscovery(visible) {
  fill(
    $("discovery"),
    SURFACES.map((s) => {
      const on = visible.has(s.name);
      return el(
        "button",
        {
          type: "button",
          class: `surface-chip ${on ? "is-visible" : "is-hidden"}`,
          disabled: !on,
          "aria-pressed": selectedSurface === s.name ? "true" : "false",
          onclick: () => {
            selectedSurface = s.name;
            renderDiscovery(visible);
            renderSample(generation);
          },
        },
        el("span", { class: "sc-name", text: s.label }),
        el("span", { class: "sc-state", text: on ? "Visible" : "Not listed" }),
      );
    }),
  );
  const n = visible.size;
  $("discovery-caption").textContent =
    n === 0
      ? "This identity can discover nothing in the package: no domain's lock admits it."
      : `This identity can discover ${n} of ${SURFACES.length} domains. The others are not listed at all, so an agent working for it cannot learn they exist from discovery.`;
}

/* ── the rows themselves ───────────────────────────────────────────────── */

function cell(value, path = "") {
  if (value === null || value === undefined)
    return el("td", { class: "null", text: "null" });
  const s = String(value);
  const iso = /^\d{4}-\d{2}-\d{2}T/.test(s)
    ? s
        .replace("T", " ")
        .replace(/:\d{2}\.\d{3}Z$/, "")
        .replace(" 00:00", "")
    : s;
  return el("td", {
    text:
      typeof value === "number" && !/year$/.test(path)
        ? value.toLocaleString("en-US")
        : iso,
  });
}

async function renderSample(gen) {
  const box = $("sample");
  if (!selectedSurface) {
    fill(
      box,
      el("p", {
        class: "muted",
        text: "This identity can read nothing: no domain admits it, so there is no domain to pick.",
      }),
    );
    return;
  }
  const surface = selectedSurface;
  const def = SAMPLES[surface];
  fill(
    box,
    el("p", { class: "muted skeleton", text: "Reading as this identity…" }),
  );
  try {
    const [rows, [total]] = await Promise.all([
      window.Publisher.query(MODEL, sampleQuery(surface), { givens: identity }),
      window.Publisher.query(
        MODEL,
        `run: ${surface} -> { aggregate: encounters is count() }`,
        { givens: identity },
      ),
    ]);
    if (gen !== generation) return;
    const label = SURFACES.find((s) => s.name === surface).label;
    const head = el(
      "tr",
      {},
      def.fields.map((path) => {
        const classes = classesByPath.get(`${surface}:${path}`) ?? [];
        return el(
          "th",
          { scope: "col" },
          el("code", { text: path }),
          el(
            "div",
            {},
            classes.map((c) =>
              el("span", {
                class: `chip class-${c.replace(/\./g, "-")}`,
                text: c,
              }),
            ),
          ),
        );
      }),
    );
    const body = rows.map((r) =>
      el(
        "tr",
        {},
        def.fields.map((path) => cell(r[path.split(".").pop()], path)),
      ),
    );
    fill(
      box,
      el(
        "p",
        { class: "sample-caption" },
        el("strong", { text: `${count(total?.encounters)} encounters` }),
        ` on ${label} are readable by this identity. The ${count(rows.length)} newest:`,
      ),
      el(
        "div",
        { class: "scroll-x" },
        el(
          "table",
          { class: "rows" },
          el("thead", {}, head),
          el("tbody", {}, body),
        ),
      ),
      el(
        "details",
        { class: "query" },
        el("summary", { text: "The query, and the identity it ran as" }),
        el("code", { class: "block", text: sampleQuery(surface) }),
        el("code", { class: "block", text: JSON.stringify(identity) }),
      ),
    );
  } catch (err) {
    if (gen !== generation) return;
    fill(
      box,
      err?.status === 403
        ? el("p", {
            class: "note",
            text: "Refused (403). This identity is not admitted to this domain.",
          })
        : errorNote(err),
    );
  }
}

/* ── an agent acting for this identity ─────────────────────────────────── */

async function runAttempt(a, on) {
  if (a.extraRole && identity.ROLES.includes(a.extraRole)) {
    return { held: true };
  }
  const givens = a.extraRole
    ? { ...identity, ROLES: [...new Set([...identity.ROLES, a.extraRole])] }
    : identity;
  try {
    const rows = await window.Publisher.query(MODEL, a.query(on), { givens });
    return { ok: true, rows };
  } catch (err) {
    return {
      ok: false,
      status: err?.status,
      message: err?.response?.message ?? err?.message ?? String(err),
    };
  }
}

async function renderAttempts(visible, gen) {
  const on = SURFACES.find((s) => visible.has(s.name))?.name ?? "research";
  const list = $("attempts");
  const items = ATTEMPTS.map((a) => {
    const result = el("div", { class: "attempt-result skeleton", text: "…" });
    const li = el(
      "li",
      { class: "attempt" },
      el(
        "div",
        { class: "attempt-head" },
        el("span", { class: "attempt-label", text: a.label }),
        result,
      ),
      el("code", { class: "block", text: a.query(on) }),
      el("p", { class: "attempt-why", text: a.why }),
    );
    return { a, li, result };
  });
  fill(
    list,
    items.map((x) => x.li),
  );
  for (const { a, li, result } of items) {
    const r = await runAttempt(a, on);
    if (gen !== generation) return;
    result.classList.remove("skeleton");
    if (r.held) {
      result.textContent = "Not a claim: this identity holds the role";
      li.classList.add("is-allowed");
      continue;
    }
    if (r.ok) {
      const n =
        r.rows.length === 1 && "encounters" in r.rows[0]
          ? `${count(r.rows[0].encounters)} encounters`
          : `${count(r.rows.length)} rows`;
      result.textContent = a.local
        ? `Answered locally: ${n}`
        : a.id === "chart"
          ? `Answered: ${n}, as the policy allows this identity`
          : `Answered: ${n}`;
      li.classList.add(
        a.local ? "is-local" : a.id === "chart" ? "is-allowed" : "is-leak",
      );
    } else {
      const kind =
        r.status === 403
          ? "Refused, 403"
          : r.status === 404
            ? "Not queryable, 404"
            : r.status === 400
              ? "Rejected, 400"
              : `Failed${r.status ? `, ${r.status}` : ""}`;
      result.textContent = kind;
      li.classList.add("is-stopped");
      li.append(
        el(
          "p",
          { class: "attempt-msg" },
          el("span", { class: "muted", text: "Server: " }),
          r.message,
        ),
      );
    }
  }
}

/* ── wiring ────────────────────────────────────────────────────────────── */

async function refresh() {
  const gen = ++generation;
  fill(
    $("discovery"),
    el("span", {
      class: "muted skeleton",
      text: "Asking the server what this identity can see…",
    }),
  );
  try {
    const visible = await discover();
    if (gen !== generation) return;
    if (!visible.has(selectedSurface))
      selectedSurface = SURFACES.find((s) => visible.has(s.name))?.name ?? "";
    renderDiscovery(visible);
    $("identity-json").textContent = JSON.stringify(identity);
    await Promise.all([renderSample(gen), renderAttempts(visible, gen)]);
  } catch (err) {
    if (gen === generation) fill($("discovery"), errorNote(err));
  }
}

/**
 * Start the view. `governance` is the export the scorecard already loaded,
 * for the class of each sample column; `preset` is the persona to start as.
 */
export function initExplore(governance, preset, onPresetChange) {
  classesByPath = new Map();
  for (const s of governance?.sources ?? []) {
    if (!s.published) continue;
    for (const f of s.fields)
      classesByPath.set(`${s.name}:${f.path}`, f.classifications);
  }
  const choose = (id) => {
    identity = identityOfPreset(id);
    renderEditor(choose);
    markPreset(id);
    onPresetChange(id);
    refresh();
  };
  choose(preset);
}
