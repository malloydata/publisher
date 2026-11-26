// Production history app. Names come from ../../history.malloy; DOM built with textContent only.
const MODEL = "history.malloy";
const el = (tag, cls, text) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
};
const pct = (v) => (v == null ? "n/a" : `${Math.round(v * 100)}%`);
const num = (v, d = 0) => (v == null ? "" : Number(v).toLocaleString("en-US", { maximumFractionDigits: d }));
const usd = (v) => (v == null ? "" : `$${Number(v).toFixed(v < 1 ? 4 : 2)}`);
const date = (v) => String(v || "").slice(0, 10);
// What a verdict judged: never compare a rate across the two.
const BASIS = { answer: "judged the answer", final_query: "judged the final query (no answer logged)" };

function stat(k, v, cls) {
  const s = el("div", "stat");
  s.append(el("span", "k", k), el("span", cls || null, v));
  return s;
}

// A table from rows: cols is [[header, (row) => text, right-aligned?, (row) => class?]].
function table(rows, cols) {
  const t = el("table"), head = el("tr");
  for (const [h, , r] of cols) head.append(el("th", r ? "r" : null, h));
  t.append(head);
  for (const row of rows) {
    const tr = el("tr");
    for (const [, f, r, c] of cols) tr.append(el("td", [r ? "r" : "", c ? c(row) : ""].join(" ").trim() || null, f(row)));
    t.append(tr);
  }
  return t;
}

async function panel(id, query, empty, render) {
  const host = document.getElementById(id);
  try {
    const rows = await Publisher.query(MODEL, query);
    host.replaceChildren(rows.length ? render(rows) : el("div", "muted", empty));
  } catch (e) {
    host.replaceChildren(el("div", "is-error", `Could not load: ${e.message}`));
  }
}

panel("versions", "run: judgments -> by_version", "No production turn judged yet. Run eval.py judge-sample.", (rows) => {
  const frag = document.createDocumentFragment();
  for (const r of rows) {
    const card = el("div", "version");
    card.append(el("div", "v-name", r.model_version || "unknown version"),
                el("div", "v-tag", `${BASIS[r.basis] || "basis unknown"} · metric ${r.metric_version || "unversioned"}`),
                el("div", "big", pct(r.quality_rate)),
                stat("right / decided", `${r.right_count} / ${r.decided}`),
                stat("judged, undecided", `${r.judged}, ${r.judged - r.decided}`));
    frag.append(card);
  }
  return frag;
});

panel("quality", "run: judgments -> by_day", "No judgments yet.", (rows) => table(rows, [
  ["day", (r) => date(r.on_day)], ["version", (r) => r.model_version], ["basis", (r) => r.basis || ""], ["metric", (r) => r.metric_version],
  ["judged", (r) => num(r.judged), 1], ["decided", (r) => num(r.decided), 1], ["quality", (r) => pct(r.quality_rate), 1],
]));

panel("failures", "run: judgments -> failures", "No wrong answer judged yet.", (rows) => {
  const max = Math.max(...rows.map((r) => r.judged));
  const frag = document.createDocumentFragment();
  for (const r of rows) {
    const row = el("div", "tag"), track = el("div", "track"), fill = el("div", "fill");
    fill.style.width = `${(r.judged / max) * 100}%`;
    track.append(fill);
    row.append(el("div", null, `${r.failure_code || "not diagnosed"} (${r.owner || "no owner"}${r.basis === "final_query" ? ", final query" : ""})`), track, el("div", "num", String(r.judged)));
    frag.append(row);
  }
  return frag;
});

async function cost() {
  const host = document.getElementById("cost");
  try {
    const [q, b] = await Promise.all([Publisher.query(MODEL, "run: queries -> by_day"), Publisher.query(MODEL, "run: builds -> by_day")]);
    if (!q.length) return host.replaceChildren(el("div", "muted", "No production queries ingested yet."));
    const built = new Map(b.map((r) => [date(r.on_day), r]));
    host.replaceChildren(table(q, [
      ["day", (r) => date(r.on_day)], ["version", (r) => r.model_version],
      ["queries", (r) => num(r.query_count), 1], ["over limit", (r) => num(r.over_limit_count), 1, (r) => (r.over_limit_count ? "bad" : "")],
      ["from a table", (r) => pct(r.table_share), 1], ["serving", (r) => usd(r.serve_usd_total), 1],
      ["builds", (r) => usd(built.get(date(r.on_day))?.build_usd_total), 1],
    ]));
  } catch (e) {
    host.replaceChildren(el("div", "is-error", `Could not load: ${e.message}`));
  }
}
cost();

panel("tails", "run: latency -> worst", "No production queries ingested yet.", (rows) => table(rows.slice(0, 15), [
  ["day", (r) => date(r.on_day)], ["source", (r) => r.source], ["served by", (r) => r.served_by],
  ["p50 ms", (r) => num(r.p50_ms), 1], ["p95 ms", (r) => num(r.p95_ms), 1], ["p99 ms", (r) => num(r.p99_ms), 1],
  ["queries", (r) => num(r.query_count), 1],
]));

panel("tables", "run: tables -> freshness", "No materialized table built yet.", (rows) => table(rows, [
  ["table", (r) => r.table_id], ["source", (r) => r.source], ["kind", (r) => r.kind],
  ["last built", (r) => String(r.last_built || "").slice(0, 16).replace("T", " ")],
  ["data age (h)", (r) => num(r.data_age_hours, 1), 1, (r) => (r.stale ? "bad" : "good")],
  ["storage a day", (r) => usd(r.storage_usd_per_day), 1],
]));
