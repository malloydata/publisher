// Eval history app. Names come from ../../optimization.malloy; DOM built with textContent only.
const MODEL = "optimization.malloy";
const el = (tag, cls, text) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
};
const pct = (v) => (v == null ? "n/a" : `${Math.round(v * 100)}%`);
const ms = (v) => (v == null ? "n/a" : `${Math.round(v).toLocaleString("en-US")} ms`);

function stat(k, v, cls) {
  const s = el("div", "stat");
  s.append(el("span", "k", k), el("span", cls || null, v));
  return s;
}

async function versions() {
  const host = document.getElementById("versions");
  try {
    const rows = await Publisher.query(MODEL, "run: versions -> by_version");
    if (!rows.length) return host.replaceChildren(el("div", "muted", "No full run or replay yet."));
    host.replaceChildren(...rows.map((r, i) => {
      const card = el("div", `version${i === 0 && !r.version.includes("unpublished") ? " latest" : ""}`);
      card.append(el("div", "v-name", r.version),
                  el("div", "v-tag", r.run ? `answer run ${r.run}` : "no full answer run"));
      if (r.decided) {
        card.append(el("div", "big", `${r.passed} / ${r.decided}`), stat("pass rate", pct(r.pass_rate)));
        if (r.vs) card.append(stat(`vs ${r.vs}`, `${r.fixed} fixed, ${r.broke} broke`, r.broke ? "bad" : "good"));
      }
      if (r.perf_run) {
        card.append(stat("query time, total", ms(r.total_ms)), stat("median query", ms(r.p50_ms)));
        if (r.faster != null && (r.faster || r.slower))
          card.append(stat("speed vs previous", `${r.faster} faster, ${r.slower} slower`, r.slower ? "bad" : "good"));
        if (r.rows_changed) card.append(stat("answers changed", String(r.rows_changed), "bad"));
      }
      return card;
    }));
  } catch (e) {
    host.replaceChildren(el("div", "is-error", `Could not load versions: ${e.message}`));
  }
}

async function tags() {
  const host = document.getElementById("tags");
  try {
    const rows = await Publisher.query(MODEL, "run: tag_scores -> by_tag");
    if (!rows.length) return host.replaceChildren(el("div", "muted", "No answer runs yet."));
    host.replaceChildren(...rows.map((r) => {
      const row = el("div", "tag"), track = el("div", "track"), fill = el("div", "fill");
      fill.style.width = `${(r.pass_rate || 0) * 100}%`;
      track.append(fill);
      row.append(el("div", null, r.tag), track, el("div", "num", `${pct(r.pass_rate)}  ${r.passed}/${r.decided}`));
      return row;
    }));
  } catch (e) {
    host.replaceChildren(el("div", "is-error", `Could not load tags: ${e.message}`));
  }
}

async function runs() {
  const host = document.getElementById("runs");
  try {
    const rows = await Publisher.query(MODEL, "run: runs -> recent");
    const t = el("table"), head = el("tr");
    for (const [h, r] of [["run"], ["kind"], ["line"], ["version"], ["started"], ["score", 1], ["query time", 1]])
      head.append(el("th", r ? "r" : null, h));
    t.append(head);
    for (const r of rows) {
      const tr = el("tr");
      const line = el("td"); line.append(el("span", `pill${r.line === "main" ? "" : " exp"}`, r.line));
      tr.append(el("td", null, r.run), el("td", null, r.slice ? `${r.kind} (${r.slice})` : r.kind), line,
                el("td", null, r.version), el("td", null, String(r.started || "").slice(0, 16).replace("T", " ")),
                el("td", "r", r.decided ? `${r.passed}/${r.decided}` : ""),
                el("td", "r", r.total_ms ? ms(r.total_ms) : ""));
      t.append(tr);
    }
    host.replaceChildren(t);
  } catch (e) {
    host.replaceChildren(el("div", "is-error", `Could not load runs: ${e.message}`));
  }
}

function grid(rows, cols) {
  const t = el("table"), head = el("tr");
  for (const [h, , r] of cols) head.append(el("th", r ? "r" : null, h));
  t.append(head);
  for (const row of rows) {
    const tr = el("tr");
    for (const [, f, r] of cols) tr.append(el("td", r ? "r" : null, f(row)));
    t.append(tr);
  }
  return t;
}
const val = (v) => (v == null ? "" : Number(v).toLocaleString("en-US", { maximumFractionDigits: 4 }));

async function predictionPanel(id, query, empty, cols) {
  const host = document.getElementById(id);
  try {
    const rows = await Publisher.query(MODEL, query);
    host.replaceChildren(rows.length ? grid(rows, cols) : el("div", "muted", empty));
  } catch (e) {
    host.replaceChildren(el("div", "is-error", `Could not load: ${e.message}`));
  }
}

versions(); tags(); runs();
predictionPanel("predictions", "run: predictions -> recent", "No prediction yet. eval.py predict records one.", [
  ["made", (r) => String(r.made_at || "").slice(0, 10)], ["loop", (r) => r.loop], ["metric", (r) => r.metric],
  ["from", (r) => r.made_from], ["production before", (r) => val(r.baseline), 1],
  ["predicted", (r) => `${val(r.predicted)} (${val(r.low)} to ${val(r.high)})`, 1],
  ["measured", (r) => val(r.measured), 1], ["on", (r) => r.version || ""], ["state", (r) => r.state],
]);
predictionPanel("calibration", "run: predictions -> calibration", "Nothing checked yet.", [
  ["loop", (r) => r.loop], ["metric", (r) => r.metric], ["metric version", (r) => r.metric_version || ""],
  ["predictions", (r) => val(r.prediction_count), 1], ["checked", (r) => val(r.checked), 1],
  ["inside range", (r) => (r.inside_rate == null ? "n/a" : `${Math.round(r.inside_rate * 100)}%`), 1],
  ["mean gap", (r) => val(r.mean_gap), 1],
]);
