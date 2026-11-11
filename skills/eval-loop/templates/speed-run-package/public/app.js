// Speed run app. Every name read here is declared in ../speed_run.malloy.
// DOM built with createElement and textContent only: query text is data.
const MODEL = "speed_run.malloy";
const lit = (s) => `'${String(s).replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
const el = (tag, cls, text) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
};
const ms = (v) => (v == null ? "n/a" : `${Math.round(v).toLocaleString("en-US")} ms`);

function kpi(label, value, cls) {
  const k = el("div", "kpi");
  k.append(el("div", "kpi-label", label), el("div", `kpi-value ${cls || ""}`, value));
  return k;
}

async function showPair(p) {
  const kpis = document.getElementById("kpis");
  const ratio = p.total_ratio;
  kpis.replaceChildren(
    kpi("Total, baseline", ms(p.before_total_ms)),
    kpi("Total, this replay", ms(p.after_total_ms)),
    kpi("After / before", ratio == null ? "n/a" : `x${ratio.toFixed(2)}`, ratio != null && ratio < 1 ? "faster" : "slower"),
    kpi("Faster / slower", `${p.faster} / ${p.slower}`),
    kpi("Answers changed", String(p.blocking), p.blocking ? "rows_changed" : "faster"),
  );
  const host = document.getElementById("queries");
  host.classList.add("muted");
  try {
    const rows = await Publisher.query(MODEL, `run: comparisons -> {
      where: baseline_run = ${lit(p.baseline_run)} and run_id = ${lit(p.run_id)}
      select: qid, source_name, verdict, before_ms, after_ms, ratio
      order_by: ratio desc
    }`);
    host.classList.remove("muted");
    const blocked = rows.filter((r) => r.verdict === "rows_changed" || r.verdict === "broke");
    const bp = document.getElementById("blocked-panel");
    bp.hidden = !blocked.length;
    document.getElementById("blocked").replaceChildren(...blocked.map((r) => el("div", r.verdict, `${r.qid}: ${r.verdict.replace("_", " ")}`)));
    const max = Math.max(1, ...rows.flatMap((r) => [r.before_ms || 0, r.after_ms || 0]));
    host.replaceChildren(...rows.map((r) => queryRow(r, max, p.run_id)));
    if (!rows.length) host.replaceChildren(el("div", "muted", "No queries in common."));
  } catch (e) {
    host.replaceChildren(el("div", "is-error", `Could not load the queries: ${e.message}`));
  }
}

function queryRow(r, max, runId) {
  const row = el("div", "row");
  row.tabIndex = 0;
  row.setAttribute("role", "button");
  row.setAttribute("aria-expanded", "false");
  const name = el("div");
  name.append(el("div", "qid", r.qid), el("div", "src", r.source_name || ""));
  const bars = el("div", "bars");
  for (const [cls, v] of [["before", r.before_ms], ["after", r.after_ms]]) {
    const b = el("div", `bar ${cls}`);
    b.style.width = `${((v || 0) / max) * 100}%`;
    b.title = ms(v);
    bars.append(b);
  }
  const verdict = r.verdict === "within_band" ? "same" : r.verdict.replace("_", " ");
  const right = el("div", `ms ${r.verdict}`);
  right.append(el("div", null, ms(r.after_ms)), el("div", null, r.ratio == null ? verdict : `x${r.ratio.toFixed(2)} ${verdict}`));
  row.append(name, bars, right);
  const toggle = async () => {
    const open = row.getAttribute("aria-expanded") === "true";
    row.querySelector(".detail")?.remove();
    row.setAttribute("aria-expanded", String(!open));
    if (open) return;
    const d = el("div", "detail", "Loading");
    row.append(d);
    try {
      const [t] = await Publisher.query(MODEL, `run: timings -> {
        where: run_id = ${lit(runId)} and qid = ${lit(r.qid)}
        select: query, served_from, min_ms, max_ms, error
      }`);
      d.textContent = t ? `${t.query}\n\nserved from: ${t.served_from && t.served_from !== "none" ? t.served_from : "live, or a table in the source's own warehouse"}   min ${ms(t.min_ms)}   max ${ms(t.max_ms)}${t.error ? `\nerror: ${t.error}` : ""}` : "No timing row.";
    } catch (e) {
      d.textContent = `Could not load the query: ${e.message}`;
    }
  };
  row.addEventListener("click", toggle);
  row.addEventListener("keydown", (e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), toggle()));
  return row;
}

async function main() {
  const select = document.getElementById("pair");
  try {
    const pairs = await Publisher.query(MODEL, "run: comparisons -> summary");
    if (!pairs.length) {
      document.getElementById("queries").replaceChildren(el("div", "muted", "One replay only: package two or more to compare them. The report has its timings."));
      return;
    }
    pairs.forEach((p, i) => select.append(Object.assign(el("option", null, `${p.run_id} vs ${p.baseline_run}`), { value: i })));
    select.addEventListener("change", () => showPair(pairs[Number(select.value)]));
    select.value = String(pairs.length - 1);
    showPair(pairs[pairs.length - 1]);
  } catch (e) {
    document.getElementById("kpis").replaceChildren(el("div", "is-error", `Could not load the replays: ${e.message}`));
  }
}
main();
