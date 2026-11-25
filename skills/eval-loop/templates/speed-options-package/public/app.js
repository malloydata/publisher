// Speed options app. Every name read here is declared in ../speed_options.malloy.
// DOM built with createElement and textContent only: option and query text are data.
const MODEL = "speed_options.malloy";
const lit = (s) => `'${String(s).replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
const el = (tag, cls, text) => {
  const n = document.createElement(tag);
  if (cls) n.className = cls;
  if (text != null) n.textContent = text;
  return n;
};
const num = (v, unit = "") => (v == null || v === "" ? "n/a" : `${Math.round(Number(v)).toLocaleString("en-US")}${unit}`);

async function load() {
  try {
    const lines = await Publisher.query(MODEL, "run: assumptions -> { select: kind, text }");
    const obj = lines.filter((l) => l.kind === "objective").map((l) => l.text);
    document.getElementById("objective").textContent = obj.join(" ") || "No objective recorded.";
    document.getElementById("assumptions").replaceChildren(
      ...lines.filter((l) => l.kind === "assumption").map((l) => el("li", null, l.text)));
    const opts = await Publisher.query(MODEL, `run: options -> {
      select: option, kind, source, grain, measures, tier, covered, objective_before, objective_after,
        net_ms_per_day, build_ms_per_day, staleness_hours, usd_per_day, meets_objective, why_not
      order_by: meets_objective desc, objective_after
    }`);
    const host = document.getElementById("options");
    const max = Math.max(1, ...opts.flatMap((o) => [Number(o.objective_before) || 0, Number(o.objective_after) || 0]));
    const head = el("div", "opt head");
    head.append(el("div", null, "Option"), el("div", null, "Objective"), el("div", "num", "Net saved / day"),
                el("div", "num", "Build / day"), el("div", "num", "Data age"));
    host.replaceChildren(head, ...opts.map((o) => optionRow(o, max)));
  } catch (e) {
    document.getElementById("options").replaceChildren(el("div", "is-error", `Could not load the options: ${e.message}`));
  }
}

function optionRow(o, max) {
  const row = el("div", "opt");
  row.tabIndex = 0;
  row.setAttribute("role", "button");
  row.setAttribute("aria-expanded", "false");
  const name = el("div", "name", o.option);
  const what = o.grain ? `${o.kind} on ${o.source} by ${o.grain}` : o.kind;
  name.append(el("div", "sub", `${what} · ${o.covered} queries answered by a table · ${o.tier}`));
  name.append(el("div", `sub ${o.meets_objective ? "ok" : "no"}`,
                 o.meets_objective ? "meets the objective" : `does not: ${o.why_not || ""}`));
  const bars = el("div", "bars");
  for (const [cls, v] of [["before", o.objective_before], ["after", o.objective_after]]) {
    const b = el("div", `bar ${cls}`);
    b.style.width = `${((Number(v) || 0) / max) * 100}%`;
    b.title = num(v);
    bars.append(b);
  }
  const usd = o.usd_per_day == null || o.usd_per_day === "" ? "" : ` · $${Number(o.usd_per_day).toFixed(3)}/day`;
  row.append(name, bars, el("div", "num", `${num(o.net_ms_per_day, " ms")}${usd}`),
             el("div", "num", num(o.build_ms_per_day, " ms")), el("div", "num", `${o.staleness_hours} h`));
  const toggle = async () => {
    const open = row.getAttribute("aria-expanded") === "true";
    row.setAttribute("aria-expanded", String(!open));
    const next = row.nextSibling;
    if (open) { if (next && next.classList.contains("detail")) next.remove(); return; }
    const d = el("div", "detail", "Loading…");
    row.after(d);
    try {
      const qs = await Publisher.query(MODEL, `run: predictions -> {
        where: option = ${lit(o.option)}
        select: qid, \`count\`, before_ms, predicted_ms, covered, why_not
        order_by: saved_ms desc
      }`);
      d.replaceChildren(...qs.map((q) => {
        const r = el("div", "q");
        r.append(el("div", null, q.qid), el("div", "num", `×${q.count}`),
                 el("div", "num", `${num(q.before_ms)} → ${num(q.predicted_ms)} ms`),
                 el("div", "why", q.covered === "yes" ? "answered by a table" : (q.why_not || "live")));
        return r;
      }));
    } catch (e) {
      d.replaceChildren(el("div", "is-error", `Could not load the queries: ${e.message}`));
    }
  };
  row.addEventListener("click", toggle);
  row.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggle(); } });
  return row;
}

load();
