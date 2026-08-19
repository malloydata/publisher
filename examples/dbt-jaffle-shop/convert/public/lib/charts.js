import { month } from "./format.js";

// Inline SVG charts. No library to vendor and no CDN: embedded page JavaScript runs with the
// viewing user's data authority, so the page ships nothing it did not author.

const esc = (s) => String(s).replace(/[&<>"]/g, (c) =>
   ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

/** Points with a null y are dropped, not zeroed, so a gap reads as missing. */
export function lineChart(rows, xKey, yKey, { height = 160, label = month } = {}) {
   const pts = rows.filter((r) => r[yKey] !== null && r[yKey] !== undefined);
   if (pts.length < 2) return `<p class="muted">Not enough points to plot.</p>`;
   const w = 100, h = height, pad = 4;
   const ys = pts.map((r) => Number(r[yKey]));
   const min = Math.min(...ys, 0), max = Math.max(...ys);
   const x = (i) => (pts.length === 1 ? w / 2 : (i / (pts.length - 1)) * w);
   const y = (v) => h - pad - ((v - min) / (max - min || 1)) * (h - pad * 2);
   const d = pts.map((r, i) => `${i ? "L" : "M"}${x(i).toFixed(2)},${y(Number(r[yKey])).toFixed(2)}`).join(" ");
   const area = `${d} L${w},${h} L0,${h} Z`;
   return `<svg class="chart" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" role="img">
     <path d="${area}" class="area"/>
     <path d="${d}" class="line" vector-effect="non-scaling-stroke"/>
   </svg>
   <div class="axis"><span>${esc(label(pts[0][xKey]))}</span><span>${esc(label(pts[pts.length - 1][xKey]))}</span></div>`;
}

export function barChart(rows, labelKey, valueKey, { fmt = (v) => v } = {}) {
   const vals = rows.map((r) => Number(r[valueKey]) || 0);
   const max = Math.max(...vals, 0) || 1;
   return `<ul class="bars">` + rows.map((r, i) => `
     <li>
       <span class="bar-label">${esc(r[labelKey] ?? "—")}</span>
       <span class="bar-track"><span class="bar-fill" style="width:${((vals[i] / max) * 100).toFixed(1)}%"></span></span>
       <span class="bar-value">${esc(fmt(r[valueKey]))}</span>
     </li>`).join("") + `</ul>`;
}

export function table(rows, cols) {
   if (!rows.length) return `<p class="muted">No rows.</p>`;
   const head = cols.map((c) => `<th${c.numeric ? ' class="num"' : ""}>${esc(c.label)}</th>`).join("");
   const body = rows.map((r) => `<tr>` + cols.map((c) =>
      `<td${c.numeric ? ' class="num"' : ""}>${esc(c.fmt ? c.fmt(r[c.key]) : r[c.key] ?? "—")}</td>`
   ).join("") + `</tr>`).join("");
   return `<table class="grid"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}
