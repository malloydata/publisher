// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A histogram of patients by age, drawn as SVG from the token contract. One
// bar per year of age; the x axis is fixed at 0 to 110 so the two histograms
// on the page share a scale and the de-identified one visibly stops at 90.

import { svg } from "./dom.js";

const token = (n) =>
  getComputedStyle(document.documentElement).getPropertyValue(n).trim();

const MAX_AGE = 110;

/**
 * rows: [{ age, patients }]. `marker`: an age to draw a labelled rule at (90).
 * Ages are integers; a null age (no birth date) is not drawn and is counted in
 * the caption instead.
 */
export function ageHistogram(rows, { marker, label }) {
  const W = 520;
  const H = 160;
  const pad = { l: 28, r: 8, t: 12, b: 22 };
  const plotW = W - pad.l - pad.r;
  const plotH = H - pad.t - pad.b;
  const bars = rows.filter((r) => r.age !== null && r.age !== undefined);
  const peak = Math.max(1, ...bars.map((r) => Number(r.patients)));
  const barW = plotW / (MAX_AGE + 1);

  const root = svg("svg", {
    viewBox: `0 0 ${W} ${H}`,
    role: "img",
    "aria-label": label,
    class: "hist",
  });
  for (let a = 0; a <= MAX_AGE; a += 10) {
    const x = pad.l + a * barW;
    root.append(
      svg("line", {
        x1: x,
        x2: x,
        y1: pad.t,
        y2: pad.t + plotH,
        stroke: token("--grid"),
      }),
    );
    const t = svg("text", {
      x,
      y: H - 6,
      "text-anchor": "middle",
      fill: token("--tick"),
      "font-size": token("--fs-xs"),
    });
    t.textContent = String(a);
    root.append(t);
  }
  for (const r of bars) {
    const h = (Number(r.patients) / peak) * plotH;
    const title = svg("title");
    title.textContent = `${r.age}${Number(r.age) === marker ? "+" : ""}: ${r.patients} patients`;
    const rect = svg("rect", {
      x: pad.l + Number(r.age) * barW + 0.5,
      y: pad.t + plotH - h,
      width: Math.max(1, barW - 1),
      height: h,
      fill:
        Number(r.age) === marker ? token("--accent-strong") : token("--accent"),
    });
    rect.append(title);
    root.append(rect);
  }
  if (marker !== undefined) {
    const x = pad.l + (marker + 0.5) * barW;
    root.append(
      svg("line", {
        x1: x,
        x2: x,
        y1: pad.t,
        y2: pad.t + plotH,
        stroke: token("--text-muted"),
        "stroke-dasharray": "3 3",
      }),
    );
  }
  return root;
}
