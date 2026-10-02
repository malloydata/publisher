// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { annotationTextProblem } from "./annotationText";
import { isBareName } from "./malloyText";

/**
 * A new dashboard file, the way the builder would have written it: the
 * dashboard-declared-givens convention with no givens yet, one extension of
 * the chosen source named the way the builder names one (`<source>_tiles`),
 * and one tile on it — the reader needs a tile to lay out, and a first view
 * is what the author picked anyway.
 */
export interface NewDashboard {
   title: string;
   /** The model that declares the source, relative to the package root. */
   modelPath: string;
   source: string;
   view: string;
}

/** `Sales by region` → `sales-by-region`: the file name, and so the slug. */
export function slugFor(title: string): string {
   return title
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "")
      .slice(0, 80);
}

/** Why this dashboard cannot be written as a file, or undefined when it can. */
export function newDashboardProblem({
   title,
   modelPath,
   source,
   view,
}: NewDashboard): string | undefined {
   for (const [what, name] of [
      ["source", source],
      ["view", view],
   ] as const)
      if (!isBareName(name))
         return `The ${what} name ${JSON.stringify(name)} cannot be written as a Malloy name.`;
   if (/["\\\r\n]/.test(modelPath))
      return `The model path ${JSON.stringify(modelPath)} cannot be written into an import.`;
   return annotationTextProblem("title", title);
}

export function newDashboardSource(dashboard: NewDashboard): string {
   const problem = newDashboardProblem(dashboard);
   if (problem) throw new Error(problem);
   return writeNewDashboard(dashboard);
}

function writeNewDashboard({
   title,
   modelPath,
   source,
   view,
}: NewDashboard): string {
   const extension = `${source}_tiles`;
   const tile = `${view}_tile`;
   const quoted = `"${title.trim().replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
   return [
      "##! experimental.givens",
      `## artifact { title=${quoted} tiles=["${extension} -> ${tile}"] } dashboard { columns=12 }`,
      `import { ${source} } from "../${modelPath}"`,
      "",
      `source: ${extension} is ${source} extend {`,
      "  # colspan=6",
      `  view: ${tile} is ${view}`,
      "}",
      "",
   ].join("\n");
}
