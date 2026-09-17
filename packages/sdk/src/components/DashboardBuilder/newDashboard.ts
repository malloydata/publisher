// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
      .replace(/^-+|-+$/g, "")
      .slice(0, 80);
}

export function newDashboardSource({
   title,
   modelPath,
   source,
   view,
}: NewDashboard): string {
   const extension = `${source}_tiles`;
   const tile = `${view}_tile`;
   const quoted = `"${title.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
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
