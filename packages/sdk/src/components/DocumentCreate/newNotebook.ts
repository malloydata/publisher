// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { importPathOf } from "../ReportBuilder/imports";
import type { ReportLayout } from "../Package/documentLocation";
import { malloyName, newDocumentProblem, type NewDocument } from "./guards";

/**
 * `top_products` → `Top products`: the title a tile derives from a view name.
 * Restated rather than imported from `drill/resolveDrill`, which would pull
 * the filter parser into the create entry this module ships in.
 */
const titleOf = (name: string) => {
   const words = name.replace(/[-_]+/g, " ").trim();
   return words.charAt(0).toUpperCase() + words.slice(1);
};

/** A Malloy tag string. */
const quote = (text: string) =>
   `"${text.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;

/** Where a new report goes when nobody says otherwise: the package root. */
const ROOT_REPORT_PATH = "report.malloy";

/**
 * A new report file, in the format the builder writes: the givens flag, the
 * artifact tag (a block, a tile per line) listing one query tile, a named
 * import of the one source it runs (written relative to where the report
 * sits, so a report at the root imports `"<model>"`), and one extension of the
 * source (`<source>_tiles`, the builder's naming) holding the picked view as
 * the query tile, labelled with the view's own title.
 *
 * No intro text tile: the page already shows the title, and offers the
 * description in place, so a generated `# <title>` and a line asking for a
 * description said both twice on the first screen of every new report.
 *
 * The two layouts differ only in `layout=` and the grid tag a grid carries.
 * `layout=` is always written, so the file never depends on the reading rule.
 */
export function newReportSource(
   input: NewDocument,
   layout: ReportLayout,
   /** The package path the report is written at; the import is relative to it. Default: the root. */
   reportPath: string = ROOT_REPORT_PATH,
): string {
   const problem = newDocumentProblem(input);
   if (problem) throw new Error(problem);
   const { modelPath, source, view } = input;
   const title = input.title.trim();
   const quoted = quote(title);
   const extension = malloyName(`${source}_tiles`);
   const tile = malloyName(`${view}_tile`);
   // The entry is a quoted string, so a back-quoted extension name is written inside it as is.
   const entry = `"${extension} -> ${tile}"`;
   return [
      "##! experimental.givens",
      `##| artifact { layout=${layout} title=${quoted}`,
      "  tiles=[",
      `    ${entry}`,
      "  ]",
      layout === "grid" ? "} dashboard { columns=12 }" : "}",
      "|##",
      `import { ${malloyName(source)} } from "${importPathOf(modelPath, reportPath)}"`,
      "",
      `source: ${extension} is ${malloyName(source)} extend {`,
      // The view's title, not the extension's `<view>_tile` name humanized.
      `  # label=${quote(titleOf(view))}`,
      `  view: ${tile} is ${malloyName(view)}`,
      "}",
      "",
   ].join("\n");
}

/** A new linear report; see {@link newReportSource}. */
export function newNotebookSource(
   input: NewDocument,
   reportPath?: string,
): string {
   return newReportSource(input, "linear", reportPath);
}
