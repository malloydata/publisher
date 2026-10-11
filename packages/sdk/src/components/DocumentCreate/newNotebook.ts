// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { importPathOf } from "../DashboardBuilder/imports";
import type { ReportLayout } from "../Package/documentLocation";
import { malloyName, newDocumentProblem, type NewDocument } from "./guards";

/** Where a new report goes when nobody says otherwise: the package root. */
const ROOT_REPORT_PATH = "report.malloy";

/**
 * A new report file, in the format the builder writes: the givens flag, the
 * artifact tag (a block, a tile per line) listing an intro text tile and one
 * query tile, a named import of the one source it runs (written relative to
 * where the report sits, so a report at the root imports `"<model>"`), the
 * intro block, and one extension of the source (`<source>_tiles`, the
 * builder's naming) holding the picked view as the query tile.
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
   const quoted = `"${title.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
   const extension = malloyName(`${source}_tiles`);
   const tile = malloyName(`${view}_tile`);
   // The entry is a quoted string, so a back-quoted extension name is written inside it as is.
   const entry = `"${extension} -> ${tile}"`;
   return [
      "##! experimental.givens",
      `##| artifact { layout=${layout} title=${quoted}`,
      "  tiles=[",
      "    intro { kind=text },",
      `    ${entry}`,
      "  ]",
      layout === "grid" ? "} dashboard { columns=12 }" : "}",
      "|##",
      `import { ${malloyName(source)} } from "${importPathOf(modelPath, reportPath)}"`,
      "",
      "##|(markdown) intro",
      `# ${title}`,
      "Say what this report is for, then let the queries below answer it.",
      "|##",
      "",
      `source: ${extension} is ${malloyName(source)} extend {`,
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
