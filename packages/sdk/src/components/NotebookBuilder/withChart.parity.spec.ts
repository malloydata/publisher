// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { chartLineText, type ChartState } from "../DashboardBuilder/chartLine";
import { withChart } from "./cellText";
import {
   notebookSourceRefused,
   readNotebookSource,
   type NotebookSource,
} from "../DashboardBuilder/legacyNotebook";
import { notebookDocumentOf, spliceNotebookDocument } from "./spliceNotebook";

const DEF = 'source: a is duckdb.sql("select 1 as x")';
const RUN = "run: a -> { select: x }";

const BODIES: Record<string, string> = {
   "no chart line": RUN,
   "canonical line": `${chartLineText("bar_chart")}\n${RUN}`,
   "bare line": `# line_chart\n${RUN}`,
   "table line": `${chartLineText("none")}\n${RUN}`,
   "caption, prose and comment": `// Why.\n#" Revenue\n#(markdown) Note.\n#|(markdown)\nnot a tag\n|#\n${RUN}`,
   // `|#` indented past the opener does not close it, so the block runs on to the column-0 closer.
   "prose block with an indented closer inside": `#|(markdown)\nnot a tag\n  |#\nstill prose\n|#\n${RUN}`,
   "caption and a line": `#" Revenue\n${chartLineText("line_chart")}\n// kept\n${RUN}`,
};

const STATES: ChartState[] = [
   "default",
   "none",
   "line_chart",
   "bar_chart",
   "big_value",
   "scatter_chart",
   "shape_map",
   "segment_map",
];

async function sourceOf(text: string): Promise<NotebookSource> {
   const read = await readNotebookSource(text);
   if (notebookSourceRefused(read)) throw new Error(read.refused);
   return read.source;
}

const sliceOf = (source: NotebookSource, index: number) =>
   source.text.slice(
      source.cells[index].span.start,
      source.cells[index].span.end,
   );

describe("the display text of a chart edit is the text the writer saves", () => {
   for (const [name, body] of Object.entries(BODIES))
      for (const eol of ["\n", "\r\n"])
         for (const state of STATES)
            it(`${name}, ${eol === "\n" ? "LF" : "CRLF"}, ${state}`, async () => {
               const text =
                  `## artifact { kind=notebook }${eol}${DEF}${eol}${eol}${body}${eol}`.replace(
                     /\r?\n/g,
                     eol,
                  );
               const source = await sourceOf(text);
               const at = source.cells.findIndex((c) => c.kind === "query");
               const cell = source.cells[at];
               const doc = notebookDocumentOf(source);
               doc.cells[at].chart = state;
               const saved = await spliceNotebookDocument(text, doc);
               expect(saved.ok).toBe(true);
               if (!saved.ok) return;

               const expected = withChart(
                  sliceOf(source, at),
                  state,
                  cell.chart!.lines.map((line) => line.text),
               );
               expect(sliceOf(await sourceOf(saved.source), at)).toBe(expected);
            });
});
