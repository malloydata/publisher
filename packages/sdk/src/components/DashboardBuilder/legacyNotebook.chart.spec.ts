// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   notebookSourceRefused,
   readNotebookSource,
   type NotebookSource,
} from "./legacyNotebook";

const HEAD =
   '## artifact { kind=notebook }\nsource: a is duckdb.sql("select 1 as x")\n\n';

async function read(text: string): Promise<NotebookSource> {
   const result = await readNotebookSource(text);
   if (notebookSourceRefused(result)) throw new Error(result.refused);
   return result.source;
}

describe("readNotebookSource: chart lines", () => {
   it("records the recognized chart line's span and where a new one goes", async () => {
      const cell = `#" Caption\n# -bar_chart line_chart\n// c\nrun: a -> { select: x }\n`;
      const source = await read(HEAD + cell);
      const chart = source.cells[1].chart!;
      expect(chart.lines.map((l) => l.text)).toEqual([
         "# -bar_chart line_chart",
      ]);
      const [line] = chart.lines;
      expect(source.text.slice(line.span.start, line.span.end)).toBe(
         "# -bar_chart line_chart\n",
      );
      expect(source.text.slice(chart.insertAt)).toBe(
         "run: a -> { select: x }\n",
      );
      expect(chart.unmodelled).toBeUndefined();
   });

   it("places a new line at the run when there are no tags, and reads CRLF", async () => {
      const source = await read(
         (HEAD + "run: a -> { select: x }\n").replace(/\n/g, "\r\n"),
      );
      const chart = source.cells[1].chart!;
      expect(chart.lines).toEqual([]);
      expect(source.text.slice(chart.insertAt)).toBe(
         "run: a -> { select: x }\r\n",
      );
   });

   it("leaves a tag with properties, or a label, as unmodelled and never as a chart line", async () => {
      for (const line of [
         "# bar_chart { size=spark }",
         '# line_chart label="Revenue"',
         "# viz=line",
      ]) {
         const source = await read(`${HEAD}${line}\nrun: a -> { select: x }\n`);
         const chart = source.cells[1].chart!;
         expect(chart.lines).toEqual([]);
         expect(chart.unmodelled).toBe(line);
      }
   });

   it("ignores chart words in captions and keeps definitions free of chart state", async () => {
      const source = await read(
         `${HEAD}#" The bar_chart story\nrun: a -> { select: x }\n`,
      );
      expect(source.cells[1].chart!.lines).toEqual([]);
      expect(source.cells[1].chart!.unmodelled).toBeUndefined();
      expect(source.cells[0].chart).toBeUndefined();
   });

   it("does not take a word inside a quoted tag value for a chart line", async () => {
      const source = await read(
         `${HEAD}# label="Sales viz"\nrun: a -> { select: x }\n`,
      );
      expect(source.cells[1].chart!.unmodelled).toBeUndefined();
   });
});
