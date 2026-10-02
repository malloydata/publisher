// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   chartLineText,
   chartStateOf,
   parseChartLine,
   type ChartState,
} from "../DashboardBuilder/chartLine";
import { spliceFailed } from "../DashboardBuilder/spliceResult";
import { syntaxErrors } from "../DashboardBuilder/spliceDocument";
import {
   notebookSourceRefused,
   readNotebookSource,
   type NotebookSource,
} from "../DashboardBuilder/legacyNotebook";
import {
   canInsertQuery,
   canMove,
   notebookDocumentOf,
   spliceNotebookDocument,
   type NotebookDocument,
} from "./spliceNotebook";
import {
   callAccessor,
   isNotebookReaderError,
   parseNotebookText,
   readNotebookCells,
   type ParseNode,
} from "../../../../server/src/service/notebook";
import { lintNotebookText } from "../../../../server/src/service/notebook_lint";

const FIXTURES = path.resolve(
   import.meta.dir,
   "../../../../server/tests/fixtures",
);

const REFUSED = path.join("notebooks-malloyyo", "notebooks", "refused.malloy");

const fixtures = [
   ...fs
      .readdirSync(FIXTURES)
      .filter((dir) => dir.startsWith("notebooks-malloyyo"))
      .flatMap((dir) =>
         fs
            .readdirSync(path.join(FIXTURES, dir, "notebooks"))
            .filter((file) => file.endsWith(".malloy"))
            .map((file) => path.join(dir, "notebooks", file)),
      ),
   path.join("notebooks-lint", "notebooks", "clean.malloy"),
]
   .filter((file) => file !== REFUSED)
   .sort();

// A Windows checkout may hand fixtures back as CRLF; the specs assert on LF text.
const read = (file: string) =>
   fs.readFileSync(path.join(FIXTURES, file), "utf8").replace(/\r\n/g, "\n");

const TRANSFORMS: [string, (text: string) => string][] = [
   ["as-is", (text) => text],
   ["CRLF", (text) => text.replace(/\r?\n/g, "\r\n")],
   ["no trailing newline", (text) => text.replace(/(\r?\n)+$/, "")],
];

/** The server's reader, with a compile stand-in holding one slot per `run:`. */
function serverCells(text: string) {
   const parse = parseNotebookText(text);
   if (isNotebookReaderError(parse)) throw new Error(parse.message);
   const root = parse.root as ParseNode;
   let runCount = 0;
   for (let i = 0; i < (root.childCount ?? 0); i++)
      if (callAccessor(root.getChild(i), "runStatement") !== undefined)
         runCount++;
   const result = readNotebookCells(
      parse,
      { queryList: Array(runCount) } as Parameters<typeof readNotebookCells>[1],
      text,
   );
   if (result.error) throw new Error(result.error.message);
   return result.cells;
}

async function sourceOf(text: string): Promise<NotebookSource> {
   const result = await readNotebookSource(text);
   if (notebookSourceRefused(result)) throw new Error(result.refused);
   return result.source;
}

interface Edit {
   name: string;
   doc: NotebookDocument;
}

/** The sources an added query may name; the writer only checks membership. */
const REACHABLE = ["s"];

/** Whether a chart request leaves the cell's lines as they are: none asked, or the very line the writer would emit. */
const chartUntouched = (want: ChartState | undefined, lines: string[]) => {
   if (want === undefined) return true;
   const have = chartStateOf(lines);
   if (want !== have) return false;
   if (have === "custom") return true;
   return have === "default"
      ? lines.length === 0
      : lines.length === 1 && lines[0].trim() === chartLineText(have);
};

const CHART_EDITS = ["line_chart", "bar_chart", "none", "default"] as const;

/** Every edit the builder offers on this notebook: edit, add above and below, remove, and every legal move. */
function legalEdits(base: NotebookDocument, original: NotebookSource): Edit[] {
   const edits: Edit[] = [];
   const cells = base.cells;
   const clone = () => structuredClone(base);
   cells.forEach((cell, i) => {
      if (cell.kind === "query") {
         const found = original.cells[i].chart;
         // A cell the rule leaves alone is refused a chart change, which the refusal specs cover.
         if (found && found.unmodelled === undefined && found.lines.length <= 1)
            for (const chart of CHART_EDITS) {
               const doc = clone();
               doc.cells[i].chart = chart;
               edits.push({ name: `chart ${i} to ${chart}`, doc });
            }
         const doc = clone();
         doc.cells.splice(i, 1);
         edits.push({ name: `remove query ${i}`, doc });
         return;
      }
      if (cell.kind !== "markdown") return;
      for (const [label, markdown] of [
         ["one line", "Edited prose."],
         ["a block", "### Edited\n\nBody with |# inside\n  |## indented"],
      ]) {
         const doc = clone();
         doc.cells[i].markdown = markdown;
         edits.push({ name: `edit ${i} to ${label}`, doc });
      }
      const doc = clone();
      doc.cells.splice(i, 1);
      edits.push({ name: `remove ${i}`, doc });
   });
   for (let at = 0; at <= cells.length; at++) {
      const doc = clone();
      doc.cells.splice(at, 0, {
         id: "new",
         kind: "markdown",
         markdown: "Added prose.",
         added: true,
      });
      edits.push({ name: `add at ${at}`, doc });
      if (!canInsertQuery(base, at)) continue;
      for (const [label, chart, caption] of [
         ["plain", undefined, undefined],
         ["with a chart and caption", "line_chart", "Added caption"],
         ["as a table", "none", undefined],
      ] as const) {
         const query = clone();
         query.cells.splice(at, 0, {
            id: "added-query",
            kind: "query",
            added: true,
            chart,
            run: { source: "s", view: "v", ...(caption && { caption }) },
         });
         edits.push({ name: `add query ${label} at ${at}`, doc: query });
      }
   }
   for (let from = 0; from < cells.length; from++)
      for (let to = 0; to < cells.length; to++) {
         if (from === to || !canMove(base, from, to)) continue;
         const doc = clone();
         const [moved] = doc.cells.splice(from, 1);
         doc.cells.splice(to, 0, moved);
         edits.push({ name: `move ${from} to ${to}`, doc });
      }
   return edits;
}

const withoutNewline = (s: string) => s.replace(/\r?\n$/, "");

async function expectInvariants(
   file: string,
   original: NotebookSource,
   edit: Edit,
   crlf: boolean,
) {
   const result = await spliceNotebookDocument(
      original.text,
      edit.doc,
      undefined,
      REACHABLE,
   );
   if (spliceFailed(result))
      throw new Error(`${file}: ${edit.name}: ${result.reason}`);
   const out = result.source;
   expect(await syntaxErrors(out)).toEqual([]);
   if (crlf) expect(/(^|[^\r])\n/.test(out)).toBe(false);

   const theirs = serverCells(out);
   const mine = await sourceOf(out);
   const wanted = edit.doc.cells.map((cell) => {
      const was = original.cells.find((c) => c.id === cell.id && !cell.added);
      return {
         kind: cell.kind,
         markdown: cell.kind === "markdown" ? cell.markdown : was?.markdown,
      };
   });
   expect(
      mine.cells.map((c) => ({ kind: c.kind, markdown: c.markdown })),
   ).toEqual(wanted);
   expect(
      theirs.map((c) => ({
         kind: c.kind,
         markdown: c.kind === "markdown" ? c.text : c.markdown,
      })),
   ).toEqual(wanted);
   expect(
      lintNotebookText("notebooks/edited.malloy", out).filter(
         (f) => f.severity === "error",
      ),
   ).toEqual([]);

   // The server's reader agrees with ours on each query cell's caption, statement and tag lines.
   const before = serverCells(original.text).filter((c) => c.kind === "query");
   const queries = theirs.filter((c) => c.kind === "query");
   const wantedQueries = edit.doc.cells.filter((c) => c.kind === "query");
   expect(queries.length).toBe(wantedQueries.length);
   type Server = (typeof queries)[number];
   const lines = (c: Server) => c.text.replace(/\r\n/g, "\n").split("\n");
   const tags = (c: Server) => lines(c).slice(0, c.codeLine ?? 0);
   const code = (c: Server) => lines(c).slice(c.codeLine ?? 0);
   wantedQueries.forEach((cell, n) => {
      const got = queries[n];
      const gotChart = tags(got).filter((line) => parseChartLine(line));
      if (cell.added) {
         expect(code(got).join("\n").trim()).toBe("run: s -> v");
         expect(got.caption).toBe(cell.run?.caption);
         expect(gotChart).toEqual(
            cell.chart && cell.chart !== "default"
               ? [chartLineText(cell.chart as "none")]
               : [],
         );
         return;
      }
      const index = original.cells.findIndex((c) => c.id === cell.id);
      const was =
         before[
            original.cells.slice(0, index).filter((c) => c.kind === "query")
               .length
         ];
      expect(code(got)).toEqual(code(was));
      expect(tags(got).filter((l) => !parseChartLine(l))).toEqual(
         tags(was).filter((l) => !parseChartLine(l)),
      );
      expect(got.caption).toBe(was.caption);
      expect(gotChart).toEqual(
         chartUntouched(
            cell.chart,
            original.cells[index].chart!.lines.map((l) => l.text),
         )
            ? tags(was).filter((l) => parseChartLine(l))
            : cell.chart === "default"
              ? []
              : [chartLineText(cell.chart as "none")],
      );
   });

   // Every cell the edit did not rewrite keeps its bytes.
   edit.doc.cells.forEach((cell, i) => {
      const was = original.cells.find((c) => c.id === cell.id);
      if (cell.added || !was) return;
      if (cell.kind === "markdown" && cell.markdown !== was.markdown) return;
      if (
         cell.kind === "query" &&
         !chartUntouched(
            cell.chart,
            was.chart!.lines.map((l) => l.text),
         )
      )
         return;
      expect(
         withoutNewline(
            out.slice(mine.cells[i].span.start, mine.cells[i].span.end),
         ),
      ).toBe(withoutNewline(original.text.slice(was.span.start, was.span.end)));
   });
}

describe("spliceNotebookDocument: invariants over every fixture", () => {
   for (const file of fixtures)
      for (const [transform, apply] of TRANSFORMS) {
         it(`${file} (${transform})`, async () => {
            const text = apply(read(file));
            const original = await sourceOf(text);
            const base = notebookDocumentOf(original);

            const unchanged = await spliceNotebookDocument(text, base);
            expect(unchanged).toEqual({ ok: true, source: text });

            for (const edit of legalEdits(base, original))
               await expectInvariants(
                  file,
                  original,
                  edit,
                  transform === "CRLF",
               );

            // A query move `canMove` refuses, the writer refuses too; a definition's refused move can equal a legal markdown move.
            for (let from = 0; from < base.cells.length; from++)
               for (let to = 0; to < base.cells.length; to++) {
                  if (
                     base.cells[from].kind !== "query" ||
                     canMove(base, from, to)
                  )
                     continue;
                  const doc = structuredClone(base);
                  const [moved] = doc.cells.splice(from, 1);
                  doc.cells.splice(to, 0, moved);
                  expect(
                     spliceFailed(await spliceNotebookDocument(text, doc)),
                  ).toBe(true);
               }
         }, 120_000);
      }
});
