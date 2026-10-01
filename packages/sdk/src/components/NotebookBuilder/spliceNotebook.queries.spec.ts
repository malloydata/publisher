// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { spliceFailed } from "../DashboardBuilder/spliceResult";
import {
   notebookSourceRefused,
   readNotebookSource,
   type NotebookSource,
} from "./readNotebookSource";
import {
   canInsertQuery,
   canMove,
   notebookDocumentOf,
   removesReadQuery,
   spliceNotebookDocument,
   type NotebookDocument,
} from "./spliceNotebook";

const DEF = 'source: a is duckdb.sql("select 1 as x")';
const QUERY = "run: a -> { select: x }";
const TAGGED = `// Why this run.\n#" Revenue\n# bar_chart\n${QUERY}`;
const TEXT = `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Intro.\n\n${TAGGED}\n\n##(markdown) After.\n`;
const BAR =
   "# -line_chart -big_value -scatter_chart -shape_map -segment_map -viz bar_chart";

type Cell = NotebookDocument["cells"][number];

async function sourceOf(text: string): Promise<NotebookSource> {
   const result = await readNotebookSource(text);
   if (notebookSourceRefused(result)) throw new Error(result.refused);
   return result.source;
}

const docOf = async (text: string) => notebookDocumentOf(await sourceOf(text));

const newQuery = (over: Partial<Cell> = {}): Cell => ({
   id: "added-1",
   kind: "query",
   added: true,
   run: { source: "a", view: "v" },
   ...over,
});

const NO_LIST = Symbol("no reachable list");

async function splice(
   text: string,
   change: (doc: NotebookDocument) => void,
   reachable: readonly string[] | typeof NO_LIST = ["a"],
) {
   const doc = await docOf(text);
   change(doc);
   return spliceNotebookDocument(
      text,
      doc,
      undefined,
      reachable === NO_LIST ? undefined : reachable,
   );
}

async function written(
   text: string,
   change: (doc: NotebookDocument) => void,
   reachable?: readonly string[],
) {
   const result = await splice(text, change, reachable);
   if (spliceFailed(result)) throw new Error(result.reason);
   return result.source;
}

async function refused(
   text: string,
   change: (doc: NotebookDocument) => void,
   reachable?: readonly string[] | typeof NO_LIST,
) {
   const result = await splice(text, change, reachable);
   if (!spliceFailed(result)) throw new Error("expected a refusal");
   return result.reason;
}

describe("spliceNotebookDocument: query cells", () => {
   it("reads a query cell's chart state into the document", async () => {
      const doc = await docOf(TEXT);
      expect(doc.cells.map((c) => c.chart)).toEqual([
         undefined,
         undefined,
         "bar_chart",
         undefined,
      ]);
      expect(await splice(TEXT, () => {})).toEqual({ ok: true, source: TEXT });
   });

   it("adds a query below every definition with its caption and chart", async () => {
      const out = await written(TEXT, (doc) => {
         doc.cells.splice(
            3,
            0,
            newQuery({
               chart: "line_chart",
               run: { source: "a", view: "v", caption: "Trend" },
            }),
         );
      });
      expect(out).toBe(
         TEXT.replace(
            "\n##(markdown) After.",
            `\n#" Trend\n# -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz line_chart\nrun: a -> v\n\n##(markdown) After.`,
         ),
      );
      const back = await sourceOf(out);
      expect(back.cells.map((c) => c.kind)).toEqual([
         "definition",
         "markdown",
         "query",
         "query",
         "markdown",
      ]);
   });

   it("adds to a CRLF notebook with no final newline, in CRLF and still without one", async () => {
      const text = `## artifact { kind=notebook }\r\n${DEF}`;
      const out = await written(text, (doc) => {
         doc.cells.push(newQuery({ chart: "none" }));
      });
      expect(out).toBe(
         `## artifact { kind=notebook }\r\n${DEF}\r\n\r\n# -line_chart -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz\r\nrun: a -> v`,
      );
      expect(/(^|[^\r])\n/.test(out)).toBe(false);
   });

   it("keeps a comment that opens the next cell out of an added query", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n// A note on the next thing.\n##(markdown) Prose.\n`;
      const out = await written(text, (doc) => {
         doc.cells.splice(1, 0, newQuery());
      });
      const back = await sourceOf(out);
      expect(back.cells.map((c) => c.kind)).toEqual([
         "definition",
         "query",
         "markdown",
      ]);
      expect(out).toContain(
         "// A note on the next thing.\n##(markdown) Prose.",
      );
   });

   it("refuses an added query above a definition, from an unlisted source, or with nothing to run", async () => {
      expect(
         await refused(TEXT, (doc) => {
            doc.cells.splice(0, 0, newQuery());
         }),
      ).toContain("above a definition");
      expect(
         await refused(TEXT, (doc) => {
            doc.cells.push(newQuery({ run: { source: "b", view: "v" } }));
         }),
      ).toContain('"b" is not one this notebook can read');
      expect(
         await refused(
            TEXT,
            (doc) => {
               doc.cells.push(newQuery());
            },
            NO_LIST,
         ),
      ).toContain("not known");
      expect(
         await refused(TEXT, (doc) => {
            doc.cells.push(newQuery({ run: undefined }));
         }),
      ).toContain("nothing to run");
      expect(
         await refused(TEXT, (doc) => {
            doc.cells.push(newQuery({ chart: "custom" }));
         }),
      ).toContain("not model");
   });

   it("back-quotes a view name that is not a bare identifier", async () => {
      const out = await written(TEXT, (doc) => {
         doc.cells.push(newQuery({ run: { source: "a", view: "v w" } }));
      });
      expect(out).toContain("run: a -> `v w`\n");
   });

   it("inserts, replaces and removes only the chart line of a read query", async () => {
      const replaced = await written(TEXT, (doc) => {
         doc.cells[2].chart = "line_chart";
      });
      expect(replaced).toBe(
         TEXT.replace(
            "# bar_chart\n",
            "# -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz line_chart\n",
         ),
      );
      const removed = await written(TEXT, (doc) => {
         doc.cells[2].chart = "default";
      });
      expect(removed).toBe(TEXT.replace("# bar_chart\n", ""));
      const inserted = await written(removed, (doc) => {
         doc.cells[2].chart = "bar_chart";
      });
      expect(inserted).toBe(TEXT.replace("# bar_chart", BAR));
      const none = await written(TEXT, (doc) => {
         doc.cells[2].chart = "none";
      });
      expect(none).toContain(
         "# -line_chart -bar_chart -big_value -scatter_chart -shape_map -segment_map -viz\n",
      );
   });

   it("puts a new chart line directly above the run, below a comment that follows the tags", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n#" Cap\n// inline\n${QUERY}\n`;
      const out = await written(text, (doc) => {
         doc.cells[1].chart = "bar_chart";
      });
      expect(out).toBe(
         `## artifact { kind=notebook }\n${DEF}\n\n#" Cap\n// inline\n${BAR}\n${QUERY}\n`,
      );
   });

   it("edits a CRLF notebook's chart line in CRLF", async () => {
      const crlf = TEXT.replace(/\n/g, "\r\n");
      const out = await written(crlf, (doc) => {
         doc.cells[2].chart = "default";
      });
      expect(out).toBe(crlf.replace("# bar_chart\r\n", ""));
      const inserted = await written(out, (doc) => {
         doc.cells[2].chart = "bar_chart";
      });
      expect(inserted).toBe(crlf.replace("# bar_chart", BAR));
   });

   it("keeps unmodelled chart lines byte for byte through other edits and refuses to change their chart", async () => {
      for (const line of [
         "# bar_chart { size=spark }",
         '# line_chart label="Revenue"',
      ]) {
         const text = `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Intro.\n\n${line}\n${QUERY}\n`;
         const out = await written(text, (doc) => {
            doc.cells[1].markdown = "Intro, edited.";
         });
         expect(out).toBe(text.replace("Intro.", "Intro, edited."));
         expect((await docOf(text)).cells[2].chart).toBe("default");
         const reason = await refused(text, (d) => {
            d.cells[2].chart = "line_chart";
         });
         expect(reason).toContain(line);
      }
   });

   it("replaces a bare recognized line, and refuses a cell with two", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n# line_chart\n${QUERY}\n`;
      const out = await written(text, (doc) => {
         doc.cells[1].chart = "bar_chart";
      });
      expect(out).toContain(`${BAR}\n${QUERY}`);
      const two = `## artifact { kind=notebook }\n${DEF}\n\n# line_chart\n# -viz\n${QUERY}\n`;
      expect(
         await refused(two, (doc) => {
            doc.cells[1].chart = "bar_chart";
         }),
      ).toContain("more than one chart line");
      expect(await splice(two, () => {})).toEqual({ ok: true, source: two });
   });

   it("never rewrites a read cell's statement or caption", async () => {
      const out = await written(TEXT, (doc) => {
         doc.cells[2].chart = "none";
         doc.cells[2].run = { source: "zzz", view: "nope", caption: "No" };
      });
      expect(out).toContain('// Why this run.\n#" Revenue\n');
      expect(out).toContain(`\n${QUERY}\n`);
   });

   it("removes a read query with its comment and tag lines", async () => {
      const out = await written(TEXT, (doc) => {
         doc.cells.splice(2, 1);
      });
      expect(out).toBe(
         `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Intro.\n\n##(markdown) After.\n`,
      );
   });
});

describe("canMove and canInsertQuery with added queries", () => {
   const doc = (): NotebookDocument => ({
      cells: [
         { id: "0", kind: "definition" },
         { id: "1", kind: "markdown", markdown: "x" },
         { id: "added-1", kind: "query", added: true },
         { id: "2", kind: "definition" },
         { id: "added-2", kind: "query", added: true },
      ],
   });

   it("keeps an added query below every definition when it moves", () => {
      expect(canMove(doc(), 2, 0)).toBe(false);
      expect(canMove(doc(), 4, 3)).toBe(false);
      expect(canMove(doc(), 4, 1)).toBe(false);
      expect(canMove(doc(), 2, 1)).toBe(true);
      expect(canMove(doc(), 2, 4)).toBe(true);
   });

   it("offers a query slot only below the last definition", () => {
      const d = doc();
      d.cells.splice(2, 1);
      expect(canInsertQuery(d, 0)).toBe(false);
      expect(canInsertQuery(d, 2)).toBe(false);
      expect(canInsertQuery(d, 3)).toBe(true);
      expect(canInsertQuery(d, 4)).toBe(true);
      expect(canInsertQuery(d, 5)).toBe(false);
      expect(canInsertQuery({ cells: [] }, 0)).toBe(true);
   });

   it("knows when a save removes a cell the file already had", () => {
      const saved: NotebookDocument = {
         cells: [
            { id: "0", kind: "query" },
            { id: "added-1", kind: "query", added: true },
         ],
      };
      expect(removesReadQuery(saved, { cells: [saved.cells[0]] })).toBe(false);
      expect(removesReadQuery(saved, { cells: [saved.cells[1]] })).toBe(true);
      expect(removesReadQuery(saved, { cells: [] })).toBe(true);
      expect(removesReadQuery({ cells: [saved.cells[1]] }, { cells: [] })).toBe(
         false,
      );
   });
});
