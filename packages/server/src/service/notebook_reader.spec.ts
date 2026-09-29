// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The served-notebook cell reader over every fixture in
 * `tests/fixtures/notebooks-malloyyo/notebooks/`, each compiled through
 * `Model.create` so the reader sees the text and IR a real load does.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection, ModelDef } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { Model } from "./model";
import { motlyTag } from "./motly";
import {
   isNotebookReaderError,
   parseNotebookText,
   readNotebookCells,
   readTextBlocks,
   type NotebookCellSpan,
   type NotebookReadResult,
} from "./notebook";

const FIXTURE_DIR = path.resolve(
   __dirname,
   "../../tests/fixtures/notebooks-malloyyo",
);

const IMPORT = 'import "../models/orders.malloy"';

const md = (startLine: number, endLine: number, text: string) => ({
   kind: "markdown",
   type: "markdown",
   text,
   startLine,
   endLine,
});
const def = (startLine: number, endLine: number, text: string) => ({
   kind: "definition",
   type: "code",
   text,
   startLine,
   endLine,
});
const query = (
   startLine: number,
   endLine: number,
   text: string,
   queryIndex: number,
) => ({ kind: "query", type: "code", text, startLine, endLine, queryIndex });

/** Every fixture notebook's cells, written out literally so a misread fails with a readable diff. */
const EXPECTED: Record<string, { cells: unknown[]; annotations: string[] }> = {
   "notebooks/revenue_review.malloy": {
      cells: [
         def(3, 3, IMPORT),
         md(
            5,
            8,
            "# Where revenue came from\nProse in **markdown**, any length.",
         ),
         def(
            10,
            11,
            "# label=\"Region\" control=select suggest { source=orders dimension=region }\ngiven: REGION :: filter<string> is f''",
         ),
         md(13, 13, "A single line of prose is a cell too."),
         query(
            15,
            18,
            '#" Caption: a doc string on the run, shown above its result.\n# bar_chart\n# label="Revenue by month"\nrun: orders -> by_month + { where: region ~ $REGION }',
            0,
         ),
      ],
      annotations: [
         "##! experimental.givens\n",
         '## artifact { kind=notebook title="Revenue review" }\n',
      ],
   },
   "notebooks/definitions_only.malloy": {
      cells: [
         def(3, 3, IMPORT),
         def(
            5,
            7,
            "source: us_orders is orders extend {\n   where: region = 'US'\n}",
         ),
         def(9, 9, "given: BRAND :: filter<string> is f''"),
      ],
      annotations: [
         "##! experimental.givens\n",
         '## artifact { kind=notebook title="Definitions only" }\n',
      ],
   },
   "notebooks/adjacent_blocks.malloy": {
      cells: [
         def(3, 3, IMPORT),
         md(
            6,
            9,
            "## A heading\nThe first block's body starts with a markdown heading.",
         ),
         md(10, 12, "The second block follows with no statement between them."),
         query(14, 14, "run: orders -> kpis", 0),
      ],
      annotations: [
         "##! experimental.givens\n",
         '## artifact { kind=notebook title="Adjacent blocks" }\n',
      ],
   },
   "notebooks/imported_prose.malloy": {
      cells: [
         def(4, 4, IMPORT),
         md(6, 6, "The only prose cell in this notebook."),
         query(8, 8, "run: orders -> kpis", 0),
      ],
      annotations: [
         "##! experimental.givens\n",
         '##" Order totals, described above the artifact tag.\n',
         '## artifact { kind=notebook title="Imported prose" }\n',
      ],
   },
   "notebooks/prose_lines.malloy": {
      cells: [
         def(3, 3, IMPORT),
         md(5, 6, "Two contiguous lines of prose\nare one markdown cell."),
         md(8, 8, "A blank line above starts a second cell."),
         query(10, 10, "run: orders -> kpis", 0),
      ],
      annotations: [
         "##! experimental.givens\n",
         '## artifact { kind=notebook title="Prose lines" }\n',
      ],
   },
   "notebooks/tagged_runs.malloy": {
      cells: [
         def(3, 3, IMPORT),
         query(
            5,
            8,
            '#" Revenue for each month.\n# bar_chart\n# label="Revenue by month"\nrun: orders -> by_month',
            0,
         ),
         query(10, 10, "run: orders -> kpis", 1),
         md(12, 12, "Trailing prose is a model note, so it may end the file."),
      ],
      annotations: [
         "##! experimental.givens\n",
         '## artifact { kind=notebook title="Tagged runs" }\n',
      ],
   },
   "notebooks/named_runs.malloy": {
      cells: [
         def(3, 3, IMPORT),
         def(5, 5, "query: q is orders -> kpis"),
         query(7, 7, "run: q", 0),
         query(9, 10, "# bar_chart\nrun: q -> { select: order_count }", 1),
         query(12, 12, "run: q + { limit: 1 }", 2),
      ],
      annotations: [
         "##! experimental.givens\n",
         '## artifact { kind=notebook title="Named runs" }\n',
      ],
   },
   "notebooks/structure.malloy": {
      cells: [
         def(6, 6, IMPORT),
         md(
            8,
            11,
            "# Not a name\nThe first line under the opener is prose, whatever it holds.",
         ),
         def(
            15,
            15,
            "source: us_orders is orders extend { where: region = 'US' }",
         ),
         def(17, 17, "export { us_orders }"),
         query(19, 19, "run: us_orders -> kpis", 0),
      ],
      annotations: [
         "##! experimental.givens\n",
         '## artifact { kind=notebook title="Structure" }\n',
         '## title="A non-prose note after the tag"\n',
         '##(filters) ["orders.region"]\n',
      ],
   },
};

describe("readNotebookCells over the fixture notebooks", () => {
   let duckdb: DuckDBConnection;
   const compiled = new Map<string, { modelDef: ModelDef; text: string }>();

   const read = (modelPath: string): NotebookReadResult => {
      const entry = compiled.get(modelPath);
      if (!entry) throw new Error(`${modelPath} did not compile`);
      const parse = parseNotebookText(entry.text);
      if (isNotebookReaderError(parse)) throw new Error(parse.message);
      return readNotebookCells(parse, entry.modelDef, entry.text);
   };

   beforeAll(async () => {
      duckdb = new DuckDBConnection("duckdb", ":memory:", FIXTURE_DIR);
      const paths = fs
         .readdirSync(path.join(FIXTURE_DIR, "notebooks"))
         .map((file) => `notebooks/${file}`);
      for (const modelPath of paths) {
         const model = await Model.create(
            "notebooks-malloyyo",
            FIXTURE_DIR,
            modelPath,
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const modelDef = model.getModelDef();
         const text = model.getCompiledSourceText();
         if (!modelDef || text === undefined) {
            throw new Error(`${modelPath} did not compile`);
         }
         // A Windows checkout hands the fixtures over as CRLF; the expectations are LF.
         compiled.set(modelPath, {
            modelDef,
            text: text.replace(/\r\n/g, "\n"),
         });
      }
   });

   afterAll(async () => {
      await duckdb.close();
   });

   it("has an expectation for every fixture notebook but the refused one", () => {
      expect([...compiled.keys()].sort()).toEqual(
         [...Object.keys(EXPECTED), "notebooks/refused.malloy"].sort(),
      );
   });

   for (const [modelPath, expected] of Object.entries(EXPECTED)) {
      it(`reads ${modelPath} into its cells, in file order`, () => {
         const result = read(modelPath);
         expect(result.error).toBeUndefined();
         expect(result.cells).toEqual(expected.cells as NotebookCellSpan[]);
      });

      it(`collects ${modelPath}'s header and non-prose notes once each, in file order`, () => {
         expect(read(modelPath).annotations).toEqual(expected.annotations);
      });
   }

   it("maps every query cell to its own compiled run, in order", () => {
      for (const modelPath of Object.keys(EXPECTED)) {
         const { modelDef } = compiled.get(modelPath)!;
         const indexes = read(modelPath)
            .cells.filter((cell) => cell.kind === "query")
            .map((cell) => cell.queryIndex);
         expect(indexes).toEqual(modelDef.queryList.map((_, k) => k));
      }
   });

   it("refuses a notebook with a statement no cell can hold, naming its line, and returns no cells", () => {
      const result = read("notebooks/refused.malloy");
      expect(result.cells).toEqual([]);
      expect(result.annotations).toEqual([]);
      expect(result.error?.line).toBe(7);
      expect(result.error?.message).toStartWith("Line 7: ");
      expect(result.error?.message).toContain("Fix: ");
   });

   it("never takes an imported include's prose for a cell", () => {
      const texts = read("notebooks/imported_prose.malloy").cells.map(
         (cell) => cell.text,
      );
      expect(texts.join("\n")).not.toContain("Shared orders model");
   });

   it("serves the same prose and annotations from a CRLF checkout, and code cells byte-exact", () => {
      for (const [modelPath, expected] of Object.entries(EXPECTED)) {
         const { modelDef, text } = compiled.get(modelPath)!;
         const crlf = text.replace(/\n/g, "\r\n");
         const parse = parseNotebookText(crlf);
         if (isNotebookReaderError(parse)) throw new Error(parse.message);
         const result = readNotebookCells(parse, modelDef, crlf);
         expect(result.error).toBeUndefined();
         expect(result.annotations).toEqual(expected.annotations);
         expect(result.cells).toEqual(
            (expected.cells as NotebookCellSpan[]).map((cell) =>
               cell.type === "code"
                  ? { ...cell, text: cell.text.replace(/\n/g, "\r\n") }
                  : cell,
            ),
         );
      }
   });
});

describe("readNotebookCells on inline text", () => {
   it('never strips or names the first line of a ##|" block, whatever it holds', () => {
      const onOpener = readText(
         '## artifact {}\n##|" intro\nbody\n|##\nrun: a -> b\n',
         1,
      );
      expect(onOpener.cells[0]).toEqual(
         md(2, 4, "intro\nbody") as NotebookCellSpan,
      );
      const below = readText(
         '## artifact {}\n##|"\nintro\nbody\n|##\nrun: a -> b\n',
         1,
      );
      expect(below.cells[0]).toEqual(
         md(2, 5, "intro\nbody") as NotebookCellSpan,
      );
   });

   it('joins ##" lines that touch into one cell, and splits at a blank line, a comment or a block', () => {
      const result = readText(
         [
            "## artifact {}",
            '##" one',
            '##" two',
            "",
            '##" three',
            "// a comment",
            '##" four',
            '##|"',
            "block",
            "|##",
            '##" five',
            "run: a -> b",
            "",
         ].join("\n"),
         1,
      );
      expect(
         result.cells.map((cell) => [cell.startLine, cell.endLine, cell.text]),
      ).toEqual([
         [2, 3, "one\ntwo"],
         [5, 5, "three"],
         [7, 7, "four"],
         [8, 10, "block"],
         [11, 11, "five"],
         [12, 12, "run: a -> b"],
      ]);
   });

   it("leaves a (text) block out of a notebook's cells, as a note that is not prose", () => {
      const result = readText(
         "## artifact {}\n##|(text) intro\nbody\n|##\nrun: a -> b\n",
         1,
      );
      expect(result.cells.map((cell) => cell.kind)).toEqual(["query"]);
      expect(result.annotations).toContain("##|(text) intro\nbody");
   });

   it("strips a (text) block's opener and reads its name, leaving the body as the tile's markdown", () => {
      const text =
         '##" d\n## artifact { tiles=[intro] }\n##|(text) intro\n## Heading\nBody\n|##\n##|(text) _b2\nMore\n|##\n';
      const parse = parseNotebookText(text);
      if (isNotebookReaderError(parse)) throw new Error(parse.message);
      expect(readTextBlocks(parse, text)).toEqual([
         { name: "intro", body: "## Heading\nBody", line: 3, endLine: 6 },
         { name: "_b2", body: "More", line: 7, endLine: 9 },
      ]);
   });

   it.each([
      ["##|(text)", undefined],
      ["##|(text)   ", undefined],
      ["##|(text) two words", undefined],
      ["##|(text) 9lives", undefined],
      ["##|(text) has-dash", undefined],
      ["##|(text) ok_1", "ok_1"],
   ])("reads the name of %j as %j", (opener, name) => {
      const text = `${opener}\nbody\n|##\n`;
      const parse = parseNotebookText(text);
      if (isNotebookReaderError(parse)) throw new Error(parse.message);
      expect(readTextBlocks(parse, text).map((block) => block.name)).toEqual([
         name,
      ]);
   });

   it('does not read ##|" or ##|(filters) blocks as text blocks', () => {
      const text = '##|"\nprose\n|##\n##|(filters)\n["a"]\n|##\n';
      const parse = parseNotebookText(text);
      if (isNotebookReaderError(parse)) throw new Error(parse.message);
      expect(readTextBlocks(parse, text)).toEqual([]);
   });

   it("gives a tag block after the artifact tag Malloy's own note text, which MOTLY can read", () => {
      const text =
         '## artifact {}\n##|\nautorun=false\n|##\n##" prose\nrun: a -> b\n';
      const result = readText(text, 1);
      expect(result.annotations).toEqual([
         "## artifact {}\n",
         "##|\nautorun=false",
      ]);
      expect(motlyTag(result.annotations)?.text("autorun")).toBe("false");
   });

   it("refuses a # tag that annotates nothing, naming its line and the move", () => {
      const result = readText('## artifact {}\nrun: a -> b\n#" trailing\n', 1);
      expect(result.cells).toEqual([]);
      expect(result.error?.line).toBe(3);
      expect(result.error?.message).toContain(
         "move the tag directly above its run:",
      );
   });

   const readText = (text: string, runs: number): NotebookReadResult => {
      const parse = parseNotebookText(text);
      if (isNotebookReaderError(parse)) throw new Error(parse.message);
      return readNotebookCells(
         parse,
         { queryList: new Array(runs).fill({}) } as Pick<ModelDef, "queryList">,
         text,
      );
   };

   it("reads a run: above the tag as a definition cell that still holds its query slot", () => {
      const result = readText("run: a -> b\n## artifact {}\nrun: c -> d\n", 2);
      expect(result.cells).toEqual([
         def(1, 1, "run: a -> b"),
         query(3, 3, "run: c -> d", 1),
      ] as NotebookCellSpan[]);
   });

   it("refuses a file with no artifact note", () => {
      expect(readText("run: a -> b\n", 1).error?.line).toBe(1);
   });

   it("refuses when the compile holds a different number of runs than the file", () => {
      const result = readText("## artifact {}\nrun: a -> b\n", 2);
      expect(result.cells).toEqual([]);
      expect(result.error?.message).toContain("found 1 run:");
   });

   it("keeps an emoji-bearing line whole, slicing by code point", () => {
      const result = readText(
         '## artifact {}\n##" Sales 📈 up\nrun: a -> b\n',
         1,
      );
      expect(result.cells[0]).toEqual(
         md(2, 2, "Sales 📈 up") as NotebookCellSpan,
      );
   });

   it("refuses a syntax error before reading anything", () => {
      const parse = parseNotebookText("## artifact {}\nsource: oops is\n");
      expect(isNotebookReaderError(parse)).toBe(true);
   });

   it("refuses a default-channel token no statement or note accounts for", () => {
      const text = "## artifact {}\nrun: a -> b\n";
      const parse = parseNotebookText(text);
      if (isNotebookReaderError(parse)) throw new Error(parse.message);
      const stream = parse.tokenStream as {
         getTokens(): { type: number; channel: number; line: number }[];
      };
      const real = stream.getTokens();
      // The tree ends at `b`; a token past it models what ANTLR's error recovery can leave behind.
      // Built field by field: an ANTLR token's fields are getters a spread would drop.
      const stray = {
         type: real[real.length - 2].type,
         channel: 0,
         startIndex: 900,
         stopIndex: 901,
         line: 3,
      };
      const result = readNotebookCells(
         {
            root: parse.root,
            tokenStream: {
               tokenSource: (parse.tokenStream as { tokenSource: unknown })
                  .tokenSource,
               getTokens: () => [...real, stray],
            },
         },
         { queryList: [{}] } as unknown as Pick<ModelDef, "queryList">,
         text,
      );
      expect(result.cells).toEqual([]);
      expect(result.error?.line).toBe(3);
   });
});
