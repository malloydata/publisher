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
   readMarkdownBlocks,
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
const def = (
   startLine: number,
   endLine: number,
   text: string,
   extra: {
      markdown?: string;
      proseLines?: [number, number][];
      codeLine?: number;
      caption?: string;
   } = {},
) => ({
   kind: "definition",
   type: "code",
   text,
   proseLines: [],
   codeLine: 0,
   ...extra,
   startLine,
   endLine,
});
const query = (
   startLine: number,
   endLine: number,
   text: string,
   queryIndex: number,
   extra: {
      markdown?: string;
      proseLines?: [number, number][];
      codeLine?: number;
      caption?: string;
   } = {},
) => ({
   kind: "query",
   type: "code",
   text,
   proseLines: [],
   codeLine: 0,
   ...extra,
   startLine,
   endLine,
   queryIndex,
});

/**
 * The prose a cell's `proseLines` name, as lines: a line note's text after its route, a block's
 * body between opener and closer. Compared with `markdown` after trimming, so de-indent is ignored.
 */
const proseOf = (cell: NotebookCellSpan): string[] => {
   const lines = cell.text.split("\n");
   return (cell.proseLines ?? [])
      .flatMap(([start, end]) =>
         lines[start].trim().startsWith("#|")
            ? lines.slice(
                 start + 1,
                 lines[end].trim().startsWith("|#") ? end : end + 1,
              )
            : [lines[start].replace(/^\s*#\(markdown\) ?/, "")],
      )
      .map((line) => line.trim())
      .filter((line) => line !== "");
};
const markdownLines = (cell: NotebookCellSpan): string[] =>
   (cell.markdown ?? "")
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line !== "");

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
            { codeLine: 1 },
         ),
         md(13, 13, "A single line of prose is a cell too."),
         query(
            15,
            18,
            '#" Caption: a doc string on the run, shown above its result.\n# bar_chart\n# label="Revenue by month"\nrun: orders -> by_month + { where: region ~ $REGION }',
            0,
            {
               caption:
                  "Caption: a doc string on the run, shown above its result.",
               codeLine: 3,
            },
         ),
      ],
      annotations: [
         "##! experimental.givens\n",
         '## artifact { kind=notebook title="Revenue review" }\n',
      ],
   },
   // A notebook written as tiles: the reader still sees a named block as a markdown cell, without its name.
   "notebooks/layout.malloy": {
      cells: [
         def(3, 3, IMPORT),
         def(
            5,
            7,
            "source: orders_tiles is orders extend {\n   view: headline is { aggregate: order_count }\n}",
         ),
         md(
            9,
            13,
            "## How to read this page\n\nTotals first, then the monthly trend.",
         ),
      ],
      annotations: [
         '##" A notebook written as one column of tiles.\n',
         '## artifact { kind=notebook title="Layout" tiles=[intro { kind=text }, "orders_tiles -> headline", "orders -> by_month"] }\n',
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
            { caption: "Revenue for each month.", codeLine: 3 },
         ),
         query(10, 10, "run: orders -> kpis", 1),
         md(12, 12, "Trailing prose is a model note, so it may end the file."),
      ],
      annotations: [
         "##! experimental.givens\n",
         '## artifact { kind=notebook title="Tagged runs" }\n',
      ],
   },
   "notebooks/attached_prose.malloy": {
      cells: [
         def(3, 3, IMPORT),
         {
            ...def(
               5,
               6,
               "#(markdown) The region filter.\ngiven: REGION :: filter<string> is f''",
            ),
            markdown: "The region filter.",
            proseLines: [[0, 0]],
            codeLine: 1,
         },
         {
            ...def(
               8,
               9,
               "#(markdown) Orders in the US only.\nsource: us_orders is orders extend { where: region = 'US' }",
            ),
            markdown: "Orders in the US only.",
            proseLines: [[0, 0]],
            codeLine: 1,
         },
         query(
            11,
            17,
            '# bar_chart\n#|(markdown)\n### Orders by region\nExcludes refunds.\n|#\n# label="Orders"\nrun: us_orders -> { aggregate: order_count }',
            0,
            {
               markdown: "### Orders by region\nExcludes refunds.",
               proseLines: [[1, 4]],
               codeLine: 6,
            },
         ),
      ],
      annotations: [
         "##! experimental.givens\n",
         '## artifact { kind=notebook title="Attached prose" }\n',
      ],
   },
   "notebooks/named_runs.malloy": {
      cells: [
         def(3, 3, IMPORT),
         def(5, 5, "query: q is orders -> kpis"),
         query(7, 7, "run: q", 0),
         query(9, 10, "# bar_chart\nrun: q -> { select: order_count }", 1, {
            codeLine: 1,
         }),
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

   it("names, for every fixture cell, exactly the lines that hold its markdown", () => {
      let checked = 0;
      for (const modelPath of Object.keys(EXPECTED)) {
         for (const cell of read(modelPath).cells) {
            if (cell.type === "code") {
               expect(cell.text.split("\n")[cell.codeLine ?? -1]).toMatch(
                  /^\s*(source|query|run|given|import|export|type)\b/,
               );
            }
            if (cell.markdown === undefined) {
               if (cell.type === "code") expect(cell.proseLines).toEqual([]);
               continue;
            }
            checked++;
            expect(proseOf(cell)).toEqual(markdownLines(cell));
         }
      }
      expect(checked).toBeGreaterThan(0);
   });

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
   it.each([
      ["##|(markdown)intro\nbody\n|##\n", "##|(markdown)intro\nbody"],
      ["##(markdown)word\n", "##(markdown)word\n"],
   ])("does not serve %j as prose, since Malloy drops it", (note, kept) => {
      const result = readText(`## artifact {}\n${note}run: a -> b\n`, 1);
      expect(result.cells.map((cell) => cell.kind)).toEqual(["query"]);
      expect(result.annotations).toContain(kept);
   });

   it("adds no whitespace-only first line for a ##|(markdown) opener with trailing spaces", () => {
      const result = readText(
         "## artifact {}\n##|(markdown)   \nbody\n|##\nrun: a -> b\n",
         1,
      );
      expect(result.cells[0]).toEqual(md(2, 4, "body") as NotebookCellSpan);
   });

   it("strips a lone bare word after the opener as the block's name, and keeps any other opener text as its first line", () => {
      const named = readText(
         "## artifact {}\n##|(markdown) intro\nbody\n|##\nrun: a -> b\n",
         1,
      );
      expect(named.cells[0]).toEqual(md(2, 4, "body") as NotebookCellSpan);
      const text = readText(
         "## artifact {}\n##|(markdown) two words\nbody\n|##\nrun: a -> b\n",
         1,
      );
      expect(text.cells[0]).toEqual(
         md(2, 4, "two words\nbody") as NotebookCellSpan,
      );
      const below = readText(
         "## artifact {}\n##|(markdown)\n# intro\nbody\n|##\nrun: a -> b\n",
         1,
      );
      expect(below.cells[0]).toEqual(
         md(2, 5, "# intro\nbody") as NotebookCellSpan,
      );
   });

   it("joins ##(markdown) lines that touch into one cell, and splits at a blank line, a comment or a block", () => {
      const result = readText(
         [
            "## artifact {}",
            "##(markdown) one",
            "##(markdown) two",
            "",
            "##(markdown) three",
            "// a comment",
            "##(markdown) four",
            "##|(markdown)",
            "block",
            "|##",
            "##(markdown) five",
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

   it("reads a floating block and a floating line at the end of the file", () => {
      const result = readText(
         "## artifact {}\nrun: a -> b\n##|(markdown)\nlast block\n|##\n##(markdown) last line\n",
         1,
      );
      expect(result.cells.map((cell) => [cell.kind, cell.text])).toEqual([
         ["query", "run: a -> b"],
         ["markdown", "last block"],
         ["markdown", "last line"],
      ]);
      expect(result.annotations).toEqual(["## artifact {}\n"]);
   });

   describe("attached (markdown) prose", () => {
      it("reads a block above a run: with render tags above and below it, and keeps text the exact slice", () => {
         const slice =
            '# bar_chart\n#|(markdown)\n### Revenue by month\nExcludes refunds.\n|#\n# label="x"\nrun: a -> b';
         const result = readText(`## artifact {}\n${slice}\n`, 1);
         expect(result.error).toBeUndefined();
         expect(result.cells).toEqual([
            {
               kind: "query",
               type: "code",
               text: slice,
               markdown: "### Revenue by month\nExcludes refunds.",
               proseLines: [[1, 4]],
               codeLine: 6,
               startLine: 2,
               endLine: 8,
               queryIndex: 0,
            },
         ]);
         expect(result.annotations).toEqual(["## artifact {}\n"]);
      });

      it("reads #(markdown) lines, joining adjacent lines with a newline and blocks with a blank line", () => {
         const result = readText(
            [
               "## artifact {}",
               "#(markdown) one",
               "#(markdown) two",
               "# bar_chart",
               "#(markdown) three",
               "#|(markdown)",
               "four",
               "|#",
               "run: a -> b",
               "",
            ].join("\n"),
            1,
         );
         expect(result.cells[0].markdown).toBe("one\ntwo\n\nthree\n\nfour");
      });

      it("normalizes a CRLF file's prose to LF and leaves code cells byte-exact", () => {
         const result = readText(
            "## artifact {}\r\n#|(markdown)\r\nfirst\r\nsecond\r\n|#\r\nrun: a -> b\r\n",
            1,
         );
         expect(result.cells[0].markdown).toBe("first\nsecond");
         expect(result.cells[0].text).toBe(
            "#|(markdown)\r\nfirst\r\nsecond\r\n|#\r\nrun: a -> b",
         );
      });

      it("reads a given: and a source: with their own prose", () => {
         const result = readText(
            [
               "##! experimental.givens",
               "## artifact {}",
               "#(markdown) The region filter.",
               "given: REGION :: filter<string> is f''",
               "#(markdown) Orders, US only.",
               "source: us is a extend { where: region = 'US' }",
               "",
            ].join("\n"),
            0,
         );
         expect(result.error).toBeUndefined();
         expect(result.cells.map((cell) => [cell.kind, cell.markdown])).toEqual(
            [
               ["definition", "The region filter."],
               ["definition", "Orders, US only."],
            ],
         );
      });

      it("leaves markdown and caption off a cell that has none, and serves an empty proseLines", () => {
         const result = readText(
            "## artifact {}\n# bar_chart\nrun: a -> b\n",
            1,
         );
         expect("markdown" in result.cells[0]).toBe(false);
         expect(result.cells[0].proseLines).toEqual([]);
         expect("caption" in result.cells[0]).toBe(false);
      });

      describe("proseLines and caption", () => {
         const cellOf = (lines: string[], runs = 1) =>
            readText(["## artifact {}", ...lines, ""].join("\n"), runs)
               .cells[0];

         it("names a line note's own line", () => {
            const cell = cellOf(["#(markdown) hi", "run: a -> b"]);
            expect(cell.proseLines).toEqual([[0, 0]]);
            expect(cell.caption).toBeUndefined();
         });

         it("spans a block from its opener to its closer", () => {
            const cell = cellOf([
               "# bar_chart",
               "#|(markdown)",
               "one",
               "two",
               "|#",
               "run: a -> b",
            ]);
            expect(cell.proseLines).toEqual([[1, 4]]);
            expect(proseOf(cell)).toEqual(markdownLines(cell));
         });

         it("gives each adjacent line its own range, and each block its own", () => {
            const cell = cellOf([
               "#(markdown) one",
               "#(markdown) two",
               "# bar_chart",
               "#(markdown) three",
               "#|(markdown)",
               "four",
               "|#",
               "run: a -> b",
            ]);
            expect(cell.proseLines).toEqual([
               [0, 0],
               [1, 1],
               [3, 3],
               [4, 6],
            ]);
            expect(cell.markdown).toBe("one\ntwo\n\nthree\n\nfour");
         });

         it("counts from the statement's first token, so a column-0 |# body line in an indented statement stays prose", () => {
            const cell = cellOf([
               "   #|(markdown)",
               "   prose",
               "|# not a closer",
               "   more",
               "   |#",
               "   # bar_chart",
               "   run: a -> b",
            ]);
            expect(cell.text.split("\n")[0]).toBe("#|(markdown)");
            expect(cell.proseLines).toEqual([[0, 4]]);
            expect(cell.markdown).toContain("|# not a closer");
            expect(cell.markdown).toContain("more");
            expect(cell.markdown).not.toContain("# bar_chart");
         });

         it("names only the leading block when an indented source nests its own", () => {
            const cell = cellOf(
               [
                  "#|(markdown)",
                  "lead",
                  "|#",
                  "   source: s is a extend {",
                  "      #|(markdown)",
                  "      nested",
                  "      |#",
                  "      view: v is x",
                  "   }",
               ],
               0,
            );
            expect(cell.proseLines).toEqual([[0, 2]]);
            expect(cell.markdown).toBe("lead");
         });

         it("counts lines the same in a CRLF file, and keeps the text byte-exact", () => {
            const result = readText(
               '## artifact {}\r\n#(markdown) a\r\n#|(markdown)\r\nb\r\n|#\r\n#" cap\r\nrun: a -> b\r\n',
               1,
            );
            const [cell] = result.cells;
            expect(cell.proseLines).toEqual([
               [0, 0],
               [1, 3],
            ]);
            expect(cell.caption).toBe("cap");
            expect(cell.text.split("\n")).toHaveLength(6);
            expect(proseOf(cell)).toEqual(markdownLines(cell));
         });

         it("counts code points, not UTF-16 units, before a statement", () => {
            const cell = readText(
               [
                  "## artifact {}",
                  "run: a -> b // \u{1F600}\u{1F600}",
                  "#(markdown) \u{1F600}\u{1F600} wide",
                  "# bar_chart",
                  "#|(markdown)",
                  "\u{1F600} body",
                  "|#",
                  "run: a -> b",
                  "",
               ].join("\n"),
               2,
            ).cells[1];
            expect(cell.proseLines).toEqual([
               [0, 0],
               [2, 4],
            ]);
            expect(cell.codeLine).toBe(5);
         });

         it("reads a #(markdown) after a `;` on the previous statement's line as the next statement's prose", () => {
            const cell = readText(
               "## artifact {}\nrun: a -> b; #(markdown) y\nrun: a -> b\n",
               2,
            ).cells[1];
            expect(cell.text).toBe("#(markdown) y\nrun: a -> b");
            expect(cell.proseLines).toEqual([[0, 0]]);
            expect(cell.codeLine).toBe(1);
         });

         describe("codeLine", () => {
            it.each([
               ["no tags", ["run: a -> b"], 0],
               ["a line note", ["#(markdown) hi", "run: a -> b"], 1],
               ["a block", ["#|(markdown)", "x", "|#", "run: a -> b"], 3],
               [
                  'a leading non-markdown #|" block',
                  ['#|"', "Region to filter by", "|#", "run: a -> b"],
                  3,
               ],
               [
                  "a comment between the tags and the code",
                  ["# bar_chart", "// why", "run: a -> b"],
                  2,
               ],
               [
                  "a /* */ comment between the tags and the code",
                  ["# bar_chart", "/* one", "two */", "run: a -> b"],
                  3,
               ],
               [
                  "an indented statement",
                  ["   #(markdown) x", "   run: a -> b"],
                  1,
               ],
            ])("is the keyword's line past %s", (_what, lines, expected) => {
               const cell = cellOf(lines);
               expect(cell.codeLine).toBe(expected);
               expect(cell.text.split("\n")[cell.codeLine ?? -1].trim()).toBe(
                  "run: a -> b",
               );
            });

            it("starts each `;`-joined statement's count at its own first token", () => {
               const cells = readText(
                  "## artifact {}\nrun: a -> b; # bar_chart\nrun: c -> d\n",
                  2,
               ).cells;
               expect(cells.map((cell) => cell.codeLine)).toEqual([0, 1]);
            });

            it("points a definition at its keyword, past a #| block", () => {
               const [definition] = readText(
                  '## artifact {}\n#|\nlabel="Orders"\n|#\nsource: s is a\n',
                  0,
               ).cells;
               expect(definition.codeLine).toBe(3);
            });
         });

         it('joins the #" lines into a caption, apart from prose and other tags', () => {
            const cell = cellOf([
               '#" First',
               "# bar_chart",
               '#" second  ',
               "#(markdown) prose",
               "run: a -> b",
            ]);
            expect(cell.caption).toBe("First second");
            expect(cell.proseLines).toEqual([[3, 3]]);
         });

         it('reads a caption on a definition too, and none from a #|" block or a nested note', () => {
            const [definition] = readText(
               '## artifact {}\n#" About s\nsource: s is a extend {\n   #" inner\n   view: v is x\n}\n',
               0,
            ).cells;
            expect(definition.caption).toBe("About s");
            const [blocked] = readText(
               '## artifact {}\n#|"\nblock caption\n|#\nrun: a -> b\n',
               1,
            ).cells;
            expect("caption" in blocked).toBe(false);
         });
      });

      it('does not read a #" caption as prose', () => {
         const result = readText(
            '## artifact {}\n#" a caption\n#(markdown) prose\nrun: a -> b\n',
            1,
         );
         expect(result.cells[0].markdown).toBe("prose");
      });

      it.each([
         ["an import", 'import "x.malloy"'],
         ["an export", "export { a }"],
      ])(
         "refuses #(markdown) above %s, naming ##(markdown) for a line",
         (_what, stmt) => {
            const result = readText(
               `## artifact {}\n#(markdown) prose\n${stmt}\nrun: a -> b\n`,
               1,
            );
            expect(result.cells).toEqual([]);
            expect(result.error?.line).toBe(2);
            expect(result.error?.message).toContain("`##(markdown)`");
            expect(result.error?.message).not.toContain("`##|(markdown)`");
         },
      );

      it("refuses a dangling #(markdown) with nothing below it, naming ##(markdown)", () => {
         const result = readText(
            "## artifact {}\nrun: a -> b\n#(markdown) trailing\n",
            1,
         );
         expect(result.cells).toEqual([]);
         expect(result.error?.line).toBe(3);
         expect(result.error?.message).toContain("`##(markdown)`");
      });

      it("names ##|(markdown) and its closer for a dangling #|(markdown) block", () => {
         const result = readText(
            "## artifact {}\nrun: a -> b\n#|(markdown)\ntrailing\n|#\n",
            1,
         );
         expect(result.cells).toEqual([]);
         expect(result.error?.line).toBe(3);
         expect(result.error?.message).toContain(
            "`##|(markdown)` block closed by `|##`",
         );
      });
   });

   describe('the earlier prose spellings after the tag (`"` and `(text)`)', () => {
      it.each([
         ['##" one line\n', md(2, 2, "one line")],
         ['##|"\nbody\n|##\n', md(2, 4, "body")],
         ['##|" first\nbody\n|##\n', md(2, 4, "first\nbody")],
         ['##|" Summary\nbody\n|##\n', md(2, 4, "Summary\nbody")],
         ["##(text) one line\n", md(2, 2, "one line")],
         ["##|(text) name\nbody\n|##\n", md(2, 4, "body")],
         ["##|(text) two words\nbody\n|##\n", md(2, 4, "two words\nbody")],
      ])("reads %j as a markdown cell, not a note", (note, cell) => {
         const lines = note.split("\n").length - 1;
         const result = readText(`## artifact {}\n${note}run: a -> b\n`, 1);
         expect(result.cells).toEqual([
            cell,
            query(2 + lines, 2 + lines, "run: a -> b", 0),
         ] as NotebookCellSpan[]);
         expect(result.annotations).toEqual(["## artifact {}\n"]);
      });

      it('never reads a `"` note below the tag as the description', () => {
         const result = readText('## artifact {}\n##" prose\nrun: a -> b\n', 1);
         expect(result.annotations).toEqual(["## artifact {}\n"]);
      });

      it('keeps a `"` note above the tag as the description', () => {
         const result = readText(
            '##" description\n## artifact {}\nrun: a -> b\n',
            1,
         );
         expect(result.annotations).toEqual([
            '##" description\n',
            "## artifact {}\n",
         ]);
         expect(result.cells.map((cell) => cell.kind)).toEqual(["query"]);
      });

      it("reads a dashboard tile block written (text) as a tile, named by its lone bare word", () => {
         const text =
            "##|(text) intro\nbody\n|##\n##|(text) two words\nb\n|##\n";
         const parse = parseNotebookText(text);
         if (isNotebookReaderError(parse)) throw new Error(parse.message);
         expect(readMarkdownBlocks(parse, text)).toEqual([
            { name: "intro", route: "text", line: 1, endLine: 3 },
            { name: undefined, route: "text", line: 4, endLine: 6 },
         ]);
      });

      it('never reads a ##|" block as a tile, whatever its opener says', () => {
         const text = '##|" intro\nbody\n|##\n';
         const parse = parseNotebookText(text);
         if (isNotebookReaderError(parse)) throw new Error(parse.message);
         expect(readMarkdownBlocks(parse, text)).toEqual([]);
      });
   });

   it("reads a (markdown) tile block's name and the lines it spans", () => {
      const text =
         '##" d\n## artifact { tiles=[intro] }\n##|(markdown) intro\n## Heading\nBody\n|##\n##|(markdown) _b2\nMore\n|##\n';
      const parse = parseNotebookText(text);
      if (isNotebookReaderError(parse)) throw new Error(parse.message);
      expect(readMarkdownBlocks(parse, text)).toEqual([
         { name: "intro", route: "markdown", line: 3, endLine: 6 },
         { name: "_b2", route: "markdown", line: 7, endLine: 9 },
      ]);
   });

   it.each([
      ["##|(markdown)", undefined],
      ["##|(markdown)   ", undefined],
      ["##|(markdown) two words", undefined],
      ["##|(markdown) 9lives", undefined],
      ["##|(markdown) has-dash", undefined],
      ["##|(markdown) ok_1", "ok_1"],
   ])("reads the name of %j as %j", (opener, name) => {
      const text = `${opener}\nbody\n|##\n`;
      const parse = parseNotebookText(text);
      if (isNotebookReaderError(parse)) throw new Error(parse.message);
      expect(
         readMarkdownBlocks(parse, text).map((block) => block.name),
      ).toEqual([name]);
   });

   it('does not read ##|" or ##|(filters) blocks as markdown blocks', () => {
      const text = '##|"\nprose\n|##\n##|(filters)\n["a"]\n|##\n';
      const parse = parseNotebookText(text);
      if (isNotebookReaderError(parse)) throw new Error(parse.message);
      expect(readMarkdownBlocks(parse, text)).toEqual([]);
   });

   it("gives a tag block after the artifact tag Malloy's own note text, which MOTLY can read", () => {
      const text =
         "## artifact {}\n##|\nautorun=false\n|##\n##(markdown) prose\nrun: a -> b\n";
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
         "## artifact {}\n##(markdown) Sales 📈 up\nrun: a -> b\n",
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
