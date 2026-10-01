// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   notebookSourceRefused,
   readNotebookSource,
   type NotebookSource,
} from "./readNotebookSource";
import {
   callAccessor,
   isNotebookReaderError,
   parseNotebookText,
   readNotebookCells,
   type NotebookReadResult,
   type ParseNode,
} from "../../../../server/src/service/notebook";
import { lintNotebookText } from "../../../../server/src/service/notebook_lint";

const FIXTURES = path.resolve(
   import.meta.dir,
   "../../../../server/tests/fixtures",
);

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
].sort();

const REFUSED = path.join("notebooks-malloyyo", "notebooks", "refused.malloy");

const read = (file: string) =>
   fs.readFileSync(path.join(FIXTURES, file), "utf8");

/** The server's own reader over the same text, with a compile stand-in that has one slot per `run:`. */
function oracle(text: string): NotebookReadResult | { parseError: string } {
   const parse = parseNotebookText(text);
   if (isNotebookReaderError(parse)) return { parseError: parse.message };
   const root = parse.root as ParseNode;
   let runCount = 0;
   for (let i = 0; i < (root.childCount ?? 0); i++)
      if (callAccessor(root.getChild(i), "runStatement") !== undefined)
         runCount++;
   return readNotebookCells(
      parse,
      { queryList: Array(runCount) } as Parameters<typeof readNotebookCells>[1],
      text,
   );
}

async function source(text: string): Promise<NotebookSource> {
   const result = await readNotebookSource(text);
   if (notebookSourceRefused(result)) throw new Error(result.refused);
   return result.source;
}

/** Header, gaps, cells and trailing bytes, in file order. */
function pieces(source: NotebookSource): string[] {
   const out = [source.text.slice(source.header.start, source.header.end)];
   let at = source.header.end;
   for (const cell of source.cells) {
      out.push(source.text.slice(at, cell.span.start));
      out.push(source.text.slice(cell.span.start, cell.span.end));
      at = cell.span.end;
   }
   out.push(source.text.slice(at));
   return out;
}

function expectTiles(source: NotebookSource) {
   const { text, header, cells } = source;
   expect(header.start).toBe(0);
   let at = header.end;
   for (const cell of cells) {
      expect(cell.span.start).toBeGreaterThanOrEqual(at);
      expect(cell.span.end).toBeGreaterThan(cell.span.start);
      // Whole lines: a span starts at a line start and ends after a newline or at EOF.
      expect(cell.span.start === 0 || text[cell.span.start - 1] === "\n").toBe(
         true,
      );
      expect(
         cell.span.end === text.length || text[cell.span.end - 1] === "\n",
      ).toBe(true);
      at = cell.span.end;
   }
   expect(pieces(source).join("")).toBe(text);
}

const lineOf = (text: string, offset: number) =>
   text.slice(0, offset).split("\n").length;

/** The locator agrees with the server on every cell, and its spans hold what the server read. */
async function expectMatchesOracle(text: string) {
   const server = oracle(text);
   if ("parseError" in server) throw new Error(server.parseError);
   expect(server.error).toBeUndefined();
   const mine = await source(text);
   expectTiles(mine);
   expect(mine.cells.map((c) => c.kind)).toEqual(
      server.cells.map((c) => c.kind),
   );
   mine.cells.forEach((cell, i) => {
      const theirs = server.cells[i];
      expect(cell.id).toBe(String(i));
      expect(cell.markdown).toEqual(
         theirs.kind === "markdown" ? theirs.text : theirs.markdown,
      );
      const slice = text.slice(cell.span.start, cell.span.end);
      if (theirs.kind !== "markdown") expect(slice).toContain(theirs.text);
      // The span may start earlier, over a comment block; it ends on the server's last line.
      expect(lineOf(text, cell.span.start)).toBeLessThanOrEqual(
         theirs.startLine,
      );
      expect(lineOf(text, cell.span.end - 1)).toBe(theirs.endLine);
   });
   return mine;
}

describe("readNotebookSource: the server reader is the oracle", () => {
   for (const file of fixtures.filter((f) => f !== REFUSED)) {
      it(file, async () => {
         await expectMatchesOracle(read(file));
      });
   }

   it("reads CRLF files with spans over the raw bytes", async () => {
      for (const file of fixtures.filter((f) => f !== REFUSED)) {
         const crlf = read(file).replace(/\r?\n/g, "\r\n");
         const mine = await expectMatchesOracle(crlf);
         for (const cell of mine.cells)
            expect(crlf.slice(cell.span.end - 2, cell.span.end)).toBe("\r\n");
      }
   });

   it("reads a file whose last cell has no trailing newline", async () => {
      for (const file of fixtures.filter((f) => f !== REFUSED)) {
         const text = read(file).replace(/\n+$/, "");
         const mine = await expectMatchesOracle(text);
         expect(mine.cells[mine.cells.length - 1].span.end).toBe(text.length);
      }
   });

   const LEGACY: [string, string][] = [
      ['##" line', '##" Legacy line prose.\n'],
      ['##|" block', '##|" Opener prose.\nBody prose.\n|##\n'],
      ["##(text) line", "##(text) Legacy text prose.\n"],
      ["##|(text) block", "##|(text) named\nBody prose.\n|##\n"],
   ];
   for (const [name, prose] of LEGACY) {
      it(`reads the legacy ${name} spelling as a markdown cell`, async () => {
         const text = `## artifact { kind=notebook }\nsource: a is duckdb.sql("select 1 as x")\n\n${prose}\nrun: a -> { select: x }\n`;
         const mine = await expectMatchesOracle(text);
         expect(mine.cells.map((c) => c.kind)).toEqual([
            "definition",
            "markdown",
            "query",
         ]);
      });
   }

   it("keeps a non-prose note in the header before the first cell, and in the gap after it", async () => {
      const text = [
         "## artifact { kind=notebook }",
         '## title="In the header"',
         "##(markdown) First cell.",
         '## title="A gap note"',
         "##(markdown) Second cell, not joined across the note.",
         "",
      ].join("\n");
      const mine = await expectMatchesOracle(text);
      expect(text.slice(mine.header.start, mine.header.end)).toBe(
         '## artifact { kind=notebook }\n## title="In the header"\n',
      );
      expect(mine.cells.map((c) => c.markdown)).toEqual([
         "First cell.",
         "Second cell, not joined across the note.",
      ]);
      expect(pieces(mine)[3]).toBe('## title="A gap note"\n');
   });

   it("carries a comment block directly above a cell, and leaves one after a blank line in the gap", async () => {
      const text = [
         "## artifact { kind=notebook }",
         'source: a is duckdb.sql("select 1 as x")',
         "",
         "// Stays in the gap.",
         "",
         "// Moves with the run.",
         "/* and so does",
         "   this. */",
         "# bar_chart",
         "run: a -> { select: x } // trailing",
         "",
      ].join("\n");
      const mine = await expectMatchesOracle(text);
      const run = mine.cells[1];
      expect(text.slice(run.span.start, run.span.end)).toBe(
         "// Moves with the run.\n/* and so does\n   this. */\n# bar_chart\nrun: a -> { select: x } // trailing\n",
      );
      expect(pieces(mine)[3]).toBe("\n// Stays in the gap.\n\n");
   });

   it("keeps a query cell's #|(markdown) block and its tags inside the cell", async () => {
      const text = read(
         path.join("notebooks-malloyyo", "notebooks", "attached_prose.malloy"),
      );
      const mine = await expectMatchesOracle(text);
      const run = mine.cells[mine.cells.length - 1];
      expect(text.slice(run.span.start, run.span.end)).toStartWith(
         "# bar_chart\n#|(markdown)\n",
      );
      expect(run.markdown).toBe("### Orders by region\nExcludes refunds.");
   });

   it("reads a block whose closer is followed only by whitespace", async () => {
      const text =
         '## artifact { kind=notebook }\nsource: a is duckdb.sql("select 1 as x")\n##|(markdown)\nBody.\n|##   \n#|(markdown)\nhi\n|# \t\nrun: a -> { select: x }\n';
      const mine = await expectMatchesOracle(text);
      expect(mine.cells.map((c) => c.markdown)).toEqual([
         undefined,
         "Body.",
         "hi",
      ]);
   });

   it("measures spans in UTF-16 units past astral characters", async () => {
      const text =
         '## artifact { kind=notebook }\n##(markdown) Rockets 🚀🚀 first.\n\nsource: a is duckdb.sql("select 1 as x")\n\nrun: a -> { select: x }\n';
      const mine = await expectMatchesOracle(text);
      expect(text.slice(mine.cells[2].span.start, mine.cells[2].span.end)).toBe(
         "run: a -> { select: x }\n",
      );
   });
});

describe("readNotebookSource: refusals", () => {
   it("refuses refused.malloy, as the server does", async () => {
      const text = read(REFUSED);
      const server = oracle(text);
      expect("parseError" in server || server.error !== undefined).toBe(true);
      const mine = await readNotebookSource(text);
      expect(notebookSourceRefused(mine) && mine.refused).toMatch(
         /^Line 7: .* is a statement the notebook reader does not recognize/,
      );
   });

   it("refuses a statement above the artifact tag, which the server serves as a header cell and lint errors", async () => {
      const text = read(
         path.join("notebooks-lint", "notebooks", "header_statement.malloy"),
      );
      const server = oracle(text) as NotebookReadResult;
      expect(server.error).toBeUndefined();
      expect(server.cells[0]).toMatchObject({
         kind: "definition",
         startLine: 1,
      });
      expect(
         lintNotebookText("notebooks/header_statement.malloy", text).some(
            (f) => f.severity === "error" && f.line === 1,
         ),
      ).toBe(true);
      const mine = await readNotebookSource(text);
      expect(mine).toMatchObject({ ok: false, line: 1 });
      expect(notebookSourceRefused(mine) && mine.refused).toMatch(
         /above the `## artifact` tag/,
      );
   });

   const REFUSE: [string, string, RegExp][] = [
      [
         "a syntax error",
         "## artifact { kind=notebook }\nrun: a -> {\n",
         /^Line \d+: Malloy could not parse this notebook .*Fix: correct the syntax/,
      ],
      [
         "no artifact note",
         '##(markdown) Prose.\nsource: a is duckdb.sql("select 1")\n',
         /no ## artifact note/,
      ],
      [
         "a # tag that annotates nothing",
         '## artifact { kind=notebook }\nsource: a is duckdb.sql("select 1")\n# bar_chart\n',
         /^Line 3: a # tag that annotates no statement/,
      ],
      [
         "two statements on one line",
         '## artifact { kind=notebook }\nsource: a is duckdb.sql("select 1 as x"); run: a -> { select: x }\n',
         /^Line 2: a line holding two statements.*Fix: put each statement and note on its own line\.$/,
      ],
      [
         "a block comment that straddles a cell's last line",
         '## artifact { kind=notebook }\nsource: a is duckdb.sql("select 1 as x") /* runs\non */\nrun: a -> { select: x }\n',
         /^Line 2: a comment that straddles a cell/,
      ],
      [
         "a lone carriage return",
         "## artifact { kind=notebook }\r##(markdown) Prose.\n",
         /^Line 1: a carriage return/,
      ],
      [
         "words after a floating block's closer",
         "## artifact { kind=notebook }\n##|(markdown)\nBody.\n|## more words\n",
         /^Line 4: the text after a block's closer \(`more words`\)/,
      ],
      [
         "a run: after a floating block's closer",
         '## artifact { kind=notebook }\nsource: a is duckdb.sql("select 1 as x")\n##|(markdown)\nBody.\n|## run: a -> { select: x }\n',
         /^Line 5: the text after a block's closer \(`run: a -> \{ select: x \}`\)/,
      ],
      [
         "words after an attached block's closer",
         '## artifact { kind=notebook }\nsource: a is duckdb.sql("select 1 as x")\n#|(markdown)\nhi\n|# extra\nrun: a -> { select: x }\n',
         /^Line 5: the text after a block's closer \(`extra`\)/,
      ],
      [
         "words after a header block's closer",
         '##|"\nDesc\n|## oops\n## artifact { kind=notebook }\n##(markdown) Prose.\n',
         /^Line 3: the text after a block's closer \(`oops`\)/,
      ],
   ];
   for (const [name, text, why] of REFUSE) {
      it(`refuses ${name}`, async () => {
         const result = await readNotebookSource(text);
         expect(notebookSourceRefused(result) && result.refused).toMatch(why);
      });
   }
});
