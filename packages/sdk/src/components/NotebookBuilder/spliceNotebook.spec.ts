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
   canMove,
   markdownProblem,
   notebookDocumentOf,
   spliceNotebookDocument,
   type NotebookDocument,
} from "./spliceNotebook";

const DEF = 'source: a is duckdb.sql("select 1 as x")';
const RUN = "run: a -> { select: x }";

async function sourceOf(text: string): Promise<NotebookSource> {
   const result = await readNotebookSource(text);
   if (notebookSourceRefused(result)) throw new Error(result.refused);
   return result.source;
}

async function docOf(text: string): Promise<NotebookDocument> {
   return notebookDocumentOf(await sourceOf(text));
}

async function written(
   text: string,
   change: (doc: NotebookDocument) => void,
): Promise<string> {
   const doc = await docOf(text);
   change(doc);
   const result = await spliceNotebookDocument(text, doc);
   if (spliceFailed(result)) throw new Error(result.reason);
   return result.source;
}

async function refused(
   text: string,
   change: (doc: NotebookDocument) => void,
): Promise<string> {
   const doc = await docOf(text);
   change(doc);
   const result = await spliceNotebookDocument(text, doc);
   if (!spliceFailed(result)) throw new Error("expected the writer to refuse");
   return result.reason;
}

const move = (doc: NotebookDocument, from: number, to: number) => {
   const [cell] = doc.cells.splice(from, 1);
   doc.cells.splice(to, 0, cell);
};

const markdownOf = async (text: string) =>
   (await sourceOf(text)).cells.map((c) => c.markdown);

describe("canMove", () => {
   const doc: NotebookDocument = {
      cells: [
         { id: "0", kind: "markdown", markdown: "Intro." },
         { id: "1", kind: "definition" },
         { id: "2", kind: "query" },
         { id: "3", kind: "markdown", markdown: "Between." },
         { id: "4", kind: "query" },
         { id: "5", kind: "definition" },
         { id: "6", kind: "query" },
      ],
   };

   it("moves markdown anywhere", () => {
      for (const to of [0, 1, 2, 3, 4, 5, 6]) {
         expect(canMove(doc, 0, to)).toBe(true);
         expect(canMove(doc, 3, to)).toBe(true);
      }
   });

   it("moves a query down freely, past definitions too", () => {
      expect(canMove(doc, 2, 6)).toBe(true);
      expect(canMove(doc, 4, 5)).toBe(true);
   });

   it("moves a query up past markdown and other queries, never past a definition", () => {
      expect(canMove(doc, 4, 2)).toBe(true);
      expect(canMove(doc, 4, 3)).toBe(true);
      expect(canMove(doc, 2, 1)).toBe(false);
      expect(canMove(doc, 2, 0)).toBe(false);
      expect(canMove(doc, 6, 5)).toBe(false);
      expect(canMove(doc, 6, 4)).toBe(false);
   });

   it("never moves a definition", () => {
      expect(canMove(doc, 1, 0)).toBe(false);
      expect(canMove(doc, 1, 2)).toBe(false);
      expect(canMove(doc, 5, 6)).toBe(false);
      expect(canMove(doc, 1, 1)).toBe(true);
   });

   it("lets a query moved down past a definition move back up to where it was read", () => {
      const moved = structuredClone(doc);
      move(moved, 2, 5);
      expect(moved.cells.map((c) => c.id)).toEqual([
         "0",
         "1",
         "3",
         "4",
         "5",
         "2",
         "6",
      ]);
      // Back up past definition "5", which it was read above.
      expect(canMove(moved, 5, 2)).toBe(true);
      expect(canMove(moved, 5, 1)).toBe(false);
      expect(canMove(moved, 5, 0)).toBe(false);
   });

   it("takes a query's opened count of definitions above it when one is given", () => {
      // Moved below definition "1" and saved: re-read, it now has one definition above it.
      const saved: NotebookDocument = {
         cells: [
            { id: "0", kind: "definition" },
            { id: "1", kind: "query" },
         ],
      };
      expect(canMove(saved, 1, 0)).toBe(false);
      expect(canMove(saved, 1, 0, new Map([["1", 0]]))).toBe(true);
      expect(canMove(saved, 1, 0, new Map([["1", 1]]))).toBe(false);
   });

   it("refuses an index outside the document", () => {
      expect(canMove(doc, -1, 0)).toBe(false);
      expect(canMove(doc, 0, 7)).toBe(false);
      expect(canMove(doc, 0.5, 1)).toBe(false);
   });
});

describe("spliceNotebookDocument", () => {
   it("returns identical text for an empty edit, CRLF and a missing final newline included", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Prose.\n\n${RUN}`;
      for (const variant of [text, text.replace(/\n/g, "\r\n")]) {
         const result = await spliceNotebookDocument(
            variant,
            await docOf(variant),
         );
         expect(result).toEqual({ ok: true, source: variant });
      }
   });

   it("keeps two single-line cells made adjacent by a move as two cells", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n##(markdown) First.\n# bar_chart\n${RUN}\n##(markdown) Second.\n`;
      const out = await written(text, (doc) => move(doc, 3, 2));
      expect(out).toContain("##(markdown) First.\n\n##(markdown) Second.\n");
      expect(await markdownOf(out)).toEqual([
         undefined,
         "First.",
         "Second.",
         undefined,
      ]);
   });

   it("keeps two line cells apart when a move hands them another slot's gap", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n${RUN}\n##(markdown) a\n\n##(markdown) b\n`;
      const viaQuery = await docOf(text);
      expect(canMove(viaQuery, 1, 3)).toBe(true);
      const viaProse = structuredClone(viaQuery);
      move(viaQuery, 1, 3);
      move(viaProse, 2, 1);
      move(viaProse, 3, 2);
      for (const doc of [viaQuery, viaProse]) {
         const result = await spliceNotebookDocument(text, doc);
         if (spliceFailed(result)) throw new Error(result.reason);
         expect(result.source).toContain("##(markdown) a\n\n##(markdown) b\n");
         expect(await markdownOf(result.source)).toEqual([
            undefined,
            "a",
            "b",
            undefined,
         ]);
      }
   });

   it("keeps two line cells apart across a gap note that holds no blank line", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n${RUN}\n##(markdown) a\n## title="t"\n##(markdown) b\n`;
      const out = await written(text, (doc) => move(doc, 1, 3));
      expect(await markdownOf(out)).toEqual([undefined, "a", "b", undefined]);
   });

   it("adds a last cell after a trailing gap comment without taking the comment into its span", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n${RUN}\n// trailing\n`;
      const out = await written(text, (doc) => {
         doc.cells.push({
            id: "new",
            kind: "markdown",
            markdown: "Added.",
            added: true,
         });
      });
      expect(out).toBe(
         `## artifact { kind=notebook }\n${DEF}\n\n${RUN}\n// trailing\n\n##(markdown) Added.\n`,
      );
      const added = (await sourceOf(out)).cells[2];
      expect(out.slice(added.span.start, added.span.end)).toBe(
         "##(markdown) Added.\n",
      );
   });

   for (const [shape, trailing] of [
      ["a line comment after a blank line", "\n// trailing\n"],
      ["a block comment after a blank line", "\n/* t\n  t */\n"],
   ])
      for (const crlf of [false, true]) {
         it(`adds a last cell after ${shape}${crlf ? " (CRLF)" : ""} without taking the comment into its span`, async () => {
            const lf = `## artifact { kind=notebook }\n${DEF}\n\n${RUN}\n${trailing}`;
            const text = crlf ? lf.replace(/\n/g, "\r\n") : lf;
            const out = await written(text, (doc) => {
               doc.cells.push({
                  id: "new",
                  kind: "markdown",
                  markdown: "Added.",
                  added: true,
               });
            });
            const nl = crlf ? "\r\n" : "\n";
            expect(out).toBe(`${text}${nl}##(markdown) Added.${nl}`);
            const back = await sourceOf(out);
            expect(back.cells.map((c) => c.markdown)).toEqual([
               undefined,
               undefined,
               "Added.",
            ]);
            const added = back.cells[2];
            expect(out.slice(added.span.start, added.span.end)).toBe(
               `##(markdown) Added.${nl}`,
            );
         });
      }

   it("uses the file's majority newline, LF on a tie", async () => {
      const base = `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Prose.\n\n${RUN}\n`;
      const edit = (doc: NotebookDocument) => {
         doc.cells[1].markdown = "Two\nlines.";
      };
      // Six LF lines, one of them CRLF: the edit writes LF.
      const mostlyLf = base.replace("\n", "\r\n");
      expect(await written(mostlyLf, edit)).toContain(
         "##|(markdown)\nTwo\nlines.\n|##\n",
      );
      const mostlyCrlf = base.replace(/\n/g, "\r\n").replace("\r\n", "\n");
      expect(await written(mostlyCrlf, edit)).toContain(
         "##|(markdown)\r\nTwo\r\nlines.\r\n|##\r\n",
      );
      const tie = "## artifact { kind=notebook }\r\n##(markdown) Prose.\n";
      expect(
         await written(tie, (doc) => {
            doc.cells[0].markdown = "Two\nlines.";
         }),
      ).toBe(
         "## artifact { kind=notebook }\r\n##|(markdown)\nTwo\nlines.\n|##\n",
      );
   });

   it("removes the comment block directly above a removed markdown cell with it", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n// About the prose.\n##(markdown) Goes.\n\n${RUN}\n`;
      const out = await written(text, (doc) => {
         doc.cells.splice(1, 1);
      });
      expect(out).toBe(`## artifact { kind=notebook }\n${DEF}\n\n${RUN}\n`);
   });

   it("moves a query with its comments, tags and #|(markdown) block, orphaning nothing", async () => {
      const cell = `// Why this run.\n/* More\n   why. */\n# bar_chart\n#|(markdown)\n### Heading\n|#\n# label="X"\n${RUN}\n`;
      const text = `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Before.\n\n${cell}\n##(markdown) After.\n`;
      const out = await written(text, (doc) => move(doc, 2, 1));
      expect(out).toBe(
         `## artifact { kind=notebook }\n${DEF}\n\n${cell}\n##(markdown) Before.\n\n##(markdown) After.\n`,
      );
      const back = await sourceOf(out);
      expect(back.cells[1].markdown).toBe("### Heading");
   });

   it("moves the last cell of a file with no final newline, and the file still ends without one", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Prose.\n\n${RUN}`;
      const out = await written(text, (doc) => move(doc, 2, 1));
      expect(out).toBe(
         `## artifact { kind=notebook }\n${DEF}\n\n${RUN}\n\n##(markdown) Prose.`,
      );
   });

   it("writes every newline of a CRLF file as CRLF", async () => {
      const text =
         `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Prose.\n\n${RUN}\n`.replace(
            /\n/g,
            "\r\n",
         );
      const out = await written(text, (doc) => {
         doc.cells[1].markdown = "Two\nlines.";
         doc.cells.push({
            id: "new",
            kind: "markdown",
            markdown: "Added.",
            added: true,
         });
      });
      expect(/(^|[^\r])\n/.test(out)).toBe(false);
      expect(out).toContain("##|(markdown)\r\nTwo\r\nlines.\r\n|##\r\n");
      expect(await markdownOf(out)).toEqual([
         undefined,
         "Two\nlines.",
         undefined,
         "Added.",
      ]);
   });

   it("writes one line as ##(markdown) and more as a column-0 block, keeping |# and an indented |##", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Prose.\n\n${RUN}\n`;
      const block = "### Heading\n|# is fine\n  |## indented is fine\n";
      const out = await written(text, (doc) => {
         doc.cells[1].markdown = block;
         doc.cells.splice(1, 0, {
            id: "new",
            kind: "markdown",
            markdown: "One line.",
            added: true,
         });
      });
      expect(out).toBe(
         `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) One line.\n\n##|(markdown)\n### Heading\n|# is fine\n  |## indented is fine\n\n|##\n\n${RUN}\n`,
      );
      expect(await markdownOf(out)).toEqual([
         undefined,
         "One line.",
         block,
         undefined,
      ]);
   });

   it("rewrites an edited legacy cell as (markdown) and leaves an unedited one as it was", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n##" Legacy one.\n\n${RUN}\n\n##(text) Legacy two.\n`;
      const out = await written(text, (doc) => {
         doc.cells[1].markdown = "Edited.";
      });
      expect(out).toBe(
         `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Edited.\n\n${RUN}\n\n##(text) Legacy two.\n`,
      );
      expect((await sourceOf(out)).cells.map((c) => c.kind)).toEqual([
         "definition",
         "markdown",
         "query",
         "markdown",
      ]);
   });

   it("keeps the comment lines above an edited cell", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n// About the prose.\n##|(markdown)\nOld.\n|##\n\n${RUN}\n`;
      const out = await written(text, (doc) => {
         doc.cells[1].markdown = "New.";
      });
      expect(out).toContain("// About the prose.\n##(markdown) New.\n");
   });

   it("keeps what sat between cells when one is removed", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Goes.\n## title="A gap note"\n\n// A gap comment.\n\n${RUN}\n`;
      const out = await written(text, (doc) => {
         doc.cells.splice(1, 1);
      });
      expect(out).toBe(
         `## artifact { kind=notebook }\n${DEF}\n## title="A gap note"\n\n// A gap comment.\n\n${RUN}\n`,
      );
   });

   it("adds a cell to a notebook with no cells yet", async () => {
      const out = await written("## artifact { kind=notebook }", (doc) => {
         doc.cells.push({
            id: "new",
            kind: "markdown",
            markdown: "First.",
            added: true,
         });
      });
      expect(out).toBe("## artifact { kind=notebook }\n\n##(markdown) First.");
   });
});

describe("spliceNotebookDocument: definitions above a query", () => {
   const QUERY = RUN.replace("a ->", "duckdb.sql('select 1 as x') ->");
   const SAVED = `## artifact { kind=notebook }\n${DEF}\n\n${QUERY}\n`;

   it("lets a query above a definition it was opened above, and no higher than that", async () => {
      const up = async (floor?: ReadonlyMap<string, number>) => {
         const doc = await docOf(SAVED);
         move(doc, 1, 0);
         return spliceNotebookDocument(SAVED, doc, floor);
      };
      expect(spliceFailed(await up())).toBe(true);
      expect(await up(new Map([["1", 0]]))).toEqual({
         ok: true,
         source: `## artifact { kind=notebook }\n${QUERY}\n\n${DEF}\n`,
      });
      expect(spliceFailed(await up(new Map([["1", 1]])))).toBe(true);
   });
});

describe("spliceNotebookDocument: refusals", () => {
   const TEXT = `## artifact { kind=notebook }\n${DEF}\n\n##(markdown) Prose.\n\n${RUN}\n\nsource: b is a\n\n${RUN}\n`;

   it("refuses a file the locator cannot open", async () => {
      const result = await spliceNotebookDocument(
         `##(markdown) Untagged.\n${DEF}\n`,
         { cells: [] },
      );
      expect(spliceFailed(result) && result.reason).toMatch(
         /cannot be edited here.*no ## artifact note/,
      );
   });

   it("refuses an empty markdown cell", async () => {
      expect(
         await refused(TEXT, (doc) => {
            doc.cells[1].markdown = "";
         }),
      ).toContain("remove the cell instead");
   });

   it("copies an untouched empty prose cell as it was while another cell changes", async () => {
      for (const empty of [
         "##(markdown)\n",
         "##|(markdown)\n|##\n",
         "##(markdown)   \n",
      ]) {
         const text = `## artifact { kind=notebook }\n${empty}\n##(markdown) Prose.\n\n${DEF}\n\n${RUN}\n`;
         expect((await markdownOf(text)).map((m) => m?.trim())).toEqual([
            "",
            "Prose.",
            undefined,
            undefined,
         ]);
         expect(
            await written(text, (doc) => {
               doc.cells[1].markdown = "Edited.";
            }),
         ).toBe(text.replace("Prose.", "Edited."));
         expect(await written(text, (doc) => move(doc, 0, 1))).toBe(
            `## artifact { kind=notebook }\n##(markdown) Prose.\n\n${empty}\n${DEF}\n\n${RUN}\n`,
         );
      }
   });

   it("refuses a whitespace-only markdown cell", async () => {
      expect(
         await refused(TEXT, (doc) => {
            doc.cells[1].markdown = "  \n\t\n";
         }),
      ).toContain("remove the cell instead");
   });

   it("refuses a line starting with |## at column 0", async () => {
      expect(
         await refused(TEXT, (doc) => {
            doc.cells[1].markdown = "Heading\n|## would close the block";
         }),
      ).toContain("starting with `|##`");
   });

   it("refuses a query moved above a definition", async () => {
      expect(await refused(TEXT, (doc) => move(doc, 4, 2))).toContain(
         "would sit above a definition",
      );
   });

   it("refuses a definition moved", async () => {
      expect(await refused(TEXT, (doc) => move(doc, 3, 0))).toContain(
         "A definition moved",
      );
   });

   it("refuses a definition removed", async () => {
      expect(
         await refused(TEXT, (doc) => {
            doc.cells.splice(3, 1);
         }),
      ).toContain("only markdown and query cells can be removed");
   });

   it("refuses an added definition", async () => {
      expect(
         await refused(TEXT, (doc) => {
            doc.cells.push({ id: "new", kind: "definition", added: true });
         }),
      ).toContain("only markdown and query cells can be added");
   });

   it("refuses a cell the file does not have, or one listed twice", async () => {
      expect(
         await refused(TEXT, (doc) => {
            doc.cells[1].id = "99";
         }),
      ).toContain("does not match any markdown cell");
      expect(
         await refused(TEXT, (doc) => {
            doc.cells.push({ ...doc.cells[1] });
         }),
      ).toContain('repeats the id "1"');
   });

   it("refuses to rewrite a cell whose prose shares a line with a comment", async () => {
      const text = `## artifact { kind=notebook }\n${DEF}\n\n/* note */ ##(markdown) Prose.\n\n${RUN}\n`;
      expect(
         await refused(text, (doc) => {
            doc.cells[1].markdown = "Edited.";
         }),
      ).toContain("shares a line with the prose");
   });
});

describe("markdownProblem", () => {
   it("names what the writer would refuse, and nothing else", () => {
      expect(markdownProblem("Fine.")).toBeUndefined();
      expect(markdownProblem("Indented\n  |## ok")).toBeUndefined();
      expect(markdownProblem("")).toContain("remove the cell instead");
      expect(markdownProblem("  \r\n ")).toContain("remove the cell instead");
      expect(markdownProblem("a\n|## b")).toContain("|##");
   });
});
