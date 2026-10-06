// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   isQueryTile,
   isTextTile,
   type DashboardDocument,
   type TextTile,
} from "./document";
import { readTileList } from "./malloyTree";
import { readDashboardDocument, readFailed } from "./readDocument";
import {
   markdownProblem,
   spliceDashboardDocument,
   spliceFailed,
   syntaxErrors,
} from "./spliceDocument";
import { openDocument, refused } from "./testing/fixtures";
import { whatMoved } from "./__test__/inventory";

const NOTEBOOK = `##! experimental.givens
##" A review.
## artifact { kind=notebook title="Review" tiles=[intro { kind=text }, "a_tiles -> cell_1", outro { kind=text colspan=6 break }] }
import { a } from "../m.malloy"

##|(markdown) intro
# Review

Welcome to **the review**.
|##

source: a_tiles is a extend {
  # label="By category"
  view: cell_1 is by_cat
}

##|(markdown) outro
Bye.
|##
`;

const DASHBOARD = `## artifact { title="D" tiles=["a_tiles -> x"] } dashboard { columns=12 }
import { a } from "../m.malloy"

source: a_tiles is a extend {
  # colspan=6
  view: x is vx
}
`;

const text = (document: DashboardDocument, name: string): TextTile => {
   const tile = document.tiles.find((t) => isTextTile(t) && t.name === name);
   if (!tile || !isTextTile(tile)) throw new Error(`no text tile ${name}`);
   return tile;
};

/** Whatever the writer produced must be a file Malloy parses and the reader reads back as asked. */
async function writes(
   source: string,
   edit: (document: DashboardDocument) => void,
   options?: {
      changeKind?: boolean;
      modelPath?: string;
      explicitKind?: boolean;
   },
): Promise<{ out: string; document: DashboardDocument }> {
   const next = structuredClone(await openDocument(source));
   edit(next);
   const result = await spliceDashboardDocument(source, next, options);
   if (spliceFailed(result)) throw new Error(result.reason);
   expect(await syntaxErrors(result.source)).toEqual([]);
   return { out: result.source, document: await openDocument(result.source) };
}

describe("readTileList", () => {
   it("splits quoted and object entries and keeps them as written", () => {
      const line = `## artifact { title="x" tiles=[a { kind=text colspan=6 }, "s -> v", b{kind=text}] }`;
      const list = readTileList(line);
      expect(list?.entries.map((e) => e.text)).toEqual([
         "a { kind=text colspan=6 }",
         '"s -> v"',
         "b{kind=text}",
      ]);
      expect(line.slice(list!.open, list!.close + 1)).toBe(
         '[a { kind=text colspan=6 }, "s -> v", b{kind=text}]',
      );
   });

   it("is not fooled by a title holding tiles=[ or ]", () => {
      const line = `## artifact { title="tiles=[\\"x\\"] ]" tiles=["a -> b"] }`;
      expect(readTileList(line)?.entries.map((e) => e.text)).toEqual([
         '"a -> b"',
      ]);
   });

   it("is undefined with no list or an unclosed one", () => {
      expect(readTileList(`## artifact { title="x" }`)).toBeUndefined();
      expect(readTileList(`## artifact { tiles=["a -> b" }`)).toBeUndefined();
   });

   it("reads an empty list as no entries", () => {
      expect(readTileList(`## artifact { tiles=[ ] }`)?.entries).toEqual([]);
   });
});

describe("readDashboardDocument: text tiles and kind", () => {
   it("reads a layout notebook: kind, text tiles in order, their blocks and list properties", async () => {
      const document = await openDocument(NOTEBOOK);
      expect(document.kind).toBe("notebook");
      expect(document.title).toBe("Review");
      expect(
         document.tiles.map((t) => `${t.kind ?? "query"}:${t.name}`),
      ).toEqual(["text:intro", "query:cell_1", "text:outro"]);
      expect(text(document, "intro")).toEqual({
         kind: "text",
         name: "intro",
         markdown: "# Review\n\nWelcome to **the review**.",
      });
      expect(text(document, "outro")).toEqual({
         kind: "text",
         name: "outro",
         markdown: "Bye.",
         colspan: 6,
         break: true,
      });
      const query = document.tiles.find(isQueryTile);
      expect(query?.label).toBe("By category");
      expect(query?.source).toBe("a_tiles");
   });

   it("leaves kind out for a dashboard", async () => {
      expect((await openDocument(DASHBOARD)).kind).toBeUndefined();
   });

   it("reads a listed text tile whose block is missing as empty prose", async () => {
      const source = NOTEBOOK.replace(/##\|\(markdown\) outro[\s\S]*$/, "");
      expect(text(await openDocument(source), "outro").markdown).toBe("");
   });

   it("reads a block's body through its first line when the opener carries text", async () => {
      const source = NOTEBOOK.replace(
         "##|(markdown) intro\n# Review",
         "##|(markdown) intro\n## Heading",
      );
      expect(text(await openDocument(source), "intro").markdown).toBe(
         "## Heading\n\nWelcome to **the review**.",
      );
   });

   it("reads an unnamed or other block as nobody's body", async () => {
      const source = NOTEBOOK.replace(
         "##|(markdown) outro",
         "##|(markdown) not a name",
      );
      expect(text(await openDocument(source), "outro").markdown).toBe("");
   });

   it("marks a notebook with no tiles list as the cell format", async () => {
      const result = await readDashboardDocument(
         `## artifact { kind=notebook title="Old" }\nimport { a } from "../m.malloy"\n\nrun: a -> v\n`,
      );
      if (!readFailed(result)) throw new Error("expected a refusal");
      expect(result.legacyNotebook).toBe(true);
   });

   it("still refuses a dashboard with no tiles list, without the legacy mark", async () => {
      const result = await readDashboardDocument(
         `## artifact { title="D" }\nimport { a } from "../m.malloy"\n`,
      );
      if (!readFailed(result)) throw new Error("expected a refusal");
      expect(result.legacyNotebook).toBeUndefined();
      expect(result.reason).toContain("tiles");
   });

   it("reads an empty tiles list on a notebook as a layout notebook with no tiles", async () => {
      const document = await openDocument(
         `## artifact { kind=notebook title="E" tiles=[] }\n`,
      );
      expect(document.tiles).toEqual([]);
      expect(document.kind).toBe("notebook");
   });

   it("refuses an entry that is neither quoted nor kind=text", async () => {
      const result = await readDashboardDocument(
         NOTEBOOK.replace(
            "outro { kind=text colspan=6 break }",
            "outro { kind=md }",
         ),
      );
      if (!readFailed(result)) throw new Error("expected a refusal");
      expect(result.reason).toContain("outro");
   });

   it("refuses a quoted entry that is not source -> view", async () => {
      const result = await readDashboardDocument(
         NOTEBOOK.replace('"a_tiles -> cell_1"', '"a_tiles"'),
      );
      if (!readFailed(result)) throw new Error("expected a refusal");
      expect(result.reason).toContain("source -> view");
   });

   it("refuses two text tiles of one name", async () => {
      const result = await readDashboardDocument(
         NOTEBOOK.replace("outro {", "intro {"),
      );
      if (!readFailed(result)) throw new Error("expected a refusal");
      expect(result.reason).toContain("intro");
   });
});

describe("spliceDashboardDocument: text tiles", () => {
   it("writes an unchanged notebook back byte for byte", async () => {
      const { out } = await writes(NOTEBOOK, () => {});
      expect(out).toBe(NOTEBOOK);
   });

   it("edits only the block when the prose changes", async () => {
      const { out, document } = await writes(NOTEBOOK, (d) => {
         text(d, "intro").markdown = "# New\n\n- one\n- two";
      });
      expect(out).toBe(
         NOTEBOOK.replace(
            "# Review\n\nWelcome to **the review**.",
            "# New\n\n- one\n- two",
         ),
      );
      expect(text(document, "intro").markdown).toBe("# New\n\n- one\n- two");
      const moved = await whatMoved(NOTEBOOK, out);
      expect(moved.declarations).toEqual([]);
      expect(moved.comments).toEqual([]);
   });

   it("round-trips prose with headings, trailing newlines and indented closers", async () => {
      for (const markdown of [
         "## Heading",
         "a\n\n\nb",
         "ends with a newline\n",
         "  |## indented, so not a closer",
         "# one\n## two\n### three",
         "",
      ]) {
         const { document } = await writes(NOTEBOOK, (d) => {
            text(d, "outro").markdown = markdown;
         });
         expect(text(document, "outro").markdown).toBe(markdown);
      }
   });

   it("adds a text tile: a list entry where it sits and a block at the end", async () => {
      const { out, document } = await writes(NOTEBOOK, (d) => {
         d.tiles.splice(1, 0, {
            kind: "text",
            name: "middle",
            markdown: "Between.",
            colspan: 6,
         });
      });
      expect(out).toContain(
         'tiles=[intro { kind=text }, middle { kind=text colspan=6 }, "a_tiles -> cell_1", outro { kind=text colspan=6 break }]',
      );
      expect(out.endsWith("##|(markdown) middle\nBetween.\n|##\n")).toBe(true);
      // Nothing above the appended block moved.
      expect(out.startsWith(NOTEBOOK.replace(/\n$/, ""))).toBe(false);
      expect(document.tiles.map((t) => t.name)).toEqual([
         "intro",
         "middle",
         "cell_1",
         "outro",
      ]);
      expect(text(document, "middle").markdown).toBe("Between.");
   });

   it("adds an empty text tile as an empty block", async () => {
      const { out, document } = await writes(NOTEBOOK, (d) => {
         d.tiles.push({ kind: "text", name: "blank", markdown: "" });
      });
      expect(out).toContain("##|(markdown) blank\n|##\n");
      expect(text(document, "blank").markdown).toBe("");
   });

   it("adds a text tile to a dashboard that has none, and to a file with no trailing newline", async () => {
      const bare = DASHBOARD.replace(/\n$/, "");
      const { out, document } = await writes(bare, (d) => {
         d.tiles.unshift({ kind: "text", name: "lead", markdown: "Lead." });
      });
      expect(out).toContain('tiles=[lead { kind=text }, "a_tiles -> x"]');
      expect(out).toContain(
         "\n\n##|(markdown) lead\nLead.\n|##\n\nsource: a_tiles is a extend",
      );
      expect(document.kind).toBeUndefined();
      expect(text(document, "lead").markdown).toBe("Lead.");
   });

   it("adds a query tile and a text tile in one edit", async () => {
      const { document } = await writes(DASHBOARD, (d) => {
         d.tiles.push({ kind: "text", name: "note", markdown: "A note." });
         d.tiles.push({
            name: "y",
            source: "a_tiles",
            declaration: { kind: "reference", from: "vy" },
         });
      });
      expect(document.tiles.map((t) => t.name)).toEqual(["x", "note", "y"]);
   });

   it("removes a text tile with its entry and its block, and nothing else", async () => {
      const { out } = await writes(NOTEBOOK, (d) => {
         d.tiles = d.tiles.filter((t) => t.name !== "outro");
      });
      expect(out).toBe(
         NOTEBOOK.replace(
            ", outro { kind=text colspan=6 break }]",
            "]",
         ).replace("\n##|(markdown) outro\nBye.\n|##\n", ""),
      );
   });

   it("removes the first block without leaving two blank lines", async () => {
      const { out } = await writes(NOTEBOOK, (d) => {
         d.tiles = d.tiles.filter((t) => t.name !== "intro");
      });
      expect(out).not.toContain("##|(markdown) intro");
      expect(out).not.toMatch(/\n\n\n/);
      expect(out).toContain('tiles=["a_tiles -> cell_1", outro');
   });

   it("removes a listed text tile whose block is already gone", async () => {
      const source = NOTEBOOK.replace(/\n##\|\(markdown\) outro[\s\S]*$/, "\n");
      const { out } = await writes(source, (d) => {
         d.tiles = d.tiles.filter((t) => t.name !== "outro");
      });
      expect(out).not.toContain("outro");
   });

   it("writes a block for a listed text tile that had none, when its prose is set", async () => {
      const source = NOTEBOOK.replace(/\n##\|\(markdown\) outro[\s\S]*$/, "\n");
      const { out, document } = await writes(source, (d) => {
         text(d, "outro").markdown = "Back.";
      });
      expect(out).toContain(
         "|##\n\n##|(markdown) outro\nBack.\n|##\n\nsource: a_tiles",
      );
      expect(text(document, "outro").markdown).toBe("Back.");
   });

   it("puts a new block after the last existing one, and at the end of a file with no block or extension", async () => {
      const afterLast = await writes(NOTEBOOK, (d) => {
         d.tiles.push({ kind: "text", name: "coda", markdown: "End." });
      });
      expect(afterLast.out).toContain(
         "Bye.\n|##\n\n##|(markdown) coda\nEnd.\n|##\n",
      );
      const bare = `## artifact { kind=notebook tiles=[] }\nimport { a } from "../m.malloy"\n`;
      const none = await writes(bare, (d) => {
         d.tiles.push({ kind: "text", name: "solo", markdown: "Only." });
      });
      expect(none.out.endsWith("\n\n##|(markdown) solo\nOnly.\n|##\n")).toBe(
         true,
      );
   });

   it("moves text tiles among the query tiles, keeping each entry as written", async () => {
      const { out, document } = await writes(NOTEBOOK, (d) => {
         d.tiles.reverse();
      });
      expect(out).toContain(
         'tiles=[outro { kind=text colspan=6 break }, "a_tiles -> cell_1", intro { kind=text }]',
      );
      expect(document.tiles.map((t) => t.name)).toEqual([
         "outro",
         "cell_1",
         "intro",
      ]);
      // A reorder moves no declaration or block.
      expect(out.slice(out.indexOf("\n", out.indexOf("## artifact")))).toBe(
         NOTEBOOK.slice(
            NOTEBOOK.indexOf("\n", NOTEBOOK.indexOf("## artifact")),
         ),
      );
   });

   it("changes a text tile's width and row break in its entry, keeping any other property", async () => {
      const source = NOTEBOOK.replace(
         "outro { kind=text colspan=6 break }",
         'outro { kind=text note="a  b break colspan=1" colspan=6 break }',
      );
      const { out, document } = await writes(source, (d) => {
         const outro = text(d, "outro");
         outro.colspan = 4;
         delete outro.break;
      });
      expect(out).toContain(
         'outro { kind=text note="a  b break colspan=1" colspan=4 }',
      );
      expect(text(document, "outro")).toMatchObject({ colspan: 4 });
      expect(text(document, "outro").break).toBeUndefined();
      // Clearing the width too.
      const cleared = await writes(source, (d) => {
         delete text(d, "outro").colspan;
      });
      expect(cleared.out).toContain(
         'outro { kind=text note="a  b break colspan=1" break }',
      );
   });

   it("sets a width on an entry that has none, without disturbing a quoted entry", async () => {
      const { out } = await writes(NOTEBOOK, (d) => {
         text(d, "intro").colspan = 12;
         text(d, "intro").break = true;
      });
      expect(out).toContain(
         'tiles=[intro { kind=text colspan=12 break }, "a_tiles -> cell_1"',
      );
   });

   it("combines a prose edit, a width edit and a settings edit", async () => {
      const { document } = await writes(NOTEBOOK, (d) => {
         d.title = "Renamed";
         d.description = "New description";
         text(d, "intro").markdown = "Changed.";
         text(d, "intro").colspan = 3;
      });
      expect(document.title).toBe("Renamed");
      expect(document.description).toBe("New description");
      expect(text(document, "intro")).toMatchObject({
         markdown: "Changed.",
         colspan: 3,
      });
   });

   it("leaves a block's neighbours alone when a query tile is removed beside it", async () => {
      const { out } = await writes(NOTEBOOK, (d) => {
         d.tiles = d.tiles.filter(isTextTile);
      });
      expect(out).toContain("##|(markdown) intro\n# Review");
      expect(out).toContain("##|(markdown) outro\nBye.\n|##\n");
      expect(out).not.toContain("view: cell_1");
   });

   describe("refusals", () => {
      const one = (edit: (d: DashboardDocument) => void) =>
         refused(NOTEBOOK, edit);

      it("refuses prose that would close its block", async () => {
         expect(
            await one((d) => {
               text(d, "intro").markdown = "a\n|## b";
            }),
         ).toContain("|##");
      });

      it("refuses a carriage return", async () => {
         expect(
            await one((d) => {
               text(d, "intro").markdown = "a\r\nb";
            }),
         ).toContain("carriage return");
      });

      it("refuses a name that is not a bare word", async () => {
         for (const name of ["two words", "1st", "a-b", ""]) {
            expect(
               await one((d) => {
                  d.tiles.push({ kind: "text", name, markdown: "x" });
               }),
            ).toContain("bare word");
         }
      });

      it("refuses a name another text tile or a block already has", async () => {
         expect(
            await one((d) => {
               d.tiles.push({ kind: "text", name: "intro", markdown: "x" });
            }),
         ).toContain("intro");
         const orphan = `${NOTEBOOK}\n##|(markdown) spare\nUnlisted.\n|##\n`;
         expect(
            await refused(orphan, (d) => {
               d.tiles.push({ kind: "text", name: "spare", markdown: "x" });
            }),
         ).toContain("already in this file");
      });

      it("refuses a width that is not a whole number of columns", async () => {
         for (const colspan of [0, -1, 1.5]) {
            expect(
               await one((d) => {
                  text(d, "intro").colspan = colspan;
               }),
            ).toContain("width");
         }
      });

      it("does not judge prose or names that did not change", async () => {
         const odd = NOTEBOOK.replace("Bye.", "  |## not a closer");
         await writes(odd, (d) => {
            d.title = "Still editable";
         });
      });

      it("refuses removing the last tile of a list", async () => {
         const reason = await refused(
            `## artifact { title="T" tiles=[only { kind=text }] }\n\n##|(markdown) only\nx\n|##\n`,
            (d) => {
               d.tiles = [];
            },
         );
         expect(reason).toContain("no tiles");
      });
   });

   it("exposes the same prose rule to an editor", () => {
      expect(markdownProblem("fine")).toBeUndefined();
      expect(markdownProblem("")).toBeUndefined();
      expect(markdownProblem("x\n|##")).toContain("|##");
      expect(markdownProblem("x\r")).toContain("carriage");
   });
});

describe("spliceDashboardDocument: text edits move nothing else", () => {
   const RICH = `// the page
##! experimental.givens
## artifact { kind=notebook title="Rich" tiles=[intro { kind=text }, "a_tiles -> cell_1", "a_tiles -> cell_2"] }
import { a } from "../m.malloy"

// about the intro
##|(markdown) intro
Hello.
|##

source: a_tiles is a extend {
  // why this leads
  # big_value
  # label="First" /* trailing */
  view: cell_1 is by_cat + { where: category ~ $CATEGORY } // after

  # bar_chart
  view: cell_2 is by_brand
}
// the end
`;

   const edits: Array<[string, (d: DashboardDocument) => void]> = [
      [
         "changing prose",
         (d) => {
            text(d, "intro").markdown = "Changed.\n\n- a";
         },
      ],
      [
         "adding a text tile",
         (d) => {
            d.tiles.splice(2, 0, { kind: "text", name: "mid", markdown: "M" });
         },
      ],
      [
         "resizing a text tile",
         (d) => {
            text(d, "intro").colspan = 8;
         },
      ],
      [
         "moving a text tile",
         (d) => {
            d.tiles.push(d.tiles.shift());
         },
      ],
   ];
   for (const [name, edit] of edits) {
      it(`${name} leaves every declaration, tag and comment where it was`, async () => {
         const { out } = await writes(RICH, edit);
         const moved = await whatMoved(RICH, out);
         expect(moved.declarations).toEqual([]);
         expect(moved.tags).toEqual([]);
         expect(moved.tagText).toEqual([]);
         expect(moved.comments).toEqual([]);
         expect(moved.attached).toEqual([]);
         // Prose is the one thing a text edit may add or drop.
         if (!/prose|adding/.test(name)) expect(moved.residue).toEqual([]);
      });
   }

   it("removing a text tile takes its block and leaves every comment and declaration", async () => {
      const { out } = await writes(RICH, (d) => {
         d.tiles.shift();
      });
      const moved = await whatMoved(RICH, out);
      expect(moved.declarations).toEqual([]);
      expect(moved.tags).toEqual([]);
      expect(moved.comments).toEqual([]);
      expect(out).toContain("// about the intro");
      expect(out).not.toContain("##|(markdown) intro");
   });
});

describe("spliceDashboardDocument: switching kind", () => {
   it("refuses a kind change unless it is asked for", async () => {
      const reason = await refused(DASHBOARD, (d) => {
         d.kind = "notebook";
      });
      expect(reason).toContain("kind");
   });

   it("turns a dashboard into a notebook in place: kind added, width cleared", async () => {
      const { out, document } = await writes(
         DASHBOARD,
         (d) => {
            d.kind = "notebook";
            delete d.columns;
         },
         { changeKind: true },
      );
      expect(out).toContain(
         '## artifact { title="D" tiles=["a_tiles -> x"] kind=notebook }',
      );
      expect(out).not.toContain("dashboard {");
      expect(document.kind).toBe("notebook");
      expect(document.columns).toBeUndefined();
      expect(out.slice(out.indexOf("\n"))).toBe(
         DASHBOARD.slice(DASHBOARD.indexOf("\n")),
      );
   });

   it("turns a notebook back into a dashboard: kind removed, width written", async () => {
      const { out, document } = await writes(
         NOTEBOOK,
         (d) => {
            d.kind = "dashboard";
            d.columns = 12;
         },
         { changeKind: true },
      );
      expect(out).not.toContain("kind=notebook");
      expect(out).toContain("kind=text");
      expect(out).toContain("dashboard { columns=12 }");
      expect(document.kind).toBeUndefined();
      expect(document.columns).toBe(12);
   });

   it("tags a dashboard kind=dashboard under notebooks/, where an untagged file reads as a notebook, and drops the tag again for a notebook", async () => {
      const flip = (source: string, kind: "dashboard" | "notebook") =>
         writes(
            source,
            (d) => {
               d.kind = kind;
               if (kind === "notebook") delete d.columns;
            },
            { changeKind: true, modelPath: "notebooks/n.malloy" },
         );
      const asDashboard = await flip(NOTEBOOK, "dashboard");
      expect(asDashboard.out).toContain("kind=dashboard");
      expect(asDashboard.out).not.toContain("kind=notebook");
      expect(asDashboard.document.kind).toBeUndefined();
      const back = await flip(asDashboard.out, "notebook");
      expect(back.out).toContain("kind=notebook");
      expect(back.out).not.toContain("kind=dashboard");
      expect(back.document.kind).toBe("notebook");
   });

   it("tags a dashboard kind=dashboard when the document has no path, as in text-source mode", async () => {
      const { out } = await writes(
         NOTEBOOK,
         (d) => {
            d.kind = "dashboard";
         },
         { changeKind: true },
      );
      expect(out).toContain("kind=dashboard");
   });

   it("writes the kind both ways in text mode, even under dashboards/, so the compile and the saved file agree", async () => {
      const options = {
         changeKind: true,
         modelPath: "dashboards/n.malloy",
         explicitKind: true,
      };
      const asDashboard = await writes(
         NOTEBOOK,
         (d) => {
            d.kind = "dashboard";
         },
         options,
      );
      expect(asDashboard.out).toContain("kind=dashboard");
      expect(asDashboard.out).not.toContain("kind=notebook");
      const back = await writes(
         asDashboard.out,
         (d) => {
            d.kind = "notebook";
            delete d.columns;
         },
         options,
      );
      expect(back.out).toContain("kind=notebook");
      expect(back.out).not.toContain("kind=dashboard");
   });

   it("leaves a dashboard under dashboards/ untagged", async () => {
      const { out } = await writes(
         NOTEBOOK,
         (d) => {
            d.kind = "dashboard";
         },
         { changeKind: true, modelPath: "dashboards/n.malloy" },
      );
      expect(out).not.toContain("kind=dashboard");
      expect(out).not.toContain("kind=notebook");
   });

   it("does not take a text tile's kind=text for the document's", async () => {
      const source = `## artifact { tiles=[a { kind=text }] title="T" }\n\n##|(markdown) a\nx\n|##\n`;
      const { out, document } = await writes(
         source,
         (d) => {
            d.kind = "notebook";
         },
         { changeKind: true },
      );
      expect(out).toContain("a { kind=text }");
      expect(out).toContain("kind=notebook");
      expect(document.kind).toBe("notebook");
      const back = await writes(
         out,
         (d) => {
            d.kind = "dashboard";
         },
         { changeKind: true },
      );
      expect(back.out).toContain("a { kind=text }");
      expect(back.out).not.toContain("kind=notebook");
   });

   it("refuses to switch to a notebook with the width still set, and never writes a notebook's width", async () => {
      const result = await spliceDashboardDocument(
         DASHBOARD,
         { ...(await openDocument(DASHBOARD)), kind: "notebook" },
         { changeKind: true },
      );
      if (!spliceFailed(result)) throw new Error("expected a refusal");
      expect(result.reason).toContain("one column");
      const reason = await refused(NOTEBOOK, (d) => {
         d.columns = 6;
      });
      expect(reason).toContain("one column");
   });

   it("keeps a stray width on a notebook as the file has it", async () => {
      const stray = NOTEBOOK.replace(
         "break }] }",
         "break }] } dashboard { columns=12 }",
      );
      const document = await openDocument(stray);
      expect(document.columns).toBe(12);
      const { out } = await writes(stray, (d) => {
         text(d, "intro").markdown = "Edited.";
      });
      expect(out).toContain("dashboard { columns=12 }");
   });
});
