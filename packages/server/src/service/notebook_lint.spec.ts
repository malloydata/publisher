// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import {
   buildDashboardManifest,
   lintDashboard,
   type DashboardModelFacts,
} from "./dashboard";
import { translateToParse } from "./notebook";
import { lintNotebookText, notebookLintProblems } from "./notebook_lint";

const FIXTURES = path.resolve(__dirname, "../../tests/fixtures");

const HEADER = "## artifact { kind=notebook }\n";
const SOURCE = 'source: a is duckdb.sql("select 1 as x")\n';
const RUN = "run: a -> { select: x }\n";

/** Findings that must never block /compile: a flip to error would stop a file that compiles. */
const WARN_ONLY = new Set([
   "notebook-markdown-block-named",
   "notebook-markdown-block-unnamed",
   "notebook-markdown-block-unreferenced",
   "notebook-markdown-nested",
]);

const lint = (text: string, modelPath = "notebooks/n.malloy") =>
   lintNotebookText(modelPath, text).map(
      ({ line, code, message, severity }) => {
         if (WARN_ONLY.has(code)) expect(severity).toBe("warn");
         return { line, code, message };
      },
   );

describe("notebook lint", () => {
   it.each([
      ["##| markdown", "##| markdown"],
      ["##|markdown", "##|markdown"],
      ["##| Markdown", "##| Markdown"],
   ])("suggests the (markdown) opener for the slip %s", (opener, shown) => {
      expect(lint(`${HEADER}${opener}\nhi\n|##\n`)).toEqual([
         {
            line: 2,
            code: "notebook-markdown-opener",
            message: `Line 2: \`${shown}\` opens a block that is not on the \`(markdown)\` route, so its body is not a markdown cell. Did you mean \`##|(markdown)\`?`,
         },
      ]);
   });

   it("does not tell a (markdown) opener to use the old prose spelling", () => {
      expect(lint(`${HEADER}##|(markdown)\nhi\n|##\n`)).toEqual([]);
      expect(lint(`${HEADER}##(markdown) hi\n`)).toEqual([]);
   });

   it("suggests the line form for the slip ##markdown", () => {
      expect(lint(`${HEADER}##markdown hi\n${SOURCE}`)).toEqual([
         {
            line: 2,
            code: "notebook-markdown-opener",
            message:
               "Line 2: `##markdown hi` is not on the `(markdown)` route, so it is not shown as prose. Did you mean `##(markdown)`?",
         },
      ]);
   });

   it.each(["#|markdown", "#| markdown"])(
      "suggests the attached form for the slip %s",
      (opener) => {
         expect(lint(`${HEADER}${SOURCE}${opener}\nhi\n|#\n${RUN}`)).toEqual([
            {
               line: 3,
               code: "notebook-markdown-opener",
               message: `Line 3: \`${opener}\` opens a block that is not on the \`(markdown)\` route, so its body is not the statement's prose. Did you mean \`#|(markdown)\`?`,
            },
         ]);
      },
   );

   it("says text tile, not markdown cell, for the slip on a dashboard", () => {
      expect(
         lint(
            `## artifact { tiles=[a] }\n##| markdown\nhi\n|##\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([
         {
            line: 2,
            code: "notebook-markdown-opener",
            message:
               "Line 2: `##| markdown` opens a block that is not on the `(markdown)` route, so its body is not a text tile. Fix: write `##|(markdown) name` and list `name { kind=text }` in `tiles`.",
         },
      ]);
   });

   it.each(["##|", "##|(filters)", "##| filters"])(
      "leaves the %s block, a tag or route block, alone",
      (opener) => {
         expect(lint(`${HEADER}${opener}\nsome=tag\n|##\n`)).toEqual([]);
      },
   );

   it("errors on text after the opener of more than one bare word, and says to move it into the body", () => {
      expect(lint(`${HEADER}##|(markdown) two words\nhi\n|##\n`)).toEqual([
         {
            line: 2,
            code: "notebook-markdown-opener-text",
            message:
               "Line 2: `two words` follows `##|(markdown)` on its opener line, where only one bare word may go (a name), so the block would show it as its first line. Fix: move it into the body, on the line below the opener.",
         },
      ]);
      expect(
         lintNotebookText(
            "notebooks/n.malloy",
            `${HEADER}##|(markdown) two words\nhi\n|##\n`,
         ).map((f) => f.severity),
      ).toEqual(["error"]);
   });

   it("warns that a name on a notebook's (markdown) block is not shown", () => {
      expect(lint(`${HEADER}##|(markdown) intro\nhi\n|##\n`)).toEqual([
         {
            line: 2,
            code: "notebook-markdown-block-named",
            message:
               "Line 2: the name `intro` on this `(markdown)` block means nothing in a notebook, which shows every block as a cell, and it is not shown. Fix: remove the name.",
         },
      ]);
   });

   it("errors on a (markdown) annotation above the artifact tag, block or line", () => {
      const found = lintNotebookText(
         "notebooks/n.malloy",
         `##(markdown) up here\n${HEADER}`,
      );
      expect(found.map((f) => [f.code, f.severity, f.line])).toEqual([
         ["notebook-markdown-above-artifact", "error", 1],
      ]);
      expect(
         lint(`##|(markdown)\nup here\n|##\n${HEADER}`).map((f) => f.code),
      ).toEqual(["notebook-markdown-above-artifact"]);
   });

   it("errors on (markdown) attached to a statement above the artifact tag", () => {
      const found = lintNotebookText(
         "notebooks/n.malloy",
         `#(markdown) up here\n${SOURCE}${HEADER}`,
      );
      expect(found.map((f) => [f.code, f.severity, f.line])).toEqual([
         ["notebook-markdown-above-artifact", "error", 1],
         ["notebook-statement-above-artifact", "error", 2],
      ]);
   });

   describe("the earlier prose spellings after the tag", () => {
      const DASH_INTRO = "## artifact { tiles=[intro { kind=text }] }\n";

      it.each([
         ['a ##" line', `${HEADER}##" a cell\n${SOURCE}`],
         ['a bare ##|" block', `${HEADER}##|"\nhi\n|##\n${SOURCE}`],
         [
            'a ##|" block with text on its opener',
            `${HEADER}##|" Revenue grew fast\nhi\n|##\n${SOURCE}`,
         ],
         [
            'a ##|" block with a single word on its opener',
            `${HEADER}##|" Summary\nhi\n|##\n${SOURCE}`,
         ],
         ["a ##(text) line", `${HEADER}##(text) a line\n${SOURCE}`],
         ["a bare ##|(text) block", `${HEADER}##|(text)\nhi\n|##\n${SOURCE}`],
      ])("reads %s in a notebook as a cell, with no finding", (_name, text) => {
         expect(lint(text)).toEqual([]);
      });

      it("reads a (text) tile block in a dashboard as a tile, with no finding", () => {
         expect(
            lint(
               `${DASH_INTRO}##|(text) intro\nhi\n|##\n${SOURCE}`,
               "dashboards/d.malloy",
            ),
         ).toEqual([]);
      });

      it("still flags a (text) tile block no tiles entry names, and a (text) line in a dashboard", () => {
         expect(
            lintNotebookText(
               "dashboards/d.malloy",
               `## artifact { tiles=["a -> v"] }\n${SOURCE}##|(text) intro\nhi\n|##\n`,
            ).map((f) => f.code),
         ).toEqual(["notebook-markdown-block-unreferenced"]);
         expect(
            lintNotebookText(
               "dashboards/d.malloy",
               `## artifact { tiles=["a -> v"] }\n##(text) a line\n${SOURCE}`,
            ).map((f) => f.code),
         ).toEqual(["notebook-markdown-block-unnamed"]);
      });

      it("applies the block rules to a (text) block as it does to a (markdown) one", () => {
         expect(
            lintNotebookText(
               "notebooks/n.malloy",
               `${HEADER}##|(text) intro\nhi\n`,
            ).map((f) => f.code),
         ).toEqual([
            "notebook-markdown-block-named",
            "notebook-unterminated-block",
         ]);
      });

      it("moves a (text) note above the tag as it does a (markdown) one", () => {
         expect(
            lintNotebookText(
               "notebooks/n.malloy",
               `##(text) a line\n${HEADER}${SOURCE}`,
            ).map((f) => [f.code, f.severity]),
         ).toEqual([["notebook-markdown-above-artifact", "error"]]);
      });
   });

   it.each([
      ['##|"intro', '##|"intro'],
      ["##|(markdown)intro", "##|(markdown)intro"],
   ])("says how to space %s in a notebook", (opener, shown) => {
      expect(lint(`${HEADER}${opener}\nhi\n|##\n`)).toEqual([
         {
            line: 2,
            code: "notebook-block-opener-spacing",
            message: `Line 2: \`${shown}\` has no space after the route, so Malloy drops the note. Fix: write \`##|(markdown)\` and put \`intro\` on the line below it.`,
         },
      ]);
   });

   it('says to keep the description route for a ##|"word block above the tag', () => {
      expect(lint(`##|"intro\nhi\n|##\n${HEADER}`)[0].message).toContain(
         'Fix: write `##|"` and put `intro` on the line below it.',
      );
   });

   it("says to put a space after the route only for a dashboard tile's bare-word name", () => {
      const dash = (opener: string) =>
         lint(
            `## artifact { tiles=[intro { kind=text }] }\n${opener}\nhi\n|##\n${SOURCE}`,
            "dashboards/d.malloy",
         )[0].message;
      expect(dash("##|(markdown)intro")).toContain(
         "Fix: put a space after the route, as in `##|(markdown) intro`, and list `intro { kind=text }` in `tiles`.",
      );
      expect(dash("##|(markdown)two words")).toContain(
         "Fix: write `##|(markdown) name`, put `two words` on the line below it, and list `name { kind=text }` in `tiles`.",
      );
   });

   it("says to move text glued to an attached #| opener onto the next line", () => {
      const found = lint(`${HEADER}#|(markdown)Summary\nhi\n|#\n${RUN}`);
      expect(found).toEqual([
         {
            line: 2,
            code: "notebook-block-opener-spacing",
            message:
               "Line 2: `#|(markdown)Summary` has no space after the route, so Malloy drops the note. Fix: write `#|(markdown)` and put `Summary` on the line below it.",
         },
      ]);
   });

   it("leaves a named (markdown) block alone in a dashboard that lists it as a tile", () => {
      expect(
         lint(
            `## artifact { tiles=[intro { kind=text }] }\n##|(markdown) intro\nhi\n|##\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([]);
   });

   it("warns about a (markdown) block no tiles entry names", () => {
      expect(
         lint(
            `## artifact { tiles=["a -> v", other { kind=text }] }\n${SOURCE}##|(markdown) intro\nhi\n|##\n`,
            "dashboards/d.malloy",
         ),
      ).toEqual([
         {
            line: 3,
            code: "notebook-markdown-block-unreferenced",
            message:
               "Line 3: the `(markdown)` block `intro` is not named by any entry in `tiles=[…]`, so it is not shown on the dashboard (text tiles do not render yet). Fix: delete the block.",
         },
      ]);
   });

   it("warns about an unnamed floating (markdown) block in a dashboard", () => {
      expect(
         lint(
            `## artifact { tiles=["a -> v"] }\n${SOURCE}##|(markdown)\nhi\n|##\n`,
            "dashboards/d.malloy",
         ),
      ).toEqual([
         {
            line: 3,
            code: "notebook-markdown-block-unnamed",
            message:
               "Line 3: an unnamed `(markdown)` block is not shown on a dashboard, whose text tiles are named blocks listed in `tiles=[…]` (text tiles do not render yet). Fix: write `##|(markdown) name` and list `name { kind=text }` in `tiles`, or delete the block.",
         },
      ]);
   });

   it("warns about a floating ##(markdown) line in a dashboard, which shows none", () => {
      expect(
         lint(
            `## artifact { tiles=["a -> v"] }\n##(markdown) hi\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([
         {
            line: 2,
            code: "notebook-markdown-block-unnamed",
            message:
               "Line 2: `##(markdown) hi` is a floating `(markdown)` line, which a dashboard does not show, since its text tiles are named blocks listed in `tiles=[…]` (text tiles do not render yet). Fix: write `##|(markdown) name` and list `name { kind=text }` in `tiles`, or delete the line.",
         },
      ]);
   });

   it("says a dashboard's tile name must be a bare word, and offers one", () => {
      const dash = (opener: string) =>
         lintNotebookText(
            "dashboards/d.malloy",
            `## artifact { tiles=["a -> v"] }\n${SOURCE}${opener}\nhi\n|##\n`,
         );
      expect(dash("##|(markdown) my-intro")[0]).toMatchObject({
         code: "notebook-markdown-opener-text",
         severity: "error",
         message:
            "Line 3: `my-intro` is not a valid name for a `(markdown)` tile, which takes one bare word of letters, digits and underscores that does not start with a digit. Fix: write `##|(markdown) my_intro` and list `my_intro { kind=text }` in `tiles`.",
      });
      expect(dash("##|(markdown) 2024")[0].message).toContain(
         "Fix: write `##|(markdown) _2024` and list",
      );
      expect(dash("##|(markdown) two words")[0].message).toContain(
         "Fix: write `##|(markdown) name`, put `two words` on the line below it",
      );
   });

   it("reads #(markdown) above a run: or a source: as clean, and errors above an import, an export or nothing", () => {
      expect(lint(`${HEADER}#(markdown) about it\n${SOURCE}`)).toEqual([]);
      expect(
         lint(`${HEADER}${SOURCE}# bar_chart\n#|(markdown)\nabout\n|#\n${RUN}`),
      ).toEqual([]);
      const nowhere = (statement: string) =>
         lintNotebookText(
            "notebooks/n.malloy",
            `${HEADER}#(markdown) about it\n${statement}`,
         ).map((f) => [f.line, f.code, f.severity]);
      expect(nowhere('import "x.malloy"\n')).toEqual([
         [2, "notebook-markdown-attached-nowhere", "error"],
      ]);
      expect(nowhere("export { a }\n")).toEqual([
         [2, "notebook-markdown-attached-nowhere", "error"],
      ]);
      expect(nowhere("")).toEqual([
         [2, "notebook-markdown-attached-nowhere", "error"],
      ]);
   });

   it("says what a dangling #(markdown) needs, with the compile error beside it", () => {
      expect(lint(`${HEADER}${SOURCE}#(markdown) trailing\n`)).toEqual([
         {
            line: 3,
            code: "notebook-markdown-attached-nowhere",
            message:
               "Line 3: `#(markdown) trailing` annotates no statement, since what follows it (the end of the file, an import or export, or a `##` model-level note) takes no annotation. Fix: write it as a floating `##(markdown)` line for prose that stands on its own, or move it directly above the statement it describes.",
         },
      ]);
   });

   it("checks an attached #|(markdown) block's closer and body like a floating one", () => {
      expect(
         lint(`${HEADER}${SOURCE}#|(markdown)\nhi\n${RUN}`).filter(
            (f) => f.code === "notebook-unterminated-block",
         ),
      ).toEqual([
         {
            line: 3,
            code: "notebook-unterminated-block",
            message:
               "Line 3: this block is never closed, so it runs to the end of the file and everything after the opener is prose, including the run: on line 5, which is prose here and never runs. Fix: add a `|#` line where the prose ends.",
         },
      ]);
      expect(
         lint(`${HEADER}${SOURCE}#|(markdown)\nhi\n|# extra\n${RUN}`).map(
            (f) => [f.code, f.message],
         ),
      ).toEqual([
         [
            "notebook-text-after-closer",
            "Line 5: the text after the closing `|#` (`extra`) is dropped, not shown. Fix: put it inside the block.",
         ],
      ]);
      expect(
         lint(
            `${HEADER}${SOURCE}#|(markdown)\nhi\n#|(markdown)\nsecond\n|#\n${RUN}`,
         ).map((f) => f.code),
      ).toEqual(["notebook-block-swallows-run"]);
   });

   it("warns when a dashboard's description sits only below its artifact tag", () => {
      expect(
         lint(
            `## artifact { tiles=["a -> v"] }\n##" Legacy\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([
         {
            line: 2,
            code: "notebook-description-below-artifact",
            message:
               "Line 2: this `\"` note below `## artifact` is the dashboard's description only because nothing sits above the tag. Fix: move it above `## artifact`.",
         },
      ]);
   });

   it('does not take an empty ##|" block above the tag for a description', () => {
      expect(
         lint(
            `##|"\n|##\n## artifact { tiles=["a -> v"] }\n##" Legacy\n${SOURCE}`,
            "dashboards/d.malloy",
         ).map((f) => f.code),
      ).toEqual(["notebook-description-below-artifact"]);
   });

   it("is quiet about a description above the tag, even with a note below it", () => {
      expect(
         lint(
            `##" Above\n## artifact { tiles=["a -> v"] }\n##" Below\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([]);
   });

   it("does not flag prose below the tag of a notebook, where it is a cell", () => {
      expect(lint(`${HEADER}##(markdown) a cell\n${SOURCE}`)).toEqual([]);
   });

   it("reads a text tile entry with kind=query, and a dashboard with kind=dashboard, as clean", () => {
      expect(
         lint(
            `## artifact { kind=dashboard tiles=[q { kind=query }] }\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([]);
   });

   it("points a ## heading line at the prose notation", () => {
      expect(
         lint(
            `${HEADER}## How to read this page\n${SOURCE}`,
            "notebooks/n.malloy",
         ),
      ).toEqual([
         {
            line: 2,
            code: "notebook-heading-line",
            message:
               "Line 2: `## How to read this page` is read as model tags, not shown as prose. Did you mean `##(markdown)`?",
         },
      ]);
      expect(
         lint(
            `## artifact { tiles=[a] }\n## A heading\n${SOURCE}`,
            "dashboards/d.malloy",
         ).map((f) => f.code),
      ).toEqual(["notebook-heading-line"]);
   });

   it.each([
      '## title="A non-prose note"',
      "## autorun=false",
      "## artifact { kind=notebook }",
      '##(filters) ["a"]',
      "## experimental",
   ])("leaves the tag line %s alone", (line) => {
      expect(lint(`${HEADER}${line}\n${SOURCE}`)).toEqual([]);
   });

   it("errors when the artifact tag does not parse, naming the tag and the parser's message", () => {
      const found = lintNotebookText(
         "notebooks/n.malloy",
         `## artifact { kind: text }\n${SOURCE}`,
      );
      expect(found).toHaveLength(1);
      expect(found[0]).toMatchObject({
         line: 1,
         code: "notebook-artifact-unparsed",
         severity: "error",
      });
      expect(found[0].message).toStartWith(
         "Line 1: the `## artifact` tag does not parse (",
      );
      expect(found[0].message).toContain("Fix: write");
   });

   it("adds the experimental line a given needs", () => {
      expect(lint(`${HEADER}given: G :: string is 'a'\n`)).toEqual([
         {
            line: 2,
            code: "notebook-givens-not-enabled",
            message:
               "Line 2: `given:` needs the givens experiment, which this file does not enable, so it does not compile. Fix: add the line `##! experimental.givens` at the top of the file.",
         },
      ]);
   });

   it.each([
      "##! experimental.givens\n",
      "##! experimental { givens compilerTestExperimentParse }\n",
   ])("accepts a given when the file enables it as %j", (flag) => {
      expect(lint(`${flag}${HEADER}given: G :: string is 'a'\n`)).toEqual([]);
   });

   it("names the line of a |## that closes a block early", () => {
      expect(
         lint(`${HEADER}##|(markdown)\nhi\n|##\nmore prose here\n\n${SOURCE}`),
      ).toEqual([
         {
            line: 4,
            code: "notebook-block-closed-early",
            message:
               "Line 4: this `|##` closes the block, so the text after it on line 5 is not prose and does not compile. Fix: a body line cannot start with `|##`, so reword it if the block should go on, or delete the stray text if the block is over.",
         },
      ]);
   });

   it("names the line of text after a closer", () => {
      expect(
         lint(`${HEADER}##|(markdown)\nhi\n|## extra text\n\n${SOURCE}`),
      ).toEqual([
         {
            line: 4,
            code: "notebook-text-after-closer",
            message:
               "Line 4: the text after the closing `|##` (`extra text`) is dropped, not shown. Fix: put it on its own `##(markdown)` line, or inside the block.",
         },
      ]);
   });

   it("flags an unterminated block at its opener and says it runs to the end", () => {
      expect(lint(`${HEADER}##|(markdown)\nhi\n${SOURCE}${RUN}`)).toEqual([
         {
            line: 2,
            code: "notebook-unterminated-block",
            message:
               "Line 2: this block is never closed, so it runs to the end of the file and everything after the opener is prose, including the run: on line 5, which is prose here and never runs. Fix: add a `|##` line where the prose ends.",
         },
      ]);
   });

   it("says where a block that swallows another opener actually ends", () => {
      expect(
         lint(`${HEADER}##|(markdown)\nhi\n##|(markdown)\nsecond\n|##\n`),
      ).toEqual([
         {
            line: 2,
            code: "notebook-block-swallows-run",
            message:
               "Line 2: this block runs to the `|##` on line 6, and line 4 inside it opens another block, so a `|##` was probably missed before it. Fix: add `|##` before line 4.",
         },
      ]);
   });

   it("leaves a fenced example or an indented run: inside prose alone", () => {
      const body =
         "Try it:\n```malloy\nrun: a -> { select: x }\n```\n  run: a -> { select: x }\n";
      expect(lint(`${HEADER}##|(markdown)\n${body}|##\n`)).toEqual([]);
   });

   it("leaves an indented block example inside prose alone", () => {
      const body = "See:\n    ##|(markdown)\n    example\n    |##\nmore\n";
      expect(lint(`${HEADER}##|(markdown)\n${body}|##\n`)).toEqual([]);
   });

   it("says render tags sit directly above the run they annotate", () => {
      expect(
         lint(`${HEADER}${SOURCE}# bar_chart\n##(markdown) a note\n${RUN}`),
      ).toEqual([
         {
            line: 3,
            code: "notebook-orphaned-tag",
            message:
               'Line 3: this # tag is followed by a note, not by a run:, so it annotates nothing. Render tags sit directly above their run:. Fix: move the tag, and any #" caption, directly above the run: on line 5.',
         },
      ]);
   });

   it("names the end of the file for a tag with nothing after it", () => {
      expect(lint(`${HEADER}${SOURCE}# bar_chart\n`)).toEqual([
         {
            line: 3,
            code: "notebook-orphaned-tag",
            message:
               'Line 3: this # tag is followed by the end of the file, not by a run:, so it annotates nothing. Render tags sit directly above their run:. Fix: move the tag, and any #" caption, directly above its run:.',
         },
      ]);
   });

   it("names the token that follows a tag, not the end of the file, when text follows it", () => {
      const found = lint(
         `${HEADER}##|(markdown)\nhi\n|##\n# Heading\nmore words\n`,
      );
      const orphan = found.find((f) => f.code === "notebook-orphaned-tag");
      expect(orphan?.message).toContain("is followed by `more`");
   });

   it("asks a notebook with no kind to add kind=notebook", () => {
      expect(lint(`## artifact { title="x" }\n${SOURCE}`)).toEqual([
         {
            line: 1,
            code: "notebook-kind-missing",
            message:
               "Line 1: this notebook's artifact tag has no `kind`. Fix: write `## artifact { kind=notebook }`.",
         },
      ]);
   });

   it("flags an unknown kind under notebooks", () => {
      expect(lint(`## artifact { kind=report }\n${SOURCE}`)).toEqual([
         {
            line: 1,
            code: "notebook-kind-unknown",
            message:
               "Line 1: `kind=report` is not a kind Publisher knows (dashboard, notebook). Fix: write `## artifact { kind=notebook }`.",
         },
      ]);
   });

   it("flags tiles under notebooks", () => {
      expect(
         lint(`## artifact { kind=notebook tiles=[a] }\n${SOURCE}`),
      ).toEqual([
         {
            line: 1,
            code: "notebook-tiles",
            message:
               "Line 1: `tiles` builds a dashboard grid and does nothing under notebooks/; a notebook's cells are the statements in the file. Fix: remove `tiles`, or move the file to dashboards/.",
         },
      ]);
   });

   it("flags kind=notebook under dashboards", () => {
      expect(
         lint(
            `## artifact { kind=notebook tiles=[a] }\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([
         {
            line: 1,
            code: "notebook-kind-under-dashboards",
            message:
               "Line 1: `kind=notebook` marks a notebook, but this file is under dashboards/, which serves dashboards. Fix: move the file to notebooks/, or remove `kind`.",
         },
      ]);
   });

   it("flags an unknown kind under dashboards, and the tile kind text on the dashboard itself", () => {
      expect(
         lint(`## artifact { kind=text }\n${SOURCE}`, "dashboards/d.malloy"),
      ).toEqual([
         {
            line: 1,
            code: "notebook-kind-text-on-dashboard",
            message:
               "Line 1: `kind=text` marks a tile entry, so it does not mark this dashboard. Fix: remove `kind`, or write `kind=dashboard`.",
         },
      ]);
      expect(
         lint(`## artifact { kind=report }\n${SOURCE}`, "dashboards/d.malloy"),
      ).toEqual([
         {
            line: 1,
            code: "notebook-kind-unknown",
            message:
               "Line 1: `kind=report` is not a kind Publisher knows (dashboard, notebook). Fix: remove `kind`.",
         },
      ]);
   });

   it.each([
      ['import "../models/orders.malloy"', 'import "../models/orders.malloy"'],
      [SOURCE.trim(), SOURCE.trim()],
      [RUN.trim(), RUN.trim()],
      ["given: G :: string is 'a'", "given: G :: string is 'a'"],
      ["query: q is a -> { select: x }", "query: q is a -> { select: x }"],
   ])(
      "errors on the statement %s above the artifact tag",
      (statement, shown) => {
         const found = lintNotebookText(
            "notebooks/n.malloy",
            `##! experimental.givens\n${statement}\n${HEADER}`,
         ).filter((f) => f.code !== "notebook-givens-not-enabled");
         expect(found).toEqual([
            {
               line: 2,
               code: "notebook-statement-above-artifact",
               severity: "error",
               message: `Line 2: \`${shown}\` sits above the \`## artifact\` tag, and only \`##!\` flags, \`//\` comments and \`"\` notes may. Fix: move it below the artifact tag.`,
            },
         ]);
      },
   );

   it("puts the statement finding on the keyword line, below its tag lines", () => {
      const found = lint(`${SOURCE}# bar_chart\n${RUN}${HEADER}`);
      expect(found.map((f) => [f.line, f.code])).toEqual([
         [1, "notebook-statement-above-artifact"],
         [3, "notebook-statement-above-artifact"],
      ]);
   });

   it.each([
      ['## title="x"', '## title="x"'],
      ['##(filters) ["a"]', '##(filters) ["a"]'],
      ["##| tags\nautorun=false\n|##", "##| tags"],
   ])("errors on the tag %j above the artifact tag", (tagText, shown) => {
      const found = lintNotebookText(
         "notebooks/n.malloy",
         `${tagText}\n${HEADER}`,
      );
      expect(
         found.map(({ code, severity, message }) => [code, severity, message]),
      ).toEqual([
         [
            "notebook-tag-above-artifact",
            "error",
            `Line 1: \`${shown}\` sits above the \`## artifact\` tag, and only \`##!\` flags, \`//\` comments and \`"\` notes may. Fix: move it below the artifact tag.`,
         ],
      ]);
   });

   it('allows ##! flags, // comments and unnamed " notes above the artifact tag', () => {
      const header =
         '##! experimental.givens\n// a comment\n##" a description\n##|"\nmore description\n|##\n';
      expect(lint(`${header}${HEADER}${SOURCE}`)).toEqual([]);
   });

   it("leaves statements above the tag alone in a dashboard", () => {
      expect(
         lint(
            `${SOURCE}## artifact { tiles=["a -> v"] }\n`,
            "dashboards/d.malloy",
         ),
      ).toEqual([]);
   });

   it("errors when dashboard_columns and dashboard { columns } disagree, naming both values", () => {
      const found = lintNotebookText(
         "dashboards/d.malloy",
         `## artifact { tiles=[a] dashboard_columns=8 } dashboard { columns=12 }\n${SOURCE}`,
      );
      expect(found).toEqual([
         {
            line: 1,
            code: "notebook-columns-conflict",
            severity: "error",
            message:
               "Line 1: `dashboard_columns=8` in the artifact tag and `dashboard { columns=12 }` disagree about the grid width. Fix: keep `dashboard { columns=… }` and remove `dashboard_columns`.",
         },
      ]);
   });

   it("finds the conflict when dashboard { columns } is its own ## line", () => {
      const found = lint(
         `## artifact { tiles=[a] dashboard_columns=8 }\n## dashboard { columns=12 }\n${SOURCE}`,
         "dashboards/d.malloy",
      );
      expect(found.map((f) => f.code)).toEqual(["notebook-columns-conflict"]);
   });

   it("warns, and only warns, that dashboard_columns alone is deprecated", () => {
      const found = lintNotebookText(
         "dashboards/d.malloy",
         `## artifact { tiles=[a] dashboard_columns=8 }\n${SOURCE}`,
      );
      expect(found).toEqual([
         {
            line: 1,
            code: "notebook-columns-alias",
            severity: "warn",
            message:
               "Line 1: `dashboard_columns=8` is a deprecated spelling of the grid width. Fix: write `dashboard { columns=8 }` instead.",
         },
      ]);
   });

   it("warns about the alias even when dashboard { columns } agrees with it", () => {
      const found = lint(
         `## artifact { tiles=[a] dashboard_columns=12 } dashboard { columns=12 }\n${SOURCE}`,
         "dashboards/d.malloy",
      );
      expect(found.map((f) => f.code)).toEqual(["notebook-columns-alias"]);
   });

   it("says nothing about dashboard { columns } alone", () => {
      expect(
         lint(
            `## artifact { tiles=[a] } dashboard { columns=12 }\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([]);
   });

   it("says nothing about a helper model in notebooks/ that has no artifact note", () => {
      expect(
         lint(
            `${SOURCE}${RUN}##| markdown\nhi\n|##\n// stray\n${RUN}given: G :: string is 'a'\n`,
         ),
      ).toEqual([]);
   });

   it("says nothing about an untagged helper file under dashboards/", () => {
      expect(
         lint(
            `## Internal notes for maintainers\n${SOURCE}${RUN}`,
            "dashboards/helper.malloy",
         ),
      ).toEqual([]);
   });

   it("still lints a single-query dashboard whose artifact tag sits on its query", () => {
      expect(
         lint(
            `${SOURCE}##| markdown\nhi\n|##\n# artifact { title="One" }\nquery: q is a -> { select: x }\n`,
            "dashboards/d.malloy",
         ).map((f) => f.code),
      ).toEqual(["notebook-markdown-opener"]);
   });

   it("leaves the dashboard_columns alias out of a single-query artifact, which has no grid", () => {
      expect(
         lint(
            `## artifact { dashboard_columns=8 }\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([]);
   });

   it("warns about a comment attached to the statement below it", () => {
      expect(lint(`${HEADER}${SOURCE}\n// about the run\n${RUN}`)).toEqual([
         {
            line: 4,
            code: "notebook-comment-not-shown",
            message:
               "Line 4: this comment sits directly above a cell but is not part of it, so the notebook does not show it. Fix: write it as a `##(markdown)` prose note, or move it inside the statement it describes.",
         },
      ]);
   });

   it("warns about every line of a comment block attached to the statement", () => {
      const found = lint(
         `${HEADER}${SOURCE}\n/* two\nlines */\n// and one\n${RUN}`,
      );
      expect(found.map((f) => f.line)).toEqual([4, 6]);
   });

   it("leaves a comment set apart by a blank line, or trailing a statement, alone", () => {
      expect(lint(`${HEADER}${SOURCE}\n// set apart\n\n${RUN}`)).toEqual([]);
      expect(lint(`${HEADER}${SOURCE.trim()} // trailing\n${RUN}`)).toEqual([]);
   });

   it("keeps a comment inside a statement, above the artifact tag, or in a block quiet", () => {
      const text = `// header comment\n${HEADER}${SOURCE}# bar_chart\n// between tag and run\n${RUN}##|(markdown)\n// prose that looks like a comment\n|##\n`;
      expect(lint(text)).toEqual([]);
   });

   it("gives a file that does not compile the fix-it for its compile error", () => {
      const found = lint(
         `${HEADER}given: G :: string is 'a'\n##|(markdown)\nhi\n`,
      );
      expect(found.map((f) => f.code)).toEqual([
         "notebook-givens-not-enabled",
         "notebook-unterminated-block",
      ]);
   });

   it("finds nothing in the clean fixture notebooks and dashboards", () => {
      const root = path.join(FIXTURES, "notebooks-malloyyo");
      const files = [
         ...fs
            .readdirSync(path.join(root, "notebooks"))
            .filter(
               (f) =>
                  ![
                     "adjacent_blocks.malloy",
                     "structure.malloy",
                     "refused.malloy",
                  ].includes(f),
            )
            .map((f) => `notebooks/${f}`),
         "dashboards/text_tiles.malloy",
      ];
      expect(files.length).toBeGreaterThan(5);
      for (const file of files) {
         const text = fs.readFileSync(path.join(root, file), "utf8");
         expect({ file, found: lint(text, file) }).toEqual({ file, found: [] });
      }
   });

   it("puts the fixture dashboard through the dashboard lint too, which only says its text tile is not rendered yet", () => {
      const text = fs.readFileSync(
         path.join(FIXTURES, "notebooks-malloyyo/dashboards/text_tiles.malloy"),
         "utf8",
      );
      const facts: DashboardModelFacts = {
         modelPath: "dashboards/text_tiles.malloy",
         modelAnnotations: text
            .split("\n")
            .filter((line) => line.startsWith("## artifact"))
            .map((line) => `${line}\n`),
         queries: [],
         givens: new Map(),
         viewGivens: new Map([["orders -> kpis", []]]),
         viewAnnotations: new Map([["orders -> kpis", []]]),
         sourceFields: new Map([["orders", new Set(["kpis"])]]),
         drills: [],
         suggestGivens: {
            forSource: () => undefined,
            forQuery: () => undefined,
         },
      };
      const manifest = buildDashboardManifest(facts);
      if (!manifest) throw new Error("expected a dashboard");
      expect(lintDashboard(facts, manifest).map((f) => f.severity)).toEqual([
         "warn",
      ]);
   });

   it("finds what the fixture notebooks carry on purpose", () => {
      const read = (file: string) =>
         fs.readFileSync(
            path.join(FIXTURES, "notebooks-malloyyo/notebooks", file),
            "utf8",
         );
      expect(lint(read("adjacent_blocks.malloy"))).toEqual([]);
      expect(
         lint(read("structure.malloy")).map((f) => [f.line, f.code]),
      ).toEqual([[13, "notebook-comment-not-shown"]]);
   });

   it("finds nothing in any example or fixture dashboard, the lint fixtures and one malformed tag aside", () => {
      const roots = [path.resolve(__dirname, "../../../../examples"), FIXTURES];
      const found: string[] = [];
      const walk = (dir: string, base: string) => {
         for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
            if (
               entry.name === "node_modules" ||
               entry.name === "notebooks-lint"
            )
               continue;
            const full = path.join(dir, entry.name);
            // Forward slashes so the match and the expected paths hold on Windows too.
            const posix = full.split(path.sep).join("/");
            if (entry.isDirectory()) walk(full, base);
            else if (/\/dashboards\/[^/]+\.malloy$/.test(posix)) {
               const rel = posix.slice(posix.lastIndexOf("/dashboards/") + 1);
               for (const f of lintNotebookText(
                  rel,
                  fs.readFileSync(full, "utf8"),
               ))
                  found.push(
                     `${path.relative(base, full).split(path.sep).join("/")} ${f.code}`,
                  );
            }
         }
      };
      for (const root of roots) walk(root, root);
      expect(found).toEqual([
         "dashboards-lint/dashboards/malformed.malloy notebook-artifact-unparsed",
      ]);
   });

   it("ignores a file outside notebooks and dashboards", () => {
      expect(
         lint(`${HEADER}##| markdown\nhi\n|##\n`, "models/m.malloy"),
      ).toEqual([]);
   });

   it('draws the spacing finding for a ##"word line too', () => {
      expect(lint(`${HEADER}##"word\n${SOURCE}`)).toEqual([
         {
            line: 2,
            code: "notebook-block-opener-spacing",
            message:
               'Line 2: `##"word` has no space after the route, so Malloy drops the note. Did you mean `##(markdown) word`?',
         },
      ]);
   });

   it('suggests ##" for a ##"word line above the tag, and a text tile in a dashboard', () => {
      expect(lint(`##"word\n${HEADER}${SOURCE}`)[0].message).toContain(
         'Did you mean `##" word`?',
      );
      expect(
         lint(
            `## artifact { tiles=["a -> v"] }\n##"word\n${SOURCE}`,
            "dashboards/d.malloy",
         )[0].message,
      ).toContain(
         "Fix: write `##|(markdown) name` and list `name { kind=text }` in `tiles`, with the text as its body.",
      );
   });

   it("errors when the artifact tag of a dashboard does not parse", () => {
      const found = lintNotebookText(
         "dashboards/d.malloy",
         `## artifact { tiles: [a] }\n${SOURCE}`,
      );
      expect(found.map((f) => [f.code, f.severity, f.line])).toEqual([
         ["notebook-artifact-unparsed", "error", 1],
      ]);
   });

   it("finds dashboard { columns } inside a ## | block, past its closer", () => {
      const found = lint(
         `## artifact { tiles=[a] dashboard_columns=8 }\n##|\ndashboard { columns=12 }\n|##\n${SOURCE}`,
         "dashboards/d.malloy",
      );
      expect(found.map((f) => f.code)).toEqual(["notebook-columns-conflict"]);
   });

   it("reports a conflict when dashboard { columns } is present but not a width", () => {
      const found = lint(
         `## artifact { tiles=[a] dashboard_columns=8 } dashboard { columns=0 }\n${SOURCE}`,
         "dashboards/d.malloy",
      );
      expect(found.map((f) => f.code)).toEqual(["notebook-columns-conflict"]);
   });

   it("errors on opener text of more than one word in a notebook and a dashboard alike", () => {
      const severity = (text: string, modelPath: string) =>
         lintNotebookText(modelPath, text)
            .filter((f) => f.code === "notebook-markdown-opener-text")
            .map((f) => f.severity);
      expect(
         severity(
            `${HEADER}##|(markdown) a b\nhi\n|##\n`,
            "notebooks/n.malloy",
         ),
      ).toEqual(["error"]);
      expect(
         severity(
            `## artifact { tiles=[a] }\n##|(markdown) a b\nhi\n|##\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual(["error"]);
   });

   it("carries an error finding's severity into its compile problem", () => {
      const problems = notebookLintProblems(
         "notebooks/n.malloy",
         `${SOURCE}${HEADER}`,
         "file:///p/notebooks/n.malloy",
      );
      expect(problems).toHaveLength(1);
      expect(problems[0]).toMatchObject({
         code: "notebook-statement-above-artifact",
         severity: "error",
         at: { range: { start: { line: 0 } } },
      });
   });

   it("turns findings into warn problems at their 0-based line", () => {
      const problems = notebookLintProblems(
         "notebooks/n.malloy",
         `${HEADER}##| markdown\nhi\n|##\n`,
         "file:///p/notebooks/n.malloy",
      );
      expect(problems).toHaveLength(1);
      expect(problems[0]).toMatchObject({
         code: "notebook-markdown-opener",
         severity: "warn",
         at: {
            url: "file:///p/notebooks/n.malloy",
            range: { start: { line: 1, character: 0 } },
         },
      });
   });
});

describe("notebook lint: attached #| blocks and one-line slips", () => {
   it("errors on text after (markdown) on an attached #| opener, and says where it goes", () => {
      const found = lintNotebookText(
         "notebooks/n.malloy",
         `${HEADER}#|(markdown) two words\nhi\n|#\n${RUN}`,
      );
      expect(found).toMatchObject([
         {
            line: 2,
            code: "notebook-markdown-opener-text",
            severity: "error",
            message:
               "Line 2: `two words` follows `#|(markdown)` on its opener line, where only one bare word may go (a name), so the block would show it as its first line. Fix: move it into the body, on the line below the opener.",
         },
      ]);
   });

   it("warns that a name on an attached #| block is not shown", () => {
      expect(lint(`${HEADER}#|(markdown) intro\nhi\n|#\n${RUN}`)).toEqual([
         {
            line: 2,
            code: "notebook-markdown-block-named",
            message:
               "Line 2: the name `intro` on this `(markdown)` block means nothing on a block attached to a statement, and it is not shown. Fix: remove the name.",
         },
      ]);
   });

   it("reports a #| block followed by stray text as attached nowhere, once", () => {
      expect(
         lint(`${HEADER}#|(markdown)\nhi\n|#\nmore prose\n${RUN}`).map(
            (f) => f.code,
         ),
      ).toEqual(["notebook-markdown-attached-nowhere"]);
   });

   it("does not say no statement follows a #(markdown) that a model-level note separates from its run:", () => {
      const [found] = lint(`${HEADER}#(markdown) a\n##(markdown) b\n${RUN}`);
      expect(found.code).toBe("notebook-markdown-attached-nowhere");
      expect(found.message).toContain("a `##` model-level note");
      expect(found.message).not.toContain("no statement follows");
   });

   it.each([
      [
         "text after a closed #| block",
         `${HEADER}#|(markdown)\nhi\n|#\nmore prose\n${RUN}`,
      ],
      [
         "a block-form given's description",
         `##! experimental.givens\n${HEADER}given:\n#|"\nthe description\n|#\nG :: string is "x"\n${RUN}`,
      ],
      [
         "a tagged item in a dimension list",
         `${HEADER}source: s is a extend {\ndimension:\n#|\nlabel="B"\n|#\nb is 2\n}\n`,
      ],
      [
         "a (markdown) block before an item in group_by",
         `${HEADER}run: a -> {\ngroup_by:\n#|(markdown)\nabout x\n|#\nx\n}\n`,
      ],
   ])("reports no closed-early finding on %s", (_name, text) => {
      expect(
         lint(text).filter((f) => f.code === "notebook-block-closed-early"),
      ).toEqual([]);
   });

   it.each([
      ["#(Markdown) text", "#(Markdown) text", "#(markdown)"],
      ["#markdown text", "#markdown text", "#(markdown)"],
   ])("suggests the route for the attached slip %s", (_name, note, fix) => {
      expect(lint(`${HEADER}${note}\n${RUN}`)).toEqual([
         {
            line: 2,
            code: "notebook-markdown-opener",
            message: `Line 2: \`${note}\` is not on the \`(markdown)\` route, so it is not shown as prose. Did you mean \`${fix}\`?`,
         },
      ]);
   });

   it("says to put a space after #(markdown) and ##(markdown)", () => {
      expect(lint(`${HEADER}#(markdown)text\n${RUN}`)).toEqual([
         {
            line: 2,
            code: "notebook-block-opener-spacing",
            message:
               "Line 2: `#(markdown)text` has no space after the route, so Malloy drops the note. Did you mean `#(markdown) text`?",
         },
      ]);
      expect(lint(`${HEADER}##(markdown)word\n${RUN}`)).toEqual([
         {
            line: 2,
            code: "notebook-block-opener-spacing",
            message:
               "Line 2: `##(markdown)word` has no space after the route, so Malloy drops the note. Did you mean `##(markdown) word`?",
         },
      ]);
   });

   it("does not flag a tag that only starts like the route", () => {
      expect(lint(`${HEADER}# markdown\n${RUN}`)).toEqual([]);
      expect(lint(`${HEADER}#(markdown_help) x\n${RUN}`)).toEqual([]);
   });
});

describe("notebook lint: a (markdown) note nothing reads", () => {
   const NESTED = `${HEADER}source: a is duckdb.sql("select 1 as x") extend {
  #|(markdown) Revenue per order
  more
  |#
  dimension: y is x
  #(markdown) about z
  dimension: z is x
}
`;

   it("warns once per note, and never errors, for markdown above a field", () => {
      const found = lintNotebookText("notebooks/n.malloy", NESTED);
      expect(found.map((f) => [f.code, f.severity, f.line])).toEqual([
         ["notebook-markdown-nested", "warn", 3],
         ["notebook-markdown-nested", "warn", 7],
      ]);
      expect(found[0].message).toBe(
         "Line 3: `#|(markdown) Revenue per order` sits inside a statement, where nothing reads a `(markdown)` note, so it is not shown. Fix: move it above the statement it describes.",
      );
   });

   it("does not fail /compile: the file compiles and no problem is an error", () => {
      expect(translateToParse(NESTED).problems).toEqual([]);
      const problems = notebookLintProblems(
         "notebooks/n.malloy",
         NESTED,
         "file:///p/notebooks/n.malloy",
      );
      expect(problems.map((p) => p.severity)).toEqual(["warn", "warn"]);
   });

   it("still errors on opener text above a top-level statement", () => {
      expect(
         lintNotebookText(
            "notebooks/n.malloy",
            `${HEADER}#|(markdown) two words\nhi\n|#\n${SOURCE}`,
         ).map((f) => [f.code, f.severity]),
      ).toEqual([["notebook-markdown-opener-text", "error"]]);
   });
});

describe("notebook lint: a fix lints clean when applied literally", () => {
   const NB = "notebooks/n.malloy";
   const DASH = "dashboards/d.malloy";
   const DASH_TAG = '## artifact { tiles=["a -> v"] }\n';
   const DASH_INTRO = "## artifact { tiles=[intro { kind=text }] }\n";
   const sub = (from: string | RegExp, to: string) => (t: string) =>
      t.replace(from, to);
   const listTile = (name: string) =>
      sub('tiles=["a -> v"]', `tiles=["a -> v", ${name} { kind=text }]`);
   const steps =
      (...fns: ((t: string) => string)[]) =>
      (t: string) =>
         fns.reduce((acc, fn) => fn(acc), t);
   const TILE =
      "write `##|(markdown) name` and list `name { kind=text }` in `tiles`";
   const TWO_WORDS =
      "write `##|(markdown) name`, put `two words` on the line below it, and list `name { kind=text }` in `tiles`";
   const NESTED = `${HEADER}source: a is duckdb.sql("select 1 as x") extend {\n  #(markdown)text\n  dimension: y is x\n}\n`;

   type Case = {
      name: string;
      path: string;
      text: string;
      code: string;
      /** A literal substring of the finding's message: the fix as it is worded. */
      fix: string;
      apply: (text: string) => string;
   };
   const cases: Case[] = [
      {
         name: 'a ##"word line below a notebook tag',
         path: NB,
         text: `${HEADER}##"word\n${SOURCE}`,
         code: "notebook-block-opener-spacing",
         fix: "Did you mean `##(markdown) word`?",
         apply: sub('##"word', "##(markdown) word"),
      },
      {
         name: 'a ##"word line above a notebook tag',
         path: NB,
         text: `##"word\n${HEADER}${SOURCE}`,
         code: "notebook-block-opener-spacing",
         fix: 'Did you mean `##" word`?',
         apply: sub('##"word', '##" word'),
      },
      {
         name: "a ##(markdown)word line",
         path: NB,
         text: `${HEADER}##(markdown)word\n${SOURCE}`,
         code: "notebook-block-opener-spacing",
         fix: "Did you mean `##(markdown) word`?",
         apply: sub("##(markdown)word", "##(markdown) word"),
      },
      {
         name: "a #(markdown)text line",
         path: NB,
         text: `${HEADER}#(markdown)text\n${RUN}`,
         code: "notebook-block-opener-spacing",
         fix: "Did you mean `#(markdown) text`?",
         apply: sub("#(markdown)text", "#(markdown) text"),
      },
      {
         name: "a #(Markdown) line",
         path: NB,
         text: `${HEADER}#(Markdown) text\n${RUN}`,
         code: "notebook-markdown-opener",
         fix: "Did you mean `#(markdown)`?",
         apply: sub("#(Markdown)", "#(markdown)"),
      },
      {
         name: "a #markdown line",
         path: NB,
         text: `${HEADER}#markdown text\n${RUN}`,
         code: "notebook-markdown-opener",
         fix: "Did you mean `#(markdown)`?",
         apply: sub("#markdown", "#(markdown)"),
      },
      {
         name: "a ##markdown line",
         path: NB,
         text: `${HEADER}##markdown hi\n${SOURCE}`,
         code: "notebook-markdown-opener",
         fix: "Did you mean `##(markdown)`?",
         apply: sub("##markdown", "##(markdown)"),
      },
      {
         name: "a ##| markdown block",
         path: NB,
         text: `${HEADER}##| markdown\nhi\n|##\n`,
         code: "notebook-markdown-opener",
         fix: "Did you mean `##|(markdown)`?",
         apply: sub("##| markdown", "##|(markdown)"),
      },
      {
         name: "a heading line in a notebook",
         path: NB,
         text: `${HEADER}## Some heading text\n${SOURCE}`,
         code: "notebook-heading-line",
         fix: "Did you mean `##(markdown)`?",
         apply: sub("## Some", "##(markdown) Some"),
      },
      {
         name: "a heading line above a notebook tag",
         path: NB,
         text: `## Some heading text\n${HEADER}${SOURCE}`,
         code: "notebook-heading-line",
         fix: 'Did you mean `##"`?',
         apply: sub("## Some", '##" Some'),
      },
      {
         name: "a heading line in a dashboard",
         path: DASH,
         text: `${DASH_TAG}## Some heading text\n${SOURCE}`,
         code: "notebook-heading-line",
         fix: "write `##|(markdown) name` and list `name { kind=text }` in `tiles`",
         apply: (t) =>
            t
               .replace(
                  "## Some heading text",
                  "##|(markdown) name\nSome heading text\n|##",
               )
               .replace(
                  'tiles=["a -> v"]',
                  'tiles=["a -> v", name { kind=text }]',
               ),
      },
      {
         name: "a glued (markdown) block opener in a notebook",
         path: NB,
         text: `${HEADER}##|(markdown)intro\nhi\n|##\n`,
         code: "notebook-block-opener-spacing",
         fix: "write `##|(markdown)` and put `intro` on the line below it",
         apply: sub("##|(markdown)intro", "##|(markdown)\nintro"),
      },
      {
         name: "a glued (markdown) attached opener",
         path: NB,
         text: `${HEADER}#|(markdown)Summary\nhi\n|#\n${RUN}`,
         code: "notebook-block-opener-spacing",
         fix: "write `#|(markdown)` and put `Summary` on the line below it",
         apply: sub("#|(markdown)Summary", "#|(markdown)\nSummary"),
      },
      {
         name: 'a glued ##|"intro block above the tag',
         path: NB,
         text: `##|"intro\nhi\n|##\n${HEADER}`,
         code: "notebook-block-opener-spacing",
         fix: 'write `##|"` and put `intro` on the line below it',
         apply: sub('##|"intro', '##|"\nintro'),
      },
      {
         name: "a glued tile name in a dashboard",
         path: DASH,
         text: `${DASH_INTRO}##|(markdown)intro\nhi\n|##\n${SOURCE}`,
         code: "notebook-block-opener-spacing",
         fix: "put a space after the route, as in `##|(markdown) intro`, and list `intro { kind=text }` in `tiles`",
         apply: sub("##|(markdown)intro", "##|(markdown) intro"),
      },
      {
         name: "a glued tile name that no tile lists",
         path: DASH,
         text: `${DASH_TAG}##|(markdown)intro\nhi\n|##\n${SOURCE}`,
         code: "notebook-block-opener-spacing",
         fix: "put a space after the route, as in `##|(markdown) intro`, and list `intro { kind=text }` in `tiles`",
         apply: steps(
            sub("##|(markdown)intro", "##|(markdown) intro"),
            listTile("intro"),
         ),
      },
      {
         name: "a glued invalid tile name in a dashboard",
         path: DASH,
         text: `${DASH_TAG}##|(markdown)my-intro\nhi\n|##\n${SOURCE}`,
         code: "notebook-block-opener-spacing",
         fix: "write `##|(markdown) name`, put `my-intro` on the line below it, and list `name { kind=text }` in `tiles`",
         apply: steps(
            sub("##|(markdown)my-intro", "##|(markdown) name\nmy-intro"),
            listTile("name"),
         ),
      },
      {
         name: "text after (markdown) on a notebook opener",
         path: NB,
         text: `${HEADER}##|(markdown) two words\nhi\n|##\n`,
         code: "notebook-markdown-opener-text",
         fix: "move it into the body, on the line below the opener",
         apply: sub("##|(markdown) two words", "##|(markdown)\ntwo words"),
      },
      {
         name: "an invalid tile name in a dashboard",
         path: DASH,
         text: `${DASH_TAG}${SOURCE}##|(markdown) my-intro\nhi\n|##\n`,
         code: "notebook-markdown-opener-text",
         fix: "write `##|(markdown) my_intro` and list `my_intro { kind=text }` in `tiles`",
         apply: steps(
            sub("##|(markdown) my-intro", "##|(markdown) my_intro"),
            listTile("my_intro"),
         ),
      },
      {
         name: "words on a dashboard tile's opener line",
         path: DASH,
         text: `${DASH_TAG}${SOURCE}##|(markdown) two words\nhi\n|##\n`,
         code: "notebook-markdown-opener-text",
         fix: TWO_WORDS,
         apply: steps(
            sub("##|(markdown) two words", "##|(markdown) name\ntwo words"),
            listTile("name"),
         ),
      },
      {
         name: "a name on a notebook block",
         path: NB,
         text: `${HEADER}##|(markdown) intro\nhi\n|##\n`,
         code: "notebook-markdown-block-named",
         fix: "remove the name",
         apply: sub("##|(markdown) intro", "##|(markdown)"),
      },
      {
         name: "an unnamed block in a dashboard",
         path: DASH,
         text: `${DASH_TAG}${SOURCE}##|(markdown)\nhi\n|##\n`,
         code: "notebook-markdown-block-unnamed",
         fix: "write `##|(markdown) name` and list `name { kind=text }` in `tiles`",
         apply: (t) =>
            t
               .replace("##|(markdown)\n", "##|(markdown) name\n")
               .replace(
                  'tiles=["a -> v"]',
                  'tiles=["a -> v", name { kind=text }]',
               ),
      },
      {
         name: "a floating line in a dashboard",
         path: DASH,
         text: `${DASH_TAG}##(markdown) hi\n${SOURCE}`,
         code: "notebook-markdown-block-unnamed",
         fix: "write `##|(markdown) name` and list `name { kind=text }` in `tiles`",
         apply: (t) =>
            t
               .replace("##(markdown) hi", "##|(markdown) name\nhi\n|##")
               .replace(
                  'tiles=["a -> v"]',
                  'tiles=["a -> v", name { kind=text }]',
               ),
      },
      {
         name: "text after a notebook closer",
         path: NB,
         text: `${HEADER}##|(markdown)\nhi\n|## trailing\n${SOURCE}`,
         code: "notebook-text-after-closer",
         fix: "put it on its own `##(markdown)` line",
         apply: sub("|## trailing", "|##\n##(markdown) trailing"),
      },
      {
         name: "a block that is never closed",
         path: NB,
         text: `${HEADER}##|(markdown)\nhi\n`,
         code: "notebook-unterminated-block",
         fix: "add a `|##` line where the prose ends",
         apply: (t) => `${t}|##\n`,
      },
      {
         name: "a dangling #(markdown) line",
         path: NB,
         text: `${HEADER}${SOURCE}#(markdown) trailing\n`,
         code: "notebook-markdown-attached-nowhere",
         fix: "write it as a floating `##(markdown)` line",
         apply: sub("#(markdown) trailing", "##(markdown) trailing"),
      },
      {
         name: "a dangling #|(markdown) block",
         path: NB,
         text: `${HEADER}${SOURCE}#|(markdown)\nhi\n|#\n`,
         code: "notebook-markdown-attached-nowhere",
         fix: "write it as a floating `##|(markdown)` block closed by `|##`",
         apply: (t) =>
            t.replace("#|(markdown)", "##|(markdown)").replace("|#\n", "|##\n"),
      },
      {
         name: "a dangling #(markdown) line in a dashboard",
         path: DASH,
         text: `${DASH_TAG}${SOURCE}#(markdown) trailing\n`,
         code: "notebook-markdown-attached-nowhere",
         fix: `${TILE}, with the text as its body, or delete it`,
         apply: steps(
            sub("#(markdown) trailing", "##|(markdown) name\ntrailing\n|##"),
            listTile("name"),
         ),
      },
      {
         name: "a dangling #|(markdown) block in a dashboard",
         path: DASH,
         text: `${DASH_TAG}${SOURCE}#|(markdown)\nhi\n|#\n`,
         code: "notebook-markdown-attached-nowhere",
         fix: `${TILE}, with the text as its body, or delete it`,
         apply: steps(
            sub("#|(markdown)\nhi\n|#", "##|(markdown) name\nhi\n|##"),
            listTile("name"),
         ),
      },
      {
         name: "a markdown note nested in a dashboard statement",
         path: DASH,
         text: `${DASH_TAG}source: a is duckdb.sql("select 1 as x") extend {\n  #(markdown) about y\n  dimension: y is x\n}\n`,
         code: "notebook-markdown-nested",
         fix: `a dashboard reads no attached note, so ${TILE}, with the text as its body, or delete it`,
         apply: steps(
            sub("  #(markdown) about y\n", ""),
            sub("source: a", "##|(markdown) name\nabout y\n|##\nsource: a"),
            listTile("name"),
         ),
      },
      {
         name: 'a ##"word line below a dashboard tag',
         path: DASH,
         text: `${DASH_TAG}##"word\n${SOURCE}`,
         code: "notebook-block-opener-spacing",
         fix: `${TILE}, with the text as its body`,
         apply: steps(
            sub('##"word', "##|(markdown) name\nword\n|##"),
            listTile("name"),
         ),
      },
      {
         name: "a ##(markdown)word line in a dashboard",
         path: DASH,
         text: `${DASH_TAG}##(markdown)word\n${SOURCE}`,
         code: "notebook-block-opener-spacing",
         fix: `${TILE}, with the text as its body`,
         apply: steps(
            sub("##(markdown)word", "##|(markdown) name\nword\n|##"),
            listTile("name"),
         ),
      },
      {
         name: "a ##markdown slip in a dashboard",
         path: DASH,
         text: `${DASH_TAG}##markdown hi\n${SOURCE}`,
         code: "notebook-markdown-opener",
         fix: `${TILE}, with the text as its body`,
         apply: steps(
            sub("##markdown hi", "##|(markdown) name\nhi\n|##"),
            listTile("name"),
         ),
      },
      {
         name: "a ##| markdown slip in a dashboard",
         path: DASH,
         text: `${DASH_TAG}##| markdown\nhi\n|##\n${SOURCE}`,
         code: "notebook-markdown-opener",
         fix: TILE,
         apply: steps(
            sub("##| markdown", "##|(markdown) name"),
            listTile("name"),
         ),
      },
      {
         name: "a ##(markdown)word line above a notebook tag",
         path: NB,
         text: `##(markdown)word\n${HEADER}${SOURCE}`,
         code: "notebook-block-opener-spacing",
         fix: "It also sits above the `## artifact` tag",
         apply: (t) =>
            t
               .replace("##(markdown)word\n", "")
               .replace(SOURCE, `##(markdown) word\n${SOURCE}`),
      },
      {
         name: "a ##markdown slip above a notebook tag",
         path: NB,
         text: `##markdown hi\n${HEADER}${SOURCE}`,
         code: "notebook-markdown-opener",
         fix: "It also sits above the `## artifact` tag",
         apply: (t) =>
            t
               .replace("##markdown hi\n", "")
               .replace(SOURCE, `##(markdown) hi\n${SOURCE}`),
      },
      {
         name: "a glued ##|(markdown) block above a notebook tag",
         path: NB,
         text: `##|(markdown)intro\nhi\n|##\n${HEADER}${SOURCE}`,
         code: "notebook-block-opener-spacing",
         fix: "It also sits above the `## artifact` tag",
         apply: (t) =>
            t
               .replace("##|(markdown)intro\nhi\n|##\n", "")
               .replace(SOURCE, `##|(markdown)\nintro\nhi\n|##\n${SOURCE}`),
      },
      {
         name: "a glued #(markdown) note nested in a statement",
         path: NB,
         text: NESTED,
         code: "notebook-block-opener-spacing",
         fix: "It also sits inside a statement",
         apply: steps(
            sub("  #(markdown)text\n", ""),
            sub("source: a", "#(markdown) text\nsource: a"),
         ),
      },
      {
         name: "a #markdown slip nested in a statement",
         path: NB,
         text: NESTED.replace("#(markdown)text", "#markdown text"),
         code: "notebook-markdown-opener",
         fix: "It also sits inside a statement",
         apply: steps(
            sub("  #markdown text\n", ""),
            sub("source: a", "#(markdown) text\nsource: a"),
         ),
      },
      {
         name: "a glued #|(markdown) block nested in a statement",
         path: NB,
         text: NESTED.replace(
            "  #(markdown)text\n",
            "  #|(markdown)text\n  hi\n  |#\n",
         ),
         code: "notebook-block-opener-spacing",
         fix: "It also sits inside a statement",
         apply: steps(
            sub("  #|(markdown)text\n  hi\n  |#\n", ""),
            sub("source: a", "#|(markdown)\ntext\nhi\n|#\nsource: a"),
         ),
      },
      {
         name: "a markdown note nested in a statement",
         path: NB,
         text: `${HEADER}source: a is duckdb.sql("select 1 as x") extend {\n  #(markdown) about y\n  dimension: y is x\n}\n`,
         code: "notebook-markdown-nested",
         fix: "move it above the statement it describes",
         apply: (t) =>
            t
               .replace("  #(markdown) about y\n", "")
               .replace("source: a", "#(markdown) about y\nsource: a"),
      },
   ];

   it.each(cases)("$name", ({ path: modelPath, text, code, fix, apply }) => {
      const found = lintNotebookText(modelPath, text).find(
         (f) => f.code === code,
      );
      expect(found).toBeDefined();
      expect(found!.message).toContain(fix);
      const fixed = apply(text);
      expect(fixed).not.toBe(text);
      const after = lintNotebookText(modelPath, fixed).map((f) => f.code);
      expect(after).toEqual([]);
   });
});
