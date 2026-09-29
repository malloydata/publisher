// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { lintNotebookText, notebookLintProblems } from "./notebook_lint";

const FIXTURES = path.resolve(__dirname, "../../tests/fixtures");

const HEADER = "## artifact { kind=notebook }\n";
const SOURCE = 'source: a is duckdb.sql("select 1 as x")\n';
const RUN = "run: a -> { select: x }\n";

const lint = (text: string, modelPath = "notebooks/n.malloy") =>
   lintNotebookText(modelPath, text).map(({ line, code, message }) => ({
      line,
      code,
      message,
   }));

describe("notebook lint", () => {
   it.each([
      ["##| markdown", "##| markdown"],
      ["##|markdown", "##|markdown"],
      ["##|(markdown)", "##|(markdown)"],
      ["##|(Markdown)", "##|(Markdown)"],
   ])("suggests the prose opener for %s", (opener, shown) => {
      expect(lint(`${HEADER}${opener}\nhi\n|##\n`)).toEqual([
         {
            line: 2,
            code: "notebook-markdown-opener",
            message: `Line 2: \`${shown}\` opens a block that is not a prose block, so its body is not a markdown cell. Did you mean \`##|"\`?`,
         },
      ]);
   });

   it("says text tile, not markdown cell, for the opener on a dashboard", () => {
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
               'Line 2: `##| markdown` opens a block that is not a prose block, so its body is not a text tile. Did you mean `##|"`?',
         },
      ]);
   });

   it.each(["##|", "##|(filters)", "##| filters"])(
      "leaves the %s block, a tag or route block, alone",
      (opener) => {
         expect(lint(`${HEADER}${opener}\nsome=tag\n|##\n`)).toEqual([]);
      },
   );

   it("names the line of a multi-word opener", () => {
      expect(lint(`${HEADER}##|" two words\nhi\n|##\n`)).toEqual([
         {
            line: 2,
            code: "notebook-multiword-opener",
            message:
               "Line 2: a `##|\"` opener takes at most one word, the block's name, but this one has `two words`, and text on the opener line is not shown. Fix: put the prose on the lines below the opener.",
         },
      ]);
   });

   it("says a named block's name is ignored in a notebook", () => {
      expect(lint(`${HEADER}##|" intro\nhi\n|##\n`)).toEqual([
         {
            line: 2,
            code: "notebook-named-block",
            message:
               "Line 2: this block is named `intro`, and names are for dashboard text tiles; a notebook ignores it. Fix: remove the name from the opener.",
         },
      ]);
   });

   it("leaves a named block alone in a dashboard", () => {
      expect(
         lint(
            `## artifact { tiles=[a] }\n##|" intro\nhi\n|##\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([]);
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
         lint(`${HEADER}##|"\nhi\n|##\nmore prose here\n\n${SOURCE}`),
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
      expect(lint(`${HEADER}##|"\nhi\n|## extra text\n\n${SOURCE}`)).toEqual([
         {
            line: 4,
            code: "notebook-text-after-closer",
            message:
               'Line 4: the text after the closing `|##` (`extra text`) is dropped, not shown. Fix: put it on its own `##"` line, or inside the block.',
         },
      ]);
   });

   it("flags an unterminated block at its opener and says it runs to the end", () => {
      expect(lint(`${HEADER}##|"\nhi\n${SOURCE}${RUN}`)).toEqual([
         {
            line: 2,
            code: "notebook-unterminated-block",
            message:
               "Line 2: this block is never closed, so it runs to the end of the file and everything after the opener is prose, including the run: on line 5, which is prose here and never runs. Fix: add a `|##` line where the prose ends.",
         },
      ]);
   });

   it("says where a block that swallows another opener actually ends", () => {
      expect(lint(`${HEADER}##|"\nhi\n##|"\nsecond\n|##\n`)).toEqual([
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
      expect(lint(`${HEADER}##|"\n${body}|##\n`)).toEqual([]);
   });

   it("leaves an indented block example inside prose alone", () => {
      const body = 'See:\n    ##|"\n    example\n    |##\nmore\n';
      expect(lint(`${HEADER}##|"\n${body}|##\n`)).toEqual([]);
   });

   it("says render tags sit directly above the run they annotate", () => {
      expect(lint(`${HEADER}${SOURCE}# bar_chart\n##" a note\n${RUN}`)).toEqual(
         [
            {
               line: 3,
               code: "notebook-orphaned-tag",
               message:
                  'Line 3: this # tag is followed by a note, not by a run:, so it annotates nothing. Render tags sit directly above their run:. Fix: move the tag, and any #" caption, directly above the run: on line 5.',
            },
         ],
      );
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
      const found = lint(`${HEADER}##|"\nhi\n|##\n# Heading\nmore words\n`);
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
               "Line 1: `kind=report` is not a kind Publisher knows (notebook). Fix: write `## artifact { kind=notebook }`.",
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

   it("flags an unknown kind under dashboards but not the reserved text kind", () => {
      expect(
         lint(`## artifact { kind=text }\n${SOURCE}`, "dashboards/d.malloy"),
      ).toEqual([]);
      expect(
         lint(`## artifact { kind=report }\n${SOURCE}`, "dashboards/d.malloy"),
      ).toEqual([
         {
            line: 1,
            code: "notebook-kind-unknown",
            message:
               "Line 1: `kind=report` is not a kind Publisher knows (notebook). Fix: remove `kind`.",
         },
      ]);
   });

   it("warns that a run above the artifact tag is a definition cell", () => {
      expect(lint(`${SOURCE}${RUN}${HEADER}`)).toEqual([
         {
            line: 2,
            code: "notebook-run-above-artifact",
            message:
               "Line 2: this run: sits above the `## artifact` tag, so it is a definition cell, not a query cell. Fix: move the run: below the artifact tag; the header above it is not cells.",
         },
      ]);
   });

   it("puts the run-above finding on the run: keyword, below its tag lines", () => {
      const found = lint(`${SOURCE}# bar_chart\n${RUN}${HEADER}`);
      expect(found.map((f) => [f.line, f.code])).toEqual([
         [3, "notebook-run-above-artifact"],
      ]);
   });

   it("says nothing about a helper model in notebooks/ that has no artifact note", () => {
      expect(
         lint(
            `${SOURCE}${RUN}##| markdown\nhi\n|##\n// stray\n${RUN}given: G :: string is 'a'\n`,
         ),
      ).toEqual([]);
   });

   it("warns about a comment attached to the statement below it", () => {
      expect(lint(`${HEADER}${SOURCE}\n// about the run\n${RUN}`)).toEqual([
         {
            line: 4,
            code: "notebook-comment-not-shown",
            message:
               'Line 4: this comment sits directly above a cell but is not part of it, so the notebook does not show it. Fix: write it as a `##"` prose note, or move it inside the statement it describes.',
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
      const text = `// header comment\n${HEADER}${SOURCE}# bar_chart\n// between tag and run\n${RUN}##|"\n// prose that looks like a comment\n|##\n`;
      expect(lint(text)).toEqual([]);
   });

   it("gives a file that does not compile the fix-it for its compile error", () => {
      const found = lint(`${HEADER}given: G :: string is 'a'\n##|"\nhi\n`);
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

   it("finds what the fixture notebooks carry on purpose", () => {
      const read = (file: string) =>
         fs.readFileSync(
            path.join(FIXTURES, "notebooks-malloyyo/notebooks", file),
            "utf8",
         );
      expect(lint(read("adjacent_blocks.malloy"))).toEqual([]);
      expect(
         lint(read("structure.malloy")).map((f) => [f.line, f.code]),
      ).toEqual([
         [7, "notebook-named-block"],
         [12, "notebook-comment-not-shown"],
      ]);
   });

   it("ignores a file outside notebooks and dashboards", () => {
      expect(
         lint(`${HEADER}##| markdown\nhi\n|##\n`, "models/m.malloy"),
      ).toEqual([]);
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
