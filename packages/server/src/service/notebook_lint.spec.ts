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

   it.each(['##|" intro', '##|" two words'])(
      'leaves text on a ##|" opener alone, since it is prose: %s',
      (opener) => {
         expect(lint(`${HEADER}${opener}\nhi\n|##\n`)).toEqual([]);
      },
   );

   it("says a (text) block in a notebook is a dashboard text tile", () => {
      expect(lint(`${HEADER}##|(text) intro\nhi\n|##\n`)).toEqual([
         {
            line: 2,
            code: "notebook-text-block",
            message:
               'Line 2: a `(text)` block is a dashboard text tile, and a notebook does not show it. Fix: write `##|"` to make it a markdown cell, or move the file to dashboards/.',
         },
      ]);
   });

   it("errors on a (text) block with no name", () => {
      expect(
         lint(
            `## artifact { tiles=[a] }\n##|(text)\nhi\n|##\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([
         {
            line: 2,
            code: "notebook-text-block-name",
            message:
               "Line 2: a `(text)` block needs a name, the tile's entry in `tiles=[…]`. Fix: write `##|(text) name`, where the name is a bare word of letters, digits and underscores.",
         },
      ]);
   });

   it.each(["two words", "9lives", "has-dash", "in.tile"])(
      "errors on the invalid (text) block name %j",
      (name) => {
         expect(
            lint(
               `## artifact { tiles=[a] }\n##|(text) ${name}\nhi\n|##\n${SOURCE}`,
               "dashboards/d.malloy",
            ),
         ).toEqual([
            {
               line: 2,
               code: "notebook-text-block-name",
               message: `Line 2: \`${name}\` is not a valid name for a \`(text)\` block, which takes exactly one bare word. Fix: write \`##|(text) name\`, where the name is letters, digits and underscores and does not start with a digit.`,
            },
         ]);
      },
   );

   it.each([
      ['##|"intro', '##|"intro'],
      ["##|(text)intro", "##|(text)intro"],
   ])("says how to space %s", (opener, shown) => {
      expect(lint(`${HEADER}${opener}\nhi\n|##\n`)).toEqual([
         {
            line: 2,
            code: "notebook-block-opener-spacing",
            message: `Line 2: \`${shown}\` has no space after the route, so Malloy drops the note. Did you mean \`##|(text) name\` or \`##|"\`?`,
         },
      ]);
   });

   it("leaves a named (text) block alone in a dashboard that lists it as a tile", () => {
      expect(
         lint(
            `## artifact { tiles=[intro { kind=text }] }\n##|(text) intro\nhi\n|##\n${SOURCE}`,
            "dashboards/d.malloy",
         ),
      ).toEqual([]);
   });

   it("warns about a (text) block no tiles entry names", () => {
      expect(
         lint(
            `## artifact { tiles=["a -> v", other { kind=text }] }\n${SOURCE}##|(text) intro\nhi\n|##\n`,
            "dashboards/d.malloy",
         ),
      ).toEqual([
         {
            line: 3,
            code: "notebook-text-block-unreferenced",
            message:
               "Line 3: the `(text)` block `intro` is not named by any entry in `tiles=[…]`, so no tile shows it. Fix: add `intro { kind=text }` to `tiles`, or delete the block.",
         },
      ]);
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
               'Line 2: `## How to read this page` is read as model tags, not shown as prose. Did you mean `##"`?',
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
            if (entry.isDirectory()) walk(full, base);
            else if (/\/dashboards\/[^/]+\.malloy$/.test(full)) {
               const rel = full.slice(full.lastIndexOf("/dashboards/") + 1);
               for (const f of lintNotebookText(
                  rel,
                  fs.readFileSync(full, "utf8"),
               ))
                  found.push(`${path.relative(base, full)} ${f.code}`);
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
               'Line 2: `##"word` has no space after the route, so Malloy drops the note. Did you mean `##" word`?',
         },
      ]);
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

   it("makes a bad (text) name a warn in a notebook and an error in a dashboard", () => {
      const severity = (text: string, modelPath: string) =>
         lintNotebookText(modelPath, text)
            .filter((f) => f.code === "notebook-text-block-name")
            .map((f) => f.severity);
      expect(
         severity(`${HEADER}##|(text)\nhi\n|##\n`, "notebooks/n.malloy"),
      ).toEqual(["warn"]);
      expect(
         severity(
            `## artifact { tiles=[a] }\n##|(text)\nhi\n|##\n${SOURCE}`,
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
