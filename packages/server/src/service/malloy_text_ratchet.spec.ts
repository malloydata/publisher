// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every name, path, string and annotation value the server writes into Malloy
 * text goes through the Malloy-text module (malloy_text.ts), so a quoting rule
 * is fixed once and an injection or round-trip bug cannot hide in a private
 * copy. This spec fails when new code splices a value into generated Malloy
 * without it.
 *
 * It reads every non-test source with the TypeScript parser and checks each
 * template literal that is generated Malloy (one carrying a Malloy statement or
 * construct: a line-leading `source:`/`query:`/`run:`, `-> {`, `extend {`, a
 * line-leading `#@ persist`, `##!`, `compose(`, `.virtual(`, `group_by:`,
 * `aggregate:`, a line-leading `import {` or `given:`), skipping prose (four
 * plain words in a row: an error message that quotes Malloy). In such a
 * template, an interpolation in a name or value position (after `source:`,
 * `run:`, `->`, `is`, `compose(`, `storage=`, `name=`, `.table(`, `from`,
 * `where:`, a comma, and so on, or between quotes) must be a call to one of the
 * module's emitters, or a variable whose initializer calls one. A template that
 * is just one interpolation in backticks is a private quoter and is flagged
 * wherever it appears.
 *
 * What it cannot see: a value built into a variable through a template with no
 * Malloy construct of its own and then spliced (the variable counts as safe if
 * its initializer calls an emitter anywhere). Review covers that; this catches
 * the common shapes.
 *
 * Legitimate exceptions are listed in ALLOWED, by file and the interpolated
 * expression's text, each with its reason. An entry that no longer matches
 * anything fails too, so the list cannot rot.
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import * as ts from "typescript";

const SRC = path.resolve(__dirname, "..");

const EMITTERS =
   /\b(identifier|quotedIdentifier|quotedPath|stringLiteral|doubleQuotedString|annotationValue|escapeString)\(/;
const MALLOY_CONTEXT =
   /(^|\n)\s*(source|query|run):\s|->\s*\{|\bextend\s*\{|(^|\n)\s*#@\s*persist(?!\s+sources?\s)|##!|\bcompose\(|\.virtual\(|\bgroup_by:|\baggregate:|(^|\n)import \{|(^|\n)given:/;
const NAME_POSITION =
   /(source:|run:|query:|->|compose\(|, |storage=|name=|\.table\(|\.virtual\(|\bfrom|group_by:|aggregate:|select:|where:|\bis|dimension:|measure:|type:|import \{|\()\s*$/;
const PROSE = /\b[a-z]{2,}\s+[a-z]{2,}\s+[a-z]{2,}\s+[a-z]{2,}\b/i;

/** Text spliced verbatim, or not Malloy at all: why each is allowed. */
const LIFTED =
   "author or caller Malloy text spliced verbatim (a lifted slice, a report tile, a caller's own query), not a value turned into Malloy";
const MESSAGE =
   "an error or help message that shows Malloy, not generated text";
const MARKDOWN = "markdown code formatting in generated docs, not Malloy";
const ALLOWED: { file: string; expr: string; reason: string }[] = [
   { file: "settings/reference.ts", expr: "text", reason: MARKDOWN },
   { file: "mcp/error_messages.ts", expr: "query", reason: MESSAGE },
   { file: "mcp/error_messages.ts", expr: "modelIdentifier", reason: MESSAGE },
   { file: "controller/version.controller.ts", expr: "k", reason: MESSAGE },
   { file: "service/report_manifest.ts", expr: "property", reason: MESSAGE },
   { file: "service/report_manifest.ts", expr: "label", reason: MESSAGE },
   { file: "service/report_manifest.ts", expr: "tile", reason: LIFTED },
   { file: "service/model.ts", expr: "tile.query", reason: LIFTED },
   {
      file: "service/materialization/plan/preaggregation_validation.ts",
      expr: "fieldName(f)",
      reason: MESSAGE,
   },
   {
      file: "service/materialization/serve/serve_verdict.ts",
      expr: "source.name",
      reason: MESSAGE,
   },
   {
      file: "service/materialization/serve/materialization_serve_transform.ts",
      expr: "lift.text",
      reason: LIFTED,
   },
   {
      file: "service/materialization/serve/materialization_serve_transform.ts",
      expr: "params.downstreamDefText",
      reason: LIFTED,
   },
   {
      file: "service/authorize.ts",
      expr: "filterText",
      reason:
         "the author's own gate expression, spliced verbatim into the probe",
   },
   { file: "service/compile_document.ts", expr: "expression", reason: LIFTED },
   { file: "service/compile_document.ts", expr: "tile", reason: LIFTED },
   { file: "service/package.ts", expr: "tile.query", reason: LIFTED },
];

interface Finding {
   file: string;
   line: number;
   expr: string;
}

/** The unquoted interpolations in one file's generated Malloy. */
export function scanForUnquotedMalloy(file: string, text: string): Finding[] {
   const sf = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true);
   const safeVars = new Set<string>();
   const collect = (n: ts.Node): void => {
      if (
         ts.isVariableDeclaration(n) &&
         ts.isIdentifier(n.name) &&
         n.initializer &&
         EMITTERS.test(n.initializer.getText(sf))
      ) {
         safeVars.add(n.name.text);
      }
      ts.forEachChild(n, collect);
   };
   collect(sf);
   const findings: Finding[] = [];
   const visit = (n: ts.Node): void => {
      if (ts.isTemplateExpression(n)) {
         const cooked =
            n.head.text +
            n.templateSpans.map((s) => " ${} " + s.literal.text).join("");
         const prose = PROSE.test(cooked.replace(/\$\{\}/g, " "));
         // A private quoter: the whole template is one interpolation in
         // backticks, the shape the module replaced.
         const quoter =
            n.templateSpans.length === 1 &&
            n.head.text === "`" &&
            n.templateSpans[0].literal.text === "`";
         if ((MALLOY_CONTEXT.test(cooked) || quoter) && !prose) {
            let before = n.head.text;
            for (const span of n.templateSpans) {
               const expr = span.expression.getText(sf);
               const after = span.literal.text;
               const quoted = /[`'"]%?$/.test(before) && /^%?[`'"]/.test(after);
               const root = /^[A-Za-z_$][\w$]*/.exec(expr)?.[0] ?? "";
               const safe = EMITTERS.test(expr) || safeVars.has(root);
               if ((NAME_POSITION.test(before) || quoted) && !safe) {
                  findings.push({
                     file,
                     line:
                        sf.getLineAndCharacterOfPosition(
                           span.expression.getStart(sf),
                        ).line + 1,
                     expr,
                  });
               }
               before = after;
            }
         }
      }
      ts.forEachChild(n, visit);
   };
   visit(sf);
   return findings;
}

function sources(dir: string, out: string[] = []): string[] {
   for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
      const p = path.join(dir, e.name);
      if (e.isDirectory()) {
         if (!/node_modules|__fixtures__|test_helpers/.test(p)) sources(p, out);
      } else if (/\.tsx?$/.test(e.name) && !/\.(spec|d)\.tsx?$/.test(e.name)) {
         out.push(p);
      }
   }
   return out;
}

describe("generated Malloy goes through the Malloy-text module", () => {
   const findings = sources(SRC).flatMap((p) =>
      scanForUnquotedMalloy(
         path.relative(SRC, p).split(path.sep).join("/"),
         fs.readFileSync(p, "utf8"),
      ),
   );
   const allowed = (f: Finding) =>
      ALLOWED.some((a) => a.file === f.file && a.expr === f.expr);

   it("splices no value into generated Malloy without an emitter", () => {
      const unexplained = findings
         .filter((f) => !allowed(f))
         .map((f) => `${f.file}:${f.line}: \${${f.expr}}`);
      expect(unexplained).toEqual([]);
   });

   it("lists no allowance that no longer matches anything", () => {
      const stale = ALLOWED.filter(
         (a) => !findings.some((f) => f.file === a.file && f.expr === a.expr),
      ).map((a) => `${a.file}: ${a.expr}`);
      expect(stale).toEqual([]);
   });
});

describe("the scanner itself", () => {
   const scan = (body: string) =>
      scanForUnquotedMalloy("x.ts", body).map((f) => f.expr);

   it("flags a bare name in generated Malloy", () => {
      expect(scan("const t = `source: ${name} is duckdb.table('t')`;")).toEqual(
         ["name"],
      );
      expect(scan("const t = `run: s -> { group_by: ${dim} }`;")).toEqual([
         "dim",
      ]);
      expect(
         scan("const t = `#@ persist storage=${dest}\\nsource: a is b`;"),
      ).toEqual(["dest"]);
   });

   it("flags a private quoter wherever it is", () => {
      expect(scan("const q = `\\`${name}\\``;")).toEqual(["name"]);
   });

   it("accepts an emitter, or a variable built with one", () => {
      expect(
         scan("const t = `source: ${identifier(name)} is duckdb.table('t')`;"),
      ).toEqual([]);
      expect(
         scan(
            "const n = identifier(x);\nconst t = `source: ${n} is duckdb.table(${stringLiteral(p)})`;",
         ),
      ).toEqual([]);
   });

   it("ignores a message that quotes Malloy", () => {
      expect(
         scan(
            "const m = `The source ${name} could not be found in the model, so run: ${name} failed`;",
         ),
      ).toEqual([]);
   });
});
