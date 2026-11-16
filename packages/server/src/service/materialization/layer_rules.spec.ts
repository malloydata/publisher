// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Materialization's mechanical checks beside the import-direction rows
 * (src/import_direction.spec.ts pins R5 to R10 and H5):
 *
 *  - H1 and H2, the documentation lint rules (eslint.config.mjs): they allow
 *    no exceptions.
 *  - H3: each layer's README lists exactly the modules in its directory.
 *  - H4: every export is imported outside its own file, or is a listed test
 *    seam.
 *  - The five ratchets: annotation payload reads outside the declaration
 *    reader, reads of the compiler's IR outside governance/ir, clock reads in
 *    serve/ and domain/, a registered dialect spelled as a string literal
 *    or an object key (or a dialect compared to any literal), and a
 *    function over 200 lines.
 *
 * Every list lives in materialization_allowlists.mjs, each entry tagged with
 * the step that removes it, and only shrinks: an entry whose violation is gone
 * fails here (delete it), and so does a violation no entry names (fix it).
 */

import { beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import * as ts from "typescript";
import * as lists from "../../../materialization_allowlists.mjs";
import { CAPABILITIES } from "./domain/dialect";

const SERVER_DIR = path.resolve(__dirname, "../../..");
const SRC_DIR = path.join(SERVER_DIR, "src");
const MZ_DIR = __dirname;
const LAYERS = ["domain", "text", "plan", "run", "serve"] as const;

/**
 * A step of the plan, as an entry's tag names it, or `table`: a count frozen
 * as part of the dialect table, which no step removes and only a reviewed
 * change grows.
 */
const STEP = /^(L|R|2[a-p]|2g\.[1-4]|5|table)$/;

function walk(dir: string): string[] {
   return fs
      .readdirSync(dir, { recursive: true, encoding: "utf8" })
      .filter((f) => /\.tsx?$/.test(f) && !f.includes("node_modules"))
      .map((f) => path.join(dir, f));
}

const isSpec = (f: string) => /\.spec\.tsx?$/.test(f);
const srcRel = (f: string) =>
   path.relative(SRC_DIR, f).split(path.sep).join("/");
const mzFiles = () => walk(MZ_DIR).filter((f) => !isSpec(f));

/** Compare a computed list with its seed, both ways. */
function expectExact(
   name: string,
   actual: Record<string, unknown>,
   listed: Record<string, unknown>,
): void {
   const unlisted = Object.keys(actual).filter((k) => !(k in listed));
   const stale = Object.keys(listed).filter((k) => !(k in actual));
   // Unlisted: a new violation. Fix it; do not extend the list.
   expect({ list: name, unlisted }).toEqual({ list: name, unlisted: [] });
   // Stale: the violation is gone. Delete the entry.
   expect({ list: name, stale }).toEqual({ list: name, stale: [] });
}

function stepOf(entry: unknown): string {
   return typeof entry === "string" ? entry : (entry as { step: string }).step;
}

// ---------------------------------------------------------------------------
// H1, H2: the lint rules' seed lists are exact.
// ---------------------------------------------------------------------------

type LintMessage = { ruleId: string | null; message: string };
type ESLintInstance = {
   lintFiles(
      patterns: string[],
   ): Promise<{ filePath: string; messages: LintMessage[] }[]>;
   lintText(
      code: string,
      options: { filePath: string },
   ): Promise<{ messages: LintMessage[] }[]>;
};
// eslint is CommonJS; require keeps tsc from asking for its declarations.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const eslintApi = require("eslint") as {
   loadESLint(opts: {
      useFlatConfig: boolean;
      cwd: string;
   }): Promise<
      new (opts: { cwd: string; overrideConfig?: unknown }) => ESLintInstance
   >;
};

describe("H1 and H2: the documentation lint rules", () => {
   let found: { headers: Set<string>; history: Map<string, number> };
   let eslint: ESLintInstance;
   beforeAll(async () => {
      const ESLint = await eslintApi.loadESLint({
         useFlatConfig: true,
         cwd: SERVER_DIR,
      });
      // Lint with empty allowlists, so every offender reports.
      eslint = new ESLint({
         cwd: SERVER_DIR,
         overrideConfig: {
            files: ["src/service/materialization/**/*.ts"],
            ignores: ["src/**/*.spec.ts"],
            rules: {
               "publisher/module-header": ["error", { allow: {} }],
               "publisher/no-history-comments": ["error", { allow: {} }],
            },
         },
      });
      const results = await eslint.lintFiles([
         "src/service/materialization/**/*.ts",
      ]);
      // A glob that matched nothing, or matched only specs, would pass both
      // checks vacuously.
      expect(
         results.some(
            (r) =>
               !isSpec(r.filePath) &&
               srcRel(r.filePath).startsWith("service/materialization/"),
         ),
      ).toBe(true);
      found = { headers: new Set(), history: new Map() };
      for (const r of results) {
         for (const m of r.messages) {
            if (m.ruleId === "publisher/module-header") {
               found.headers.add(srcRel(r.filePath));
            }
            if (m.ruleId === "publisher/no-history-comments") {
               found.history.set(
                  srcRel(r.filePath),
                  Number(/has (\d+)/.exec(m.message)![1]),
               );
            }
         }
      }
   }, 120_000);

   it("H1 and H2 fire on a planted module", async () => {
      // A module with no header and a ticket, a step label and a date in a
      // line comment, linted as if it lived in the layer.
      const [planted] = await eslint.lintText(
         'import * as fs from "fs";\n// Fixed in #1234, step 2c, 2026-01-02.\nexport const x = fs;\n',
         {
            filePath: path.join(MZ_DIR, "plan", "__planted__.ts"),
         },
      );
      const rules = planted.messages.map((m) => m.ruleId);
      expect(rules).toContain("publisher/module-header");
      const history = planted.messages.find(
         (m) => m.ruleId === "publisher/no-history-comments",
      );
      expect(history?.message).toContain("has 3");
   });

   it("H1: the modules with no header are exactly the seeded ones", () => {
      expectExact(
         "H1",
         Object.fromEntries([...found.headers].map((f) => [f, true])),
         lists.MATERIALIZATION_HEADER_ALLOWLIST,
      );
      for (const step of Object.values(lists.MATERIALIZATION_HEADER_ALLOWLIST))
         expect(step).toMatch(STEP);
   });

   it("H2: history in comments is exactly the seeded counts", () => {
      const allowed = lists.MATERIALIZATION_HISTORY_ALLOWLIST as Record<
         string,
         { count: number; step: string }
      >;
      expectExact("H2", Object.fromEntries(found.history), allowed);
      for (const [file, entry] of Object.entries(allowed)) {
         expect({ file, count: found.history.get(file) }).toEqual({
            file,
            count: entry.count,
         });
         expect(entry.step).toMatch(STEP);
      }
   });
});

// ---------------------------------------------------------------------------
// H3: each README's module table names exactly the modules in its directory.
// ---------------------------------------------------------------------------

/** The `.ts` files a README's tables name in their first column. */
function readmeModules(readme: string): Set<string> {
   const names = new Set<string>();
   for (const line of fs.readFileSync(readme, "utf8").split("\n")) {
      const cell = /^\|\s*`([\w.]+\.ts)`\s*\|/.exec(line);
      if (cell) names.add(cell[1]);
   }
   return names;
}

describe("H3: the READMEs list every module", () => {
   for (const dir of ["", ...LAYERS]) {
      it(`${dir || "materialization"}/README.md names exactly the modules beside it`, () => {
         const at = path.join(MZ_DIR, dir);
         const modules = fs
            .readdirSync(at)
            .filter((f) => f.endsWith(".ts") && !isSpec(f))
            .sort();
         expect([...readmeModules(path.join(at, "README.md"))].sort()).toEqual(
            modules,
         );
      });
   }
});

// ---------------------------------------------------------------------------
// H4: every export has an importer outside its own file.
// ---------------------------------------------------------------------------

function parse(file: string): ts.SourceFile {
   return ts.createSourceFile(
      file,
      fs.readFileSync(file, "utf8"),
      ts.ScriptTarget.Latest,
      true,
   );
}

function resolveModule(from: string, spec: string): string | undefined {
   if (!spec.startsWith(".")) return undefined;
   const p = path.resolve(path.dirname(from), spec).replace(/\.(js|mjs)$/, "");
   for (const c of [`${p}.ts`, `${p}.tsx`, path.join(p, "index.ts"), p]) {
      if (fs.existsSync(c) && fs.statSync(c).isFile()) return c;
   }
   return undefined;
}

const hasExport = (n: ts.Node) =>
   ts.canHaveModifiers(n) &&
   (ts.getModifiers(n) ?? []).some(
      (m) => m.kind === ts.SyntaxKind.ExportKeyword,
   );

/** The names a file exports, by its own declarations and export lists. */
function exportedNames(sf: ts.SourceFile): string[] {
   const out: string[] = [];
   for (const st of sf.statements) {
      if (ts.isExportDeclaration(st)) {
         if (st.exportClause && ts.isNamedExports(st.exportClause)) {
            for (const e of st.exportClause.elements) out.push(e.name.text);
         }
         continue;
      }
      if (!hasExport(st)) continue;
      if (ts.isVariableStatement(st)) {
         for (const d of st.declarationList.declarations) {
            if (ts.isIdentifier(d.name)) out.push(d.name.text);
         }
      } else if (
         (ts.isFunctionDeclaration(st) ||
            ts.isClassDeclaration(st) ||
            ts.isInterfaceDeclaration(st) ||
            ts.isTypeAliasDeclaration(st) ||
            ts.isEnumDeclaration(st)) &&
         st.name
      ) {
         out.push(st.name.text);
      }
   }
   return out;
}

/** file → the names other files import from it ("*" for all of them). */
function importedNames(files: string[]): Map<string, Set<string>> {
   const used = new Map<string, Set<string>>();
   const mark = (target: string | undefined, name: string) => {
      if (!target) return;
      let s = used.get(target);
      if (!s) used.set(target, (s = new Set()));
      s.add(name);
   };
   for (const file of files) {
      const sf = parse(file);
      const visit = (n: ts.Node): void => {
         if (
            (ts.isImportDeclaration(n) || ts.isExportDeclaration(n)) &&
            n.moduleSpecifier &&
            ts.isStringLiteral(n.moduleSpecifier)
         ) {
            const target = resolveModule(file, n.moduleSpecifier.text);
            if (ts.isImportDeclaration(n)) {
               const clause = n.importClause;
               if (clause?.name) mark(target, "default");
               const b = clause?.namedBindings;
               if (b && ts.isNamespaceImport(b)) mark(target, "*");
               if (b && ts.isNamedImports(b)) {
                  for (const e of b.elements)
                     mark(target, (e.propertyName ?? e.name).text);
               }
            } else if (n.exportClause && ts.isNamedExports(n.exportClause)) {
               for (const e of n.exportClause.elements)
                  mark(target, (e.propertyName ?? e.name).text);
            } else {
               mark(target, "*");
            }
         }
         if (
            ts.isCallExpression(n) &&
            n.expression.kind === ts.SyntaxKind.ImportKeyword &&
            n.arguments[0] &&
            ts.isStringLiteral(n.arguments[0])
         ) {
            mark(resolveModule(file, n.arguments[0].text), "*");
         }
         if (
            ts.isImportTypeNode(n) &&
            ts.isLiteralTypeNode(n.argument) &&
            ts.isStringLiteral(n.argument.literal)
         ) {
            const target = resolveModule(file, n.argument.literal.text);
            mark(
               target,
               n.qualifier && ts.isIdentifier(n.qualifier)
                  ? n.qualifier.text
                  : "*",
            );
         }
         ts.forEachChild(n, visit);
      };
      visit(sf);
   }
   return used;
}

describe("H4: every export has an importer", () => {
   it("names exactly the seeded exports no other file imports", () => {
      const everything = [
         ...walk(SRC_DIR),
         ...walk(path.join(SERVER_DIR, "tests")),
         ...walk(path.join(SERVER_DIR, "scripts")),
      ];
      const used = importedNames(everything);
      const actual: Record<string, true> = {};
      for (const file of mzFiles()) {
         // The module's door re-exports for callers outside it.
         if (srcRel(file) === "service/materialization/index.ts") continue;
         const names = used.get(file) ?? new Set<string>();
         if (names.has("*")) continue;
         for (const name of exportedNames(parse(file))) {
            if (!names.has(name)) actual[`${srcRel(file)}#${name}`] = true;
         }
      }
      const listed = lists.MATERIALIZATION_UNIMPORTED_EXPORTS as Record<
         string,
         string
      >;
      expectExact("H4", actual, listed);
      for (const step of Object.values(listed)) expect(step).toMatch(STEP);
   });
});

// ---------------------------------------------------------------------------
// The five ratchets.
// ---------------------------------------------------------------------------

/**
 * Non-comment code of a file, line by line, with string contents kept. The
 * comments are read off the parsed file, each token's leading and trailing
 * trivia, so a template literal or a regular expression cannot put a lone
 * scanner out of step and leave a comment in, or blank code.
 */
function codeLines(file: string): string[] {
   const text = fs.readFileSync(file, "utf8");
   const sf = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true);
   // Blank every comment, keeping line breaks so line numbers hold.
   const chars = text.split("");
   const blank = (ranges: ts.CommentRange[] | undefined) => {
      for (const r of ranges ?? []) {
         for (let i = r.pos; i < r.end; i++) {
            if (chars[i] !== "\n") chars[i] = " ";
         }
      }
   };
   const visit = (n: ts.Node): void => {
      blank(ts.getLeadingCommentRanges(text, n.getFullStart()));
      blank(ts.getTrailingCommentRanges(text, n.getEnd()));
      for (const c of n.getChildren(sf)) visit(c);
   };
   visit(sf);
   return chars.join("").split("\n");
}

/** How many times `pattern` matches in `lines`. */
function matchesIn(lines: string[], pattern: RegExp): number {
   let n = 0;
   for (const line of lines) {
      n += (line.match(new RegExp(pattern.source, "g")) ?? []).length;
   }
   return n;
}

/** file → how many code lines match `pattern`, over `files`. */
function countMatches(
   files: string[],
   pattern: RegExp,
): Record<string, number> {
   const out: Record<string, number> = {};
   for (const file of files) {
      const n = matchesIn(codeLines(file), pattern);
      if (n > 0) out[srcRel(file)] = n;
   }
   return out;
}

/** Malloy's registered dialects, the dialect table's rows, as alternatives. */
const REGISTERED = Object.keys(CAPABILITIES).join("|");

/** A JavaScript type test (`typeof dialect !== "string"`), not a dialect. */
const TYPE_TEST =
   /typeof\s+[\w.]+\s*[!=]==?\s*["'`](string|undefined|object|number|boolean|function|symbol|bigint)["'`]/g;

/**
 * A dialect-named value compared to a string literal that is not a registered
 * dialect (one that is, is counted as a literal), either way round.
 */
const DIALECT_COMPARISON = new RegExp(
   `\\b\\w*[dD]ialect\\w*\\s*[!=]==?\\s*["'\`](?!(?:${REGISTERED})["'\`])[a-z_]+["'\`]` +
      `|["'\`](?!(?:${REGISTERED})["'\`])[a-z_]+["'\`]\\s*[!=]==?\\s*[\\w.]*[dD]ialect\\w*\\b`,
);

/**
 * A string literal exactly a registered dialect, quoted any way, whatever
 * surrounds it: a comparison on any name, a `case` label, an element of an
 * array or a set on any line, a type, an argument.
 */
const DIALECT_LITERAL = new RegExp(`["'\`](${REGISTERED})["'\`]`);

/** A registered dialect as a bare object key (`{ postgres: 50 }`). */
const DIALECT_KEY = new RegExp(`(?<![\\w$."'\`])(${REGISTERED})\\s*:(?!:)`);

/** A registered dialect as a `case` label. */
const DIALECT_CASE = new RegExp(`\\bcase\\s+["'\`](${REGISTERED})["'\`]`, "g");

/**
 * How many dialect literals `lines` hold, once type tests are taken out: every
 * string literal that is exactly a registered dialect, every bare object key
 * that is one, and every comparison of a dialect-named value to another
 * literal. `caseLabels` frees `case` labels naming a registered dialect.
 */
function dialectLiterals(lines: string[], caseLabels = false): number {
   const code = lines.map((l) => {
      const line = l.replace(TYPE_TEST, "");
      return caseLabels ? line.replace(DIALECT_CASE, "") : line;
   });
   return (
      matchesIn(code, DIALECT_COMPARISON) +
      matchesIn(code, DIALECT_LITERAL) +
      matchesIn(code, DIALECT_KEY)
   );
}

function expectCounts(
   name: string,
   actual: Record<string, number>,
   listed: Record<string, { count: number; step: string }>,
): void {
   expectExact(name, actual, listed);
   for (const [file, entry] of Object.entries(listed)) {
      expect({ list: name, file, count: actual[file] }).toEqual({
         list: name,
         file,
         count: entry.count,
      });
      expect(entry.step).toMatch(STEP);
   }
}

describe("the ratchets", () => {
   it("annotation payloads are read only by the declaration reader", () => {
      const files = mzFiles().filter(
         (f) => srcRel(f) !== "service/materialization/plan/declaration.ts",
      );
      expectCounts(
         "annotation reads",
         countMatches(
            files,
            /\b(parseRouteTag|routeHasKey|parseMotly|motlyTag|tagText|tagNumeric)\(/,
         ),
         lists.RATCHET_ANNOTATION_READS,
      );
   });

   it("Malloy runtimes are made only by the Compiler port", () => {
      const port = new Set([
         "service/materialization/text/generated_files.ts",
         "service/materialization/text/package_files.ts",
      ]);
      const files = mzFiles().filter((f) => !port.has(srcRel(f)));
      expect(
         countMatches(files, /\bcreateModelRuntime\(|\bnew Runtime\(/),
      ).toEqual({});
   });

   it("the compiler's IR is read only under governance/ir", () => {
      expectCounts(
         "IR reads",
         countMatches(mzFiles(), /\._(modelDef|query|sourceDef|explore)\b/),
         lists.RATCHET_IR_READS,
      );
   });

   it("serve/ and domain/ read no clock", () => {
      const files = mzFiles().filter((f) =>
         /^service\/materialization\/(serve|domain)\//.test(srcRel(f)),
      );
      expectCounts(
         "clock reads",
         countMatches(
            files,
            /\bDate\.now\(\)|\bnew Date\(\)|\bperformance\.now\(\)/,
         ),
         lists.RATCHET_CLOCK_READS,
      );
   });

   it("a dialect literal is counted wherever it is", () => {
      // run/warehouse_sql.ts, the SQL half of the dialect table, switches
      // over a registered dialect, so its case labels are free; its
      // comparisons are counted like any other file's.
      const sqlHalf = "service/materialization/run/warehouse_sql.ts";
      const actual: Record<string, number> = {};
      for (const file of mzFiles()) {
         const n = dialectLiterals(codeLines(file), srcRel(file) === sqlHalf);
         if (n > 0) actual[srcRel(file)] = n;
      }
      expectCounts("dialect literals", actual, lists.RATCHET_DIALECT_LITERALS);
   });

   it("the dialect-literal count reads a type test as no dialect", () => {
      // A type test goes; a dialect after `typeof` does not.
      expect(dialectLiterals([`typeof dialect === "postgres"`])).toBe(1);
      expect(dialectLiterals([`typeof dialect !== "string"`])).toBe(0);
      expect(dialectLiterals([`typeof d.dialect !== "string"`])).toBe(0);
      expect(dialectLiterals([`typeof args.dialect === "undefined"`])).toBe(0);
      // A comparison either way round, a case label and a bracketed literal.
      expect(dialectLiterals([`if (dialect === "mysql") {`])).toBe(1);
      expect(dialectLiterals([`"mysql" !== args.dialectName`])).toBe(1);
      expect(dialectLiterals([`case "snowflake":`])).toBe(1);
      expect(dialectLiterals([`case "snowflake":`], true)).toBe(0);
      expect(dialectLiterals([`new Set(["sqlserver"])`])).toBe(1);
      // A literal counts wherever it is: a multi-line set, a later element,
      // a comparison on a name that does not say dialect.
      expect(dialectLiterals([`new Set([`, `   "postgres",`, `])`])).toBe(1);
      expect(dialectLiterals([`["bigquery", "postgres"]`])).toBe(1);
      expect(dialectLiterals([`d === 'postgres'`, "x(`trino`)"])).toBe(2);
      // A bare object key counts; a property read or a longer word does not.
      expect(dialectLiterals([`{ postgres: 50, mysql: 1 }`])).toBe(2);
      expect(dialectLiterals([`a.postgres: 1`, `"postgres_x"`])).toBe(0);
      // Only a registered dialect counts as a literal.
      expect(dialectLiterals([`case "bigquery":`])).toBe(0);
      expect(dialectLiterals([`if (dialect === "bigquery") {`])).toBe(1);
   });

   it("no function is over 200 lines", () => {
      const scope = [
         ...mzFiles(),
         path.join(SRC_DIR, "service/model.ts"),
         path.join(SRC_DIR, "service/query_serving.ts"),
      ];
      const actual: Record<string, number> = {};
      for (const file of scope) {
         const sf = parse(file);
         const lineOf = (pos: number) =>
            sf.getLineAndCharacterOfPosition(pos).line;
         const visit = (n: ts.Node, owner?: string): void => {
            let name: string | undefined;
            if (
               (ts.isFunctionDeclaration(n) ||
                  ts.isMethodDeclaration(n) ||
                  ts.isGetAccessorDeclaration(n)) &&
               n.name
            ) {
               name = n.name.getText(sf);
            } else if (ts.isConstructorDeclaration(n)) {
               name = "constructor";
            } else if (
               (ts.isArrowFunction(n) || ts.isFunctionExpression(n)) &&
               (ts.isVariableDeclaration(n.parent) ||
                  ts.isPropertyAssignment(n.parent) ||
                  ts.isPropertyDeclaration(n.parent)) &&
               n.parent.name
            ) {
               name = n.parent.name.getText(sf);
            }
            if (name && (n as ts.FunctionLikeDeclaration).body) {
               const lines = lineOf(n.getEnd()) - lineOf(n.getStart(sf)) + 1;
               const key = `${srcRel(file)}#${owner ? `${owner}.` : ""}${name}`;
               if (lines > 200) actual[key] = lines;
            }
            const nextOwner =
               ts.isClassDeclaration(n) && n.name ? n.name.text : owner;
            ts.forEachChild(n, (c) => visit(c, nextOwner));
         };
         visit(sf);
      }
      const listed = lists.RATCHET_LONG_FUNCTIONS as Record<string, string>;
      expectExact("long functions", actual, listed);
      for (const step of Object.values(listed))
         expect(stepOf(step)).toMatch(STEP);
   });
});
