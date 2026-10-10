// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The served-notebook lint through the real server: the package's warnings, and
 * /compile at file, package and append scope.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const ENV_NAME = "notebook-lint-env";
const PKG = "notebooks-lint";
const LINTY = "notebooks/linty.malloy";
const BROKEN_SOURCE =
   "## artifact { kind=notebook }\ngiven: G :: string is 'a'\n##|\"\nprose\n";
const CLEAN = "notebooks/clean.malloy";
const WRONG_KIND = "dashboards/wrong_kind.malloy";

const fixtureDir = path.resolve(__dirname, `../../fixtures/${PKG}`);

interface Problem {
   severity: string;
   code?: string;
   message: string;
   model?: string;
   at?: { range: { start: { line: number } } };
}

describe("notebook lint through the real server (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   const pkgUrl = (sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}${sub}`;
   const compile = async (
      model: string,
      scope: "append" | "file" | "package",
      source?: string,
   ) => {
      const res = await fetch(pkgUrl(`/models/${model}/compile`), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            scope,
            source:
               source ?? fs.readFileSync(path.join(fixtureDir, model), "utf8"),
         }),
      });
      expect(res.status).toBe(200);
      return (await res.json()) as { status: string; problems: Problem[] };
   };
   const lintOf = (problems: Problem[], model: string) =>
      problems.filter(
         (p) => p.model === model && p.severity === "warn" && p.at,
      );

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const res = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PKG, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!res.ok) {
         throw new Error(
            `Failed to create test environment (${res.status}): ${await res.text()}`,
         );
      }
      const loaded = await fetch(pkgUrl(""));
      if (!loaded.ok) throw new Error(`${PKG} did not load (${loaded.status})`);
   });

   afterAll(async () => {
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort
         }
      }
      await env?.stop();
      env = null;
   });

   describe("the package's warnings", () => {
      let warnings: { model?: string; message: string; severity?: string }[];
      beforeAll(async () => {
         const res = await fetch(pkgUrl(""));
         warnings =
            ((await res.json()) as { warnings?: typeof warnings }).warnings ??
            [];
      });

      it("carries each finding as a warn with its line", () => {
         const of = (model: string) =>
            warnings
               .filter((w) => w.model === model && /^Line \d+:/.test(w.message))
               .map((w) => [
                  w.severity,
                  Number(/^Line (\d+):/.exec(w.message)?.[1]),
               ]);
         expect(of(LINTY)).toEqual([
            ["warn", 1],
            ["warn", 4],
            ["warn", 8],
         ]);
         expect(of(WRONG_KIND)).toEqual([["warn", 1]]);
      });

      it("says nothing about a clean notebook, and no notebook or dashboard draws an unknown-render-tag warning", () => {
         expect(warnings.filter((w) => w.model === CLEAN)).toEqual([]);
         // The malformed colspan in linty proves the validator visits these files at all.
         expect(
            warnings.filter((w) => /Invalid # colspan/.test(w.message)),
         ).toHaveLength(1);
         expect(
            warnings.filter((w) => /unknown render tag/i.test(w.message)),
         ).toEqual([]);
      });

      it("carries a header statement, a column conflict and a bad text block as errors, and the alias and orphan block as warns", () => {
         const severityOf = (model: string) =>
            warnings
               .filter((w) => w.model === model && /^Line \d+:/.test(w.message))
               .map((w) => [w.severity, w.message.split(":")[0]]);
         expect(severityOf("notebooks/header_statement.malloy")).toEqual([
            ["error", "Line 1"],
         ]);
         expect(severityOf("dashboards/columns_conflict.malloy")).toEqual([
            ["error", "Line 1"],
         ]);
         expect(severityOf("dashboards/columns_alias.malloy")).toEqual([
            ["warn", "Line 1"],
         ]);
         expect(severityOf("dashboards/text_block_orphan.malloy")).toEqual([
            ["warn", "Line 4"],
         ]);
         expect(severityOf("dashboards/description_below.malloy")).toEqual([
            ["warn", "Line 2"],
         ]);
      });

      it("reports a dashboard whose artifact tag does not parse once, from the dashboard lint", () => {
         const of = warnings.filter(
            (w) => w.model === "dashboards/artifact_unparsed.malloy",
         );
         expect(of).toHaveLength(1);
         expect(of[0].message).not.toMatch(/^Line \d+:/);
      });

      it("serves the alias's width and, on a conflict, the canonical one", async () => {
         const width = async (name: string) =>
            (
               (await (await fetch(pkgUrl(`/dashboards/${name}`))).json()) as {
                  dashboardColumns?: number;
               }
            ).dashboardColumns;
         expect(await width("columns_alias")).toBe(8);
         expect(await width("columns_conflict")).toBe(12);
      });

      it("does not read kind as an unknown dashboard property", () => {
         expect(
            warnings.filter((w) =>
               /`kind` in the artifact tag/.test(w.message),
            ),
         ).toEqual([]);
      });
   });

   describe("/compile", () => {
      it("returns the findings as warn problems with a success status at file scope", async () => {
         const { status, problems } = await compile(LINTY, "file");
         expect(status).toBe("success");
         expect(
            lintOf(problems, LINTY).map((p) => [
               p.code,
               p.at?.range.start.line,
            ]),
         ).toEqual([
            ["notebook-kind-missing", 0],
            ["notebook-markdown-opener", 3],
            ["notebook-comment-not-shown", 7],
         ]);
      });

      it("returns them at package scope too", async () => {
         const { status, problems } = await compile(LINTY, "package");
         // The package holds files whose findings are errors, and an error fails the compile.
         expect(status).toBe("error");
         expect(
            problems
               .filter((p) => p.severity === "error")
               .map((p) => [p.model, p.code]),
         ).toEqual([
            ["dashboards/columns_conflict.malloy", "notebook-columns-conflict"],
            [
               "notebooks/header_statement.malloy",
               "notebook-statement-above-artifact",
            ],
            ["notebooks/linty.malloy", "render-tag"],
            // Once, from the dashboard lint, as a reload reports it.
            ["dashboards/artifact_unparsed.malloy", "dashboard-lint"],
         ]);
         expect(lintOf(problems, LINTY).map((p) => p.code)).toEqual([
            "notebook-kind-missing",
            "notebook-markdown-opener",
            "notebook-comment-not-shown",
         ]);
         expect(lintOf(problems, WRONG_KIND).map((p) => p.code)).toEqual([
            "notebook-other-folder",
         ]);
      });

      it("fails the compile of a notebook with a statement above its artifact tag, at that line", async () => {
         const model = "notebooks/header_statement.malloy";
         const { status, problems } = await compile(model, "file");
         expect(status).toBe("error");
         expect(
            problems
               .filter((p) => p.model === model && p.severity === "error")
               .map((p) => [p.code, p.at?.range.start.line]),
         ).toEqual([["notebook-statement-above-artifact", 0]]);
      });

      describe("a notebook written as tiles", () => {
         const SOURCE_LINE = `source: a is duckdb.sql("select 1 as x") extend { view: v is { select: x } }\n`;
         const BLOCK = "##|(markdown) intro\nHello\n|##\n";
         const findings = async (model: string, source: string) => {
            const { status, problems } = await compile(model, "file", source);
            expect(status).toBe("success");
            return lintOf(problems, model).map((p) => [
               p.code,
               p.at?.range.start.line,
            ]);
         };

         it("warns of a run: that no tile shows", async () => {
            expect(
               await findings(
                  "notebooks/tiles_run.malloy",
                  `## artifact { kind=notebook tiles=[intro { kind=text }, "a -> v"] }\n${SOURCE_LINE}${BLOCK}\nrun: a -> v\n`,
               ),
            ).toEqual([["notebook-layout-run", 6]]);
         });

         it("warns that a grid width other than one is ignored", async () => {
            expect(
               await findings(
                  "notebooks/tiles_columns.malloy",
                  `## artifact { kind=notebook tiles=[intro { kind=text }, "a -> v"] } dashboard { columns=12 }\n${SOURCE_LINE}${BLOCK}`,
               ),
            ).toEqual([["notebook-columns-ignored", 0]]);
         });

         it("warns that colspan and break on a tile entry are ignored", async () => {
            expect(
               await findings(
                  "notebooks/tiles_layout.malloy",
                  `## artifact { kind=notebook tiles=[intro { kind=text colspan=2 break }, "a -> v"] }\n${SOURCE_LINE}${BLOCK}`,
               ),
            ).toEqual([["notebook-tile-layout-ignored", 0]]);
         });

         it("says a dashboard served from notebooks/ works, and where the kinds are created", async () => {
            expect(
               await findings(
                  "notebooks/tiles_dashboard.malloy",
                  `## artifact { kind=dashboard tiles=["a -> v"] }\n${SOURCE_LINE}`,
               ),
            ).toEqual([["notebook-other-folder", 0]]);
         });
      });

      it("returns nothing for a clean notebook", async () => {
         const { status, problems } = await compile(CLEAN, "file");
         expect(status).toBe("success");
         expect(lintOf(problems, CLEAN)).toEqual([]);
      });

      it("does not return them at append scope, where positions are in the concatenated file", async () => {
         const file = await compile(LINTY, "file");
         expect(
            file.problems.filter((p) => /^notebook-/.test(p.code ?? "")),
         ).not.toEqual([]);
         const { problems } = await compile(
            LINTY,
            "append",
            "run: a -> { select: x }",
         );
         expect(
            problems.filter((p) => /^notebook-/.test(p.code ?? "")),
         ).toEqual([]);
      });

      it("puts the fix-its beside the compile error of a broken file", async () => {
         for (const scope of ["file", "package"] as const) {
            const { status, problems } = await compile(
               CLEAN,
               scope,
               BROKEN_SOURCE,
            );
            expect(status).toBe("error");
            expect(problems).toContainEqual(
               expect.objectContaining({ severity: "error" }),
            );
            expect(
               problems
                  .filter(
                     (p) => p.model === CLEAN && p.severity === "warn" && p.at,
                  )
                  .map((p) => [p.code, p.at?.range.start.line]),
            ).toEqual([
               ["notebook-givens-not-enabled", 1],
               ["notebook-unterminated-block", 2],
            ]);
         }
      });
   });
});
