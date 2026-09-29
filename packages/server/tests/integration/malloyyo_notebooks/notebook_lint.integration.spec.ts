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
         expect(
            warnings.filter((w) => /unknown render tag/i.test(w.message)),
         ).toEqual([]);
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
         expect(status).toBe("success");
         expect(lintOf(problems, LINTY).map((p) => p.code)).toEqual([
            "notebook-kind-missing",
            "notebook-markdown-opener",
            "notebook-comment-not-shown",
         ]);
         expect(lintOf(problems, WRONG_KIND).map((p) => p.code)).toEqual([
            "notebook-kind-under-dashboards",
         ]);
      });

      it("returns nothing for a clean notebook", async () => {
         const { status, problems } = await compile(CLEAN, "file");
         expect(status).toBe("success");
         expect(lintOf(problems, CLEAN)).toEqual([]);
      });

      it("does not return them at append scope, where positions are in the concatenated file", async () => {
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
