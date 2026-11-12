// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * What-if over v1 annotations (materialization v2, T3), end to end: what the
 * route predicts a proposal would serve must be what a real build of that
 * proposal serves. One package holds the model without `#@ persist` (the
 * what-if target); a second holds the edit applied, is built for real, and its
 * served query's path report is compared with the prediction.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";
import { pollFor, TERMINAL_STATUSES, waitUntilOk } from "./polling";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ENV = "what-if-env";
const BASE = "what-if-base";
const APPLIED = "what-if-applied";
const ROLLUP_APPLIED = "what-if-rollup-applied";
const MODEL = "persist_test.malloy";

const UNANNOTATED = `##! experimental.persistence

source: raw_orders is duckdb.table('data/orders.csv')

source: order_summary is raw_orders -> {
  group_by: category
  aggregate:
    total_orders is count()
    total_revenue is amount.sum()
}
`;
const PROPOSED = UNANNOTATED.replace(
   "source: order_summary",
   '#@ persist name="order_summary"\nsource: order_summary',
);

/** The rollup proposal: a measure on the base, rolled up by category. */
const ROLLUP_PROPOSED = UNANNOTATED.replace(
   "source: raw_orders is duckdb.table('data/orders.csv')",
   `source: raw_orders is duckdb.table('data/orders.csv') extend {
  #@ preaggregate grain="category"
  measure: revenue is amount.sum()
}`,
);
const ROLLUP_QUERY =
   "run: raw_orders -> { group_by: category; aggregate: revenue }";

const SUMMARY_QUERY = "run: order_summary -> { aggregate: c is count() }";

/**
 * Two persisted siblings, and an `index.malloy` that publishes only one: the
 * other is a building block the package does not publish.
 */
const CURATED_MODEL = `${PROPOSED}
#@ persist name="hidden_summary"
source: hidden_summary is raw_orders -> {
  group_by: category
  aggregate: hidden_total is amount.sum()
}
`;
const CURATED_INDEX = `import "${MODEL}"\nexport { order_summary }\n`;
const RAW_QUERY = "run: raw_orders -> { aggregate: c is count() }";

interface Candidate {
   tableId: string;
   kind: string;
   outcome: string;
   reason?: string;
}
interface Path {
   servedBy: { kind: string; tableId?: string };
   considered: Candidate[];
}
interface WhatIf {
   plan: {
      added: { name: string }[];
      removed: unknown[];
      changed: unknown[];
      unchanged: unknown[];
   };
   refused: unknown[];
   answers: { query?: string; path?: Path; error?: string }[];
}

/** A copy of the persist-test fixture with `model` as its model file. */
function packageDir(root: string, name: string, model: string): string {
   const fixture = path.resolve(__dirname, "../../fixtures/persist-test");
   const dir = path.join(root, name);
   fs.cpSync(fixture, dir, { recursive: true });
   fs.writeFileSync(path.join(dir, MODEL), model);
   return dir;
}

/** What a comparison can rely on: the content address names the table. */
function routing(p: Path | undefined) {
   return {
      served: p?.servedBy.kind,
      considered: (p?.considered ?? []).map((c) => [
         c.kind,
         c.outcome,
         c.reason ?? null,
      ]),
   };
}

describe("what-if over v1 annotations (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let root: string;

   const api = (pkg: string, p: string) =>
      `${baseUrl}/api/v0/environments/${ENV}/packages/${pkg}${p}`;

   async function whatIf(body: unknown, query = ""): Promise<Response> {
      return fetch(api(BASE, `/build-plan/what-if${query}`), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify(body),
      });
   }

   async function servedPath(pkg: string, query: string): Promise<Path> {
      const res = await fetch(api(pkg, `/models/${MODEL}/query`), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ query }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { path?: Path };
      expect(body.path).toBeDefined();
      return body.path as Path;
   }

   /** Build `pkg` for real and wait until `query` serves from a table. */
   async function buildAndServe(pkg: string, query: string): Promise<Path> {
      const created = await fetch(api(pkg, "/materializations"), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({}),
      });
      expect(created.status).toBe(201);
      const { id } = (await created.json()) as { id: string };
      await pollFor(
         async () =>
            (await (
               await fetch(api(pkg, `/materializations/${id}`))
            ).json()) as { status: string },
         (run) => TERMINAL_STATUSES.includes(run.status),
         "the build never finished",
         90_000,
      );
      return pollFor(
         () => servedPath(pkg, query),
         (p) => p.servedBy.kind !== "live",
         `${pkg} never served from its table`,
         30_000,
      );
   }

   beforeAll(async () => {
      root = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-what-if-"));
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const res = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV,
            packages: [
               { name: BASE, location: packageDir(root, BASE, UNANNOTATED) },
               { name: APPLIED, location: packageDir(root, APPLIED, PROPOSED) },
               {
                  name: ROLLUP_APPLIED,
                  location: packageDir(root, ROLLUP_APPLIED, ROLLUP_PROPOSED),
               },
            ],
            connections: [],
         }),
      });
      if (!res.ok) {
         throw new Error(
            `Failed to create the environment (${res.status}): ${await res.text()}`,
         );
      }
      await waitUntilOk(api(BASE, ""), "The what-if package");
      await waitUntilOk(api(APPLIED, ""), "The applied package");
      await waitUntilOk(api(ROLLUP_APPLIED, ""), "The applied rollup package");
   });

   afterAll(async () => {
      if (baseUrl) {
         await fetch(`${baseUrl}/api/v0/environments/${ENV}`, {
            method: "DELETE",
         }).catch(() => {});
      }
      await env?.stop();
      env = null;
      fs.rmSync(root, { recursive: true, force: true });
   });

   it("answers live for a package with nothing planned", async () => {
      const res = await whatIf({
         queries: [{ modelPath: MODEL, query: SUMMARY_QUERY }],
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as WhatIf;
      expect(body.plan.added).toEqual([]);
      expect(body.answers[0]?.path?.servedBy.kind).toBe("live");
   });

   it(
      "predicts what a real build of the proposal serves",
      async () => {
         const res = await whatIf({
            files: [{ path: MODEL, content: PROPOSED }],
            queries: [
               { modelPath: MODEL, query: SUMMARY_QUERY },
               { modelPath: MODEL, query: RAW_QUERY },
            ],
         });
         expect(res.status).toBe(200);
         const predicted = (await res.json()) as WhatIf;
         expect(predicted.plan.added.map((s) => s.name)).toEqual([
            "order_summary",
         ]);
         expect(predicted.refused).toEqual([]);
         const [summary, raw] = predicted.answers;
         expect(summary?.error).toBeUndefined();
         expect(summary?.path?.servedBy.kind).toBe("materialized");
         expect(summary?.path?.servedBy.tableId).toBeTruthy();
         expect(raw?.path?.servedBy.kind).toBe("live");

         // Nothing was built or bound on the served package.
         expect((await servedPath(BASE, SUMMARY_QUERY)).servedBy.kind).toBe(
            "live",
         );

         // Build the applied package for real, then read its served path.
         const actual = await buildAndServe(APPLIED, SUMMARY_QUERY);
         expect(routing(actual)).toEqual(routing(summary?.path));
         expect(routing(await servedPath(APPLIED, RAW_QUERY))).toEqual(
            routing(raw?.path),
         );
      },
      { timeout: 150_000 },
   );

   it(
      "predicts a proposed rollup answers its grain, and not a finer one, as a real build does",
      async () => {
         const res = await whatIf({
            files: [{ path: MODEL, content: ROLLUP_PROPOSED }],
            queries: [
               { modelPath: MODEL, query: ROLLUP_QUERY },
               {
                  modelPath: MODEL,
                  query: "run: raw_orders -> { group_by: amount; aggregate: revenue }",
               },
            ],
         });
         expect(res.status).toBe(200);
         const body = (await res.json()) as WhatIf;
         expect(body.refused).toEqual([]);
         expect(body.plan.added.length).toBeGreaterThan(0);
         const [atGrain, finer] = body.answers;
         expect(atGrain?.error).toBeUndefined();
         expect(atGrain?.path?.servedBy.kind).toBe("rollup");
         expect(finer?.path?.servedBy.kind).toBe("live");

         const actual = await buildAndServe(ROLLUP_APPLIED, ROLLUP_QUERY);
         expect(routing(actual)).toEqual(routing(atGrain?.path));
         expect(
            routing(
               await servedPath(
                  ROLLUP_APPLIED,
                  "run: raw_orders -> { group_by: amount; aggregate: revenue }",
               ),
            ),
         ).toEqual(routing(finer?.path));
      },
      { timeout: 150_000 },
   );

   it("refuses a proposal that does not compile, as a publish would", async () => {
      const res = await whatIf({
         files: [
            {
               path: MODEL,
               content: `${UNANNOTATED}\nsource: broken is nope\n`,
            },
         ],
         queries: [{ modelPath: MODEL, query: SUMMARY_QUERY }],
      });
      // The load's own model-compilation refusal, as a dry-run publish answers.
      expect(res.status).toBe(424);
      expect(await res.text()).toContain("nope");
   });

   it("answers a query that does not compile with its error, not a refusal", async () => {
      const res = await whatIf({
         files: [{ path: MODEL, content: PROPOSED }],
         queries: [
            {
               modelPath: MODEL,
               query: "run: no_such_source -> { aggregate: c is count() }",
            },
         ],
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as WhatIf;
      expect(body.answers[0]?.error).toBeTruthy();
      expect(body.answers[0]?.path).toBeUndefined();
   });

   it("names no hidden source, and refuses a query against one, as a query does", async () => {
      const body = {
         files: [
            { path: MODEL, content: CURATED_MODEL },
            { path: "index.malloy", content: CURATED_INDEX },
         ],
         queries: [
            { modelPath: "index.malloy", query: SUMMARY_QUERY },
            {
               modelPath: "index.malloy",
               query: "run: hidden_summary -> { aggregate: c is count() }",
            },
         ],
      };
      const res = await whatIf(body);
      expect(res.status).toBe(200);
      const hidden = (await res.json()) as WhatIf;
      // The plan keeps the build plan's semantics: every planned source.
      expect(hidden.plan.added.map((s) => s.name).sort()).toEqual([
         "hidden_summary",
         "order_summary",
      ]);
      const [published, offSurface] = hidden.answers;
      expect(published?.path).toBeDefined();
      expect(JSON.stringify(published?.path)).not.toContain("hidden_summary");
      expect(offSurface?.path).toBeUndefined();
      expect(offSurface?.error).toBeTruthy();

      // With hidden sources included, the off-surface query is answered.
      const opened = await whatIf(body, "?includeHiddenFilesAndSources=true");
      expect(opened.status).toBe(200);
      const included = (await opened.json()) as WhatIf;
      expect(included.answers[1]?.error).toBeUndefined();
      expect(included.answers[1]?.path).toBeDefined();
   });

   it("refuses an unsafe proposal path before anything loads", async () => {
      const res = await whatIf({
         files: [{ path: "../escape.malloy", content: PROPOSED }],
         queries: [{ modelPath: MODEL, query: SUMMARY_QUERY }],
      });
      expect(res.status).toBe(400);
   });
});
