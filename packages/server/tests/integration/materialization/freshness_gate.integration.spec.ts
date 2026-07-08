/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fsp from "fs/promises";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "freshness-gate-project";
const PACKAGE_NAME = "persist-test";
const MODEL_PATH = "persist_test.malloy";
const API = `/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;

/**
 * Covers the per-query freshness gate (persistence.md §9.3, §14 Phase B): a
 * query on a `#@ persist` source uses its materialized table only while the
 * table is within its declared freshness window; otherwise it falls back per the
 * entry's declared `fallback`.
 *
 *  - fresh (or un-gated)      → routes to the materialized table
 *  - stale + fallback live    → serves live SQL (entry dropped)
 *  - stale + fallback stale_ok→ serves the stale table
 *  - a fresh entry that crosses its window → serves live on the NEXT query,
 *    with no rebind (proves per-query re-evaluation, not bind-time filtering)
 *
 * The manifest is bound once physically (auto-run builds the table), then a
 * manifest URI keyed by the source's real sourceEntityId routes served queries.
 * `executedSql()` is the routing evidence: it scans `order_summary` when routed
 * to the table, and `data/orders.csv` when serving live.
 */
describe("Per-query freshness gate (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let tmpDir: string;
   let sourceEntityId: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "freshness-gate-"));

      const fixtureDir = path.resolve(__dirname, "../../fixtures/persist-test");
      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PROJECT_NAME,
            packages: [{ name: PACKAGE_NAME, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         throw new Error(
            `Failed to create test project (${createRes.status}): ${await createRes.text()}`,
         );
      }

      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(`${baseUrl}${API}`);
         if (res.ok) break;
         await new Promise((r) => setTimeout(r, 500));
      }

      // Physically build the persist source (auto-run self-assigns
      // physicalTableName = "order_summary"), then revert to live so the
      // manifest-URI bind is the only thing that can route the served query.
      await buildTableThenRevertToLive();
      sourceEntityId = await orderSummarySourceEntityId();
   }, 120_000);

   afterAll(async () => {
      if (baseUrl) {
         await fetch(`${baseUrl}/api/v0/environments/${PROJECT_NAME}`, {
            method: "DELETE",
         }).catch(() => {});
      }
      await env?.stop();
      await fsp.rm(tmpDir, { recursive: true, force: true }).catch(() => {});
      env = null;
   });

   function url(p: string): string {
      return `${baseUrl}${API}${p}`;
   }

   const ROUTING_QUERY = "run: order_summary -> { aggregate: c is count() }";

   /** The SQL the served query actually compiled to (routing evidence). */
   async function executedSql(): Promise<string> {
      const res = await fetch(`${baseUrl}${API}/models/${MODEL_PATH}/query`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ query: ROUTING_QUERY }),
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { result: string };
      return (JSON.parse(body.result) as { sql: string }).sql;
   }

   async function orderSummarySourceEntityId(): Promise<string> {
      const res = await fetch(url(""));
      expect(res.status).toBe(200);
      const pkg = (await res.json()) as {
         buildPlan?: { sources?: Record<string, { sourceEntityId?: string }> };
      };
      const sources = pkg.buildPlan?.sources ?? {};
      const id = Object.values(sources)[0]?.sourceEntityId;
      expect(typeof id).toBe("string");
      return id as string;
   }

   async function pollUntilTerminal(
      id: string,
      timeoutMs = 90_000,
   ): Promise<Record<string, unknown>> {
      const terminal = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
         const res = await fetch(url(`/materializations/${id}`));
         expect(res.status).toBe(200);
         const data = (await res.json()) as Record<string, unknown>;
         if (terminal.includes(data.status as string)) return data;
         await new Promise((r) => setTimeout(r, 250));
      }
      throw new Error(`Materialization ${id} did not reach a terminal state`);
   }

   async function buildTableThenRevertToLive(): Promise<void> {
      const createRes = await fetch(url("/materializations"), {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({}),
      });
      expect(createRes.status).toBe(201);
      const { id } = (await createRes.json()) as { id: string };
      const built = await pollUntilTerminal(id);
      expect(built.status).toBe("MANIFEST_FILE_READY");
      await fetch(url("?reload=true"));
      await fetch(url(`/materializations/${id}`), { method: "DELETE" });
   }

   /** Write a CP-shaped manifest carrying the freshness fields. */
   async function writeFreshnessManifest(opts: {
      dataAsOf: string;
      freshnessWindowSeconds: number;
      freshnessFallback: "live" | "stale_ok" | "fail";
   }): Promise<string> {
      const file = path.join(
         tmpDir,
         `freshness-${Date.now()}-${Math.random()}.json`,
      );
      await fsp.writeFile(
         file,
         JSON.stringify({
            builtAt: new Date().toISOString(),
            strict: false,
            entries: {
               [sourceEntityId]: {
                  sourceEntityId,
                  sourceName: "order_summary",
                  physicalTableName: "order_summary",
                  connectionName: "duckdb",
                  dataAsOf: opts.dataAsOf,
                  freshnessWindowSeconds: opts.freshnessWindowSeconds,
                  freshnessFallback: opts.freshnessFallback,
               },
            },
         }),
         "utf8",
      );
      return file;
   }

   async function bind(manifestFile: string): Promise<void> {
      const patchRes = await fetch(url(""), {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            manifestLocation: manifestFile,
         }),
      });
      expect(patchRes.status).toBe(200);
      const patched = (await patchRes.json()) as Record<string, unknown>;
      // The bind is recorded regardless of per-query freshness — freshness is
      // evaluated per query, not at bind.
      expect(patched.manifestBindingStatus).toBe("bound");
      expect(patched.manifestEntryCount).toBe(1);
   }

   async function revertToLive(): Promise<void> {
      await fetch(url(""), {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ name: PACKAGE_NAME, manifestLocation: null }),
      });
      await fetch(url("?reload=true"));
   }

   it(
      "routes a fresh entry to the materialized table",
      async () => {
         const manifestFile = await writeFreshnessManifest({
            dataAsOf: new Date().toISOString(),
            freshnessWindowSeconds: 86_400, // 1 day — comfortably fresh
            freshnessFallback: "live",
         });
         await bind(manifestFile);

         const routed = await executedSql();
         expect(routed).not.toContain("data/orders.csv");
         expect(routed).toContain("order_summary");

         await revertToLive();
      },
      { timeout: 120_000 },
   );

   it(
      "serves live for a stale entry with fallback live",
      async () => {
         const manifestFile = await writeFreshnessManifest({
            // 2h old against a 1h window ⇒ stale.
            dataAsOf: new Date(Date.now() - 7200_000).toISOString(),
            freshnessWindowSeconds: 3600,
            freshnessFallback: "live",
         });
         await bind(manifestFile);

         const served = await executedSql();
         expect(served).toContain("data/orders.csv");

         await revertToLive();
      },
      { timeout: 120_000 },
   );

   it(
      "serves the stale table for a stale entry with fallback stale_ok",
      async () => {
         const manifestFile = await writeFreshnessManifest({
            dataAsOf: new Date(Date.now() - 7200_000).toISOString(),
            freshnessWindowSeconds: 3600,
            freshnessFallback: "stale_ok",
         });
         await bind(manifestFile);

         const served = await executedSql();
         expect(served).not.toContain("data/orders.csv");
         expect(served).toContain("order_summary");

         await revertToLive();
      },
      { timeout: 120_000 },
   );

   it(
      "serves live once a fresh entry crosses its window, with no rebind",
      async () => {
         const windowSeconds = 10;
         const anchor = Date.now();
         const manifestFile = await writeFreshnessManifest({
            dataAsOf: new Date(anchor).toISOString(),
            freshnessWindowSeconds: windowSeconds,
            freshnessFallback: "live",
         });
         await bind(manifestFile);

         // While inside the window, the served query routes to the table.
         expect(Date.now() - anchor).toBeLessThan(windowSeconds * 1000);
         const fresh = await executedSql();
         expect(fresh).not.toContain("data/orders.csv");
         expect(fresh).toContain("order_summary");

         // Wait for the window to elapse — no rebind, no reload.
         const waitMs = windowSeconds * 1000 - (Date.now() - anchor) + 1500;
         await new Promise((r) => setTimeout(r, Math.max(waitMs, 0)));

         // The very next query re-evaluates freshness and now serves live.
         const afterCrossing = await executedSql();
         expect(afterCrossing).toContain("data/orders.csv");

         await revertToLive();
      },
      { timeout: 120_000 },
   );
});
