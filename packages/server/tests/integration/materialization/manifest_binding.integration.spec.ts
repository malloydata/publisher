/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fsp from "fs/promises";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "manifest-bind-project";
const PACKAGE_NAME = "persist-test";
const MODEL_PATH = "persist_test.malloy";
const API = `/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;

/**
 * Covers the publisher's manifestLocation plumbing: create/update accepts a
 * manifest URI, it is persisted to publisher.json (so it survives a reload),
 * the package loader fetches + binds it through the existing reloadAllModels
 * primitive on every (re)load, an unreachable manifest degrades to serving
 * live, and clearing it reverts.
 *
 * The build-time rewrite of upstream persist references to physical tables is
 * Malloy's reloadAllModels behavior, exercised by the materialization lifecycle
 * spec; here we assert the publisher-owned fetch/persist/bind wiring.
 */
describe("Manifest binding via Package.manifestLocation (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let tmpDir: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "manifest-bind-"));

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
   });

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

   async function getPackage(reload = false): Promise<Record<string, unknown>> {
      const res = await fetch(url(reload ? "?reload=true" : ""));
      expect(res.status).toBe(200);
      return (await res.json()) as Record<string, unknown>;
   }

   async function patchPackage(
      body: Record<string, unknown>,
   ): Promise<Response> {
      return fetch(url(""), {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ name: PACKAGE_NAME, ...body }),
      });
   }

   async function queryOrderSummaryStatus(): Promise<number> {
      const res = await fetch(`${baseUrl}${API}/models/${MODEL_PATH}/query`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            query: "run: order_summary -> { aggregate: c is count() }",
         }),
      });
      return res.status;
   }

   async function writeManifest(): Promise<string> {
      const file = path.join(tmpDir, `manifest-${Date.now()}.json`);
      await fsp.writeFile(
         file,
         JSON.stringify({
            builtAt: new Date().toISOString(),
            strict: false,
            entries: {
               build123: {
                  buildId: "build123",
                  sourceName: "order_summary",
                  physicalTableName: "main.order_summary_mz",
                  connectionName: "duckdb",
               },
            },
         }),
         "utf8",
      );
      return file;
   }

   it("starts with no manifestLocation and serves live", async () => {
      expect((await getPackage()).manifestLocation ?? null).toBeNull();
      expect(await queryOrderSummaryStatus()).toBe(200);
   });

   it(
      "accepts, persists, reloads, and clears a manifestLocation",
      async () => {
         const manifestFile = await writeManifest();

         // Accept on update; the response echoes the bound location.
         const patchRes = await patchPackage({
            manifestLocation: manifestFile,
         });
         expect(patchRes.status).toBe(200);
         expect((await patchRes.json()).manifestLocation).toBe(manifestFile);

         // In-memory metadata reflects it; binding did not break serving.
         expect((await getPackage()).manifestLocation).toBe(manifestFile);
         expect(await queryOrderSummaryStatus()).toBe(200);

         // Persisted to publisher.json: a reload re-reads and re-binds it.
         expect((await getPackage(true)).manifestLocation).toBe(manifestFile);
         expect(await queryOrderSummaryStatus()).toBe(200);

         // Clearing reverts to live and survives a reload.
         const clearRes = await patchPackage({ manifestLocation: null });
         expect(clearRes.status).toBe(200);
         expect((await clearRes.json()).manifestLocation ?? null).toBeNull();
         expect((await getPackage(true)).manifestLocation ?? null).toBeNull();
         expect(await queryOrderSummaryStatus()).toBe(200);
      },
      { timeout: 60_000 },
   );

   it("degrades to serving live when the manifest is unreachable", async () => {
      const missing = path.join(tmpDir, "does-not-exist.json");

      const patchRes = await patchPackage({ manifestLocation: missing });
      expect(patchRes.status).toBe(200);
      expect((await patchRes.json()).manifestLocation).toBe(missing);

      // A fetch failure must not brick the package — it still serves live.
      expect(await queryOrderSummaryStatus()).toBe(200);
      expect((await getPackage(true)).manifestLocation).toBe(missing);
      expect(await queryOrderSummaryStatus()).toBe(200);

      await patchPackage({ manifestLocation: null });
   });
});
