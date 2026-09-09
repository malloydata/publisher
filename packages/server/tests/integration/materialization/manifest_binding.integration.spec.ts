// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
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

   // These tests share one package and run serially, so a failed assertion would
   // otherwise leak its binding into the next one and fail it for the wrong
   // reason — the failed-rebind case deliberately ends in `live_fallback`. Revert
   // to live after every test, unconditionally, and without assertions of its own
   // so a cleanup failure cannot mask the real one.
   afterEach(async () => {
      if (!baseUrl) return;
      await fetch(url(""), {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ name: PACKAGE_NAME, manifestLocation: null }),
      }).catch(() => {});
      await fetch(url("?reload=true")).catch(() => {});
   });

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

   const ROUTING_QUERY = "run: order_summary -> { aggregate: c is count() }";

   async function queryOrderSummaryStatus(): Promise<number> {
      const res = await fetch(`${baseUrl}${API}/models/${MODEL_PATH}/query`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ query: ROUTING_QUERY }),
      });
      return res.status;
   }

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

   /** Read the package's build plan and return the persist source's real sourceEntityId. */
   async function orderSummarySourceEntityId(): Promise<string> {
      const res = await fetch(url(""));
      expect(res.status).toBe(200);
      const pkg = (await res.json()) as {
         buildPlan?: { sources?: Record<string, { sourceEntityId?: string }> };
      };
      const sources = pkg.buildPlan?.sources ?? {};
      const sourceEntityId = Object.values(sources)[0]?.sourceEntityId;
      expect(typeof sourceEntityId).toBe("string");
      return sourceEntityId as string;
   }

   /** Poll a materialization until it reaches a terminal state. */
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

   /**
    * Build + physicalise the persist source (auto-run self-assigns
    * physicalTableName = "order_summary" from `#@ persist name=...`) so a
    * `SELECT * FROM order_summary` is actually queryable, then revert the
    * package to live (unbound) — keeping the table — so a subsequent
    * manifest-URI bind is what routes the served query.
    */
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
      // Revert to live: drop the auto-load binding but leave the built table.
      await fetch(url("?reload=true"));
      // Retire the run record so it doesn't hold the per-package active slot.
      await fetch(url(`/materializations/${id}`), { method: "DELETE" });
   }

   /** Write a manifest file keyed by the persist source's real sourceEntityId. */
   async function writeRoutingManifest(
      sourceEntityId: string,
      physicalTableName: string,
   ): Promise<string> {
      const file = path.join(tmpDir, `routing-manifest-${Date.now()}.json`);
      await fsp.writeFile(
         file,
         JSON.stringify({
            builtAt: new Date().toISOString(),
            strict: false,
            entries: {
               [sourceEntityId]: {
                  sourceEntityId,
                  sourceName: "order_summary",
                  physicalTableName,
                  connectionName: "duckdb",
               },
            },
         }),
         "utf8",
      );
      return file;
   }

   /**
    * Write a manifest carrying a `storage=` (cross-connection) entry — one that
    * names a `storageDestinationName` and carries a captured `schema`. Such an
    * entry serves through the virtual-source transform, so it must bind as a
    * serve BINDING, never as a same-connection tableName substitution.
    */
   async function writeStorageManifest(
      sourceEntityId: string,
   ): Promise<string> {
      const file = path.join(tmpDir, `storage-manifest-${Date.now()}.json`);
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
                  storageDestinationName: "lake",
                  schema: [{ name: "c", type: "BIGINT" }],
               },
            },
         }),
         "utf8",
      );
      return file;
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
                  sourceEntityId: "build123",
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

         // Accept on update; the response echoes the bound location and the
         // server-computed binding observability fields (surfaced on /status so
         // the control plane can confirm the worker actually bound the manifest).
         const patchRes = await patchPackage({
            manifestLocation: manifestFile,
         });
         expect(patchRes.status).toBe(200);
         const patched = await patchRes.json();
         expect(patched.manifestLocation).toBe(manifestFile);
         expect(patched.manifestBindingStatus).toBe("bound");
         expect(patched.manifestEntryCount).toBe(1);
         expect(patched.boundManifestUri).toBe(manifestFile);

         // In-memory metadata reflects it; binding did not break serving.
         const bound = await getPackage();
         expect(bound.manifestLocation).toBe(manifestFile);
         expect(bound.manifestBindingStatus).toBe("bound");
         expect(bound.manifestEntryCount).toBe(1);
         expect(await queryOrderSummaryStatus()).toBe(200);

         // Persisted to publisher.json: a reload re-reads and re-binds it.
         expect((await getPackage(true)).manifestLocation).toBe(manifestFile);
         expect(await queryOrderSummaryStatus()).toBe(200);

         // Clearing reverts to live (unbound) and survives a reload.
         const clearRes = await patchPackage({ manifestLocation: null });
         expect(clearRes.status).toBe(200);
         const cleared = await clearRes.json();
         expect(cleared.manifestLocation ?? null).toBeNull();
         expect(cleared.manifestBindingStatus).toBe("unbound");
         expect(cleared.manifestEntryCount).toBe(0);
         expect(cleared.boundManifestUri ?? null).toBeNull();
         expect((await getPackage(true)).manifestLocation ?? null).toBeNull();
         expect(await queryOrderSummaryStatus()).toBe(200);
      },
      { timeout: 60_000 },
   );

   it(
      "routes served queries to the materialized table after a manifest-URI bind",
      async () => {
         // Start from live so the baseline recomputes from the base CSV.
         await getPackage(true);
         expect(await executedSql()).toContain("data/orders.csv");

         // Physically build the persist source, then revert to live (keeping
         // the table) so the manifest-URI bind below is the only thing that
         // could route the served query.
         await buildTableThenRevertToLive();
         expect(await executedSql()).toContain("data/orders.csv");

         // Bind the CP-shaped manifest via manifestLocation, keyed by the
         // source's REAL sourceEntityId (the value Malloy recomputes at serve
         // time to resolve the persist reference). Anything else silently misses.
         const sourceEntityId = await orderSummarySourceEntityId();
         const manifestFile = await writeRoutingManifest(
            sourceEntityId,
            "order_summary",
         );
         const patchRes = await patchPackage({
            manifestLocation: manifestFile,
         });
         expect(patchRes.status).toBe(200);
         const patched = await patchRes.json();
         expect(patched.manifestBindingStatus).toBe("bound");
         expect(patched.manifestEntryCount).toBe(1);

         // The payoff: the served query now scans the materialized table and no
         // longer recomputes from the base CSV.
         const routed = await executedSql();
         expect(routed).not.toContain("data/orders.csv");
         expect(routed).toContain("order_summary");

         // Cleanup: revert to live so the dangling binding doesn't leak.
         await patchPackage({ manifestLocation: null });
         await getPackage(true);
      },
      { timeout: 120_000 },
   );

   it(
      "binds a storage= entry as a cross-connection serve binding, not a tableName substitution",
      async () => {
         await getPackage(true); // start from live (unbound)
         const sourceEntityId = await orderSummarySourceEntityId();
         const manifestFile = await writeStorageManifest(sourceEntityId);

         const patchRes = await patchPackage({
            manifestLocation: manifestFile,
         });
         expect(patchRes.status).toBe(200);
         const patched = (await patchRes.json()) as Record<string, unknown>;

         // The storage entry never enters the same-connection tableName manifest,
         // so there is nothing to substitute (and no recompile happened).
         expect(patched.manifestEntryCount).toBe(0);
         // It surfaces instead as a cross-connection storage serve binding.
         expect(patched.storageServeBindings).toEqual([
            {
               sourceName: "order_summary",
               // An entry with no `origin` describes a source the modeler
               // annotated, which is what the wire default means. Asserted rather
               // than omitted because a caller reads this field to know whether
               // `sourceName` resolves against the package's models at all.
               origin: "persist",
               storageDestinationName: "lake",
               tablePath: "lake.order_summary",
            },
         ]);
         // The bound URI is recorded even though no tableName manifest bound.
         expect(patched.boundManifestUri).toBe(manifestFile);
         // And it reports BOUND despite the zero colocated entry count. This is
         // what a control plane reads to decide whether the worker honoured the
         // manifest it distributed; reporting `unbound` after a successful bind
         // reads as drift, and the package is rebound on every reconcile tick.
         expect(patched.manifestBindingStatus).toBe("bound");

         // Clearing reverts: the storage serve binding is dropped and the package
         // reports unbound again, so the bound status tracks real state in both
         // directions rather than latching on.
         const clearRes = await patchPackage({ manifestLocation: null });
         expect(clearRes.status).toBe(200);
         const cleared = (await clearRes.json()) as Record<string, unknown>;
         expect(cleared.storageServeBindings ?? null).toBeNull();
         expect(cleared.manifestBindingStatus).toBe("unbound");
         expect(cleared.boundManifestUri ?? null).toBeNull();
         await getPackage(true);
      },
      { timeout: 60_000 },
   );

   it(
      "recovers from live_fallback when a later storage= manifest binds",
      async () => {
         await getPackage(true); // start from live (unbound)

         // Degrade first: a manifest URI that cannot be fetched reports
         // live_fallback.
         const missing = path.join(tmpDir, `absent-${Date.now()}.json`);
         const failed = (await (
            await patchPackage({ manifestLocation: missing })
         ).json()) as Record<string, unknown>;
         expect(failed.manifestBindingStatus).toBe("live_fallback");

         // A pure-`storage=` manifest binds with no colocated entry to recompile, so
         // nothing on that path recomputes the status — the successful bind has to
         // report itself. Otherwise the package stays live_fallback while healthily
         // bound, which reads as degraded to a caller that alerts on it.
         const sourceEntityId = await orderSummarySourceEntityId();
         const manifestFile = await writeStorageManifest(sourceEntityId);
         const bound = (await (
            await patchPackage({ manifestLocation: manifestFile })
         ).json()) as Record<string, unknown>;
         expect(bound.manifestBindingStatus).toBe("bound");
         expect(bound.boundManifestUri).toBe(manifestFile);
         expect(
            (bound.storageServeBindings as unknown[] | undefined)?.length,
         ).toBe(1);

         await patchPackage({ manifestLocation: null });
         await getPackage(true);
      },
      { timeout: 60_000 },
   );

   it(
      "a failed rebind keeps the previously bound manifest in every field but the status",
      async () => {
         await getPackage(true); // start from live (unbound)

         const manifestFile = await writeManifest();
         const bound = (await (
            await patchPackage({ manifestLocation: manifestFile })
         ).json()) as Record<string, unknown>;
         expect(bound.manifestBindingStatus).toBe("bound");
         expect(bound.manifestEntryCount).toBe(1);
         expect(bound.boundManifestUri).toBe(manifestFile);

         // Rebind to a URI that cannot be fetched. The CONFIGURED manifest is not
         // applied, but the previously bound one stays fully in place: the fetch
         // fails before either tier applies, so nothing was even partially
         // replaced — and only the status moves. A reclaim gate reading
         // live_fallback as "not serving from manifest tables" would drop the
         // generation boundManifestUri still points at, out from under a package
         // actively serving it.
         const missing = path.join(tmpDir, `absent-rebind-${Date.now()}.json`);
         const failed = (await (
            await patchPackage({ manifestLocation: missing })
         ).json()) as Record<string, unknown>;
         expect(failed.manifestBindingStatus).toBe("live_fallback");
         expect(failed.manifestEntryCount).toBe(1);
         expect(failed.boundManifestUri).toBe(manifestFile);
         expect(await queryOrderSummaryStatus()).toBe(200);

         await patchPackage({ manifestLocation: null });
         await getPackage(true);
      },
      { timeout: 60_000 },
   );

   it(
      "a rebind to a manifest whose storage entries vanished clears the old bindings",
      async () => {
         // Regression (MED-2): bindManifest must drop storage serve bindings when
         // a NEW manifest (still a real URI, not a clear) no longer carries them —
         // otherwise stale bindings keep routing at a table the host no longer
         // vouches for.
         await getPackage(true);
         const sourceEntityId = await orderSummarySourceEntityId();
         const withStorage = await writeStorageManifest(sourceEntityId);
         const bound = (await (
            await patchPackage({ manifestLocation: withStorage })
         ).json()) as Record<string, unknown>;
         expect(
            (bound.storageServeBindings as unknown[] | undefined)?.length,
         ).toBe(1);

         // Rebind to a DIFFERENT manifest URI that carries no storage entries.
         const noStorage = path.join(tmpDir, `no-storage-${Date.now()}.json`);
         await fsp.writeFile(
            noStorage,
            JSON.stringify({ builtAt: new Date().toISOString(), entries: {} }),
            "utf8",
         );
         const rebound = (await (
            await patchPackage({ manifestLocation: noStorage })
         ).json()) as Record<string, unknown>;
         // Stale storage binding must be gone, not left routing.
         expect(rebound.storageServeBindings ?? null).toBeNull();
         // An empty manifest is still a manifest that was fetched and applied, so
         // it reports bound with neither tier populated. That combination — bound
         // with a zero count and no storage bindings — is what tells a caller the
         // manifest carried nothing, as against the bind having been missed.
         // Reporting `unbound` here would instead be permanent drift to a caller
         // that rebinds on anything but `bound`.
         expect(rebound.manifestBindingStatus).toBe("bound");
         expect(rebound.manifestEntryCount).toBe(0);
         expect(rebound.boundManifestUri).toBe(noStorage);

         await patchPackage({ manifestLocation: null });
         await getPackage(true);
      },
      { timeout: 60_000 },
   );

   it("degrades to serving live when the manifest is unreachable", async () => {
      const missing = path.join(tmpDir, "does-not-exist.json");

      const patchRes = await patchPackage({ manifestLocation: missing });
      expect(patchRes.status).toBe(200);
      const patched = await patchRes.json();
      expect(patched.manifestLocation).toBe(missing);
      // The configured location is retained, but the binding is reported as a
      // degraded live fallback (not "bound") with nothing actually bound.
      expect(patched.manifestBindingStatus).toBe("live_fallback");
      expect(patched.manifestEntryCount).toBe(0);
      expect(patched.boundManifestUri ?? null).toBeNull();

      // A fetch failure must not brick the package — it still serves live.
      expect(await queryOrderSummaryStatus()).toBe(200);
      expect((await getPackage(true)).manifestLocation).toBe(missing);
      expect(await queryOrderSummaryStatus()).toBe(200);

      await patchPackage({ manifestLocation: null });
   });
});
