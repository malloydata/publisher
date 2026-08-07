/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "incremental-refresh-project";
// The publish gate unloads (or rolls back) the package it rejects, so the
// rejection test gets its own environment rather than tearing down the one the
// load and build tests are reading.
const PUBLISH_PROJECT_NAME = "incremental-publish-project";
const PACKAGE_NAME = "persist-incremental-test";
const API = `/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;

// Incremental refresh over the real REST surface: the load-time advisory, the
// publish gate, and the build dispatch.
//
// The fixture runs on DuckDB, which the incremental dialect allowlist EXCLUDES,
// and that is what makes it the right fixture at this level. It proves the two
// things a wrong wiring would break for every deployment — that a package
// declaring incremental refresh is REFUSED at publish where the delta cannot be
// applied, and that it still LOADS and still BUILDS in full where it was already
// published — without needing a live Postgres or BigQuery to reach them. The
// delta DML's own behavior is executed against a real engine in
// incremental_dml_semantics.spec.ts, and the SEED/DELTA/SKIP decisions are
// covered in incremental_apply.spec.ts and materialization_service.spec.ts.
describe("Incremental refresh over REST", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   const fixtureDir = path.resolve(
      __dirname,
      "../../fixtures/persist-incremental-test",
   );

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      // Added through environment creation, which does not run the publish gate
      // — the same way a package published before these rules existed arrives.
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
      for (const name of [PROJECT_NAME, PUBLISH_PROJECT_NAME]) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${name}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort
         }
      }
      await env?.stop();
      env = null;
   });

   async function getPackage(): Promise<Record<string, unknown>> {
      const res = await fetch(`${baseUrl}${API}`);
      expect(res.status).toBe(200);
      return (await res.json()) as Record<string, unknown>;
   }

   it("loads the package and reports the declaration on the build plan", async () => {
      // A load-tolerated declaration: serving must not depend on a refresh mode,
      // even one this dialect cannot honor.
      const pkg = await getPackage();
      const plan = pkg.buildPlan as Record<string, unknown>;
      const sources = plan?.sources as Record<string, Record<string, unknown>>;
      const source = Object.values(sources ?? {})[0];
      expect(source?.name).toBe("daily_orders");
      expect(source?.refresh).toBe("incremental");
      // watermark= and merge_key= ride annotationFields; the wire contract gains
      // no typed fields in this phase.
      expect(source?.annotationFields).toMatchObject({
         name: "daily_orders",
         refresh: "incremental",
         watermark: "order_date",
      });
   });

   it("warns that a keyless incremental source REPLACES its watermark range", async () => {
      const pkg = await getPackage();
      const warnings = (pkg.warnings ?? []) as {
         message: string;
         target?: string;
      }[];
      const advisory = warnings.find((w) =>
         w.message.includes("no merge_key="),
      );
      expect(advisory).toBeDefined();
      expect(advisory?.target).toBe("daily_orders");
      // The advisory's job is to name the consequence, not just the fact.
      expect(advisory?.message).toContain("appears twice");
   });

   it("REJECTS a publish of the same package, naming the dialect", async () => {
      // The gate is strict at publish and log-only at load, so a publish is the
      // one place the refusal is visible to a caller.
      const createEnv = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PUBLISH_PROJECT_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(createEnv.ok).toBe(true);

      const res = await fetch(
         `${baseUrl}/api/v0/environments/${PUBLISH_PROJECT_NAME}/packages`,
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: PACKAGE_NAME, location: fixtureDir }),
         },
      );
      expect(res.status).toBe(400);
      const body = (await res.json()) as { message?: string };
      expect(body.message).toContain('refresh="incremental"');
      expect(body.message).toContain('dialect "duckdb"');
      expect(body.message).toContain(
         "postgres, snowflake, standardsql (BigQuery)",
      );
   });

   it(
      "still builds the source, in full, on an unsupported dialect",
      async () => {
         // The fallback that keeps an already-published package working: the
         // delta path declines, and the ordinary CTAS + rename runs instead.
         const createRes = await fetch(`${baseUrl}${API}/materializations`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({}),
         });
         expect(createRes.status).toBe(201);
         const { id } = (await createRes.json()) as { id: string };

         const deadline = Date.now() + 60_000;
         let record: Record<string, unknown> = {};
         while (Date.now() < deadline) {
            const res = await fetch(`${baseUrl}${API}/materializations/${id}`);
            record = (await res.json()) as Record<string, unknown>;
            const status = record.status as string;
            if (
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(status)
            ) {
               break;
            }
            await new Promise((r) => setTimeout(r, 250));
         }
         expect(record.status).toBe("MANIFEST_FILE_READY");
         const manifest = record.manifest as Record<string, unknown>;
         const entries = Object.values(
            (manifest?.entries ?? {}) as Record<
               string,
               Record<string, unknown>
            >,
         );
         expect(entries.length).toBe(1);
         expect(entries[0].physicalTableName).toBe("daily_orders");

         await fetch(
            `${baseUrl}${API}/materializations/${id}?dropTables=true`,
            {
               method: "DELETE",
            },
         );
      },
      { timeout: 120_000 },
   );
});
