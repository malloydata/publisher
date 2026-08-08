/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "incremental-refresh-project";
// The publish gate unloads (or rolls back) the package it rejects, so the
// publish test gets its own environment rather than tearing down the one the
// load tests are reading.
const PUBLISH_PROJECT_NAME = "incremental-publish-project";
const REJECTED_PACKAGE = "persist-incremental-test";
const ADVISORY_PACKAGE = "persist-advisory-test";

// The incremental gate's SEVERITY over the real REST surface: a rejected
// declaration fails the load and a publish of it, while an advisory rides the
// package's warnings and fails nothing.
//
// Both fixtures run on DuckDB, which the incremental dialect allowlist EXCLUDES,
// and that is what makes them the right fixtures at this level: they reach a
// rejection without needing a live Postgres or BigQuery. The two paths matter
// separately because a package can arrive by either — `POST /packages` for a
// publish, and environment creation (as here) for the load a control plane's
// worker performs, which never passes through the publish endpoint.
//
// The delta DML's own behavior is executed against a real engine in
// incremental_dml_semantics.spec.ts, and the SEED/DELTA/SKIP decisions are
// covered in incremental_apply.spec.ts and materialization_service.spec.ts.
describe("Incremental refresh over REST", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   const fixtureDir = (pkg: string) =>
      path.resolve(__dirname, "../../fixtures", pkg);

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      // Added through environment creation, which does not run the publish gate
      // — the same way a package uploaded to a control plane's storage arrives.
      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PROJECT_NAME,
            packages: [
               {
                  name: REJECTED_PACKAGE,
                  location: fixtureDir(REJECTED_PACKAGE),
               },
               {
                  name: ADVISORY_PACKAGE,
                  location: fixtureDir(ADVISORY_PACKAGE),
               },
            ],
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
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${PROJECT_NAME}/packages/${ADVISORY_PACKAGE}`,
         );
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

   it("REFUSES to serve a package whose declaration the gate rejects", async () => {
      // The change this fixture exists for: the package used to load, serve, and
      // rebuild in full forever with the rejection in a worker log. A read now
      // answers the gate's own text, as a 400 — an authoring error, not the 424 a
      // dependency failure would map to.
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${PROJECT_NAME}/packages/${REJECTED_PACKAGE}`,
      );
      expect(res.status).toBe(400);
      const body = (await res.json()) as { message?: string };
      expect(body.message).toContain('refresh="incremental"');
      expect(body.message).toContain('dialect "duckdb"');
   });

   it("names the failed package, and why, in /status loadErrors", async () => {
      // The operator's half. A package that does not load is skipped rather than
      // fatal, so this is the one place the reason is reported server-wide.
      const res = await fetch(`${baseUrl}/api/v0/status`);
      expect(res.status).toBe(200);
      const status = (await res.json()) as {
         loadErrors?: {
            environment: string;
            package?: string;
            message: string;
         }[];
      };
      const failure = (status.loadErrors ?? []).find(
         (e) => e.package === REJECTED_PACKAGE,
      );
      expect(failure).toBeDefined();
      expect(failure?.environment).toBe(PROJECT_NAME);
      expect(failure?.message).toContain('dialect "duckdb"');
   });

   it("REJECTS a publish of the same package, naming the dialect", async () => {
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
            body: JSON.stringify({
               name: REJECTED_PACKAGE,
               location: fixtureDir(REJECTED_PACKAGE),
            }),
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

   it("LOADS a package whose only finding is an advisory, and warns on it", async () => {
      // The other side of the split, which a severity change is exactly what
      // would break: an unrecognized #@ persist key is legal, so the package
      // serves and the finding rides `warnings`.
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${PROJECT_NAME}/packages/${ADVISORY_PACKAGE}`,
      );
      expect(res.status).toBe(200);
      const pkg = (await res.json()) as {
         warnings?: { message: string; subject?: string }[];
         buildPlan?: { sources?: Record<string, { name?: string }> };
      };
      const advisory = (pkg.warnings ?? []).find((w) =>
         w.message.includes('"mergekey"'),
      );
      expect(advisory).toBeDefined();
      expect(advisory?.subject).toBe("daily_orders");
      // And the package really is serving: the source is on its build plan.
      expect(
         Object.values(pkg.buildPlan?.sources ?? {}).map((s) => s.name),
      ).toEqual(["daily_orders"]);
   });
});
