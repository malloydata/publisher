// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";
import { waitUntilOk } from "./polling";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "incremental-refresh-project";
// The publish gate unloads (or rolls back) the package it rejects, so the
// publish test gets its own environment rather than tearing down the one the
// load tests are reading.
const PUBLISH_PROJECT_NAME = "incremental-publish-project";
const REFUSED_ALONE_PACKAGE = "persist-incremental-test";
const INVALID_PACKAGE = "persist-advisory-test";

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
// covered in incremental_apply.spec.ts and materialization_service_*.spec.ts.
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
                  name: REFUSED_ALONE_PACKAGE,
                  location: fixtureDir(REFUSED_ALONE_PACKAGE),
               },
               {
                  name: INVALID_PACKAGE,
                  location: fixtureDir(INVALID_PACKAGE),
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

      await waitUntilOk(
         `${baseUrl}/api/v0/environments/${PROJECT_NAME}/packages/${REFUSED_ALONE_PACKAGE}`,
         "Test package",
      );
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

   it("REFUSES to serve a package with an invalid annotation, loaded as a control plane loads it", async () => {
      // Added through environment creation, which never runs the publish
      // checks: the load itself fails, with the mistake and its code, as a 400
      // (an authoring error, not the 424 a dependency failure maps to).
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${PROJECT_NAME}/packages/${INVALID_PACKAGE}`,
      );
      expect(res.status).toBe(400);
      const body = (await res.json()) as { message?: string };
      expect(body.message).toContain("mergekey=");
      expect(body.message).toContain("[persist-unknown-key]");
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
         (e) => e.package === INVALID_PACKAGE,
      );
      expect(failure).toBeDefined();
      expect(failure?.environment).toBe(PROJECT_NAME);
      expect(failure?.message).toContain("[persist-unknown-key]");
      // The valid declaration refused alone is not a load error.
      expect(
         (status.loadErrors ?? []).some(
            (e) => e.package === REFUSED_ALONE_PACKAGE,
         ),
      ).toBe(false);
   });

   it("REJECTS a publish of the invalid package, naming the key", async () => {
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
               name: INVALID_PACKAGE,
               location: fixtureDir(INVALID_PACKAGE),
            }),
         },
      );
      expect(res.status).toBe(400);
      const body = (await res.json()) as { message?: string };
      expect(body.message).toContain("did you mean `merge_key=`?");
      expect(body.message).toContain("[persist-unknown-key]");
   });

   it("LOADS a package whose incremental declaration DuckDB cannot honour, refusing that source alone", async () => {
      // Valid but unbuildable here: the source is not built and answers live,
      // and the package serves.
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${PROJECT_NAME}/packages/${REFUSED_ALONE_PACKAGE}`,
      );
      expect(res.status).toBe(200);
      const pkg = (await res.json()) as {
         warnings?: {
            message: string;
            subject?: string;
            code?: string;
            severity?: string;
         }[];
         buildPlan?: { sources?: Record<string, { name?: string }> };
      };
      const refused = (pkg.warnings ?? []).find(
         (w) => w.code === "persist-not-built",
      );
      expect(refused).toMatchObject({
         severity: "warn",
         subject: "daily_orders",
      });
      expect(refused?.message).toContain('dialect "duckdb"');
      expect(refused?.message).toContain(
         "postgres, snowflake, standardsql (BigQuery)",
      );
      // Not built: off the build plan.
      expect(Object.values(pkg.buildPlan?.sources ?? {})).toEqual([]);
   });
});
