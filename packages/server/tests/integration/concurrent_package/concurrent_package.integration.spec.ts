/// <reference types="bun-types" />

/**
 * Regression test for the concurrent-package directory race that PR #752
 * tried to address. The publisher used to load packages directly out of
 * `<env>/<pkg>/`, so a `POST /packages` re-downloading the directory while
 * a `GET /packages/:name?reload=true` was scanning the same directory would
 * crash the loader with "Package manifest for ... does not exist" or
 * "model path not found". With the versioned-dir + database-CAS write path
 * the controller stages every download into a fresh
 * `<env>/<pkg>.versions/<uuid>/` directory and atomically swings the
 * package row at it; readers always resolve to whichever directory the
 * database currently points at, and any in-flight reader holding the old
 * directory keeps using it until the sweeper retires it.
 *
 * This suite hammers the public REST surface with overlapping POST / GET
 * / PATCH cycles for the same package and asserts that none of them
 * surface the symptomatic error fragments. Any reintroduction of the race
 * should make this test flake.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ENVIRONMENT_NAME = "concurrent-package-test-env";
const PACKAGE_NAME = "persist-test";

/**
 * Substrings the loader emits when it observes a torn package directory.
 * The new design must never produce any of these because the directory a
 * reader has acquired is never mutated in place.
 */
const FORBIDDEN_ERROR_FRAGMENTS = [
   "Package manifest for",
   "does not exist",
   "compiling model path not found",
   "model path not found",
];

interface Outcome {
   label: string;
   status: number;
   bodyText: string;
}

function assertNoTornReads(outcomes: Outcome[]): void {
   for (const outcome of outcomes) {
      if (outcome.status >= 200 && outcome.status < 300) continue;
      for (const fragment of FORBIDDEN_ERROR_FRAGMENTS) {
         expect(
            outcome.bodyText.includes(fragment),
            `${outcome.label} (status ${outcome.status}) leaked race-symptom fragment "${fragment}":\n${outcome.bodyText}`,
         ).toBe(false);
      }
   }
}

async function captureOutcome(
   label: string,
   request: Promise<Response>,
): Promise<Outcome> {
   try {
      const res = await request;
      const bodyText = await res.text();
      return { label, status: res.status, bodyText };
   } catch (error) {
      return { label, status: -1, bodyText: String(error) };
   }
}

describe("Concurrent package operations (REST E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let fixtureDir: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      fixtureDir = path.resolve(__dirname, "../../fixtures/persist-test");

      // Create the environment up front; tests reuse it.
      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENVIRONMENT_NAME,
            packages: [],
            connections: [],
         }),
      });
      if (!createRes.ok && createRes.status !== 409) {
         throw new Error(
            `Failed to create test environment: ${createRes.status} ${await createRes.text()}`,
         );
      }
   });

   afterAll(async () => {
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${ENVIRONMENT_NAME}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort
         }
      }
      await env?.stop();
      env = null;
   });

   it("interleaves POST add, GET reload, and PATCH without leaking torn-read errors", async () => {
      const outcomes: Outcome[] = [];

      const post = (label: string) =>
         captureOutcome(
            label,
            fetch(
               `${baseUrl}/api/v0/environments/${ENVIRONMENT_NAME}/packages`,
               {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                     name: PACKAGE_NAME,
                     location: fixtureDir,
                  }),
               },
            ),
         );

      const get = (label: string, reload: boolean) =>
         captureOutcome(
            label,
            fetch(
               `${baseUrl}/api/v0/environments/${ENVIRONMENT_NAME}/packages/${PACKAGE_NAME}${
                  reload ? "?reload=true" : ""
               }`,
            ),
         );

      const patch = (label: string, description: string) =>
         captureOutcome(
            label,
            fetch(
               `${baseUrl}/api/v0/environments/${ENVIRONMENT_NAME}/packages/${PACKAGE_NAME}`,
               {
                  method: "PATCH",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                     name: PACKAGE_NAME,
                     description,
                     location: fixtureDir,
                  }),
               },
            ),
         );

      // Initial create. After this the package row exists in the DB and
      // the cache is warm.
      outcomes.push(await post("initial-post"));
      assertNoTornReads(outcomes);

      // Spawn two waves of concurrent traffic. Each wave fans out the
      // three write/read paths against the same package name.
      for (let wave = 0; wave < 3; wave++) {
         const fan = await Promise.all([
            post(`wave-${wave}-post-1`),
            post(`wave-${wave}-post-2`),
            get(`wave-${wave}-get-reload-1`, true),
            get(`wave-${wave}-get-reload-2`, true),
            get(`wave-${wave}-get-1`, false),
            get(`wave-${wave}-get-2`, false),
            patch(`wave-${wave}-patch-1`, `desc-${wave}-1`),
            patch(`wave-${wave}-patch-2`, `desc-${wave}-2`),
         ]);
         outcomes.push(...fan);
         assertNoTornReads(outcomes);
      }

      // Final consistency check: the package must still resolve cleanly.
      const finalGet = await get("final-get", false);
      outcomes.push(finalGet);
      assertNoTornReads(outcomes);
      expect(finalGet.status).toBe(200);
      expect(finalGet.bodyText).toContain(PACKAGE_NAME);
   }, 60_000);
});
