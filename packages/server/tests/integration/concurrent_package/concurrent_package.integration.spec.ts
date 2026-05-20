/// <reference types="bun-types" />

/**
 * Regression test for the per-package download/unzip race.
 *
 * Before fix: concurrent POST/PATCH/GET-with-reload against the same package
 * name landed in `downloadPackage` *before* acquiring the per-package mutex.
 * Multiple callers would `rm -rf <targetPath>` and then `cp -r` / `extractAllTo`
 * in parallel, leaving the directory in an inconsistent state. Subsequent
 * reads failed with "Package manifest for ... does not exist" and any
 * in-flight model compilation would 500 with "compiling model path not found".
 *
 * After fix: download is run inside the per-package mutex, so concurrent
 * callers serialize on the same target path. All N requests must succeed
 * and the package must be usable afterwards.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Use a unique env name per run so stale state from a previous run (env
// directory, persisted env metadata in publisher.db) can't poison startup.
const ENV_NAME = `concurrent-package-test-env-${Date.now()}`;
const PACKAGE_NAME = "gcs_faa";
const CONCURRENCY = 12;

const FORBIDDEN_ERROR_FRAGMENTS = [
   "Package manifest for",
   "does not exist",
   "compiling model path not found",
   "model path not found",
];

function findForbiddenError(body: unknown): string | undefined {
   const text = typeof body === "string" ? body : JSON.stringify(body ?? "");
   return FORBIDDEN_ERROR_FRAGMENTS.find((frag) => text.includes(frag));
}

describe("Concurrent package operations (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let fixtureDir: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      fixtureDir = "gs://publisher_test_packages/gcs_faa.zip";

      // Seed the environment with the package once so the env exists on disk.
      // addEnvironment requires at least one package.
      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PACKAGE_NAME, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         const body = await createRes.text();
         throw new Error(
            `Failed to seed test environment (${createRes.status}): ${body}`,
         );
      }

      // Wait for the seeded package to be loadable.
      const deadline = Date.now() + 30_000;
      let ready = false;
      while (!ready && Date.now() < deadline) {
         try {
            const res = await fetch(
               `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`,
            );
            if (res.ok) {
               ready = true;
               break;
            }
         } catch {
            // not ready yet
         }
         await new Promise((r) => setTimeout(r, 250));
      }
      if (!ready) {
         throw new Error("Seeded package did not become available in time");
      }
   });

   afterAll(async () => {
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort cleanup
         }
      }
      await env?.stop();
      env = null;
   });

   it("concurrent POST /packages for the same name all succeed", async () => {
      const requests = Array.from({ length: CONCURRENCY }, () =>
         fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}/packages`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               name: PACKAGE_NAME,
               location: fixtureDir,
            }),
         }),
      );

      const responses = await Promise.all(requests);
      const bodies = await Promise.all(
         responses.map(async (r) => ({
            status: r.status,
            body: await r.json().catch(() => null),
         })),
      );

      for (const { status, body } of bodies) {
         expect(status).toBe(200);
         const forbidden = findForbiddenError(body);
         expect(forbidden).toBeUndefined();
      }

      // Package must be loadable after the storm.
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`,
      );
      expect(res.status).toBe(200);
      const meta = (await res.json()) as { name?: string };
      expect(meta.name).toBe(PACKAGE_NAME);

      // Models must be listable — proves the model files survived the unzip
      // race and Package.create read a consistent directory.
      const modelsRes = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}/models`,
      );
      expect(modelsRes.status).toBe(200);
      const models = (await modelsRes.json()) as Array<{ path?: string }>;
      expect(Array.isArray(models)).toBe(true);
      expect(models.length).toBeGreaterThan(0);
   });

   it("concurrent GET /packages/:name?reload=true all succeed", async () => {
      const requests = Array.from({ length: CONCURRENCY }, () =>
         fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}?reload=true`,
         ),
      );
      const responses = await Promise.all(requests);
      const bodies = await Promise.all(
         responses.map(async (r) => ({
            status: r.status,
            body: await r.json().catch(() => null),
         })),
      );
      for (const { status, body } of bodies) {
         expect(status).toBe(200);
         const forbidden = findForbiddenError(body);
         expect(forbidden).toBeUndefined();
      }

      // Models must still list cleanly.
      const modelsRes = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}/models`,
      );
      expect(modelsRes.status).toBe(200);
   });

   it("simultaneous POST + PATCH for the same package serialize cleanly", async () => {
      // Fire create and update at the same time. Both should land under the
      // same per-package mutex. Whichever wins the mutex first runs first;
      // the loser sees a coherent post-condition. After both settle the
      // package must be loadable and the description must reflect the PATCH
      // when the PATCH ran *after* a successful POST.
      const newDescription = `concurrent-update-${Date.now()}`;
      const [postRes, patchRes] = await Promise.all([
         fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}/packages`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               name: PACKAGE_NAME,
               location: fixtureDir,
            }),
         }),
         fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`,
            {
               method: "PATCH",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({
                  name: PACKAGE_NAME,
                  description: newDescription,
               }),
            },
         ),
      ]);

      // POST is idempotent here — always succeeds.
      expect(postRes.status).toBe(200);
      const postBody = await postRes.json().catch(() => null);
      expect(findForbiddenError(postBody)).toBeUndefined();

      // PATCH either succeeds (ran after POST loaded the package) or 404s
      // (ran before POST populated `this.packages`). It must NOT silently
      // rewrite disk and then 404, and must never surface the forbidden
      // error fragments.
      const patchBody = await patchRes.json().catch(() => null);
      expect(findForbiddenError(patchBody)).toBeUndefined();
      expect([200, 404]).toContain(patchRes.status);

      // Whatever happened, the package must remain loadable.
      const getRes = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`,
      );
      expect(getRes.status).toBe(200);
      const meta = (await getRes.json()) as {
         name?: string;
         description?: string;
      };
      expect(meta.name).toBe(PACKAGE_NAME);
   });

   it("interleaved POST + GET-reload + model list never surface stale-dir errors", async () => {
      // Worst-case interleave: writers and readers hammering the same
      // package. None of them should observe a missing manifest or a
      // half-extracted directory.
      const work: Array<Promise<{ status: number; body: unknown }>> = [];
      for (let i = 0; i < CONCURRENCY; i++) {
         work.push(
            fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}/packages`, {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({
                  name: PACKAGE_NAME,
                  location: fixtureDir,
               }),
            }).then(async (r) => ({
               status: r.status,
               body: await r.json().catch(() => null),
            })),
         );
         work.push(
            fetch(
               `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}?reload=true`,
            ).then(async (r) => ({
               status: r.status,
               body: await r.json().catch(() => null),
            })),
         );
         work.push(
            fetch(
               `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}/models`,
            ).then(async (r) => ({
               status: r.status,
               body: await r.json().catch(() => null),
            })),
         );
      }

      const results = await Promise.all(work);
      for (const { status, body } of results) {
         const forbidden = findForbiddenError(body);
         expect(forbidden).toBeUndefined();
         // Model list while the package is being rewritten can transiently
         // return 404 (the package is unloaded mid-rewrite), but never 5xx,
         // and never with the forbidden error fragments.
         expect(status).toBeLessThan(500);
      }
   });
});
