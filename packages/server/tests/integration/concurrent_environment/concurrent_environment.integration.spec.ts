/// <reference types="bun-types" />

/**
 * Regression test for per-environment load/scaffold races.
 *
 * Before fix: concurrent GET (especially ?reload=true), POST, and PATCH against
 * the same environment name could enter `getEnvironment` / `addEnvironment` in
 * parallel. Multiple callers would scaffold or re-load the same directory
 * concurrently, leaving publisher.db and on-disk state inconsistent. Lazy loads
 * then failed with `Environment "…" could not be resolved to a path.`
 *
 * After fix: environment operations serialize on a per-environment mutex in
 * `EnvironmentStore.getEnvironment`, so concurrent callers share one load path.
 * All N requests must succeed and the environment must remain usable afterwards.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = `concurrent-environment-test-env-${Date.now()}`;
const PACKAGE_NAME = "gcs_faa";
const FIXTURE_LOCATION = "gs://publisher_test_packages/gcs_faa.zip";
const CONCURRENCY = 12;

const FORBIDDEN_ERROR_FRAGMENTS = [
   "could not be resolved to a path",
   "Package manifest for",
   "does not exist",
   "compiling model path not found",
   "model path not found",
];

function findForbiddenError(body: unknown): string | undefined {
   const text = typeof body === "string" ? body : JSON.stringify(body ?? "");
   return FORBIDDEN_ERROR_FRAGMENTS.find((frag) => text.includes(frag));
}

function environmentPayload(description?: string) {
   return {
      name: ENV_NAME,
      packages: [{ name: PACKAGE_NAME, location: FIXTURE_LOCATION }],
      connections: [],
      ...(description !== undefined ? { description } : {}),
   };
}

async function waitForPackageReady(
   baseUrl: string,
   deadlineMs = 30_000,
): Promise<void> {
   const deadline = Date.now() + deadlineMs;
   while (Date.now() < deadline) {
      try {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`,
         );
         if (res.ok) {
            return;
         }
      } catch {
         // not ready yet
      }
      await new Promise((r) => setTimeout(r, 250));
   }
   throw new Error("Seeded package did not become available in time");
}

describe("Concurrent environment operations (E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify(environmentPayload()),
      });
      if (!createRes.ok) {
         const body = await createRes.text();
         throw new Error(
            `Failed to seed test environment (${createRes.status}): ${body}`,
         );
      }
      await waitForPackageReady(baseUrl);
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

   it("concurrent POST /environments for the same name all succeed", async () => {
      const requests = Array.from({ length: CONCURRENCY }, () =>
         fetch(`${baseUrl}/api/v0/environments`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(environmentPayload()),
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
         const meta = body as { name?: string };
         expect(meta.name).toBe(ENV_NAME);
      }

      await waitForPackageReady(baseUrl);

      const getRes = await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`);
      expect(getRes.status).toBe(200);
      const forbidden = findForbiddenError(
         await getRes.json().catch(() => null),
      );
      expect(forbidden).toBeUndefined();
   });

   it("concurrent GET /environments/:name?reload=true all succeed", async () => {
      const requests = Array.from({ length: CONCURRENCY }, () =>
         fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}?reload=true`),
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
         const meta = body as { name?: string };
         expect(meta.name).toBe(ENV_NAME);
      }

      await waitForPackageReady(baseUrl);
   });

   it("simultaneous POST + PATCH for the same environment serialize cleanly", async () => {
      const newReadme = `concurrent-env-update-${Date.now()}`;
      const [postRes, patchRes] = await Promise.all([
         fetch(`${baseUrl}/api/v0/environments`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(environmentPayload()),
         }),
         fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               name: ENV_NAME,
               readme: newReadme,
            }),
         }),
      ]);

      expect(postRes.status).toBe(200);
      expect(
         findForbiddenError(await postRes.json().catch(() => null)),
      ).toBeUndefined();

      const patchBody = await patchRes.json().catch(() => null);
      expect(findForbiddenError(patchBody)).toBeUndefined();
      expect([200, 404]).toContain(patchRes.status);

      const getRes = await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`);
      expect(getRes.status).toBe(200);
      const meta = (await getRes.json()) as { name?: string; readme?: string };
      expect(meta.name).toBe(ENV_NAME);
   });

   it("interleaved POST + GET-reload + package list never surface path errors", async () => {
      const work: Array<Promise<{ status: number; body: unknown }>> = [];
      for (let i = 0; i < CONCURRENCY; i++) {
         work.push(
            fetch(`${baseUrl}/api/v0/environments`, {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify(environmentPayload()),
            }).then(async (r) => ({
               status: r.status,
               body: await r.json().catch(() => null),
            })),
         );
         work.push(
            fetch(
               `${baseUrl}/api/v0/environments/${ENV_NAME}?reload=true`,
            ).then(async (r) => ({
               status: r.status,
               body: await r.json().catch(() => null),
            })),
         );
         work.push(
            fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}/packages`).then(
               async (r) => ({
                  status: r.status,
                  body: await r.json().catch(() => null),
               }),
            ),
         );
      }

      const results = await Promise.all(work);
      for (const { status, body } of results) {
         const forbidden = findForbiddenError(body);
         expect(forbidden).toBeUndefined();
         expect(status).toBeLessThan(500);
      }

      await waitForPackageReady(baseUrl);
   });
});
