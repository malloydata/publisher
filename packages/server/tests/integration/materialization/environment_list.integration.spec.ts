/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT = "env-mat-list-project";
const PKG_A = "persist-test";
const PKG_B = "persist-schedule-test";
const ENV_MATS_PATH = `/api/v0/environments/${PROJECT}/materializations`;

// Verifies the environment-scoped list: GET /environments/{env}/materializations
// returns runs from every package in the env, newest-first, each labeled with
// its packageName.
describe("Environment-scoped materialization list", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   const fixture = (name: string) =>
      path.resolve(__dirname, `../../fixtures/${name}`);

   async function createAndFinish(pkg: string): Promise<string> {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${PROJECT}/packages/${pkg}/materializations`,
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ forceRefresh: true }),
         },
      );
      expect(res.status).toBe(201);
      const created = (await res.json()) as { id: string };
      const deadline = Date.now() + 60_000;
      while (Date.now() < deadline) {
         const poll = await fetch(
            `${baseUrl}/api/v0/environments/${PROJECT}/packages/${pkg}/materializations/${created.id}`,
         );
         const m = (await poll.json()) as { status: string };
         if (["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)) {
            break;
         }
         await new Promise((r) => setTimeout(r, 250));
      }
      return created.id;
   }

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PROJECT,
            packages: [
               { name: PKG_A, location: fixture(PKG_A) },
               { name: PKG_B, location: fixture(PKG_B) },
            ],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         throw new Error(
            `Failed to create project (${createRes.status}): ${await createRes.text()}`,
         );
      }

      // Wait for both packages to load.
      for (const pkg of [PKG_A, PKG_B]) {
         const deadline = Date.now() + 30_000;
         while (Date.now() < deadline) {
            const res = await fetch(
               `${baseUrl}/api/v0/environments/${PROJECT}/packages/${pkg}`,
            );
            if (res.ok) break;
            await new Promise((r) => setTimeout(r, 500));
         }
      }
   });

   afterAll(async () => {
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${PROJECT}`, {
               method: "DELETE",
            });
         } catch {
            // best-effort
         }
      }
      await env?.stop();
      env = null;
   });

   it("returns materializations from every package in the environment", async () => {
      await createAndFinish(PKG_A);
      await createAndFinish(PKG_B);

      const res = await fetch(`${baseUrl}${ENV_MATS_PATH}`);
      expect(res.status).toBe(200);
      const rows = (await res.json()) as Array<{
         packageName: string;
         createdAt?: string;
      }>;

      const packages = new Set(rows.map((r) => r.packageName));
      expect(packages.has(PKG_A)).toBe(true);
      expect(packages.has(PKG_B)).toBe(true);

      // Newest-first ordering.
      const times = rows
         .map((r) => (r.createdAt ? new Date(r.createdAt).getTime() : 0))
         .filter((t) => t > 0);
      const sorted = [...times].sort((a, b) => b - a);
      expect(times).toEqual(sorted);
   });

   it("does not shadow the per-package list route", async () => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${PROJECT}/packages/${PKG_A}/materializations`,
      );
      expect(res.status).toBe(200);
      const rows = (await res.json()) as Array<{ packageName: string }>;
      expect(rows.every((r) => r.packageName === PKG_A)).toBe(true);
   });
});
