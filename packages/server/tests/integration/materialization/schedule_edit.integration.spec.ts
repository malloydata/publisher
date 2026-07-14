/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "sched-edit-project";
const PACKAGE_NAME = "persist-schedule-test";
const API = `/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;

// Exercises the editable-schedule PATCH path: materialization.schedule and
// scope are writable via updatePackage, validated with the same rules a publish
// enforces (schedule requires scope: version, valid 5-field cron), and
// persisted to publisher.json.
describe("Editable materialization schedule (PATCH)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const fixtureDir = path.resolve(
         __dirname,
         "../../fixtures/persist-schedule-test",
      );
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
         try {
            await fetch(`${baseUrl}/api/v0/environments/${PROJECT_NAME}`, {
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

   function scheduleOf(
      pkg: Record<string, unknown>,
   ): string | null | undefined {
      return (pkg.materialization as { schedule?: string | null } | null)
         ?.schedule;
   }

   async function patch(body: Record<string, unknown>): Promise<Response> {
      return fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ name: PACKAGE_NAME, ...body }),
      });
   }

   it("starts from the fixture's version-scoped schedule", async () => {
      const pkg = await getPackage();
      expect(pkg.scope).toBe("version");
      expect(scheduleOf(pkg)).toBe("0 6 * * *");
   });

   it("edits and persists a new schedule", async () => {
      const res = await patch({
         materialization: { schedule: "*/30 * * * *" },
      });
      expect(res.ok).toBe(true);
      expect(scheduleOf(await getPackage())).toBe("*/30 * * * *");
   });

   it("rejects an invalid cron (400) and leaves the schedule unchanged", async () => {
      const res = await patch({ materialization: { schedule: "not a cron" } });
      expect(res.status).toBe(400);
      expect(scheduleOf(await getPackage())).toBe("*/30 * * * *");
   });

   it("rejects a schedule without version scope (400)", async () => {
      const res = await patch({
         scope: "package",
         materialization: { schedule: "0 6 * * *" },
      });
      expect(res.status).toBe(400);
      // Prior good state preserved (scope + schedule both unchanged).
      const pkg = await getPackage();
      expect(pkg.scope).toBe("version");
      expect(scheduleOf(pkg)).toBe("*/30 * * * *");
   });

   it("clears the schedule with schedule: null", async () => {
      const res = await patch({ materialization: { schedule: null } });
      expect(res.ok).toBe(true);
      expect(scheduleOf(await getPackage())).toBeNull();
   });
});
