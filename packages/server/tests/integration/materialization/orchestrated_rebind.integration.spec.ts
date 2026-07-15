/// <reference types="bun-types" />

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fsp from "fs/promises";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROJECT_NAME = "rebind-safety-project";
const PACKAGE_NAME = "persist-schedule-warn-test";
const API = `/api/v0/environments/${PROJECT_NAME}/packages/${PACKAGE_NAME}`;

// Guards the orchestrated control plane's post-build rebind PATCH against the
// editable-policy gate added in this PR. The fixture loads with a pre-existing,
// load-tolerated persistence-policy warning (a schedule without scope: version),
// so if the policy gate ran on a rebind PATCH it would reject and fail the
// orchestrated run. These tests assert it does not: a manifestLocation-only
// PATCH, and a PATCH carrying explicit scope: null / materialization: null, both
// succeed and leave scope/materialization untouched (null == absent, not a wipe).
describe("Orchestrated rebind PATCH safety", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const fixtureDir = path.resolve(
         __dirname,
         "../../fixtures/persist-schedule-warn-test",
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

   it("loads the fixture despite its load-tolerated policy warning", async () => {
      // scope defaults to "package" (no scope in the fixture), and the schedule
      // is present — the exact combination that warns but is load-tolerated.
      const pkg = await getPackage();
      expect(pkg.scope ?? "package").toBe("package");
      expect(scheduleOf(pkg)).toBe("0 6 * * *");
   });

   it("a manifestLocation-only rebind PATCH succeeds and preserves scope/materialization", async () => {
      const manifestFile = path.join(
         os.tmpdir(),
         `rebind-empty-manifest-${PROJECT_NAME}.json`,
      );
      await fsp.writeFile(
         manifestFile,
         JSON.stringify({
            builtAt: new Date(0).toISOString(),
            strict: false,
            entries: {},
         }),
         "utf8",
      );

      // The control plane's finalize PATCH: name/manifestLocation only, no
      // scope/materialization keys. Must not run the policy gate.
      const res = await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            manifestLocation: manifestFile,
         }),
      });
      expect(res.ok).toBe(true);

      const pkg = await getPackage();
      expect(pkg.scope ?? "package").toBe("package");
      expect(scheduleOf(pkg)).toBe("0 6 * * *");

      // Reset the binding so the next test starts from the live package.
      await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({ name: PACKAGE_NAME, manifestLocation: null }),
      });
   });

   it("a PATCH with explicit scope: null / materialization: null preserves both (null == absent, no wipe, no reject)", async () => {
      const res = await fetch(`${baseUrl}${API}`, {
         method: "PATCH",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: PACKAGE_NAME,
            scope: null,
            materialization: null,
         }),
      });
      // The gate must treat null as absent: not rejected by the warning...
      expect(res.ok).toBe(true);
      // ...and the persisted policy is preserved, not wiped.
      const pkg = await getPackage();
      expect(pkg.scope ?? "package").toBe("package");
      expect(scheduleOf(pkg)).toBe("0 6 * * *");
   });
});
