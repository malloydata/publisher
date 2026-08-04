/// <reference types="bun-types" />

/**
 * Load-time render-tag validation surfaces every finding the renderer reports,
 * at the renderer's own severity, on the package's non-fatal `warnings`.
 *
 * The case that matters here is `warn`. A tag that is well-formed but inert
 * where it sits -- `# colspan` outside `# dashboard { columns=N }` -- compiles,
 * loads, and renders without complaint; only the layout is not what the author
 * wrote. Filtering these out left an author with no signal anywhere: nothing is
 * broken at query time, so load time is the only place it can surface.
 *
 * Run over real HTTP against the real Express app because the contract being
 * pinned is the `warnings` array on the package response, which is assembled by
 * Package from Model.validateRenderTags and only exists on that response.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ENV_NAME = "render-tags-test-env";
const PACKAGE_NAME = "render-tags-test";
const fixtureDir = path.resolve(__dirname, "../../fixtures/render-tags-test");

interface Warning {
   model?: string;
   target?: string;
   message?: string;
   severity?: string;
}

describe("load-time render-tag validation", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let warnings: Warning[] = [];

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;

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
         throw new Error(
            `Failed to create test environment (${createRes.status}): ${await createRes.text()}`,
         );
      }

      const deadline = Date.now() + 60_000;
      while (Date.now() < deadline) {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}`,
         );
         if (res.ok) {
            warnings =
               ((await res.json()) as { warnings?: Warning[] }).warnings ?? [];
            return;
         }
         await new Promise((r) => setTimeout(r, 500));
      }
      throw new Error(`Package ${PACKAGE_NAME} did not become available`);
   });

   afterAll(async () => {
      // Always tear down the env so a partially-set-up run can't leave residue
      // in the shared EnvironmentStore for later test files in this process.
      if (baseUrl) {
         try {
            await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
               method: "DELETE",
            });
         } catch {
            /* server already down */
         }
      }
      await env?.stop();
      env = null;
   });

   const findingsOn = (target: string) =>
      warnings.filter((w) => w.target === `nums -> ${target}`);

   it("surfaces the inert colspan as a warn-severity finding", () => {
      // The regression this exists for: severity-filtering to `error` dropped
      // this entirely, so a silently-ignored layout tag had no signal anywhere.
      const found = findingsOn("inert_colspan");
      expect(found).toHaveLength(1);
      expect(found[0].severity).toBe("warn");
      // Pin the message, not just that something fired: the text is what tells
      // an author which tag is inert and how to fix it.
      expect(found[0].message).toContain("Ignored # colspan");
      expect(found[0].message).toContain(
         "colspan only applies in columns mode",
      );
   });

   it("still surfaces a malformed colspan as an error-severity finding", () => {
      const found = findingsOn("invalid_colspan");
      expect(found).toHaveLength(1);
      expect(found[0].severity).toBe("error");
      expect(found[0].message).toContain("Invalid # colspan");
   });

   it("reports nothing for a correctly-attached colspan", () => {
      // Without this, "surface both severities" could be satisfied by reporting
      // every dashboard, which would make the warnings array useless noise.
      expect(findingsOn("clean_dashboard")).toEqual([]);
   });

   it("tags each finding with the model it came from", () => {
      expect(warnings.length).toBeGreaterThan(0);
      for (const w of warnings) {
         expect(w.model).toBe("dashboards.malloy");
      }
   });

   it("does not fail the package load", async () => {
      // Findings are non-fatal by contract: the package with two broken tags
      // still loads and still serves its models.
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PACKAGE_NAME}/models/dashboards.malloy`,
      );
      expect(res.status).toBe(200);
      const model = (await res.json()) as { sources?: { name?: string }[] };
      expect(model.sources?.some((s) => s.name === "nums")).toBe(true);
   });
});
