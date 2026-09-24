// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * HTTP end-to-end for `./filter_binding_guard.ts` (the fix for
 * `docs/authorize.md`'s former "Known hole"): proves the guard runs on the
 * REAL request path — through the express route, `internalErrorToHttpError`,
 * and `Model.getQueryResults` — not just against the in-process `Model`
 * helpers `filter_binding_guard_integration.spec.ts` exercises.
 *
 * `gated_parent` is gated `org_id in $GROUPS`; `org_id`/`owner` are
 * deliberately different per row (see the fixture model) so a misbound query
 * would return a DIFFERENT row set, not merely an empty one.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { type RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ENV_NAME = "filter-binding-guard-env";
const PKG = "filter-binding-guard";
const MODEL = "model.malloy";

describe("filter binding guard (HTTP E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const fixtureDir = path.resolve(
         __dirname,
         "../../fixtures/filter-binding-guard",
      );
      const createRes = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PKG, location: fixtureDir }],
            connections: [],
         }),
      });
      if (!createRes.ok) {
         throw new Error(
            `Failed to create test environment (${createRes.status}): ${await createRes.text()}`,
         );
      }
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}`,
         );
         if (res.ok) break;
         await new Promise((r) => setTimeout(r, 250));
      }
   });

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => {});
      await env?.stop();
   });

   const query = (body: Record<string, unknown>) =>
      fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/models/${MODEL}/query`,
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ compactJson: true, ...body }),
         },
      );

   it("querying the declaring source directly still serves, filtered correctly", async () => {
      const res = await query({
         query: "run: gated_parent -> { aggregate: n is count() }",
         givens: { GROUPS: [1] },
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { result: string };
      const rows = JSON.parse(body.result) as { n: number }[];
      expect(rows[0].n).toBe(2);
   });

   it("except:-ing the gated column then rename:-ing another onto its exact name denies (403), not misbinds", async () => {
      const res = await query({
         query:
            "run: gated_parent extend { except: org_id } extend { rename: org_id is owner } -> " +
            "{ group_by: id; aggregate: n is count() }",
         givens: { GROUPS: [1] },
      });
      expect(res.status).toBe(403);
      const json = (await res.json()) as { code: number; message: string };
      expect(json.code).toBe(403);
      expect(json.message).toBe('Access denied for source "gated_parent".');
      // Redaction: the 403 must not leak the gate's own field name.
      expect(json.message).not.toContain("org_id");
      expect(json.message).not.toContain("GROUPS");
   });

   it("renaming a field the gate does not read still serves (negative)", async () => {
      const res = await query({
         query:
            "run: gated_parent extend { rename: label is val } -> " +
            "{ aggregate: n is count() }",
         givens: { GROUPS: [1] },
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { result: string };
      const rows = JSON.parse(body.result) as { n: number }[];
      expect(rows[0].n).toBe(2);
   });

   it("a caller-text inline extend redeclaring the gated column as a constant denies (403), not misbinds", async () => {
      const res = await query({
         query:
            "run: gated_parent extend { except: org_id; dimension: org_id is 1 } -> " +
            "{ group_by: id; aggregate: n is count() }",
         givens: { GROUPS: [1] },
      });
      expect(res.status).toBe(403);
      const json = (await res.json()) as { code: number; message: string };
      expect(json.code).toBe(403);
      expect(json.message).toBe('Access denied for source "gated_parent".');
      expect(json.message).not.toContain("org_id");
      expect(json.message).not.toContain("GROUPS");
   });

   it("a plain where: filter with no access_filter annotation stays bound when the caller re-extends it inline", async () => {
      const legit = await query({
         query: "run: where_filtered -> { aggregate: n is count() }",
      });
      expect(legit.status).toBe(200);
      const legitBody = (await legit.json()) as { result: string };
      expect((JSON.parse(legitBody.result) as { n: number }[])[0].n).toBe(2);

      const res = await query({
         query:
            "run: where_filtered extend { except: org_id; dimension: org_id is 1 } -> " +
            "{ aggregate: n is count() }",
      });
      expect(res.status).toBe(403);
      const json = (await res.json()) as { code: number; message: string };
      expect(json.code).toBe(403);
      // No `#(access_filter)`/`#(authorize)` gate is declared here at all, so
      // this denial comes from `assertNoMisboundInheritedFilters`'s own catch
      // (not the gate's), which names the struct by its PHYSICAL identity
      // rather than the model's local alias — still a 403, still no data.
      expect(json.message).toMatch(/^Access denied for source ".+"\.$/);
      expect(json.message).not.toContain("org_id");
   });
});
