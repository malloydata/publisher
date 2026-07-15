/// <reference types="bun-types" />

/**
 * HTTP E2E for the givens × `#(authorize)` interaction. Malloy is the single
 * given validator, so on a GATED source a bad given (unknown NAME or wrong-typed
 * VALUE) is caught inside the authorize probe, which fails closed — surfacing as
 * a 403, not the 400 the ungated path returns. This suite pins that documented
 * asymmetry so it stays intentional:
 *
 *   - unknown given name        -> 403 (probe can't bind it -> fails closed)
 *   - authorized + valid givens -> 200 (retargets rows)
 *   - authorize denies          -> 403
 *   - valid name, BAD value     -> 403 (probe can't evaluate it -> fails closed)
 *
 * On an UNGATED source the same unknown name / bad value are a clean 400 (Malloy
 * `runtime-given-*` mapped by model.ts) — see query_givens.integration.spec.ts.
 *
 * See packages/server/src/service/authorize.ts (evaluateAuthorize, fail-closed).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { type RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ENV_NAME = "query-givens-authz-env";
const PKG = "query-givens";
const MODEL = "model.malloy";

type Row = Record<string, unknown>;

describe("givens × authorize on /query (HTTP E2E)", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const fixtureDir = path.resolve(__dirname, "../../fixtures/query-givens");
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

   const queryGated = (body: Record<string, unknown>) =>
      fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/models/${MODEL}/query`,
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               sourceName: "gated_orders",
               queryName: "by_given_region",
               compactJson: true,
               ...body,
            }),
         },
      );

   it("unknown given name -> 403 on a gated source (authorize probe fails closed)", async () => {
      // Even with role=admin, the unknown given makes the authorize probe throw
      // (`runtime-given-unknown`), which the gate treats as not-granting. On an
      // ungated source the same name is a clean 400 (see query_givens suite).
      const res = await queryGated({
         givens: { role: "admin", NOtaGiven: 1 },
      });
      expect(res.status).toBe(403);
   });

   it("authorized caller with valid givens -> 200 and retargets rows", async () => {
      const res = await queryGated({
         givens: { role: "admin", target_region: "EU" },
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { result: string };
      const r = JSON.parse(body.result) as Row[];
      expect(Number(r[0].order_count)).toBe(3);
   });

   it("authorize deny (non-admin role) -> 403", async () => {
      const res = await queryGated({ givens: { role: "guest" } });
      expect(res.status).toBe(403);
   });

   it("valid given name with a bad value -> 403 on a gated source (fail-closed authorize)", async () => {
      // The authorize probe binds the supplied givens; a value it can't evaluate
      // makes the probe throw and the gate denies. On the UNGATED path the same
      // bad value is a 400 (Malloy at prepare time) — this asymmetry is by design.
      const res = await queryGated({
         givens: { role: "admin", min_amount: "not-a-number" },
      });
      expect(res.status).toBe(403);
   });
});
