/// <reference types="bun-types" />

/**
 * HTTP E2E for the givens × `#(authorize)` interaction. Every gate is a row
 * filter, so there is no separate admit/deny probe to fail closed and no
 * gated-vs-ungated asymmetry left to pin. A bad given (unknown NAME or
 * wrong-typed VALUE) is now the SAME clean 400 on a gated source as on an
 * ungated one, and the gate's own verdict is expressed in the rows returned:
 *
 *   - unknown given name        -> 400 (as ungated; nothing executes)
 *   - authorized + valid givens -> 200 (retargets rows)
 *   - authorize denies          -> 200 with ZERO rows
 *   - valid name, BAD value     -> 400 (as ungated; nothing executes)
 *
 * The 400s still fail closed — they are refused before execution, so no row is
 * served and the response is identical to the ungated path, revealing nothing
 * about whether a gate exists. Only a package-level FGA denial is still a 403.
 *
 * See packages/server/src/service/authorize.ts and
 * `Model.authorizeAndBindRunnable` (the graft that applies the filter).
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

   it("unknown given name -> 400 on a gated source, same as ungated", async () => {
      // `runtime-given-unknown` is raised before anything executes, so this
      // refuses without serving a row and without disclosing that `gated` is
      // gated at all — the ungated path returns the identical 400.
      const res = await queryGated({
         givens: { role: "admin", NOtaGiven: 1 },
      });
      expect(res.status).toBe(400);
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

   it("authorize deny (non-admin role) -> 200 with zero rows", async () => {
      // The gate is a row filter, so a caller it excludes gets the query's own
      // result schema with nothing in it rather than a 403.
      const res = await queryGated({ givens: { role: "guest" } });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { result: string };
      const r = JSON.parse(body.result) as Row[];
      expect(Number(r[0].order_count)).toBe(0);
   });

   it("valid given name with a bad value -> 400 on a gated source, same as ungated", async () => {
      // `runtime-given-bad-value` is raised at prepare time, before the graft
      // runs — refused without serving a row, identical to the ungated path.
      const res = await queryGated({
         givens: { role: "admin", min_amount: "not-a-number" },
      });
      expect(res.status).toBe(400);
   });
});
