// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * HTTP E2E for the givens × `#(authorize)` interaction over the wire. The
 * fixture's gate is a LOCK, so the matrix has three outcomes rather than two,
 * and which one a caller gets turns on WHERE the request fails:
 *
 *   - unknown given name        -> 400 (as ungated; nothing executes)
 *   - authorized + valid givens -> 200 (retargets rows)
 *   - lock denies the caller    -> 403
 *   - valid name, BAD value     -> 400 (as ungated; nothing executes)
 *
 * The 400s fail closed before execution, so the response is identical to the
 * ungated path and reveals nothing about whether a gate exists. The 403 is the
 * lock's own verdict: a caller it does not admit gets no answer about this
 * source at all, rather than a zero-row result it could mistake for data.
 *
 * See packages/server/src/service/authorize_lock.ts (the decision) and
 * `Model.authorizeAndBindRunnable` (the graft, which a refused caller never
 * reaches).
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

   it("lock denies (non-admin role) -> 403", async () => {
      const res = await queryGated({ givens: { role: "guest" } });
      expect(res.status).toBe(403);
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
