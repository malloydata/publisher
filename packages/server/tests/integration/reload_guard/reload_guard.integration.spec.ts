/// <reference types="bun-types" />

/**
 * Two ways a `reload` request can fail to mean what the caller thought, both
 * answered rather than ignored:
 *
 *   - on a collection route, where reload has no meaning at all, because reload
 *     recompiles one named resource;
 *   - on a per-resource route, where the value is not one the route can honor
 *     (`?reload=1`, `?reload=yes`, `?reload=TRUE`).
 *
 * Both used to answer 200, which reads as a reload that worked: the caller edits
 * a model, sees 200, and queries a model the server never recompiled. The value
 * case is the sneakier of the two, since the route and the parameter are both
 * right and only the spelling is wrong.
 *
 * Covered over HTTP against the real Express app, because the contract being
 * pinned is the status code and the message a caller reads, and both are
 * produced by the route rather than by anything unit-testable underneath it.
 * (Which values are valid, and why, is pinned in `query_param_utils.spec.ts`.)
 * The guards fire before any environment lookup, so no fixture is needed and a
 * deliberately nonexistent environment name is enough.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const MISSING_ENV = "no-such-environment";

describe("collection routes refuse ?reload", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
   });

   afterAll(async () => {
      await env?.stop();
      env = null;
   });

   const body = async (path: string) => {
      const res = await fetch(`${baseUrl}${path}`);
      return {
         status: res.status,
         json: (await res.json()) as { message: string },
      };
   };

   it("packages collection: 400 naming the per-package route", async () => {
      const { status, json } = await body(
         `/api/v0/environments/${MISSING_ENV}/packages?reload=true`,
      );
      expect(status).toBe(400);
      // The message has to carry the route that works, filled in with the
      // caller's own environment name — that is the whole point of the refusal.
      expect(json.message).toContain(
         `/api/v0/environments/${MISSING_ENV}/packages/{packageName}?reload=true`,
      );
   });

   it("environments collection: 400 naming the per-environment route", async () => {
      const { status, json } = await body(`/api/v0/environments?reload=true`);
      expect(status).toBe(400);
      expect(json.message).toContain(
         "/api/v0/environments/{environmentName}?reload=true",
      );
   });

   it("refuses any value, not just reload=true", async () => {
      // A collection does not model `reload` at all, so `reload=false` is an
      // assertion about a parameter that has no meaning here rather than a
      // request for no reload. Refusing every value keeps the two collections
      // consistent with each other and with the versionId guard alongside them.
      for (const value of ["false", "0", ""]) {
         const { status } = await body(
            `/api/v0/environments/${MISSING_ENV}/packages?reload=${value}`,
         );
         expect(status).toBe(400);
      }
   });

   it("still lists normally with no reload parameter", async () => {
      // The guard must not have turned the collections into 400-always routes.
      const res = await fetch(`${baseUrl}/api/v0/environments`);
      expect(res.status).toBe(200);
      expect(Array.isArray(await res.json())).toBe(true);
   });

   it("the per-resource routes still accept reload", async () => {
      // These honor `reload`, so they must get past the guard and fail on the
      // environment instead — 404, not the collection's 400. Pinning the status
      // rather than just the message is what catches the guard being attached
      // to the wrong route.
      for (const path of [
         `/api/v0/environments/${MISSING_ENV}?reload=true`,
         `/api/v0/environments/${MISSING_ENV}/packages/nope?reload=true`,
      ]) {
         const { status, json } = await body(path);
         expect(status).toBe(404);
         expect(json.message).toContain(MISSING_ENV);
      }
   });

   // The per-resource half. `reload=true` and `reload=false` are the only
   // spellings of a boolean the API declares, and anything else is refused
   // instead of silently read as "do not reload".
   describe("per-resource routes refuse an unusable ?reload value", () => {
      const PER_RESOURCE = [
         `/api/v0/environments/${MISSING_ENV}`,
         `/api/v0/environments/${MISSING_ENV}/packages/nope`,
         // The legacy `/projects/...` twins are the same routes under the
         // pre-rename surface, so a typo'd value has to fail there identically —
         // otherwise the bug just moves to the older path.
         `/api/v0/projects/${MISSING_ENV}`,
         `/api/v0/projects/${MISSING_ENV}/packages/nope`,
      ];

      it("400s on a value that is neither true nor false", async () => {
         for (const base of PER_RESOURCE) {
            for (const value of ["1", "yes", "TRUE", "0", ""]) {
               const { status, json } = await body(`${base}?reload=${value}`);
               expect(status).toBe(400);
               // The message has to quote the value back and name a form that
               // works, or the caller cannot tell what it did wrong.
               expect(json.message).toContain("Invalid reload value");
               expect(json.message).toContain(`?reload=true`);
            }
         }
      });

      it("400s on a repeated parameter", async () => {
         // No single value to honor, so it cannot be read as either boolean.
         const { status, json } = await body(
            `/api/v0/environments/${MISSING_ENV}?reload=true&reload=1`,
         );
         expect(status).toBe(400);
         expect(json.message).toContain("Invalid reload value");
      });

      it("accepts reload=false, and absent, without reloading", async () => {
         // These are valid and must get past the guard to the real lookup (404
         // on a nonexistent environment) rather than being lumped in with the
         // typos — `reload=false` is a legitimate "just read it" request.
         for (const base of PER_RESOURCE) {
            for (const suffix of ["?reload=false", ""]) {
               const { status } = await body(`${base}${suffix}`);
               expect(status).toBe(404);
            }
         }
      });
   });
});
