/// <reference types="bun-types" />

/**
 * Reload is per-resource. A collection route cannot serve it, so it must refuse
 * rather than answer 200 with the list — a 200 there reads as a reload that
 * worked, and the caller goes on to query a model the server never recompiled.
 *
 * Covered over HTTP against the real Express app, because the contract being
 * pinned is the status code and the message a caller reads, and both are
 * produced by the route rather than by anything unit-testable underneath it.
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
});
