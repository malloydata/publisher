// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * An unversioned package, on every route that takes a `versionId`: naming a
 * version it never published is 404 VERSION_NOT_FOUND, never an answer from
 * its one tree as if the version had been honored (a request naming a version
 * and getting some other version's answer is the failure this pins), and
 * naming none serves it exactly as before versions existed.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.resolve(__dirname, "../../fixtures/html-data-apps-test");
const ENV_NAME = "unversioned-package-env";
const PKG = "html-data-apps-test";

describe("an unversioned package and versionId", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   const pkgUrl = (sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}${sub}`;

   async function expectVersionNotFound(res: Response): Promise<void> {
      expect(res.status).toBe(404);
      expect(((await res.json()) as { reason?: string }).reason).toBe(
         "VERSION_NOT_FOUND",
      );
   }

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PKG, location: FIXTURE }],
            connections: [],
         }),
      });
      expect(created.status).toBe(200);
   });

   afterAll(async () => {
      if (env) {
         await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
            method: "DELETE",
         });
         await env.stop();
      }
   });

   it("404s the versions routes that change a version, for a package that has none", async () => {
      const json = { "Content-Type": "application/json" };
      for (const res of [
         await fetch(pkgUrl("/versions/1.0.0"), {
            method: "PATCH",
            headers: json,
            body: JSON.stringify({ archiveStatus: "archive" }),
         }),
         await fetch(pkgUrl("/versions/1.0.0/manifest"), {
            method: "PUT",
            headers: json,
            body: JSON.stringify({ manifestLocation: null }),
         }),
         await fetch(pkgUrl("/latest"), {
            method: "PUT",
            headers: json,
            body: JSON.stringify({ versionId: "1.0.0" }),
         }),
      ]) {
         expect(res.status).toBe(404);
         expect(((await res.json()) as { reason?: string }).reason).toBe(
            "VERSION_NOT_FOUND",
         );
      }
   });

   it("lists no versions for a package that has none", async () => {
      const list = await fetch(pkgUrl("/versions"));
      expect(list.status).toBe(200);
      expect(await list.json()).toEqual([]);
      const one = await fetch(pkgUrl("/versions/1.0.0"));
      expect(one.status).toBe(404);
      expect(((await one.json()) as { reason?: string }).reason).toBe(
         "VERSION_NOT_FOUND",
      );
   });

   it("refuses a versionId the package never published, on the routes that newly declare one", async () => {
      for (const sub of [
         "/data-apps",
         "/events",
         "/connections/duckdb/schemas",
      ]) {
         const res = await fetch(pkgUrl(`${sub}?versionId=1.0.0`));
         expect(res.status).toBe(404);
         expect(((await res.json()) as { reason?: string }).reason).toBe(
            "VERSION_NOT_FOUND",
         );
      }
      const compiled = await fetch(
         pkgUrl("/models/report.malloy/compile?versionId=1.0.0"),
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               source: "query: q is report -> { select: n }",
            }),
         },
      );
      expect(compiled.status).toBe(404);
   });

   it("refuses a versionId on the materialization routes", async () => {
      await expectVersionNotFound(
         await fetch(pkgUrl("/materializations?versionId=1.0.0")),
      );
      // create-materialization carries it in the body (see api-doc.yaml).
      await expectVersionNotFound(
         await fetch(pkgUrl("/materializations"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ versionId: "1.0.0" }),
         }),
      );
      for (const method of ["GET", "DELETE"]) {
         // A run that does not exist would 404 anyway; the version is what
         // is checked first, and its reason says so.
         await expectVersionNotFound(
            await fetch(pkgUrl("/materializations/any-id?versionId=1.0.0"), {
               method,
            }),
         );
      }
   });

   it("still serves those routes when no version is named, an empty one included", async () => {
      expect((await fetch(pkgUrl("/data-apps"))).status).toBe(200);
      expect((await fetch(pkgUrl("/connections/duckdb/schemas"))).status).toBe(
         200,
      );
      expect((await fetch(pkgUrl("/materializations"))).status).toBe(200);
      // A proxy sends an empty versionId; it is absent, not a version.
      expect((await fetch(pkgUrl("/materializations?versionId="))).status).toBe(
         200,
      );
   });

   // Credible's router sends `versionId=` (empty) on its worker calls. It names
   // no version, so it must be served as if absent, never refused.
   it("reads an empty versionId as no version", async () => {
      for (const sub of [
         "/data-apps",
         "/connections/duckdb/schemas",
         "/materializations",
      ]) {
         expect((await fetch(pkgUrl(`${sub}?versionId=`))).status).toBe(200);
      }
      const compiled = await fetch(
         pkgUrl("/models/report.malloy/compile?versionId="),
         {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               source: "query: q is report -> { select: n }",
            }),
         },
      );
      expect(compiled.status).toBe(200);
   });
});
