// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The package-versions contract is declared in api-doc.yaml ahead of the
 * implementation. Until a route is implemented it must say so with 501, never
 * by answering as if the version had been honored: a request naming a version
 * and getting some other version's answer is the failure this pins.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import path from "path";
import { fileURLToPath } from "url";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.resolve(__dirname, "../../fixtures/html-data-apps-test");
const ENV_NAME = "versions-not-implemented-env";
const PKG = "html-data-apps-test";

describe("package versions, before they are implemented", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   const pkgUrl = (sub: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}${sub}`;

   async function expect501(
      res: Response,
      message = "Version IDs not implemented",
   ): Promise<void> {
      expect(res.status).toBe(501);
      expect(((await res.json()) as { message?: string }).message).toContain(
         message,
      );
   }
   const expectVersionsRoute501 = (res: Response) =>
      expect501(res, "Package versions are not implemented yet");

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

   it("answers 501 on every versions route", async () => {
      const json = { "Content-Type": "application/json" };
      await expectVersionsRoute501(await fetch(pkgUrl("/versions")));
      await expectVersionsRoute501(await fetch(pkgUrl("/versions/1.0.0")));
      await expectVersionsRoute501(
         await fetch(pkgUrl("/versions/1.0.0"), {
            method: "PATCH",
            headers: json,
            body: JSON.stringify({ archiveStatus: "archive" }),
         }),
      );
      await expectVersionsRoute501(
         await fetch(pkgUrl("/versions/1.0.0/manifest"), {
            method: "PUT",
            headers: json,
            body: JSON.stringify({ manifestLocation: null }),
         }),
      );
      await expectVersionsRoute501(
         await fetch(pkgUrl("/latest"), {
            method: "PUT",
            headers: json,
            body: JSON.stringify({ versionId: "1.0.0" }),
         }),
      );
   });

   it("answers 501 for a versionId on the routes that newly declare one", async () => {
      for (const sub of [
         "/data-apps",
         "/events",
         "/connections/duckdb/schemas",
         "/materializations",
      ]) {
         await expect501(await fetch(pkgUrl(`${sub}?versionId=1.0.0`)));
      }
      await expect501(
         await fetch(pkgUrl("/models/report.malloy/compile?versionId=1.0.0"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
               source: "query: q is report -> { select: n }",
            }),
         }),
      );
      // create-materialization carries it in the body (see api-doc.yaml).
      await expect501(
         await fetch(pkgUrl("/materializations"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ versionId: "1.0.0" }),
         }),
      );
   });

   it("still serves those routes when no version is named", async () => {
      expect((await fetch(pkgUrl("/data-apps"))).status).toBe(200);
      expect((await fetch(pkgUrl("/connections/duckdb/schemas"))).status).toBe(
         200,
      );
   });

   // Credible's router sends `versionId=` (empty) on its worker calls. It names
   // no version, so it must be served as if absent, never refused with 501.
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
