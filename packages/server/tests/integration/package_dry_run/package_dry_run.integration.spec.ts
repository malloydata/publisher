// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * `POST …/packages?dryRun=true` over HTTP: the tree is loaded and checked as a
 * publish would, the answer is what a publish would answer, and nothing is
 * installed, listed or swapped in for the package already served.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "package-dry-run-env";
const ROWS = `duckdb.sql("select * from (values (1, 'web'), (2, 'store')) as t(id, channel)")`;

describe("a dry-run publish", () => {
   let server: Awaited<ReturnType<typeof startRestE2E>>;
   let api: string;
   let tempDir: string;

   const writePackage = (name: string, files: Record<string, string>) => {
      const dir = fs.mkdtempSync(path.join(tempDir, `${name}-`));
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name }),
      );
      for (const [file, text] of Object.entries(files)) {
         fs.writeFileSync(path.join(dir, file), text);
      }
      return dir;
   };
   const post = async (name: string, location: string, dryRun: boolean) => {
      const res = await fetch(
         `${api}/environments/${ENV_NAME}/packages${dryRun ? "?dryRun=true" : ""}`,
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name, location }),
         },
      );
      return {
         status: res.status,
         json: (await res.json()) as Record<string, unknown>,
      };
   };
   const get = async (rel: string) => {
      const res = await fetch(`${api}${rel}`);
      return {
         status: res.status,
         json: (await res.json()) as Record<string, unknown>,
      };
   };
   const listed = async () =>
      (
         (await get(`/environments/${ENV_NAME}/packages`)).json as unknown as {
            name: string;
         }[]
      ).map((p) => p.name);

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), `${ENV_NAME}-`));
      server = await startRestE2E();
      api = `${server.baseUrl}/api/v0`;
      const created = await fetch(`${api}/environments`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBeLessThan(300);
   }, 120_000);

   afterAll(async () => {
      await fetch(`${api}/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await server.stop();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   it("answers a loadable tree with the package as it would load, and installs nothing", async () => {
      const location = writePackage("fresh", {
         "orders.malloy": `source: orders is ${ROWS}\n`,
      });
      const { status, json } = await post("fresh", location, true);
      expect(status).toBe(200);
      expect(json.name).toBe("fresh");
      expect(await listed()).not.toContain("fresh");
      expect(
         (await get(`/environments/${ENV_NAME}/packages/fresh`)).status,
      ).toBe(404);
   }, 60_000);

   it("answers a tree that does not compile with the refusal a publish would give", async () => {
      const location = writePackage("broken", {
         "orders.malloy": `source: orders is ${ROWS} extend { dimension: x is nope }\n`,
      });
      const dry = await post("broken", location, true);
      const real = await post(
         "broken",
         writePackage("broken", {
            "orders.malloy": `source: orders is ${ROWS} extend { dimension: x is nope }\n`,
         }),
         false,
      );
      expect(dry.status).toBeGreaterThanOrEqual(400);
      expect(dry.status).toBeLessThan(500);
      expect(dry.status).toBe(real.status);
      expect(await listed()).not.toContain("broken");
   }, 60_000);

   it("leaves the package already served under the name untouched", async () => {
      const served = writePackage("live", {
         "orders.malloy": `source: orders is ${ROWS}\n`,
      });
      expect((await post("live", served, false)).status).toBe(200);
      const before = (
         await get(`/environments/${ENV_NAME}/packages/live/models`)
      ).json;

      const next = writePackage("live", {
         "orders.malloy": `source: orders is ${ROWS}\n`,
         "extra.malloy": `source: extra is ${ROWS}\n`,
      });
      expect((await post("live", next, true)).status).toBe(200);
      const after = (
         await get(`/environments/${ENV_NAME}/packages/live/models`)
      ).json;
      expect(after).toEqual(before);
   }, 60_000);

   it("refuses a dry run without a location", async () => {
      const res = await fetch(
         `${api}/environments/${ENV_NAME}/packages?dryRun=true`,
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ name: "nowhere" }),
         },
      );
      expect(res.status).toBe(400);
   }, 30_000);
});
