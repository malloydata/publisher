// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A package's `secureGivens` rides the responses a host reads after a load:
 * the publish (POST), a get, a reload, and a versioned publish. Over HTTP
 * against the real Express app and the real worker-pool load, because the
 * contract is that the load's own compile answers it, on the response body.
 */

import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "secure-givens-env";
const FLAG = "PUBLISHER_PACKAGE_VERSIONING";
const savedFlag = process.env[FLAG];

const ROWS = `duckdb.sql("select * from (values (1, 'a'), (2, 'b')) as t(id, role)")`;

/** Declares the secure given; `orders.malloy` imports it. */
const SHARED = `##! experimental.givens
#(secure)
given: ROLES :: string[]

given: REGION :: string is "us"

source: base is ${ROWS} extend { measure: c is count() }
`;

const ORDERS = `##! experimental.givens
import "shared.malloy"

source: orders is base extend { dimension: tagged is concat(role, $REGION) }
`;

/** No givens at all, secure or otherwise. */
const PLAIN = `source: plain is ${ROWS} extend { measure: c is count() }
`;

type SecureGiven = { name: string; type: string; modelPaths: string[] };

describe("secureGivens on the package responses", () => {
   let env: Awaited<ReturnType<typeof startRestE2E>> | undefined;
   let baseUrl = "";
   let tempDir = "";

   function writePackage(
      name: string,
      files: Record<string, string>,
      manifest: Record<string, unknown> = {},
   ): string {
      const dir = path.join(tempDir, name);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name, ...manifest }),
      );
      for (const [file, text] of Object.entries(files)) {
         fs.mkdirSync(path.dirname(path.join(dir, file)), { recursive: true });
         fs.writeFileSync(path.join(dir, file), text);
      }
      return dir;
   }

   async function publish(
      name: string,
      location: string,
   ): Promise<{ status: number; json: Record<string, unknown> }> {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
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
   }

   async function get(
      name: string,
      query = "",
   ): Promise<Record<string, unknown>> {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}${query}`,
      );
      expect(res.status).toBe(200);
      return (await res.json()) as Record<string, unknown>;
   }

   beforeAll(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "secure-givens-"));
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBeLessThan(300);
   }, 180_000);

   afterEach(() => {
      delete process.env[FLAG];
   });

   afterAll(async () => {
      if (savedFlag === undefined) delete process.env[FLAG];
      else process.env[FLAG] = savedFlag;
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   const EXPECTED: SecureGiven[] = [
      {
         name: "ROLES",
         type: "string[]",
         // The importer surfaces the given too; the notebook does not count.
         modelPaths: ["orders.malloy", "shared.malloy"],
      },
   ];

   const SECURE_FILES = {
      "shared.malloy": SHARED,
      "orders.malloy": ORDERS,
      "nb.malloynb": `>>>malloy\n${SHARED}`,
   };

   it("the publish, get and reload responses carry the summary", async () => {
      // `shared.malloy` is off the surface and still counts.
      const location = writePackage("secure", SECURE_FILES, {
         explores: ["orders.malloy"],
      });
      const created = await publish("secure", location);
      expect(created.status).toBe(200);
      expect(created.json.secureGivens as SecureGiven[]).toEqual(EXPECTED);

      expect((await get("secure")).secureGivens as SecureGiven[]).toEqual(
         EXPECTED,
      );
      expect(
         (await get("secure", "?reload=true")).secureGivens as SecureGiven[],
      ).toEqual(EXPECTED);
   }, 120_000);

   it("a package with no secure given carries an empty array", async () => {
      const location = writePackage("plain", { "plain.malloy": PLAIN });
      const created = await publish("plain", location);
      expect(created.status).toBe(200);
      expect(created.json.secureGivens).toEqual([]);
      expect((await get("plain")).secureGivens).toEqual([]);
   }, 120_000);

   it("a versioned publish carries it too", async () => {
      process.env[FLAG] = "on";
      const location = writePackage("versioned", SECURE_FILES, {
         version: "1.0.0",
      });
      const created = await publish("versioned", location);
      expect(created.status).toBeLessThan(300);
      expect(created.json.versionId).toBe("1.0.0");
      expect(created.json.secureGivens as SecureGiven[]).toEqual(EXPECTED);
      expect((await get("versioned")).secureGivens as SecureGiven[]).toEqual(
         EXPECTED,
      );
   }, 120_000);
});
