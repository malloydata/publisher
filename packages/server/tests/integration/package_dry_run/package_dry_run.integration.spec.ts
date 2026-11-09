// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * `POST …/packages?dryRun=true` over HTTP: the tree is loaded and checked as a
 * publish would, the answer is what a publish would answer, and nothing is
 * installed, listed or swapped in for the package already served.
 */

import { MAX_CONCURRENT_DRY_RUNS } from "../../../src/constants";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { MalloyConfig } from "@malloydata/malloy";
import { logger } from "../../../src/logger";
import { Package } from "../../../src/service/package";
import { startRestE2E } from "../../harness/rest_e2e";
import { bootMainServer } from "../../harness/main_server";
import { parseEnvironmentName } from "../../../src/service/identity";

const ENV_NAME = "package-dry-run-env";
const ROWS = `duckdb.sql("select * from (values (1, 'web'), (2, 'store')) as t(id, channel)")`;

describe("a dry-run publish", () => {
   let server: Awaited<ReturnType<typeof startRestE2E>>;
   let api: string;
   let tempDir: string;

   const writePackage = (
      name: string,
      files: Record<string, string>,
      version = "1.0.0",
   ) => {
      const dir = fs.mkdtempSync(path.join(tempDir, `${name}-`));
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name, version }),
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
      expect(json.location).toBe(location);
      expect(json.latestVersion).toBeUndefined();
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

      const next = writePackage(
         "live",
         {
            "orders.malloy": `source: orders is ${ROWS}\n`,
            "extra.malloy": `source: extra is ${ROWS}\n`,
         },
         "1.1.0",
      );
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

   /**
    * Counts `shutdown("close")` on every package's connections while `run`
    * runs: a dry run's package is never served, so it must close its own.
    */
   const closesDuring = async (run: () => Promise<unknown>) => {
      let closed = 0;
      const original = Package.prototype.getMalloyConfig;
      const spy = spyOn(
         Package.prototype,
         "getMalloyConfig",
      ).mockImplementation(function (this: Package) {
         const config = original.call(this);
         const shutdown = config.shutdown.bind(config);
         config.shutdown = async (...args: Parameters<typeof shutdown>) => {
            if (args[0] === "close") closed++;
            return shutdown(...args);
         };
         return config;
      });
      try {
         await run();
      } finally {
         spy.mockRestore();
      }
      return closed;
   };

   it("closes the connections of the package it loaded", async () => {
      const location = writePackage("closes", {
         "orders.malloy": `source: orders is ${ROWS}\n`,
      });
      const closed = await closesDuring(async () =>
         expect((await post("closes", location, true)).status).toBe(200),
      );
      expect(closed).toBeGreaterThanOrEqual(1);
   }, 60_000);

   it("closes the connections of a load the publish checks refuse", async () => {
      const dir = writePackage("refused", {
         "orders.malloy": `source: orders is ${ROWS}\n`,
      });
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({
            name: "refused",
            version: "1.0.0",
            explores: ["missing.malloy"],
         }),
      );
      const closed = await closesDuring(async () =>
         expect((await post("refused", dir, true)).status).toBe(400),
      );
      expect(closed).toBeGreaterThanOrEqual(1);
   }, 60_000);

   it("closes the connections of a load that fails to compile", async () => {
      const shutdown = spyOn(MalloyConfig.prototype, "shutdown");
      try {
         const broken = writePackage("brokenclose", {
            "orders.malloy": `source: orders is ${ROWS} extend { dimension: x is nope }\n`,
         });
         const status = (await post("brokenclose", broken, true)).status;
         expect(status).toBeGreaterThanOrEqual(400);
         expect(status).toBeLessThan(500);
         expect(
            shutdown.mock.calls.filter((call) => call[0] === "close").length,
         ).toBeGreaterThanOrEqual(1);
      } finally {
         shutdown.mockRestore();
      }
   }, 60_000);

   it("is not counted or logged as a load of the package it names", async () => {
      const histogram = (
         Package as unknown as {
            packageLoadHistogram: { record: (...args: unknown[]) => void };
         }
      ).packageLoadHistogram;
      const recorded = spyOn(histogram, "record");
      const errorLog = spyOn(logger, "error");
      try {
         const ok = writePackage("quiet", {
            "orders.malloy": `source: orders is ${ROWS}\n`,
         });
         expect((await post("quiet", ok, true)).status).toBe(200);
         const broken = writePackage("quiet", {
            "orders.malloy": `source: orders is ${ROWS} extend { dimension: x is nope }\n`,
         });
         const refused = (await post("quiet", broken, true)).status;
         expect(refused).toBeGreaterThanOrEqual(400);
         expect(refused).toBeLessThan(500);
         const named = recorded.mock.calls.filter(
            (call) =>
               (call[1] as { malloy_package_name?: string })
                  ?.malloy_package_name === "quiet",
         );
         expect(named).toEqual([]);
         const loadErrors = errorLog.mock.calls.filter((call) =>
            String(call[0]).includes("Error loading package quiet"),
         );
         expect(loadErrors).toEqual([]);
      } finally {
         recorded.mockRestore();
         errorLog.mockRestore();
      }
   }, 60_000);

   it("refuses a dry run past the server's concurrent dry runs with 503, and admits one once a slot frees", async () => {
      const { environmentStore } = await bootMainServer();
      const environment = await environmentStore.getEnvironment(
         parseEnvironmentName(ENV_NAME),
         false,
      );
      const source = writePackage("held", {
         "orders.malloy": `source: orders is ${ROWS}\n`,
      });
      let release!: () => void;
      const downloaded = new Promise<void>((resolve) => {
         release = resolve;
      });
      // Every slot is held while its download waits.
      const held = Array.from({ length: MAX_CONCURRENT_DRY_RUNS }, (_, i) =>
         environment.dryRunInstall(
            `held${i}`,
            async (stagingPath) => {
               await downloaded;
               fs.cpSync(source, stagingPath, { recursive: true });
            },
            { describe: (pkg) => pkg.getPackageName() },
         ),
      );
      const refused = await post("other", source, true);
      expect(refused.status).toBe(503);
      expect(String(refused.json.message)).toContain(
         "as many as it runs at once",
      );
      release();
      await Promise.all(held);
      expect((await post("other", source, true)).status).toBe(200);
   }, 60_000);

   describe("and the version a tree declares", () => {
      it("refuses a tree whose publisher.json declares no version, as a publish does", async () => {
         const location = writePackage("unversioned", {
            "orders.malloy": `source: orders is ${ROWS}\n`,
         });
         fs.writeFileSync(
            path.join(location, "publisher.json"),
            JSON.stringify({ name: "unversioned" }),
         );
         const dry = await post("unversioned", location, true);
         expect(dry.status).toBe(400);
         expect(dry.json.reason).toBe("MANIFEST_VERSION_MISSING");
         const real = await post("unversioned", location, false);
         expect(real.status).toBe(dry.status);
         expect(real.json.reason).toBe(dry.json.reason);
      }, 60_000);

      it("names the version the tree would publish as, and publishes nothing", async () => {
         const dir = writePackage("versioned", {
            "orders.malloy": `source: orders is ${ROWS}\n`,
         });
         fs.writeFileSync(
            path.join(dir, "publisher.json"),
            JSON.stringify({ name: "versioned", version: "1.2.0" }),
         );
         const { status, json } = await post("versioned", dir, true);
         expect(status).toBe(200);
         expect(json.versionId).toBe("1.2.0");
         expect(await listed()).not.toContain("versioned");
      }, 60_000);
   });
});
