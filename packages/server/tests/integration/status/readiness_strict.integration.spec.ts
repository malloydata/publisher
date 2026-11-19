// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * `GET /health/readiness?strict=true` over the real stack: 503 naming each
 * package that failed to load or serves a stale model, 200 when everything
 * loaded, while plain `/health/readiness` stays 200 once the server is ready
 * in every case. The probe's own rules are unit-tested in
 * `src/health.spec.ts`; this pins what the store feeds it.
 */

import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import fs from "fs";
import { writeTempPackage } from "../../../src/test_helpers/temp_package";
import { startPackageEnv, type PackageEnv } from "../../harness/reports";
import {
   spawnServer,
   type SpawnedServer,
   writeSourcePackage,
} from "../../harness/spawn_server";

setDefaultTimeout(150_000);

interface Readiness {
   status: string;
   loadErrors?: { environment: string; package?: string; stale?: boolean }[];
}

async function readiness(
   baseUrl: string,
   query = "",
): Promise<{ status: number; body: Readiness }> {
   const res = await fetch(`${baseUrl}/health/readiness${query}`);
   return { status: res.status, body: (await res.json()) as Readiness };
}

describe("strict readiness with a package that failed to load", () => {
   const ENV = "strict-failed-env";
   let server: SpawnedServer;
   const cleanups: (() => void)[] = [];

   beforeAll(async () => {
      const good = writeSourcePackage("good");
      const broken = writeSourcePackage("broken", {
         "broken.malloy": "source: broken is duckdb.sql(\n",
      });
      cleanups.push(good.cleanup, broken.cleanup);
      server = await spawnServer({
         config: {
            frozenConfig: false,
            environments: [
               {
                  name: ENV,
                  packages: [
                     { name: "good", location: good.dir },
                     { name: "broken", location: broken.dir },
                  ],
                  connections: [],
               },
            ],
         },
      });
      await server.waitUntilServing();
   });

   afterAll(async () => {
      await server?.stop();
      for (const cleanup of cleanups) cleanup();
   });

   it("plain readiness is 200 once ready", async () => {
      expect(await readiness(server.baseUrl)).toEqual({
         status: 200,
         body: { status: "UP" },
      });
   });

   it("strict readiness is 503 and names the package", async () => {
      const { status, body } = await readiness(server.baseUrl, "?strict=true");
      expect(status).toBe(503);
      expect(body.status).toBe("DOWN");
      expect(body.loadErrors).toEqual([
         expect.objectContaining({ environment: ENV, package: "broken" }),
      ]);
   });

   it("the MCP listener answers the same", async () => {
      const res = await fetch(
         `http://127.0.0.1:${server.mcpPort}/health/readiness?strict=true`,
      );
      expect(res.status).toBe(503);
   });
});

describe("strict readiness with a stale package", () => {
   const ENV = "strict-stale-env";
   const PKG = "strict_pkg";
   let env: PackageEnv;
   let removePackage: (() => void) | undefined;
   const ownErrors = (body: Readiness) =>
      (body.loadErrors ?? []).filter((e) => e.environment === ENV);

   beforeAll(async () => {
      const { dir, cleanup } = writeTempPackage(
         { "m.malloy": 'source: s is duckdb.sql("select 1 as n")\n' },
         { name: PKG },
      );
      removePackage = cleanup;
      env = await startPackageEnv(ENV, { [PKG]: dir });
   }, 300_000);

   afterAll(async () => {
      await env?.stop();
      removePackage?.();
   });

   it("is 200 while everything loaded", async () => {
      const { status, body } = await readiness(env.baseUrl, "?strict=true");
      expect(ownErrors(body)).toEqual([]);
      // The suite runs specs in one process, so another spec's environment
      // may be the one failing; with none, this is the all-good answer.
      if ((body.loadErrors ?? []).length === 0) {
         expect({ status, body }).toEqual({
            status: 200,
            body: { status: "UP" },
         });
      } else {
         expect(status).toBe(503);
      }
   });

   it("is 503 naming the package once a reload fails, and plain readiness stays 200", async () => {
      fs.writeFileSync(env.served("m.malloy"), "source: s is duckdb.sql(\n");
      const reload = await fetch(env.api("?reload=true"));
      expect(reload.ok).toBe(false);

      const strict = await readiness(env.baseUrl, "?strict=true");
      expect(strict.status).toBe(503);
      expect(ownErrors(strict.body)).toEqual([
         expect.objectContaining({ package: PKG, stale: true }),
      ]);
      expect((await readiness(env.baseUrl)).status).toBe(200);
   });
});
