// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * `GET /api/v0/status` over the real stack: the fields every poller reads are
 * always there, a healthy package has no `loadErrors` entry, and a failed
 * reload leaves a `stale: true` entry for a package `environments` still lists
 * as serving. The store's own rules are unit-tested in
 * `src/service/environment_store.spec.ts`; this pins the body the route sends.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import { writeTempPackage } from "../../../src/test_helpers/temp_package";
import { startPackageEnv, type PackageEnv } from "../../harness/reports";

const ENV = "status-http-env";
const PKG = "status_pkg";

interface Status {
   operationalState?: string;
   version?: string;
   theme?: object;
   environments?: { name: string; packages?: { name: string }[] }[];
   loadErrors?: {
      environment: string;
      package?: string;
      stale?: boolean;
      failedAt?: string;
   }[];
}

describe("GET /status (E2E)", () => {
   let env: PackageEnv;
   let removePackage: () => void;
   const status = () => env.getJson<Status>(`${env.baseUrl}/api/v0/status`);
   // The app is shared with other specs, so only this environment's entries.
   const ownErrors = (s: Status) =>
      (s.loadErrors ?? []).filter((e) => e.environment === ENV);
   const listed = (s: Status) =>
      s.environments
         ?.find((e) => e.name === ENV)
         ?.packages?.some((p) => p.name === PKG);

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

   it("always carries operationalState, version, environments and theme", async () => {
      const s = await status();
      expect(s.operationalState).toBe("serving");
      expect(typeof s.version).toBe("string");
      expect(s.theme).toBeInstanceOf(Object);
      expect(listed(s)).toBe(true);
      expect(ownErrors(s)).toEqual([]);
   });

   it("reports a failed reload as stale while the package keeps serving", async () => {
      fs.writeFileSync(env.served("m.malloy"), "source: s is duckdb.sql(\n");
      const reload = await fetch(env.api("?reload=true"));
      expect(reload.ok).toBe(false);

      const s = await status();
      expect(listed(s)).toBe(true);
      expect(ownErrors(s)).toEqual([
         expect.objectContaining({
            package: PKG,
            stale: true,
            failedAt: expect.any(String),
         }),
      ]);
   });
});
