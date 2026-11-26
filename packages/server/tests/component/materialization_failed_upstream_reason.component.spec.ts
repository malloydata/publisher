// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P5-03: a persisted source whose persisted upstream failed to build fails
 * for its upstream's reason, said in the author's terms: the upstream's name,
 * its file and its own build error, never the compiler's "not found in
 * manifest". End to end through a real Environment and MaterializationService
 * on DuckDB.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as path from "path";
import { Environment } from "../../src/service/environment";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../src/test_helpers/temp_storage";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const BOOM = "bad_build_boom_7311";

const FILES = {
   // Compiles (the schema read never evaluates error()), fails at its build.
   "fail.malloy": [
      "##! experimental.persistence",
      `source: bad_base is duckdb.sql("SELECT error('${BOOM}')::VARCHAR AS who")`,
      "#@ persist",
      "source: bad is bad_base -> { group_by: who }",
      "",
   ].join("\n"),
   "down.malloy": [
      "##! experimental.persistence",
      'import "fail.malloy"',
      `source: base is duckdb.sql("SELECT 'a' AS who")`,
      "#@ persist",
      "source: good is base -> { group_by: who }",
      "#@ persist",
      "source: bad_down is bad -> { group_by: who; aggregate: n is count() }",
      "",
   ].join("\n"),
};

type Failure = { sourceName: string; reason: string };

describe("a persisted source over an upstream that failed to build", () => {
   let root: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restore: () => void;

   beforeEach(async () => {
      restore = installSettings({
         env: { PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      const storage = await openTempStorage("failed-upstream-", {
         id: ENV_ID,
         name: ENV,
      });
      ({ root, db, repo } = storage);
      writePackageFiles(path.join(storage.envPath, PKG), FILES, {
         manifest: { name: PKG, description: "fixture" },
      });
      env = await Environment.create(ENV, storage.envPath, []);
      await env.addPackage(PKG);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
   });

   afterEach(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await disposeTempStorage({ db, root });
      restore();
   });

   async function build(): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, PKG, {});
      const deadline = Date.now() + 60_000;
      for (;;) {
         const m = await repo.getMaterializationById(created.id);
         if (
            m &&
            ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
         ) {
            const running = (
               service as unknown as {
                  runningAbortControllers: Map<string, unknown>;
               }
            ).runningAbortControllers;
            while (running.has(created.id)) {
               await new Promise((resolve) => setTimeout(resolve, 10));
            }
            return (await repo.getMaterializationById(created.id)) ?? m;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((resolve) => setTimeout(resolve, 20));
      }
   }

   it("names the upstream, its file and its build error, never the manifest", async () => {
      const m = await build();
      expect(m.status).toBe("MANIFEST_FILE_READY");
      const failures = Object.values(
         ((m.manifest as { failures?: Record<string, Failure> } | null)
            ?.failures ?? {}) as Record<string, Failure>,
      );
      const byName = new Map(failures.map((f) => [f.sourceName, f.reason]));
      expect([...byName.keys()].sort()).toEqual(["bad", "bad_down"]);
      expect(byName.get("bad")).toContain(BOOM);

      const reason = byName.get("bad_down") ?? "";
      expect(reason).toContain(
         "its persisted upstream 'bad' (fail.malloy) failed to build in this run:",
      );
      expect(reason).toContain(BOOM);
      expect(reason).not.toContain("not found in manifest");
      expect(reason).not.toContain("buildId");
      expect(reason).not.toContain("file://");
   });
});
