// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A host instructs a base and its extension by `sourceID`, and the two share
 * one address. The run's own plan refuses the extension (the host read the
 * plan before it did). The base builds its table on the address, and the
 * extension's refusal is reported with the plan's reason in the run's
 * `refusedSources`, never dropped because the base's instruction on the same
 * address applied: every instructed source ends in an entry or a failure.
 * DuckDB. The run's plan is made to hold no table for the extension, as a
 * plan that refused it after the host read it holds none.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import type { Package } from "../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { freshLoadPool } from "../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ENV = "e1";
const PKG = "pkg";

/**
 * `s` extends `a` and adds only a dimension, so the two share one address.
 * `s` places its table in a `storage=` destination its DuckDB connection
 * cannot pass through to, so the plan refuses it; a host that read the plan
 * before that line was added still instructs it, on `a`'s address.
 */
const MODEL = `##! experimental { persistence }
source: raw is warehouse.table('main.ir_raw')

#@ persist name="ir_a"
source: a is raw -> { select: cat, amt }

#@ persist storage="lake"
source: s is a extend { dimension: big is amt > 1 }
`;

describe("a host instructing a base and its refused extension on one address", () => {
   let restore: () => void;
   let root: string;
   let env: Environment;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let service: MaterializationService;
   let pkg: Package;

   beforeAll(async () => {
      restore = installSettings({
         env: {
            PUBLISHER_MATERIALIZE: "true",
            PERSIST_STORAGE_MODE: "off",
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      await freshLoadPool();
      root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "ir-")));
      const envPath = path.join(root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(
         path.join(envPath, PKG),
         { "m.malloy": MODEL },
         { manifest: { name: PKG, description: "fixture" } },
      );
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         ["env-1", ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      env = await Environment.create(
         ENV,
         envPath,
         [
            {
               name: "warehouse",
               type: "duckdb",
               duckdbConnection: { setupSQL: "SELECT 1" },
            } as never,
         ],
         [],
      );
      const warehouse = await env.getMalloyConnection("warehouse");
      await warehouse.runSQL(
         `CREATE TABLE main.ir_raw AS SELECT * FROM (VALUES ('a', 1.0), ('b', 2.0)) AS t(cat, amt)`,
      );
      await env.addPackage(PKG);
      pkg = await env.getPackage(parsePackageName(PKG), false);
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
      // The run's plan holds no table for `s`, as one refused since the host
      // read the plan holds none (a refusal that drops it, such as
      // `upstream_table_unusable` judged against tables bound since):
      // its instruction then resolves to nothing in the loop.
      const real = service.collaborators.executeInstructedBuild;
      (
         service.collaborators as {
            executeInstructedBuild: typeof real;
         }
      ).executeInstructedBuild = (opts) => {
         const keep = (id: string) => !id.startsWith("s@");
         return real({
            ...opts,
            compiled: {
               ...opts.compiled,
               tables: Object.fromEntries(
                  Object.entries(opts.compiled.tables).filter(([id]) =>
                     keep(id),
                  ),
               ),
               builds: opts.compiled.builds.map((b) => ({
                  ...b,
                  order: b.order.filter(keep),
                  dependsOn: Object.fromEntries(
                     Object.entries(b.dependsOn).filter(([id]) => keep(id)),
                  ),
               })),
            },
         });
      };
   }, 300_000);
   afterAll(async () => {
      await env?.closeAllConnections().catch(() => undefined);
      await db?.close();
      fs.rmSync(root, { recursive: true, force: true });
      restore();
      await freshLoadPool();
   });

   it("builds the base's table and reports the extension's plan refusal, beside it", async () => {
      const plan = pkg.getBuildPlan()!;
      const a = Object.values(plan.sources).find((x) => x.name === "a")!;
      const refusal = Object.values(plan.refusedSources ?? {}).find(
         (r) => r.name === "s",
      )!;
      expect(refusal.reason).toBe("storage_source_unsupported");
      const instruction = (sourceID: string) => ({
         sourceEntityId: a.sourceEntityId,
         sourceID,
         materializedTableId: "mt-a",
         physicalTableName: "host_a",
         realization: "COPY" as const,
      });
      const created = await service.createMaterialization(ENV, PKG, {
         buildInstructions: [
            instruction(a.sourceID),
            instruction(refusal.sourceID),
         ],
      });
      let run: Materialization | null = null;
      const deadline = Date.now() + 120_000;
      for (;;) {
         run = await repo.getMaterializationById(created.id);
         if (run && ["MANIFEST_FILE_READY", "FAILED"].includes(run.status)) {
            break;
         }
         if (Date.now() > deadline) throw new Error("run never settled");
         await new Promise((r) => setTimeout(r, 20));
      }
      const manifest = run!.manifest as {
         entries?: Record<string, { physicalTableName?: string }>;
         failures?: Record<string, unknown>;
      };
      expect(run!.status).toBe("MANIFEST_FILE_READY");
      expect(manifest.entries?.[a.sourceEntityId]?.physicalTableName).toBe(
         "host_a",
      );
      expect(manifest.failures?.[a.sourceEntityId]).toBeUndefined();
      const refused = (
         run!.metadata as {
            refusedSources?: Record<string, { reason?: string }>;
         } | null
      )?.refusedSources;
      expect(refused?.[refusal.sourceID]).toMatchObject({
         reason: "storage_source_unsupported",
      });
   });
});
