// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Upgrading straight to the release that retires the v1 cutover's switch
 * (6g) from one that ran it `off`: a v1 `#@ preaggregate` rollup that release
 * built as `__preagg__` is not this release's, so this release answers live
 * until its first build makes the `__rollup__` table, and the old table is
 * reclaimed by the general run reclaim (deleting the old run with its
 * tables), which replaced the cutover's own rebuild-and-GC spec.
 *
 * The old run cannot be built here (its builder is gone), so it is the
 * record that release left: a settled run whose manifest names the
 * `__preagg__` table under that release's address, and the table itself,
 * marked as this package's.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../access_record";
import { __setPackageLoadPoolForTests } from "../../../package_load/package_load_pool";
import type {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../../storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../../storage/duckdb/schema";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { Environment } from "../../environment";
import type { EnvironmentStore } from "../../environment_store";
import { parsePackageName } from "../../identity";
import { splitManifestEntries } from "./manifest_loader";
import { MaterializationService } from "./materialization_service";
import { ownedTableComment } from "./table_ownership";

const ENV = "e1";
const ENV_ID = "env-1";
const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

const MODEL = `##! experimental { persistence composite_sources }
source: orders is warehouse.sql("""
  SELECT * FROM (VALUES
    (10, 'A'), (20, 'A'), (35, 'B')
  ) AS t(amount, category)
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`;
const QUERY =
   "run: orders -> { group_by: category; aggregate: total; order_by: category }";
const LIVE = [
   { category: "A", total: 30 },
   { category: "B", total: 35 },
];
const OLD_TABLE = "orders__preagg__category__0123abcd";

describe("a v1 rollup table from a release that ran the cutover off, after the upgrade", () => {
   let root: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restoreSettings: () => void;

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: {
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "false",
            // What an upgraded deployment may still set: no longer read.
            PUBLISHER_V1_SERVED_COPY: "off",
         },
      }).restore;
      await __setPackageLoadPoolForTests(null);
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "v1-rollup-upgrade-")),
      );
      const envPath = path.join(root, "env");
      fs.mkdirSync(envPath);
      writePackageFiles(
         path.join(envPath, "pa"),
         { "model.malloy": MODEL },
         { manifest: { name: "pa", description: "fixture" } },
      );
      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db);
      await db.run(
         "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
         [ENV_ID, ENV, envPath],
      );
      repo = createDuckDBRepository(db);
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      await env.addPackage("pa");
      const store = {
         storageManager: { getRepository: () => repo },
         getEnvironment: async () => env,
         getLoadedEnvironments: () => [env],
      } as unknown as EnvironmentStore;
      service = new MaterializationService(store);
   });

   afterEach(async () => {
      await env.closeAllConnections().catch(() => undefined);
      await db.close();
      fs.rmSync(root, { recursive: true, force: true });
      restoreSettings();
      await __setPackageLoadPoolForTests(null);
   });

   async function build(): Promise<Materialization> {
      const created = await service.createMaterialization(ENV, "pa", {});
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

   async function sql(text: string): Promise<Record<string, unknown>[]> {
      const connection = await env.getMalloyConnection("warehouse");
      return (await connection.runSQL(text)).rows as Record<string, unknown>[];
   }

   async function tables(): Promise<string[]> {
      return (
         await sql(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'main' ORDER BY 1",
         )
      ).map((r) => String(r.table_name));
   }

   async function ask(): Promise<{ rows: unknown; servedBy?: string }> {
      const pkg = await env.getPackage(parsePackageName("pa"), false);
      const r = await pkg
         .getModel("model.malloy")!
         .runQuery(unrecordedAudit(), { query: QUERY });
      return {
         rows: (r as unknown as { compactResult: unknown }).compactResult,
         servedBy: r.path?.servedBy?.kind,
      };
   }

   /**
    * The run the earlier release left: settled, its manifest naming the
    * `__preagg__` table at that release's address, the table in the
    * warehouse, marked as this package's, holding wrong totals so an answer
    * read from it is told apart from live.
    */
   async function olderRelease(): Promise<Materialization> {
      await sql(
         `CREATE TABLE ${OLD_TABLE} AS SELECT * FROM (VALUES ('A', 1000), ('B', 1000)) AS t(category, total)`,
      );
      await sql(
         `COMMENT ON TABLE ${OLD_TABLE} IS '${ownedTableComment(`${ENV}/pa`)}'`,
      );
      const id = "f".repeat(64);
      const run = await repo.createMaterialization(
         ENV_ID,
         "pa",
         "MANIFEST_FILE_READY",
      );
      return repo.updateMaterialization(run.id, {
         status: "MANIFEST_FILE_READY",
         completedAt: new Date(Date.now() - 60_000),
         manifest: {
            entries: {
               [id]: {
                  sourceEntityId: id,
                  sourceName: OLD_TABLE,
                  origin: "preaggregate",
                  physicalTableName: OLD_TABLE,
                  connectionName: "warehouse",
               },
            },
         } as never,
      });
   }

   it("answers live until its first build, then from its own __rollup__ table, and drops the old table with the old run", async () => {
      const before = await olderRelease();
      // Its manifest bound, as a restart or a host binds it: not this
      // release's table, so live.
      await env.bindPackageColocatedServeManifest(
         "pa",
         splitManifestEntries(
            before.manifest!.entries as never,
            "the earlier release's run",
         ).tableNameManifest,
      );
      expect(await ask()).toEqual({ rows: LIVE, servedBy: "live" });

      const after = await build();
      expect(after.status).toBe("MANIFEST_FILE_READY");
      const rollup = (await tables()).filter((t) => t.includes("__rollup__"));
      expect(rollup).toHaveLength(1);
      expect(
         Object.values(after.manifest?.entries ?? {}).map(
            (e) => e.physicalTableName,
         ),
      ).not.toContainEqual(expect.stringContaining("__preagg__"));
      expect(await ask()).toEqual({ rows: LIVE, servedBy: "rollup" });

      await service.deleteMaterialization(ENV, "pa", before.id, {
         dropTables: true,
      });
      expect(await tables()).not.toContain(OLD_TABLE);
      expect(await tables()).toContain(rollup[0]);
      expect(await ask()).toEqual({ rows: LIVE, servedBy: "rollup" });
   });

   it("never drops the old table when another package has marked it since", async () => {
      const before = await olderRelease();
      await build();
      await sql(
         `COMMENT ON TABLE ${OLD_TABLE} IS '${ownedTableComment(`${ENV}/other`)}'`,
      );
      await service.deleteMaterialization(ENV, "pa", before.id, {
         dropTables: true,
      });
      expect(await tables()).toContain(OLD_TABLE);
   });
});
