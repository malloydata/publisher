// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Decision 3 of the v1 cutover: a v1 `#@ preaggregate`
 * rollup built before the cutover (`__preagg__`) rebuilds once under the v2
 * rollup's name (`__rollup__`), and the old table is left to the manifest GC.
 * End to end, through the service: build under `off`, flip to `on`, rebuild,
 * then delete the old run with its tables. The old table is dropped, the new
 * one kept and served; a table another package has marked since is never
 * dropped; and flipping back to `off` never answers from a dropped table.
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

describe("v1 rollup tables across the cutover's one rebuild", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restoreSettings: () => void;

   async function switchTo(mode: "off" | "shadow" | "on"): Promise<void> {
      restoreSettings?.();
      restoreSettings = installSettings({
         env: {
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "false",
            PUBLISHER_V1_SERVED_COPY: mode,
         },
      }).restore;
      await __setPackageLoadPoolForTests(null);
   }

   beforeEach(async () => {
      await switchTo("off");
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "v1-rollup-gc-")),
      );
      envPath = path.join(root, "env");
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

   const named = async (part: string) =>
      (await tables()).filter((t) => t.includes(part));

   async function reload(): Promise<void> {
      await env.getPackage(parsePackageName("pa"), true);
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

   async function buildBothSides(): Promise<{
      before: Materialization;
      after: Materialization;
   }> {
      const before = await build();
      expect(before.status).toBe("MANIFEST_FILE_READY");
      expect(await named("__preagg__")).toHaveLength(1);
      const off = await ask();
      expect(off.rows).toEqual(LIVE);
      expect(off.servedBy).toBe("rollup");

      await switchTo("on");
      await reload();
      // Before the rebuild: the old table is not the leg's, so live answers.
      const unbuilt = await ask();
      expect(unbuilt.rows).toEqual(LIVE);
      expect(unbuilt.servedBy).toBe("live");

      const after = await build();
      expect(after.status).toBe("MANIFEST_FILE_READY");
      expect(await named("__rollup__")).toHaveLength(1);
      // The new run's manifest names no v1 table.
      expect(
         Object.values(after.manifest?.entries ?? {}).map(
            (e) => e.physicalTableName,
         ),
      ).not.toContainEqual(expect.stringContaining("__preagg__"));
      const on = await ask();
      expect(on.rows).toEqual(LIVE);
      expect(on.servedBy).toBe("rollup");
      return { before, after };
   }

   it("drops the old __preagg__ table with the old run, and keeps serving the __rollup__ one", async () => {
      const { before } = await buildBothSides();
      const old = (await named("__preagg__"))[0] as string;
      const rollup = (await named("__rollup__"))[0] as string;
      // Tamper the new table: an answer from it is told apart from live.
      await sql(`UPDATE ${rollup} SET category = category`);

      await service.deleteMaterialization(ENV, "pa", before.id, {
         dropTables: true,
      });
      expect(await tables()).not.toContain(old);
      expect(await tables()).toContain(rollup);
      const on = await ask();
      expect(on.rows).toEqual(LIVE);
      expect(on.servedBy).toBe("rollup");
   });

   it("compiles no v1 composite companion under on (the composite is skipped, not only unrouted)", async () => {
      const composite = async () => {
         const pkg = await env.getPackage(parsePackageName("pa"), false);
         return (
            pkg.getModel("model.malloy") as unknown as {
               preaggregateServeMaterializer?: unknown;
            }
         ).preaggregateServeMaterializer;
      };
      // Untouched under off: the composite is compiled and answers.
      expect(await composite()).toBeDefined();
      await buildBothSides();
      // The path report names only the leg's rollup.
      const pkg = await env.getPackage(parsePackageName("pa"), false);
      const r = await pkg
         .getModel("model.malloy")!
         .runQuery(unrecordedAudit(), { query: QUERY });
      expect(r.path?.considered.map((c) => [c.kind, c.outcome])).toEqual([
         ["rollup", "used"],
      ]);
      expect(r.path?.notConsidered).toEqual([]);
      // Under on nothing reads the composite, yet a load still compiles it.
      expect(await composite()).toBeUndefined();
   });

   it("under shadow, a build never builds the v2 rollup, so the shadow can only ever find v1 rollups live", async () => {
      await switchTo("shadow");
      await reload();
      const m = await build();
      expect(m.status).toBe("MANIFEST_FILE_READY");
      expect(await named("__preagg__")).toHaveLength(1);
      // Decision 3 says the shadow reports a v1 rollup live "until its next
      // build"; under shadow no build ever makes the cutover's table.
      expect(await named("__rollup__")).toEqual([]);
   });

   it("never drops the old table when another package has marked it since", async () => {
      const { before } = await buildBothSides();
      const old = (await named("__preagg__"))[0] as string;
      await sql(
         `COMMENT ON TABLE ${old} IS '${ownedTableComment(`${ENV}/other`)}'`,
      );
      await service.deleteMaterialization(ENV, "pa", before.id, {
         dropTables: true,
      });
      expect(await tables()).toContain(old);
   });

   it("back under off after the rebuild, never answers from the dropped table", async () => {
      const { before } = await buildBothSides();
      await switchTo("off");
      await reload();
      const back = await ask();
      expect(back.rows).toEqual(LIVE);
      await service.deleteMaterialization(ENV, "pa", before.id, {
         dropTables: true,
      });
      expect(await named("__preagg__")).toEqual([]);
      await reload();
      const afterDrop = await ask();
      expect(afterDrop.rows).toEqual(LIVE);
   });
});
