// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A fleet mid-rollout of `PUBLISHER_V1_SERVED_COPY` has workers on both
 * sides, and a host may bind a manifest built on one side to a worker on the
 * other. A v1 `#@ preaggregate` rollup is `__preagg__` (off) on one side and
 * `__rollup__` (on) on the other, with different `sourceEntityId`s. Each
 * worker must match a manifest entry only against its own plan, so a foreign
 * entry costs a rebuild (the query answers live) and never an answer read
 * from the other side's table.
 *
 * Each side's table is tampered with its own offset, so an answer read from
 * either is told apart from live and from the other: a foreign entry alone
 * answers live, and beside the worker's own entry, the worker reads its own.
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

describe("a host manifest from the other side of the v1 cutover (mixed fleet)", () => {
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
         fs.mkdtempSync(path.join(os.tmpdir(), "v1-mixed-fleet-")),
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

   async function tamper(part: string, offset: number): Promise<void> {
      const [table] = await named(part);
      const columns = (
         await sql(
            `SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = '${table}'`,
         )
      )
         .filter((c) => !String(c.column_name).includes("category"))
         .filter((c) =>
            /INT|DOUBLE|DECIMAL|FLOAT|NUMERIC/i.test(String(c.data_type)),
         )
         .map((c) => String(c.column_name));
      expect(columns.length).toBeGreaterThan(0);
      await sql(
         `UPDATE "${table}" SET ${columns.map((c) => `"${c}" = "${c}" + ${offset}`).join(", ")}`,
      );
   }

   /** Bind `entries` to the package as a host does, through its manifestLocation. */
   async function bindHostManifest(
      entries: Record<string, unknown>,
   ): Promise<void> {
      const file = path.join(
         root,
         `manifest-${Date.now()}-${Math.random()}.json`,
      );
      fs.writeFileSync(
         file,
         JSON.stringify({
            builtAt: new Date().toISOString(),
            strict: false,
            entries,
         }),
      );
      await env.updatePackage("pa", { name: "pa", manifestLocation: file });
   }

   const shifted = (offset: number) =>
      LIVE.map((row) => ({ ...row, total: row.total + offset }));

   it("ignores a foreign entry in both directions, and reads only its own beside one", async () => {
      // Off: the v1 composite's __preagg__ table.
      const off = await build();
      expect(off.status).toBe("MANIFEST_FILE_READY");
      const offEntries = off.manifest?.entries ?? {};
      expect(Object.values(offEntries).map((e) => e.physicalTableName)).toEqual(
         [expect.stringContaining("__preagg__")],
      );

      // On: the rollup leg's __rollup__ table.
      await switchTo("on");
      await reload();
      const on = await build();
      expect(on.status).toBe("MANIFEST_FILE_READY");
      const onEntries = on.manifest?.entries ?? {};
      expect(Object.values(onEntries).map((e) => e.physicalTableName)).toEqual([
         expect.stringContaining("__rollup__"),
      ]);
      // Different content addresses, so no key is shared.
      expect(Object.keys(offEntries).filter((id) => id in onEntries)).toEqual(
         [],
      );

      await tamper("__preagg__", 1000);
      await tamper("__rollup__", 2000);

      // An on worker, handed only the off side's entry: live.
      await bindHostManifest(offEntries);
      let answer = await ask();
      expect(answer.rows).toEqual(LIVE);
      expect(answer.servedBy).toBe("live");
      // Both sides' entries: its own table, never the foreign one.
      await bindHostManifest({ ...offEntries, ...onEntries });
      answer = await ask();
      expect(answer.rows).toEqual(shifted(2000));
      expect(answer.servedBy).toBe("rollup");

      // An off worker, handed only the on side's entry: live.
      await switchTo("off");
      await reload();
      await bindHostManifest(onEntries);
      answer = await ask();
      expect(answer.rows).toEqual(LIVE);
      expect(answer.servedBy).toBe("live");
      // Both sides' entries: its own table, never the foreign one.
      await bindHostManifest({ ...onEntries, ...offEntries });
      answer = await ask();
      expect(answer.rows).toEqual(shifted(1000));
      expect(answer.servedBy).toBe("rollup");
   }, 120_000);
});
