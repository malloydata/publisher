// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Two database rollups of one build (the same SQL) placed apart, one by its
 * `name=` and one at its default name, are two tables on two addresses
 * (foldDistinctTableNames), and Malloy files both under one key, the build's
 * `buildId`, which is the default-named one's address. The named one is read
 * under that key: before, its compile found no entry there and recomputed, so
 * the rollup leg declined it as NOT_BUILT (campaign R-01: a `#(rollup)` on
 * `trips_r`, beside a storage twin of one build), and a persisted view placed
 * the same way answered from its twin's table.
 *
 * Each table is tampered with its own offset, so an answer read from either
 * is told apart from live and from the other.
 */

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as path from "path";
import { unrecordedAudit } from "../../access_record";
import { __setPackageLoadPoolForTests } from "../../../package_load/package_load_pool";
import type {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { Environment } from "../../environment";
import { parsePackageName } from "../../identity";
import { MaterializationService } from "./materialization_service";
import {
   openTempStorage,
   disposeTempStorage,
} from "../../../test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../../test_helpers/materialization_service_harness";

const ENV = "e1";
const ENV_ID = "env-1";
const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

const TRIPS = `warehouse.sql("""
  SELECT * FROM (VALUES (1, 1), (2, 2), (3, 1)) AS t(trip_id, station_id)
""")`;

// The campaign's shape: the named rollup's source joins on its grain key, the
// default-named one does not; the join is unused, so both build one SQL.
const MODEL = `##! experimental { persistence composite_sources }
source: stations is warehouse.sql("""
  SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, sname)
""") extend { primary_key: id }

source: trips_named is ${TRIPS} extend {
  primary_key: trip_id
  join_one: station is stations on station_id = station.id
  #(rollup) grain=["station_id"] name="main.trips_by_station"
  measure: trip_count is count()
}

source: trips_default is ${TRIPS} extend {
  primary_key: trip_id
  #(rollup) grain=["station_id"]
  measure: trip_count is count()
}
`;
// Two persisted views of one build likewise, one placed by its `name=`.
const VIEW_MODEL = `##! experimental { persistence }
source: trips_named is ${TRIPS} extend {
  measure: trip_count is count()
  #@ persist name="main.trips_view_named"
  view: by_station is { group_by: station_id; aggregate: trip_count; order_by: station_id }
}

source: trips_default is ${TRIPS} extend {
  measure: trip_count is count()
  #@ persist
  view: by_station is { group_by: station_id; aggregate: trip_count; order_by: station_id }
}
`;

const NAMED_OFFSET = 1000;
const OTHER_OFFSET = 2000;
const LIVE = [
   { station_id: 1, trip_count: 2 },
   { station_id: 2, trip_count: 1 },
];

describe("two tables of one build, placed apart", () => {
   let root: string;
   let envPath: string;
   let db: DuckDBConnection;
   let repo: ResourceRepository;
   let env: Environment;
   let service: MaterializationService;
   let restoreSettings: () => void;

   beforeEach(async () => {
      restoreSettings = installSettings({
         env: {
            PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         },
      }).restore;
      await __setPackageLoadPoolForTests(null);
      ({ root, envPath, db, repo } = await openTempStorage("folded-rollup-", {
         id: ENV_ID,
         name: ENV,
      }));
      writePackageFiles(
         path.join(envPath, "pa"),
         { "model.malloy": MODEL, "views.malloy": VIEW_MODEL },
         { manifest: { name: "pa", description: "fixture" } },
      );
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      await env.addPackage("pa");
      service = serviceOverEnvironment(
         () => repo,
         () => env,
      );
   });

   afterEach(async () => {
      await env.closeAllConnections().catch(() => undefined);
      await disposeTempStorage({ db, root });
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

   async function ask(
      model: string,
      query: string,
   ): Promise<{ rows: unknown; servedBy?: string }> {
      const pkg = await env.getPackage(parsePackageName("pa"), false);
      const r = await pkg.getModel(model)!.runQuery(unrecordedAudit(), {
         query,
      });
      return {
         rows: (r as unknown as { compactResult: unknown }).compactResult,
         servedBy: r.path?.servedBy?.kind,
      };
   }

   const shifted = (offset: number) =>
      LIVE.map((row) => ({ ...row, trip_count: row.trip_count + offset }));

   /**
    * Build, then tamper the table whose name holds `named` and the one whose
    * name holds `other`, each by its own offset.
    */
   async function builtAndTampered(
      parts: { named: string; other: string },
      column: string,
   ): Promise<void> {
      const built = await build();
      expect(built.status).toBe("MANIFEST_FILE_READY");
      const tables = Object.values(built.manifest?.entries ?? {}).map(
         (e) => e.physicalTableName,
      );
      for (const [part, offset] of [
         [parts.named, NAMED_OFFSET],
         [parts.other, OTHER_OFFSET],
      ] as const) {
         const hits = tables.filter((t) => t.includes(part));
         expect(hits).toHaveLength(1);
         await sql(
            `UPDATE ${hits[0]} SET "${column}" = "${column}" + ${offset}`,
         );
      }
   }

   it("answers each rollup from its own table, the moved address under Malloy's key", async () => {
      await builtAndTampered(
         { named: "trips_by_station", other: "pa__model__trips_default" },
         "trip_count__partial",
      );
      for (const [source, offset] of [
         ["trips_named", NAMED_OFFSET],
         ["trips_default", OTHER_OFFSET],
      ] as const) {
         const answer = await ask(
            "model.malloy",
            `run: ${source} -> { group_by: station_id; aggregate: trip_count; order_by: station_id }`,
         );
         expect(answer.servedBy).toBe("rollup");
         expect(answer.rows).toEqual(shifted(offset));
      }
   }, 120_000);

   it("answers each persisted view from its own table, the moved address under Malloy's key", async () => {
      await builtAndTampered(
         { named: "trips_view_named", other: "pa__views__trips_default" },
         "trip_count",
      );
      for (const [source, offset] of [
         ["trips_named", NAMED_OFFSET],
         ["trips_default", OTHER_OFFSET],
      ] as const) {
         const answer = await ask(
            "views.malloy",
            `run: ${source} -> by_station`,
         );
         expect(answer.servedBy).toBe("view");
         expect(answer.rows).toEqual(shifted(offset));
      }
   }, 120_000);
});
