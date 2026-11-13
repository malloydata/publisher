// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The rollup key check (known limitation 29): a `#(preaggregate)` rollup
 * whose grain is the key of a join_one is checked, after its build, for a
 * key value its base holds more than once. Through real builds run after run
 * as auto-run drives them: the record describes the table the checking run
 * wrote, is carried while that table stands, and is never trusted for a
 * table written since (a rebuild checks again). Then the binds: the record
 * reaches the serving entries from the run that made it, or from the store
 * under the same proof the inline check's passes use, and from nowhere else.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { makeMaterialization } from "../../../test_helpers/materialization_test_fixtures";
import { installSettings } from "../../../test_helpers/settings_env";
import { computeSourceEntityId } from "../plan/build_plan";
import { compilePackageBuildPlan } from "../plan/build_plan_compile";
import type { RollupVariantPlan } from "../plan/rollup_plan";
import {
   PackageMaterialization,
   type PackageMaterializationOwner,
} from "../package_materialization";
import type {
   BuildInstruction,
   FreshnessManifest,
   ManifestEntry,
} from "../types";
import {
   INLINE_UNIQUENESS_KEY,
   keyUniquenessAt,
   keyUniquenessOf,
   rollupKeyRecordKey,
   rollupKeyRecordsIn,
   tableIdentityKey,
   UniquenessLedger,
   withoutUniquenessPasses,
   withRollupKeyRecords,
   type RollupKeyRecord,
} from "./inline_uniqueness";
import { InstructedBuild } from "./instructed_build";
import { ManifestCommitter } from "./manifest_committer";
import { splitManifestEntries } from "./manifest_loader";
import { deriveServeBindings } from "../serve/materialization_serve_transform";
import { storedKeyChecksOf } from "../plan/rollup_plan";
import type { BuildEnvironment } from "./materialization_run_support";
import { SourceBuilder } from "./source_build";
import * as fs from "node:fs";
import * as path from "node:path";
import { storageDestinationRoot } from "../../connection_config";

const packages = tempBuildPlanPackages();
let restore: () => void;
beforeEach(() => {
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true", PERSIST_STORAGE_MODE: "on" },
   }).restore;
});
afterEach(async () => {
   restore();
   await packages.cleanup();
});

const SEED = [
   "CREATE OR REPLACE TABLE crew_src AS SELECT * FROM (VALUES (1, 3), (3, 2), (5, 4)) AS t(flight_id, size)",
   "CREATE OR REPLACE TABLE flights_src AS SELECT * FROM (VALUES (1, 'AA'), (2, 'AA'), (3, 'BB')) AS t(id, carrier)",
   "CREATE OR REPLACE TABLE segs_src AS SELECT * FROM (VALUES (10, 1, 2.0), (11, 1, 3.0), (12, 3, 4.0)) AS t(seg_id, flight_id, miles)",
];

const CREW = `source: crew is duckdb.table('crew_src') extend {
  primary_key: flight_id
  #(preaggregate) grain=["flight_id"]
  measure: crew_size is size.sum()
}
`;

/** `flights` joins `crew` one-to-one on the rollup's grain. */
const JOINED = `${CREW}
source: flights is duckdb.table('flights_src') extend {
  primary_key: id
  join_one: crew with id
  measure: flight_count is count()
}
`;

/**
 * `flights` reads `crew` through a join_many on its primary key, so the leg
 * re-points it as a join_one at the key: the query source's key is checked.
 */
const MANY = `${CREW}
source: flights is duckdb.table('flights_src') extend {
  primary_key: id
  join_many: crew on crew.flight_id = id
  measure: flight_count is count()
}
`;

type Metadata = NonNullable<ReturnType<UniquenessLedger["toMetadata"]>>;

async function scenario(model: string) {
   const pkg = await packages.create(model);
   const duckdb = (await pkg.getMalloyConnection(
      "duckdb",
   )) as unknown as MalloyConnection;
   for (const sql of SEED) await duckdb.runSQL(sql);
   const compiled = await compilePackageBuildPlan(pkg);
   const [rollupID] = Object.keys(compiled.preaggregatePlans ?? {});
   const rollup = compiled.preaggregatePlans?.[rollupID ?? ""] as
      | RollupVariantPlan
      | undefined;
   const source = compiled.sources[rollupID ?? ""];
   const address = source
      ? computeSourceEntityId(
           source,
           compiled.connectionDigests,
           compiled.targets?.[rollupID ?? ""],
        )
      : "";
   const environment = {
      getApiConnection: () => ({ name: "duckdb", type: "duckdb" }),
      getStorageDestination: () => {
         throw new Error("no storage in this spec");
      },
      getEnvironmentPath: () => pkg.getPackagePath(),
   } as unknown as BuildEnvironment;
   const builder = new SourceBuilder(async () => {
      throw new Error("no storage in this spec");
   });
   const build = new InstructedBuild({
      buildOneSource: (...a) => builder.build(...a),
      reclaimStorageTablesFromFailedRun: async () => {},
   });
   const instruction = {
      sourceEntityId: address,
      materializedTableId: "mt-rollup",
      physicalTableName: "crew_rollup_t",
      realization: "COPY",
   } as BuildInstruction;

   let previous: {
      entries: Record<string, ManifestEntry>;
      metadata?: Metadata;
   } = { entries: {} };
   let runs = 0;
   /**
    * One auto-run: the rollup is rebuilt when `rebuild`, else its entry is
    * carried. The run before's records are relied on unless `prior` says
    * otherwise (false: none; a value: that one).
    */
   async function run(rebuild: boolean, prior: boolean | Metadata = true) {
      runs += 1;
      const relied =
         prior === true
            ? previous.metadata
            : prior === false
              ? undefined
              : prior;
      const ledger = new UniquenessLedger(
         undefined,
         `run-${runs}`,
         relied
            ? rollupKeyRecordsIn({ [INLINE_UNIQUENESS_KEY]: relied })
            : undefined,
      );
      const result = await build.run(
         compiled,
         environment,
         rebuild ? [instruction] : [],
         rebuild ? {} : previous.entries,
         new AbortController().signal,
         false,
         undefined,
         undefined,
         undefined,
         [],
         undefined,
         false,
         "recompute",
         ledger,
      );
      expect(result.failures).toEqual({});
      previous = { entries: result.entries, metadata: result.uniquenessChecks };
      return result;
   }
   const record = (m: Metadata | undefined) => m?.rollupKeys?.[address];
   const parentRecord = (m: Metadata | undefined) => {
      const sourceID = rollup?.parentKeyChecks?.[0]?.sourceID;
      return sourceID === undefined
         ? undefined
         : m?.rollupKeys?.[rollupKeyRecordKey(address, sourceID)];
   };
   return { duckdb, compiled, rollup, address, run, record, parentRecord };
}

describe("the rollup key check, on a build", () => {
   it("is planned for a rollup whose grain a join_one keys on, and only then", async () => {
      const joined = await scenario(JOINED);
      expect(joined.rollup?.keyCheck?.keyColumn).toBe("flight_id");
      expect(joined.rollup?.keyCheck?.sql).toMatch(/HAVING/i);
      // No join_one reads the rollup's key: no check, no scan.
      const alone = await scenario(CREW);
      expect(alone.rollup).toBeDefined();
      expect(alone.rollup?.keyCheck).toBeUndefined();
   });

   it(
      "records unique, carries it while the table stands, and checks again on a rebuild",
      async () => {
         const s = await scenario(JOINED);
         const first = await s.run(true);
         expect(s.record(first.uniquenessChecks)).toMatchObject({
            keyColumn: "flight_id",
            unique: true,
            checkedInRun: "run-1",
         });

         // Carried, unchanged: the record rides along, naming the run that
         // checked.
         const second = await s.run(false);
         expect(s.record(second.uniquenessChecks)).toEqual(
            s.record(first.uniquenessChecks),
         );

         // A duplicate in the warehouse does not reach the table until it
         // is rebuilt: the record still describes the rows the table holds.
         await s.duckdb.runSQL("INSERT INTO crew_src VALUES (1, 9)");
         const third = await s.run(false);
         expect(s.record(third.uniquenessChecks)?.unique).toBe(true);

         // The rebuild holds the duplicate: the pass recorded before is not
         // trusted, the check runs again and says so.
         const fourth = await s.run(true);
         expect(s.record(fourth.uniquenessChecks)).toMatchObject({
            unique: false,
            checkedInRun: "run-4",
         });
         // And a duplicate is carried as faithfully as a pass.
         const fifth = await s.run(false);
         expect(s.record(fifth.uniquenessChecks)?.unique).toBe(false);
      },
      { timeout: 60000 },
   );

   it(
      "carries nothing from a run that cannot be relied on, or for another table or check",
      async () => {
         const s = await scenario(JOINED);
         const first = await s.run(true);
         const pass = s.record(first.uniquenessChecks) as RollupKeyRecord;
         expect(pass.unique).toBe(true);

         // The run before is not one to rely on (one overlapped it, say).
         expect(s.record((await s.run(false, false)).uniquenessChecks)).toBe(
            undefined,
         );
         // A record over another table, or from another check, is not this
         // table's.
         for (const forged of [
            { ...pass, table: tableIdentityKey("connection", "duckdb", "x") },
            { ...pass, digest: "another check" },
         ]) {
            const carried = await s.run(false, {
               passes: {},
               checks: [],
               rollupKeys: { [s.address]: forged },
            });
            expect(s.record(carried.uniquenessChecks)).toBeUndefined();
         }
      },
      { timeout: 60000 },
   );

   it(
      "records nothing when the check cannot run, and never fails the build",
      async () => {
         const s = await scenario(JOINED);
         if (s.rollup?.keyCheck)
            s.rollup.keyCheck.sql = "SELECT * FROM nowhere";
         const first = await s.run(true);
         expect(Object.keys(first.entries)).toContain(s.address);
         expect(s.record(first.uniquenessChecks)).toBeUndefined();
      },
      { timeout: 60000 },
   );
});

describe("the query source's primary key check, on a build", () => {
   it("is planned for a rollup a source reads through a join_many on its key, and only then", async () => {
      const many = await scenario(MANY);
      expect(many.rollup?.parentKeyChecks?.map((c) => c.keyColumn)).toEqual([
         "id",
      ]);
      expect(many.rollup?.parentKeyChecks?.[0]?.connectionName).toBe("duckdb");
      // A join_one read at its key needs none.
      const one = await scenario(JOINED);
      expect(one.rollup?.parentKeyChecks).toBeUndefined();
   });

   it("is not planned for a source whose rows depend on a given, with or without a default", async () => {
      // Compiled once at plan time, the check would read the default
      // caller's rows (or not compile), not the rows another caller reads.
      for (const given of [
         "given: CARRIER :: string is 'AA'",
         "given: CARRIER :: string",
      ]) {
         const scoped = await scenario(`##! experimental.givens
${given}
${MANY.replace(
   "primary_key: id\n",
   "primary_key: id\n  where: carrier = $CARRIER\n",
)}`);
         expect(scoped.rollup).toBeDefined();
         expect(scoped.rollup?.parentKeyChecks).toBeUndefined();
      }
   });

   it(
      "records unique, carries it while the rollup's table stands, and checks again on a rebuild",
      async () => {
         const s = await scenario(MANY);
         const first = await s.run(true);
         expect(s.parentRecord(first.uniquenessChecks)).toMatchObject({
            keyColumn: "id",
            unique: true,
            checkedInRun: "run-1",
         });
         const second = await s.run(false);
         expect(s.parentRecord(second.uniquenessChecks)).toEqual(
            s.parentRecord(first.uniquenessChecks),
         );
         // A repeated key, and a NULL one, each found on the next build.
         await s.duckdb.runSQL("INSERT INTO flights_src VALUES (1, 'BB')");
         const third = await s.run(true);
         expect(s.parentRecord(third.uniquenessChecks)).toMatchObject({
            unique: false,
            checkedInRun: "run-3",
         });
         await s.duckdb.runSQL("DELETE FROM flights_src WHERE carrier = 'BB'");
         await s.duckdb.runSQL("INSERT INTO flights_src VALUES (NULL, 'CC')");
         const fourth = await s.run(true);
         expect(s.parentRecord(fourth.uniquenessChecks)?.unique).toBe(false);
         // Not relied on: nothing carried.
         expect(
            s.parentRecord((await s.run(false, false)).uniquenessChecks),
         ).toBeUndefined();
      },
      { timeout: 60000 },
   );
});

/**
 * `flights` joins `segments` on `flight_id`, not on `segments`' own primary
 * key: since malloydata/malloy#3208 live counts and sums `segments` through
 * the join over the rows whose `seg_id` is not NULL, and its rollup over
 * every row, so the rollup's build checks `seg_id` unique and never NULL.
 */
const SEGS = `source: segments is duckdb.table('segs_src') extend {
  primary_key: seg_id
  #(preaggregate) grain=["flight_id"]
  measure: seg_miles is miles.sum()
}
source: flights is duckdb.table('flights_src') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
}
`;

describe("the joined source's own primary key check, on a build", () => {
   it("is planned for a rollup whose base a join reaches on another column, and only then", async () => {
      const segs = await scenario(SEGS);
      expect(
         segs.rollup?.parentKeyChecks?.map((c) => [
            c.sourceID.split("@")[0],
            c.keyColumn,
         ]),
      ).toEqual(
         expect.arrayContaining([
            ["flights", "id"],
            ["segments", "seg_id"],
         ]),
      );
      // A join keyed on the joined source's primary key needs none.
      const many = await scenario(MANY);
      expect(many.rollup?.parentKeyChecks?.map((c) => c.keyColumn)).toEqual([
         "id",
      ]);
   });

   it(
      "records a NULL key as not unique on the next build",
      async () => {
         const s = await scenario(SEGS);
         const base = s.rollup?.parentKeyChecks?.find((c) =>
            c.sourceID.startsWith("segments@"),
         );
         expect(base).toBeDefined();
         const key = rollupKeyRecordKey(s.address, base!.sourceID);
         const first = await s.run(true);
         expect(first.uniquenessChecks?.rollupKeys?.[key]).toMatchObject({
            keyColumn: "seg_id",
            unique: true,
         });
         await s.duckdb.runSQL("INSERT INTO segs_src VALUES (NULL, 1, 9.0)");
         const second = await s.run(true);
         expect(second.uniquenessChecks?.rollupKeys?.[key]?.unique).toBe(false);
      },
      { timeout: 60000 },
   );
});

/**
 * A source stored in a destination, checked over its table at its own build
 * (the join-to-rollup leg in a destination reads that table). The destination
 * is a plain DuckDB file under the environment, as a `storage=` destination
 * of type duckdb is; the stand-in build writes the source's rows there.
 */
async function storedScenario(
   /** Base key checks to attach to the rollup's plan, as for a storage rollup. */
   baseChecks: { sourceID: string; keyColumn: string; sql: string }[] = [],
) {
   const pkg = await packages.create(JOINED);
   const duckdb = (await pkg.getMalloyConnection(
      "duckdb",
   )) as unknown as MalloyConnection;
   for (const sql of SEED) await duckdb.runSQL(sql);
   const compiled = await compilePackageBuildPlan(pkg);
   const [sourceID] = Object.keys(compiled.preaggregatePlans ?? {});
   const source = compiled.sources[sourceID ?? ""];
   if (!sourceID || !source) throw new Error("no planned source");
   compiled.storedKeyChecks = { [sourceID]: "flight_id" };
   if (baseChecks.length > 0) {
      (
         compiled.preaggregatePlans?.[sourceID] as RollupVariantPlan
      ).parentKeyChecks = baseChecks.map((c) => ({
         ...c,
         connectionName: "duckdb",
      }));
   }
   const address = computeSourceEntityId(
      source,
      compiled.connectionDigests,
      compiled.targets?.[sourceID],
   );
   const root = storageDestinationRoot(pkg.getPackagePath());
   fs.mkdirSync(root, { recursive: true });
   const file = path.join(root, "lake.duckdb");
   const environment = {
      getApiConnection: () => ({ name: "duckdb", type: "duckdb" }),
      getStorageDestination: () => ({ name: "lake", type: "duckdb" }),
      getEnvironmentPath: () => pkg.getPackagePath(),
   } as unknown as BuildEnvironment;
   // The stored rows: the base's, so a duplicate in it reaches the table.
   const builder = new SourceBuilder(async (request) => {
      const table = request.instruction.physicalTableName;
      // Its own alias: the scenario's connection may be shared, and the
      // check attaches the file as `lake` in a session of its own.
      const alias = `stored_${Math.random().toString(36).slice(2)}`;
      await duckdb.runSQL(`ATTACH '${file}' AS ${alias}`);
      try {
         await duckdb.runSQL(
            `CREATE OR REPLACE TABLE ${alias}.${table} AS SELECT * FROM crew_src`,
         );
      } finally {
         await duckdb.runSQL(`DETACH ${alias}`);
      }
      return {
         sourceEntityId: request.contentSourceEntityId ?? "",
         sourceName: request.persistSource.name,
         physicalTableName: table,
         connectionName: "duckdb",
         storageDestinationName: "lake",
         schema: [
            { name: "flight_id", type: "INTEGER" },
            { name: "size", type: "INTEGER" },
         ],
      } as ManifestEntry;
   });
   const build = new InstructedBuild({
      buildOneSource: (...a) => builder.build(...a),
      reclaimStorageTablesFromFailedRun: async () => {},
   });
   const instruction = {
      sourceEntityId: address,
      materializedTableId: "mt-stored",
      physicalTableName: "crew_stored_t",
      realization: "COPY",
      destination: "lake",
   } as BuildInstruction;
   let previous: {
      entries: Record<string, ManifestEntry>;
      metadata?: Metadata;
   } = { entries: {} };
   let runs = 0;
   async function run(rebuild: boolean, prior = true) {
      runs += 1;
      const ledger = new UniquenessLedger(
         undefined,
         `run-${runs}`,
         prior && previous.metadata
            ? rollupKeyRecordsIn({ [INLINE_UNIQUENESS_KEY]: previous.metadata })
            : undefined,
      );
      const result = await build.run(
         compiled,
         environment,
         rebuild ? [instruction] : [],
         rebuild ? {} : previous.entries,
         new AbortController().signal,
         false,
         undefined,
         undefined,
         undefined,
         [],
         undefined,
         false,
         "recompute",
         ledger,
      );
      expect(result.failures).toEqual({});
      previous = { entries: result.entries, metadata: result.uniquenessChecks };
      return result;
   }
   const record = (m: Metadata | undefined, id: string = sourceID) =>
      m?.rollupKeys?.[rollupKeyRecordKey(address, id)];
   return { duckdb, run, record, address };
}

describe("a stored source's primary key check, on a build into a destination", () => {
   it(
      "records unique over the stored rows, carries it while the table stands, and checks again on a rebuild",
      async () => {
         const s = await storedScenario();
         const first = await s.run(true);
         expect(s.record(first.uniquenessChecks)).toMatchObject({
            keyColumn: "flight_id",
            unique: true,
            checkedInRun: "run-1",
            table: tableIdentityKey("destination", "lake", "crew_stored_t"),
         });
         const second = await s.run(false);
         expect(s.record(second.uniquenessChecks)).toEqual(
            s.record(first.uniquenessChecks),
         );
         // A repeated key and a NULL one, each found when the table is rebuilt.
         await s.duckdb.runSQL("INSERT INTO crew_src VALUES (1, 9)");
         const third = await s.run(true);
         expect(s.record(third.uniquenessChecks)).toMatchObject({
            unique: false,
            checkedInRun: "run-3",
         });
         await s.duckdb.runSQL("DELETE FROM crew_src WHERE size = 9");
         await s.duckdb.runSQL("INSERT INTO crew_src VALUES (NULL, 9)");
         expect(s.record((await s.run(true)).uniquenessChecks)?.unique).toBe(
            false,
         );
         // Not relied on: nothing carried.
         expect(
            s.record((await s.run(false, false)).uniquenessChecks),
         ).toBeUndefined();
      },
      { timeout: 60000 },
   );
});

describe("a storage rollup's base key check, on a build into a destination", () => {
   it(
      "runs on the base's connection, records against the table in the destination, and carries while it stands",
      async () => {
         const s = await storedScenario([
            {
               sourceID: "base@x",
               keyColumn: "seg_id",
               sql: "SELECT 1 AS k WHERE false",
            },
            {
               sourceID: "nulls@x",
               keyColumn: "seg_id",
               sql: "SELECT 1 AS k",
            },
         ]);
         const first = await s.run(true);
         const table = tableIdentityKey("destination", "lake", "crew_stored_t");
         expect(s.record(first.uniquenessChecks, "base@x")).toMatchObject({
            table,
            unique: true,
         });
         expect(s.record(first.uniquenessChecks, "nulls@x")?.unique).toBe(
            false,
         );
         const second = await s.run(false);
         expect(s.record(second.uniquenessChecks, "base@x")).toEqual(
            s.record(first.uniquenessChecks, "base@x"),
         );
         // Bound with the storage entry, as the leg reads it.
         const entry = second.entries[s.address]!;
         expect(
            keyUniquenessAt(
               rollupKeyRecordsIn({
                  [INLINE_UNIQUENESS_KEY]: second.uniquenessChecks!,
               }),
               s.address,
               entry,
            )?.parentKeys,
         ).toMatchObject({ "base@x": true, "nulls@x": false });
      },
      { timeout: 60000 },
   );
});

describe("the rollup key record in the run ledger", () => {
   const table = tableIdentityKey("connection", "duckdb", "r_t");
   const record: RollupKeyRecord = {
      table,
      keyColumn: "k",
      digest: "d",
      unique: true,
      checkedInRun: "run-1",
   };

   it("loses a record whose table or address this run writes", () => {
      const byAddress = new UniquenessLedger(undefined, "run-2", { a: record });
      byAddress.markWritten("a", "elsewhere");
      byAddress.carryRollupKey("a", table, "d");
      expect(byAddress.rollupKeyRecords()).toEqual({});

      const byTable = new UniquenessLedger(undefined, "run-2", { a: record });
      byTable.markWritten("other", table);
      byTable.carryRollupKey("a", table, "d");
      expect(byTable.rollupKeyRecords()).toEqual({});

      // One this run recorded, then its table written again by another
      // source on it.
      const after = new UniquenessLedger(undefined, "run-2");
      after.recordRollupKey("a", record);
      after.markWritten("b", table);
      expect(after.toMetadata()).toBeUndefined();
   });

   it("holds a query source's key check beside the rollup's, lost and carried with it", () => {
      const parent: RollupKeyRecord = {
         ...record,
         keyColumn: "id",
         sourceID: "s",
      };
      const key = rollupKeyRecordKey("a", "s");
      expect(key).not.toBe("a");
      const recorded = new UniquenessLedger(undefined, "run-2");
      recorded.recordRollupKey("a", { ...parent, checkedInRun: undefined });
      expect(Object.keys(recorded.rollupKeyRecords())).toEqual([key]);
      recorded.markWritten("a", "elsewhere");
      expect(recorded.rollupKeyRecords()).toEqual({});

      const prior = { [key]: parent };
      const carried = new UniquenessLedger(undefined, "run-2", prior);
      carried.carryRollupKey("a", table, "d", "s");
      expect(carried.rollupKeyRecords()).toEqual(prior);
      // Not the rollup's own record, nor another source's.
      const own = new UniquenessLedger(undefined, "run-2", prior);
      own.carryRollupKey("a", table, "d");
      own.carryRollupKey("a", table, "d", "t");
      expect(own.rollupKeyRecords()).toEqual({});
      // A record filed under one source's key but naming another is not it.
      const misfiled = new UniquenessLedger(undefined, "run-2", {
         [key]: { ...parent, sourceID: "t" },
      });
      misfiled.carryRollupKey("a", table, "d", "s");
      expect(misfiled.rollupKeyRecords()).toEqual({});
      expect(
         rollupKeyRecordsIn({
            [INLINE_UNIQUENESS_KEY]: { rollupKeys: prior },
         }),
      ).toEqual(prior);
   });

   it("reads only the records this Publisher writes", () => {
      expect(rollupKeyRecordsIn(null)).toBeUndefined();
      expect(
         rollupKeyRecordsIn({ [INLINE_UNIQUENESS_KEY]: { rollupKeys: [] } }),
      ).toBeUndefined();
      expect(
         rollupKeyRecordsIn({
            [INLINE_UNIQUENESS_KEY]: {
               rollupKeys: {
                  a: record,
                  b: { ...record, unique: "yes" },
                  c: { table },
               },
            },
         }),
      ).toEqual({ a: record });
   });

   it("is forgotten with the passes when a run record is deleted", () => {
      const remaining = withoutUniquenessPasses({
         mode: "auto",
         [INLINE_UNIQUENESS_KEY]: {
            passes: {},
            checks: [],
            rollupKeys: { a: record },
         },
      });
      expect(remaining?.[INLINE_UNIQUENESS_KEY]).toEqual({
         passes: {},
         checks: [],
      });
   });
});

describe("the rollup key record, bound for serving", () => {
   const entry: ManifestEntry = {
      sourceEntityId: "a",
      physicalTableName: "r_t",
      connectionName: "duckdb",
   } as ManifestEntry;
   const record: RollupKeyRecord = {
      table: tableIdentityKey("connection", "duckdb", "r_t"),
      keyColumn: "k",
      digest: "d",
      unique: true,
   };
   const at = (minute: number) => new Date(Date.UTC(2026, 0, 1, 0, minute));
   const committed = (over: Parameters<typeof makeMaterialization>[0] = {}) =>
      makeMaterialization({
         id: "prev",
         status: "MANIFEST_FILE_READY",
         createdAt: at(10),
         startedAt: at(10),
         completedAt: at(11),
         metadata: {
            mode: "auto",
            [INLINE_UNIQUENESS_KEY]: {
               passes: {},
               checks: [],
               rollupKeys: { a: record },
            },
         },
         ...over,
      });
   const older = makeMaterialization({
      id: "older",
      status: "MANIFEST_FILE_READY",
      createdAt: at(1),
      completedAt: at(2),
   });

   it("says what a record says only of the table it was taken over", () => {
      expect(keyUniquenessOf(record, entry)).toEqual({
         keyColumn: "k",
         unique: true,
      });
      expect(
         keyUniquenessOf(record, { ...entry, physicalTableName: "other" }),
      ).toBeUndefined();
      expect(
         keyUniquenessOf(record, { ...entry, connectionName: "pg" }),
      ).toBeUndefined();
      expect(keyUniquenessOf(undefined, entry)).toBeUndefined();
   });

   it("says what each query source's key check found of the same table", () => {
      const records = {
         a: record,
         [rollupKeyRecordKey("a", "s")]: { ...record, sourceID: "s" },
         [rollupKeyRecordKey("a", "t")]: {
            ...record,
            sourceID: "t",
            unique: false,
         },
         [rollupKeyRecordKey("a", "u")]: {
            ...record,
            sourceID: "u",
            table: tableIdentityKey("connection", "duckdb", "x"),
         },
         [rollupKeyRecordKey("b", "s")]: { ...record, sourceID: "s" },
      };
      expect(keyUniquenessAt(records, "a", entry)).toEqual({
         keyColumn: "k",
         unique: true,
         parentKeys: { s: true, t: false },
      });
      // A source's record is never read as the rollup's own.
      expect(
         keyUniquenessOf(records[rollupKeyRecordKey("a", "s")], entry),
      ).toBe(undefined);
      expect(
         splitManifestEntries(
            {
               a: { ...entry, keyUniqueness: { parentKeys: { s: true } } },
            } as never,
            "store",
            true,
         ).tableNameManifest.a?.keyUniqueness,
      ).toEqual({ parentKeys: { s: true } });
   });

   it("says what a stored table's own key check found, only of that table in that destination", () => {
      const stored = {
         sourceEntityId: "h",
         physicalTableName: "flights_t",
         connectionName: "duckdb",
         storageDestinationName: "lake",
      } as ManifestEntry;
      const at = (destination: string, table = "flights_t") => ({
         ...record,
         keyColumn: "id",
         sourceID: "flights",
         table: tableIdentityKey("destination", destination, table),
      });
      expect(
         keyUniquenessAt(
            { [rollupKeyRecordKey("h", "flights")]: at("lake") },
            "h",
            stored,
         ),
      ).toEqual({ parentKeys: { flights: true } });
      for (const other of [at("other"), at("lake", "x")]) {
         expect(
            keyUniquenessAt(
               { [rollupKeyRecordKey("h", "flights")]: other },
               "h",
               stored,
            ),
         ).toBeUndefined();
      }
      // A rollup's own check never describes a stored table.
      expect(keyUniquenessAt({ h: record }, "h", stored)).toBeUndefined();
   });

   it("restores a stored table's own key check from an uncontested auto-run", () => {
      const stored = {
         sourceEntityId: "h",
         physicalTableName: "flights_t",
         storageDestinationName: "lake",
      } as ManifestEntry;
      const run = committed({
         metadata: {
            mode: "auto",
            [INLINE_UNIQUENESS_KEY]: {
               passes: {},
               checks: [],
               rollupKeys: {
                  [rollupKeyRecordKey("h", "flights")]: {
                     ...record,
                     sourceID: "flights",
                     unique: false,
                     table: tableIdentityKey(
                        "destination",
                        "lake",
                        "flights_t",
                     ),
                  },
               },
            },
         },
      });
      expect(
         withRollupKeyRecords({ h: stored }, [run]).h.keyUniqueness,
      ).toEqual({ parentKeys: { flights: false } });
   });

   it("reaches a storage binding from the store, and never from a host's manifest", () => {
      const stored = {
         sourceEntityId: "h",
         sourceName: "flights",
         physicalTableName: "flights_t",
         connectionName: "duckdb",
         storageDestinationName: "lake",
         schema: [{ name: "id", type: "INTEGER" }],
         keyUniqueness: { parentKeys: { flights: true } },
      } as unknown as ManifestEntry;
      const fromStore = splitManifestEntries(
         { h: stored },
         "store",
         true,
      ).storageEntries;
      const fromHost = splitManifestEntries(
         { h: stored },
         "host",
      ).storageEntries;
      expect(
         (fromStore.h as { keyUniqueness?: unknown }).keyUniqueness,
      ).toEqual({ parentKeys: { flights: true } });
      expect(
         (fromHost.h as { keyUniqueness?: unknown }).keyUniqueness,
      ).toBeUndefined();
      expect(deriveServeBindings(fromStore, {})[0]?.primaryKeys).toEqual({
         flights: true,
      });
      expect(deriveServeBindings(fromHost, {})[0]?.primaryKeys).toBeUndefined();
   });

   it("forgets a storage binding's key check when a run is about to write its table", () => {
      const materialization = new PackageMaterialization({
         models: () => new Map(),
         packageName: () => "pkg",
      } as unknown as PackageMaterializationOwner);
      const held = materialization as unknown as {
         storageServeBindings: ReturnType<typeof deriveServeBindings>;
      };
      const binding = {
         sourceName: "flights",
         destinationName: "lake",
         virtualHandle: "h",
         tablePath: "lake.flights_t",
         schema: [],
         primaryKeys: { flights: true },
      };
      held.storageServeBindings = [
         binding,
         { ...binding, sourceName: "other", virtualHandle: "o" },
      ];
      materialization.forgetKeyUniqueness(["h"]);
      expect(held.storageServeBindings[0]?.primaryKeys).toBeUndefined();
      expect(held.storageServeBindings[1]?.primaryKeys).toEqual({
         flights: true,
      });
   });

   it("asks for stored-table key checks only when a storage rollup is read as a join_many", () => {
      const need = {
         sourceID: "flights",
         name: "flights",
         keyColumn: "id",
         connectionName: "duckdb",
         joinedSourceID: "segments",
         joinKeyColumn: "flight_id",
         relationship: "many" as const,
      };
      const rollup = (storage?: string) =>
         ({
            variant: true,
            baseSourceID: "segments",
            grainDimensions: ["flight_id"],
            ...(storage ? { storage } : {}),
         }) as unknown as RollupVariantPlan;
      const pk = (id: string) => (id === "flights" ? "id" : undefined);
      expect(
         storedKeyChecksOf([rollup("lake")], [need], pk, ["flights", "x"]),
      ).toEqual({ flights: "id" });
      expect(storedKeyChecksOf([rollup()], [need], pk, ["flights"])).toEqual(
         {},
      );
      expect(
         storedKeyChecksOf(
            [rollup("lake")],
            [{ ...need, relationship: "one" }],
            pk,
            ["flights"],
         ),
      ).toEqual({});
   });

   it("restores from the store only an uncontested auto-run's records", () => {
      const entries = { a: entry };
      expect(
         withRollupKeyRecords(entries, [committed(), older]).a.keyUniqueness,
      ).toEqual({ keyColumn: "k", unique: true });
      // A run started after it (failed or still running) may have rewritten
      // the table: nothing is restored.
      for (const later of [
         makeMaterialization({ id: "f", status: "FAILED", createdAt: at(20) }),
         makeMaterialization({ id: "r", status: "PENDING", createdAt: at(5) }),
      ]) {
         expect(
            withRollupKeyRecords(entries, [committed(), later]).a.keyUniqueness,
         ).toBeUndefined();
      }
      // An orchestrated run's records are a host's: never read.
      expect(
         withRollupKeyRecords(entries, [
            committed({
               metadata: {
                  ...committed().metadata,
                  mode: "orchestrated",
               },
            }),
         ]).a.keyUniqueness,
      ).toBeUndefined();
      // A storage entry is never read through a join.
      expect(
         withRollupKeyRecords(
            { a: { ...entry, storageDestinationName: "lake" } },
            [committed()],
         ).a.keyUniqueness,
      ).toBeUndefined();
   });

   it("reaches the colocated manifest from the store, and never from a host's manifest", () => {
      const annotated = withRollupKeyRecords({ a: entry }, [committed()]);
      expect(
         splitManifestEntries(annotated, "store", true).tableNameManifest.a
            ?.keyUniqueness,
      ).toEqual({ keyColumn: "k", unique: true });
      expect(
         splitManifestEntries(annotated, "host").tableNameManifest.a
            ?.keyUniqueness,
      ).toBeUndefined();
   });

   it("binds the records of the run that made them after it commits", async () => {
      let bound: FreshnessManifest | undefined;
      const committer = new ManifestCommitter({
         repository: () => {
            throw new Error("unused");
         },
         host: {} as never,
         lifecycle: {} as never,
      });
      await committer.autoLoad(
         {
            reloadAllModelsForPackage: async (_p, manifest) => {
               bound = manifest;
            },
            bindPackageStorageServeBindings: async () => {},
         },
         "pkg",
         {
            a: entry,
            b: { ...entry, sourceEntityId: "b", physicalTableName: "s_t" },
         },
         undefined,
         { a: record, b: record },
      );
      expect(bound?.a?.keyUniqueness).toEqual({ keyColumn: "k", unique: true });
      // `b`'s record names another table.
      expect(bound?.b?.keyUniqueness).toBeUndefined();
   });

   it("binds a stored table's own key check with its storage entry after the run commits", async () => {
      let storage: Record<string, ManifestEntry> | undefined;
      const committer = new ManifestCommitter({
         repository: () => {
            throw new Error("unused");
         },
         host: {} as never,
         lifecycle: {} as never,
      });
      await committer.autoLoad(
         {
            reloadAllModelsForPackage: async () => {},
            bindPackageStorageServeBindings: async (_p, entries) => {
               storage = entries;
            },
         },
         "pkg",
         {
            h: {
               sourceEntityId: "h",
               physicalTableName: "flights_t",
               storageDestinationName: "lake",
            } as ManifestEntry,
         },
         undefined,
         {
            [rollupKeyRecordKey("h", "flights")]: {
               ...record,
               sourceID: "flights",
               table: tableIdentityKey("destination", "lake", "flights_t"),
            },
         },
      );
      expect(
         (storage?.h as { keyUniqueness?: unknown } | undefined)?.keyUniqueness,
      ).toEqual({ parentKeys: { flights: true } });
   });

   it("forgets a bound record when a run is about to write its table", () => {
      const materialization = new PackageMaterialization({
         models: () => new Map(),
         packageName: () => "pkg",
      } as unknown as PackageMaterializationOwner);
      materialization.recordManifestBinding({
         a: {
            tableName: "r_t",
            keyUniqueness: { keyColumn: "k", unique: true },
         },
         b: {
            tableName: "s_t",
            keyUniqueness: { keyColumn: "k", unique: false },
         },
      });
      expect(
         materialization.materializedTableCatalog().bound.a?.keyUniqueness,
      ).toEqual({ keyColumn: "k", unique: true });
      materialization.forgetKeyUniqueness(["a"]);
      const bound = materialization.materializedTableCatalog().bound;
      expect(bound.a).toEqual({ tableName: "r_t" });
      expect(bound.b?.keyUniqueness?.unique).toBe(false);
   });

   it("warns the package of a rollup whose build found its join_one key repeated, while that is bound", () => {
      const materialization = new PackageMaterialization({
         models: () => new Map(),
         packageName: () => "pkg",
      } as unknown as PackageMaterializationOwner);
      const rollup = (id: string, base: string) => ({
         name: `${base}__rollup`,
         sourceEntityId: id,
         modelPath: "m.malloy",
         preaggregate: {
            baseSourceName: base,
            grainDimensions: ["flight_id"],
            measures: ["crew_size"],
         },
      });
      materialization.buildPlan = {
         sources: {
            "r1@m": rollup("a", "crew"),
            "r2@m": rollup("b", "crew_ok"),
            "p@m": { name: "plain", sourceEntityId: "c", preaggregate: null },
         },
      } as never;
      const repeated = { keyColumn: "flight_id", unique: false };
      materialization.recordManifestBinding({
         a: { tableName: "r_t", keyUniqueness: repeated },
         b: {
            tableName: "s_t",
            keyUniqueness: { keyColumn: "flight_id", unique: true },
         },
         // Not a rollup: no warning, whatever it carries.
         c: { tableName: "p_t", keyUniqueness: repeated },
      });
      const warnings = materialization.repeatedJoinOneKeyWarnings();
      expect(warnings.map((w) => [w.subject, w.code, w.model])).toEqual([
         ["crew", "preaggregate-join-one-key-repeated", "m.malloy"],
      ]);
      expect(warnings[0].message).toContain("`flight_id` more than once");
      // A run about to rewrite the table forgets the check, and the warning.
      materialization.forgetKeyUniqueness(["a"]);
      expect(materialization.repeatedJoinOneKeyWarnings()).toEqual([]);
   });

   it("does not warn of a join_one key for a rollup whose build ran only query source key checks", () => {
      const materialization = new PackageMaterialization({
         models: () => new Map(),
         packageName: () => "pkg",
      } as unknown as PackageMaterializationOwner);
      materialization.buildPlan = {
         sources: {
            "r1@m": {
               name: "segments__rollup",
               sourceEntityId: "a",
               modelPath: "m.malloy",
               preaggregate: {
                  baseSourceName: "segments",
                  grainDimensions: ["flight_id"],
                  measures: ["seg_miles"],
               },
            },
         },
      } as never;
      // No own key check (no join_one reads the rollup through its key):
      // only what each query source's primary key check found.
      for (const unique of [true, false]) {
         materialization.recordManifestBinding({
            a: {
               tableName: "r_t",
               keyUniqueness: { parentKeys: { "flights@m": unique } },
            },
         });
         expect(materialization.repeatedJoinOneKeyWarnings()).toEqual([]);
      }
   });
});
