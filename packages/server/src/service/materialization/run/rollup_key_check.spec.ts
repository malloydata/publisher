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
   keyUniquenessOf,
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
import type { BuildEnvironment } from "./materialization_run_support";
import { SourceBuilder } from "./source_build";

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
   return { duckdb, compiled, rollup, address, run, record };
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
});
