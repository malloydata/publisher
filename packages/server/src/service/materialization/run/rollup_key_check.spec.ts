// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The rollup key check (known limitation 29): a `#(rollup)` rollup
 * whose grain is the key of a join_one is checked, after its build, for a
 * key value its base holds more than once. Through real builds run after run
 * as auto-run drives them: the verdict is written into the table's column
 * contract, rides on its entry while that table stands, and a rebuild checks
 * again. The contract is the one store of key verdicts a bind reads.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import {
   rollupPlansIn,
   sourcesIn,
   targetsIn,
} from "../../../test_helpers/materialization_test_fixtures";
import { useMaterializationOn } from "../../../test_helpers/settings_env";
import { computeSourceEntityId } from "../plan/build_plan";
import { compilePackageBuildPlan } from "../plan/build_plan_compile";
import type { MaterializePlan, RollupVariantPlan } from "../domain/table_plans";
import {
   PackageMaterialization,
   type PackageMaterializationOwner,
} from "../package_materialization";
import type {
   BuildInstruction,
   KeyUniqueness,
   ManifestEntry,
} from "../domain/types";
import { contractKeyName, parseContract } from "../domain/column_contract";
import {
   INLINE_UNIQUENESS_KEY,
   UniquenessLedger,
   withoutUniquenessPasses,
} from "./inline_uniqueness";
import { InstructedBuild } from "./instructed_build";
import { splitManifestEntries } from "./manifest_loader";
import { deriveServeBindings } from "../domain/binding";
import { storedKeyChecksOf } from "../plan/rollup_plan";
import { type BuildEnvironment } from "./build_upstreams";
import { buildSource } from "./source_build";
import * as fs from "node:fs";
import * as path from "node:path";
import { storageDestinationRoot } from "../../connection_config";
import {
   contractFor,
   storageContractFor,
} from "../../../test_helpers/contract_fixture";
import { packageRelativeSourceID } from "../text/read_text";

const packages = tempBuildPlanPackages();
useMaterializationOn({ each: true });
afterEach(async () => {
   await packages.cleanup();
});

const SEED = [
   "CREATE OR REPLACE TABLE crew_src AS SELECT * FROM (VALUES (1, 3), (3, 2), (5, 4)) AS t(flight_id, size)",
   "CREATE OR REPLACE TABLE flights_src AS SELECT * FROM (VALUES (1, 'AA'), (2, 'AA'), (3, 'BB')) AS t(id, carrier)",
   "CREATE OR REPLACE TABLE segs_src AS SELECT * FROM (VALUES (10, 1, 2.0), (11, 1, 3.0), (12, 3, 4.0)) AS t(seg_id, flight_id, miles)",
];

const CREW = `source: crew is duckdb.table('crew_src') extend {
  primary_key: flight_id
  #(rollup) grain=["flight_id"]
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

/**
 * The key check `entry`'s contract records under `level`, `subject` and
 * `keyColumn` ({@link contractKeyName}), as the key column and what it
 * found; undefined when it records none.
 */
function contractCheck(
   entry: ManifestEntry | undefined,
   level: "rollup" | "parent" | "stored",
   subject: string,
   keyColumn: string,
): { keyColumn: string; unique: boolean } | undefined {
   const check = parseContract(entry?.contract)?.keys?.[
      contractKeyName(level, subject, keyColumn)
   ];
   return check ? { keyColumn, unique: check.unique } : undefined;
}

async function scenario(model: string) {
   const pkg = await packages.create(model);
   const duckdb = (await pkg.getMalloyConnection(
      "duckdb",
   )) as unknown as MalloyConnection;
   for (const sql of SEED) await duckdb.runSQL(sql);
   const compiled = await compilePackageBuildPlan(pkg);
   const [rollupID] = Object.keys(rollupPlansIn(compiled) ?? {});
   const rollup = rollupPlansIn(compiled)?.[rollupID ?? ""] as
      | RollupVariantPlan
      | undefined;
   const source = sourcesIn(compiled)[rollupID ?? ""];
   const address = source
      ? computeSourceEntityId(
           source,
           compiled.connectionDigests,
           targetsIn(compiled)?.[rollupID ?? ""],
        )
      : "";
   const environment = {
      getApiConnection: () => ({ name: "duckdb", type: "duckdb" }),
      getStorageDestination: () => {
         throw new Error("no storage in this spec");
      },
      getEnvironmentName: () => "env",
      getEnvironmentPath: () => pkg.getPackagePath(),
      // Contract keys name a checked source by its package-relative sourceID.
      getPackagePath: () => pkg.getPackagePath(),
   } as unknown as BuildEnvironment;
   const storage: Parameters<typeof buildSource>[1] = async () => {
      throw new Error("no storage in this spec");
   };
   const build = new InstructedBuild({
      buildOneSource: (options) => buildSource(options, storage),
      tableNotOwned: async () => undefined,
      reclaimStorageTablesFromFailedRun: async () => {},
   });
   const instruction = {
      sourceEntityId: address,
      materializedTableId: "mt-rollup",
      physicalTableName: "crew_rollup_t",
      realization: "COPY",
   } as BuildInstruction;

   let previous: Record<string, ManifestEntry> = {};
   let runs = 0;
   /** One auto-run: the rollup is rebuilt when `rebuild`, else its entry is carried. */
   async function run(rebuild: boolean) {
      runs += 1;
      const ledger = new UniquenessLedger(undefined, `run-${runs}`);
      const result = await build.run({
         compiled,
         environment,
         instructions: rebuild ? [instruction] : [],
         seedEntries: rebuild ? {} : previous,
         signal: new AbortController().signal,
         priorRefusals: [],
         hostInstructed: false,
         uniqueness: ledger,
      });
      expect(result.failures).toEqual({});
      previous = result.entries;
      return result;
   }
   type Run = Awaited<ReturnType<typeof run>>;
   /** The rollup's own key check its table's contract records. */
   const record = (r: Run) =>
      rollup?.keyCheck
         ? contractCheck(
              r.entries[address],
              "rollup",
              "",
              rollup.keyCheck.keyColumn,
           )
         : undefined;
   /** A query source's primary key check (the first planned, else `sourceID`'s). */
   const parentRecord = (r: Run, sourceID?: string) => {
      const check = rollup?.parentKeyChecks?.find(
         (c) => sourceID === undefined || c.sourceID === sourceID,
      );
      return check
         ? contractCheck(
              r.entries[address],
              "parent",
              packageRelativeSourceID(pkg.getPackagePath(), check.sourceID),
              check.keyColumn,
           )
         : undefined;
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
      "records unique, carries it with the table, and checks again on a rebuild",
      async () => {
         const s = await scenario(JOINED);
         const first = await s.run(true);
         expect(s.record(first)).toEqual({
            keyColumn: "flight_id",
            unique: true,
         });

         // Carried, unchanged: the contract rides along on the entry.
         const second = await s.run(false);
         expect(s.record(second)).toEqual(s.record(first));

         // A duplicate in the warehouse does not reach the table until it
         // is rebuilt: the check still describes the rows the table holds.
         await s.duckdb.runSQL("INSERT INTO crew_src VALUES (1, 9)");
         const third = await s.run(false);
         expect(s.record(third)?.unique).toBe(true);

         // The rebuild holds the duplicate: the check runs again and says so.
         const fourth = await s.run(true);
         expect(s.record(fourth)?.unique).toBe(false);
         // And a duplicate is carried as faithfully as a pass.
         const fifth = await s.run(false);
         expect(s.record(fifth)?.unique).toBe(false);
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
         expect(s.record(first)).toBeUndefined();
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
      "records unique, carries it with the rollup's table, and checks again on a rebuild",
      async () => {
         const s = await scenario(MANY);
         const first = await s.run(true);
         expect(s.parentRecord(first)).toEqual({
            keyColumn: "id",
            unique: true,
         });
         const second = await s.run(false);
         expect(s.parentRecord(second)).toEqual(s.parentRecord(first));
         // A repeated key, and a NULL one, each found on the next build.
         await s.duckdb.runSQL("INSERT INTO flights_src VALUES (1, 'BB')");
         const third = await s.run(true);
         expect(s.parentRecord(third)?.unique).toBe(false);
         await s.duckdb.runSQL("DELETE FROM flights_src WHERE carrier = 'BB'");
         await s.duckdb.runSQL("INSERT INTO flights_src VALUES (NULL, 'CC')");
         const fourth = await s.run(true);
         expect(s.parentRecord(fourth)?.unique).toBe(false);
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
  #(rollup) grain=["flight_id"]
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
         const first = await s.run(true);
         expect(s.parentRecord(first, base!.sourceID)).toEqual({
            keyColumn: "seg_id",
            unique: true,
         });
         await s.duckdb.runSQL("INSERT INTO segs_src VALUES (NULL, 1, 9.0)");
         const second = await s.run(true);
         expect(s.parentRecord(second, base!.sourceID)?.unique).toBe(false);
      },
      { timeout: 60000 },
   );
});

/**
 * A source stored in a destination, checked over its table at its own build
 * (the join-to-rollup branch in a destination reads that table). The destination
 * is a plain DuckDB file under the environment, as a `storage=` destination
 * of type duckdb is; the stand-in build writes the source's rows there.
 */
async function storedScenario(
   /** Base key checks to attach to the rollup's plan, as for a storage rollup. */
   baseChecks: { sourceID: string; keyColumn: string; sql: string }[] = [],
   /** Whether each build stacks on a stored upstream, as over a stored base. */
   stacked = false,
) {
   const pkg = await packages.create(JOINED);
   const duckdb = (await pkg.getMalloyConnection(
      "duckdb",
   )) as unknown as MalloyConnection;
   for (const sql of SEED) await duckdb.runSQL(sql);
   const compiled = await compilePackageBuildPlan(pkg);
   const [sourceID] = Object.keys(rollupPlansIn(compiled) ?? {});
   const source = sourcesIn(compiled)[sourceID ?? ""];
   if (!sourceID || !source) throw new Error("no planned source");
   compiled.tables[sourceID].storedKeyCheck = "flight_id";
   if (baseChecks.length > 0) {
      (
         rollupPlansIn(compiled)?.[sourceID] as RollupVariantPlan
      ).parentKeyChecks = baseChecks.map((c) => ({
         ...c,
         connectionName: "duckdb",
      }));
   }
   const address = computeSourceEntityId(
      source,
      compiled.connectionDigests,
      targetsIn(compiled)?.[sourceID],
   );
   const root = storageDestinationRoot(pkg.getPackagePath());
   fs.mkdirSync(root, { recursive: true });
   const file = path.join(root, "lake.duckdb");
   const environment = {
      getApiConnection: () => ({ name: "duckdb", type: "duckdb" }),
      getStorageDestination: () => ({ name: "lake", type: "duckdb" }),
      getEnvironmentName: () => "env",
      getEnvironmentPath: () => pkg.getPackagePath(),
      // As a build's package-scoped environment does: contract keys name a
      // checked source by its package-relative sourceID.
      getPackagePath: () => pkg.getPackagePath(),
   } as unknown as BuildEnvironment;
   // The stored rows: the base's, so a duplicate in it reaches the table.
   const storage: Parameters<typeof buildSource>[1] = async (request) => {
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
         // Its column contract, as a storage build records it: the run
         // writes its key checks into it.
         contract: storageContractFor([
            { name: "flight_id", type: "INTEGER" },
            { name: "size", type: "INTEGER" },
         ]),
         ...(stacked ? { upstreamReuse: "reused" } : {}),
      } as ManifestEntry;
   };
   const build = new InstructedBuild({
      buildOneSource: (options) => buildSource(options, storage),
      tableNotOwned: async () => undefined,
      reclaimStorageTablesFromFailedRun: async () => {},
   });
   const instruction = {
      sourceEntityId: address,
      materializedTableId: "mt-stored",
      physicalTableName: "crew_stored_t",
      realization: "COPY",
      destination: "lake",
   } as BuildInstruction;
   let previous: Record<string, ManifestEntry> = {};
   let runs = 0;
   async function run(rebuild: boolean) {
      runs += 1;
      const ledger = new UniquenessLedger(undefined, `run-${runs}`);
      const result = await build.run({
         compiled,
         environment,
         instructions: rebuild ? [instruction] : [],
         seedEntries: rebuild ? {} : previous,
         signal: new AbortController().signal,
         priorRefusals: [],
         hostInstructed: false,
         uniqueness: ledger,
      });
      expect(result.failures).toEqual({});
      previous = result.entries;
      return result;
   }
   const relativeID = packageRelativeSourceID(pkg.getPackagePath(), sourceID);
   /**
    * The check of `id`'s primary key the table's contract records: the
    * stored source's own (`stored:`), or a query source's beside the
    * rollup (`parent:`).
    */
   const record = (
      r: Awaited<ReturnType<typeof run>>,
      id: string = sourceID,
   ) => {
      const entry = r.entries[address];
      if (id === sourceID) {
         return contractCheck(entry, "stored", relativeID, "flight_id");
      }
      const check = (
         compiled.tables[sourceID]?.rollup as RollupVariantPlan
      ).parentKeyChecks?.find((c) => c.sourceID === id);
      return check
         ? contractCheck(
              entry,
              "parent",
              packageRelativeSourceID(pkg.getPackagePath(), id),
              check.keyColumn,
           )
         : undefined;
   };
   /** The rollup's own key check the table's contract records. */
   const own = (r: Awaited<ReturnType<typeof run>>) =>
      contractCheck(r.entries[address], "rollup", "", "flight_id");
   const baseSourceID = (compiled.tables[sourceID]?.rollup as RollupVariantPlan)
      .baseSourceID;
   return {
      duckdb,
      run,
      record,
      own,
      address,
      baseSourceID,
      compiled,
      sourceID,
      relativeID,
   };
}

describe("a stored source's primary key check, on a build into a destination", () => {
   it(
      "records unique over the stored rows, carries it with the table, and checks again on a rebuild",
      async () => {
         const s = await storedScenario();
         const first = await s.run(true);
         expect(s.record(first)).toEqual({
            keyColumn: "flight_id",
            unique: true,
         });
         const second = await s.run(false);
         expect(s.record(second)).toEqual(s.record(first));
         // A repeated key and a NULL one, each found when the table is rebuilt.
         await s.duckdb.runSQL("INSERT INTO crew_src VALUES (1, 9)");
         expect(s.record(await s.run(true))?.unique).toBe(false);
         await s.duckdb.runSQL("DELETE FROM crew_src WHERE size = 9");
         await s.duckdb.runSQL("INSERT INTO crew_src VALUES (NULL, 9)");
         expect(s.record(await s.run(true))?.unique).toBe(false);
      },
      { timeout: 60000 },
   );
});

describe("a storage rollup's base key check, on a build into a destination", () => {
   it(
      "runs on the base's connection, records in the table's contract, and carries it with the table",
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
         expect(s.record(first, "base@x")?.unique).toBe(true);
         expect(s.record(first, "nulls@x")?.unique).toBe(false);
         const second = await s.run(false);
         // Carried with the table, under the source's relative sourceID.
         const keys = parseContract(second.entries[s.address]?.contract)?.keys;
         expect(keys?.[contractKeyName("parent", "base@x", "seg_id")]).toEqual({
            unique: true,
            level: "parent",
         });
         expect(keys?.[contractKeyName("parent", "nulls@x", "seg_id")]).toEqual(
            { unique: false, level: "parent" },
         );
      },
      { timeout: 60000 },
   );
});

describe("a storage rollup's own key check, on a build into a destination", () => {
   it(
      "runs over the base's rows, records in the table's contract, carries it with the table, and checks again on a rebuild",
      async () => {
         const s = await storedScenario();
         const first = await s.run(true);
         expect(s.own(first)).toEqual({ keyColumn: "flight_id", unique: true });
         const second = await s.run(false);
         expect(s.own(second)).toEqual(s.own(first));
         // A rebuild over a duplicate checks again and says so.
         await s.duckdb.runSQL("INSERT INTO crew_src VALUES (1, 9)");
         expect(s.own(await s.run(true))?.unique).toBe(false);
      },
      { timeout: 60000 },
   );
});

/** `crew`'s rows are a persisted source's: the rollup's build reads its table. */
const STACKED = `##! experimental { persistence }
#@ persist name="crew_p"
source: crew_base is duckdb.table('crew_src') -> { select: * }

source: crew is crew_base extend {
  primary_key: flight_id
  #(rollup) grain=["flight_id"]
  measure: crew_size is size.sum()
}

source: flights is duckdb.table('flights_src') extend {
  primary_key: id
  join_one: crew with id
  measure: flight_count is count()
}
`;

describe("a database rollup over a base that is itself stored", () => {
   it(
      "records no verdict for its key: its build grouped the stored table's rows, not the base's",
      async () => {
         const pkg = await packages.create(STACKED);
         const duckdb = (await pkg.getMalloyConnection(
            "duckdb",
         )) as unknown as MalloyConnection;
         for (const sql of SEED) await duckdb.runSQL(sql);
         const compiled = await compilePackageBuildPlan(pkg);
         const [rollupID] = Object.keys(compiled.tables).filter(
            (id) => compiled.tables[id]?.rollup,
         );
         const rollup = compiled.tables[rollupID ?? ""]
            ?.rollup as RollupVariantPlan;
         // A join_one is keyed on its grain, so a check is planned.
         expect(rollup.keyCheck?.keyColumn).toBe("flight_id");
         const addressOf = (id: string) =>
            computeSourceEntityId(
               compiled.tables[id]!.source,
               compiled.connectionDigests,
               compiled.tables[id]?.target,
            );
         const baseID = Object.keys(compiled.tables).find((id) =>
            id.startsWith("crew_base@"),
         )!;
         const instruction = (id: string, table: string) =>
            ({
               sourceEntityId: addressOf(id),
               materializedTableId: `mt-${table}`,
               physicalTableName: table,
               realization: "COPY",
            }) as BuildInstruction;
         const environment = {
            getApiConnection: () => ({ name: "duckdb", type: "duckdb" }),
            getStorageDestination: () => {
               throw new Error("no storage in this spec");
            },
            getEnvironmentName: () => "env",
            getEnvironmentPath: () => pkg.getPackagePath(),
         } as unknown as BuildEnvironment;
         const storage: Parameters<typeof buildSource>[1] = async () => {
            throw new Error("no storage in this spec");
         };
         const build = new InstructedBuild({
            buildOneSource: (options) => buildSource(options, storage),
            tableNotOwned: async () => undefined,
            reclaimStorageTablesFromFailedRun: async () => {},
         });
         const run = (
            instructions: BuildInstruction[],
            seed: Record<string, ManifestEntry>,
            id: string,
         ) =>
            build.run({
               compiled,
               environment,
               instructions,
               seedEntries: seed,
               signal: new AbortController().signal,
               priorRefusals: [],
               hostInstructed: false,
               uniqueness: new UniquenessLedger(undefined, id),
            });
         // The stored base holds a duplicate the warehouse no longer has.
         await duckdb.runSQL("INSERT INTO crew_src VALUES (1, 9)");
         const first = await run(
            [instruction(baseID, "crew_p"), instruction(rollupID!, "crew_r")],
            {},
            "run-1",
         );
         expect(first.failures).toEqual({});
         await duckdb.runSQL("DELETE FROM crew_src WHERE size = 9");
         // The rollup alone, over the base's table carried from run 1.
         const second = await run(
            [instruction(rollupID!, "crew_r")],
            first.entries,
            "run-2",
         );
         expect(second.failures).toEqual({});
         const rollupAddress = addressOf(rollupID!);
         expect(second.entries[rollupAddress]?.upstreamReuse).toBe("reused");
         // The base's own rows are unique now; the rows the rollup grouped
         // are not. No verdict: the join leg reads the key as unknown.
         expect(
            parseContract(second.entries[rollupAddress]?.contract)?.keys?.[
               contractKeyName("rollup", "", "flight_id")
            ],
         ).toBeUndefined();
      },
      { timeout: 60000 },
   );
});

describe("a rollup built over a stored base, on a build into a destination", () => {
   it(
      "records no verdict over the base's own rows, which are not the rows it grouped",
      async () => {
         const s = await storedScenario([], true);
         const check = {
            keyColumn: "flight_id",
            sql: "SELECT 1 AS k WHERE false",
         };
         (
            s.compiled.tables[s.sourceID]?.rollup as RollupVariantPlan
         ).parentKeyChecks = [
            { ...check, sourceID: s.baseSourceID, connectionName: "duckdb" },
            { ...check, sourceID: "other@x", connectionName: "duckdb" },
         ];
         const first = await s.run(true);
         // Neither the rollup's own check nor its base's runs.
         expect(s.own(first)).toBeUndefined();
         expect(s.record(first, s.baseSourceID)).toBeUndefined();
         // Another source's rows are not the rollup's: its check still runs.
         expect(s.record(first, "other@x")?.unique).toBe(true);
      },
      { timeout: 60000 },
   );
});

describe("a run's ledger, beside the key checks", () => {
   // The table's contract is the one store of key verdicts: a run's ledger
   // keeps only the inlined join passes a later run may skip a check on.
   it("records no key verdict, and a record an earlier release wrote is not read", () => {
      const ledger = new UniquenessLedger(undefined, "run-1");
      ledger.markWritten("a", "t");
      expect(ledger.toMetadata()).toBeUndefined();
      // An earlier release's run record: its `rollupKeys` hold no pass, so
      // deleting a run leaves the record as it is.
      expect(
         withoutUniquenessPasses({
            mode: "auto",
            [INLINE_UNIQUENESS_KEY]: {
               passes: {},
               checks: [],
               rollupKeys: { a: { table: "t", keyColumn: "k", unique: true } },
            },
         }),
      ).toBeUndefined();
   });
});

/**
 * A package's materialization whose one model declares `contents`' sources,
 * with their primary keys, as a bind reads them.
 */
function declaring(
   contents: Record<string, { sourceID: string; primaryKey: string }>,
): PackageMaterialization {
   const model = {
      setServedCopyContext: () => {},
      setStringCapabilityReader: () => {},
      setScopeFactsRecorder: () => {},
      prepareServedCopy: async () => {},
      persistedViewBuilds: () => [],
      servedCopyProblems: () => [],
      gateLoadWarnings: () => [],
      storedColumnTypeMismatches: () => [],
      getModelDef: () => ({ contents }),
   };
   return new PackageMaterialization({
      models: () => new Map([["m.malloy", model]]),
      packageName: () => "pkg",
      packagePath: () => "/nowhere",
      malloyConfig: () => undefined,
   } as unknown as PackageMaterializationOwner);
}

/** A table's contract recording its rollup's own key check over `keyColumn`. */
const rollupChecked = (keyColumn: string, unique: boolean) => ({
   ...contractFor(),
   keys: {
      [contractKeyName("rollup", "", keyColumn)]: { unique, level: "rollup" },
   },
});

describe("a key check, bound for serving", () => {
   it("never reaches a binding through a manifest split, from the store or a host", () => {
      const database = {
         sourceEntityId: "a",
         physicalTableName: "r_t",
         connectionName: "duckdb",
         keyUniqueness: { keyColumn: "k", unique: true },
      } as unknown as ManifestEntry;
      const stored = {
         sourceEntityId: "h",
         sourceName: "flights",
         physicalTableName: "flights_t",
         connectionName: "duckdb",
         storageDestinationName: "lake",
         schema: [{ name: "id", type: "INTEGER" }],
         keyUniqueness: {
            parentKeys: { flights: true },
            parentKeyColumns: { flights: "id" },
         },
      } as unknown as ManifestEntry;
      // The contract is the one store of key verdicts: one an entry carries
      // is stripped, whoever sent it.
      const split = splitManifestEntries({ a: database, h: stored }, "store");
      expect(split.tableNameManifest.a?.keyUniqueness).toBeUndefined();
      expect(
         (split.storageEntries.h as { keyUniqueness?: unknown }).keyUniqueness,
      ).toBeUndefined();
      expect(
         deriveServeBindings(split.storageEntries, {})[0]?.primaryKeys,
      ).toBeUndefined();
      // Read off the contract and set on the entry, a check reaches the binding.
      expect(deriveServeBindings({ h: stored }, {})[0]?.primaryKeys).toEqual({
         flights: true,
      });
   });

   it("forgets a storage binding's key check when a run is about to write its table", () => {
      const materialization = new PackageMaterialization({
         models: () => new Map(),
         packageName: () => "pkg",
         packagePath: () => "/nowhere",
      } as unknown as PackageMaterializationOwner);
      const held = materialization as unknown as {
         storageBindings: ReturnType<typeof deriveServeBindings>;
      };
      const binding: ReturnType<typeof deriveServeBindings>[number] = {
         kind: "persist",
         tier: "storage",
         sourceName: "flights",
         destinationName: "lake",
         virtualHandle: "h",
         tablePath: "lake.flights_t",
         schema: [],
         primaryKeys: { flights: true },
      };
      held.storageBindings = [
         binding,
         { ...binding, sourceName: "other", virtualHandle: "o" },
      ];
      materialization.forgetKeyUniqueness(["h"]);
      expect(held.storageBindings[0]?.primaryKeys).toBeUndefined();
      expect(held.storageBindings[1]?.primaryKeys).toEqual({
         flights: true,
      });
   });

   it("binds a stored rollup's own key check, warns while it says repeated, and forgets it", () => {
      const stored = (id: string, unique: boolean) =>
         ({
            sourceEntityId: id,
            sourceName: `crew_${id}__rollup`,
            physicalTableName: `crew_${id}_t`,
            connectionName: "duckdb",
            storageDestinationName: "lake",
            origin: "rollup",
            schema: [{ name: "flight_id", type: "INTEGER" }],
            keyUniqueness: { keyColumn: "flight_id", unique },
         }) as unknown as ManifestEntry;
      // Each entry carries its check as a bind reads it off the contract.
      const bindings = deriveServeBindings(
         { a: stored("a", false), b: stored("b", true) },
         {},
      );
      expect(bindings.map((b) => [b.virtualHandle, b.rollupKey])).toEqual([
         ["a", { keyColumn: "flight_id", unique: false }],
         ["b", { keyColumn: "flight_id", unique: true }],
      ]);
      // A manifest split never brings one.
      expect(
         deriveServeBindings(
            splitManifestEntries({ a: stored("a", false) }, "host")
               .storageEntries,
            {},
         )[0]?.rollupKey,
      ).toBeUndefined();
      const materialization = new PackageMaterialization({
         models: () => new Map(),
         packageName: () => "pkg",
         packagePath: () => "/nowhere",
      } as unknown as PackageMaterializationOwner);
      materialization.plan.buildPlan = {
         sources: {
            "ra@m": {
               name: "crew_a__rollup",
               sourceEntityId: "a",
               modelPath: "m.malloy",
               preaggregate: {
                  baseSourceName: "crew",
                  grainDimensions: ["flight_id"],
                  measures: ["crew_size"],
               },
            },
         },
      } as never;
      (
         materialization as unknown as {
            storageBindings: typeof bindings;
         }
      ).storageBindings = bindings;
      expect(
         materialization
            .repeatedJoinOneKeyWarnings()
            .map((w) => [w.subject, w.code]),
      ).toEqual([["crew", "rollup-join-one-key-repeated"]]);
      materialization.forgetKeyUniqueness(["a"]);
      expect(materialization.repeatedJoinOneKeyWarnings()).toEqual([]);
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

   it("forgets a bound record when a run is about to write its table", () => {
      const materialization = new PackageMaterialization({
         models: () => new Map(),
         packageName: () => "pkg",
         packagePath: () => "/nowhere",
      } as unknown as PackageMaterializationOwner);
      materialization.bind({
         databaseAsGiven: true,
         database: {
            a: { tableName: "r_t", contract: rollupChecked("k", true) },
            b: { tableName: "s_t", contract: rollupChecked("k", false) },
         },
      });
      expect(
         materialization.verdicts.tableCatalog().bound.a?.keyUniqueness,
      ).toEqual({ keyColumn: "k", unique: true });
      materialization.forgetKeyUniqueness(["a"]);
      const bound = materialization.verdicts.tableCatalog().bound;
      expect(bound.a).toEqual({
         tableName: "r_t",
         contract: rollupChecked("k", true),
      });
      expect(bound.b?.keyUniqueness?.unique).toBe(false);
   });

   it("warns the package of a rollup whose build found its join_one key repeated, while that is bound", () => {
      const materialization = new PackageMaterialization({
         models: () => new Map(),
         packageName: () => "pkg",
         packagePath: () => "/nowhere",
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
      materialization.plan.buildPlan = {
         sources: {
            "r1@m": rollup("a", "crew"),
            "r2@m": rollup("b", "crew_ok"),
            "p@m": { name: "plain", sourceEntityId: "c", preaggregate: null },
         },
      } as never;
      const repeated = rollupChecked("flight_id", false);
      materialization.bind({
         databaseAsGiven: true,
         database: {
            a: { tableName: "r_t", contract: repeated },
            b: { tableName: "s_t", contract: rollupChecked("flight_id", true) },
            // Not a rollup: no warning, whatever it carries.
            c: { tableName: "p_t", contract: repeated },
         },
      });
      const warnings = materialization.repeatedJoinOneKeyWarnings();
      expect(warnings.map((w) => [w.subject, w.code, w.model])).toEqual([
         ["crew", "rollup-join-one-key-repeated", "m.malloy"],
      ]);
      expect(warnings[0].message).toContain("`flight_id` more than once");
      // A run about to rewrite the table forgets the check, and the warning.
      materialization.forgetKeyUniqueness(["a"]);
      expect(materialization.repeatedJoinOneKeyWarnings()).toEqual([]);
   });

   it("does not warn of a join_one key for a rollup whose build ran only query source key checks", () => {
      const materialization = declaring({
         flights: { sourceID: "flights@m", primaryKey: "id" },
      });
      materialization.plan.buildPlan = {
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
         materialization.bind({
            databaseAsGiven: true,
            database: {
               a: {
                  tableName: "r_t",
                  contract: {
                     ...contractFor(),
                     keys: {
                        "parent:flights@m.id": { unique, level: "parent" },
                     },
                  },
               },
            },
         });
         // Bound, and no warning.
         expect(
            materialization.verdicts.tableCatalog().bound.a?.keyUniqueness
               ?.parentKeys,
         ).toEqual({ "flights@m": unique });
         expect(materialization.repeatedJoinOneKeyWarnings()).toEqual([]);
      }
   });
});

// Q8, frozen: the table's contract is the one store of key verdicts. A bind
// reads every check off it, under the primary key each source declares now,
// for a table this server built and one a host's worker built alike; a
// verdict an entry brings any other way (a local run's record) is dropped,
// and a check that could not run, or ran under another key, is not passed.
describe("a bind's key checks, read only off the table's contract", () => {
   // `flights` keyed on `id`, and `segs` on `seg_id`, now; `a` is a rollup's
   // table, `p` a pair table that inlines `flights`' join_many `segs`.
   const contract = (
      keys: Record<string, boolean | "unverified">,
      producer?: string,
   ) => ({
      ...contractFor(),
      ...(producer ? { producer } : {}),
      keys: Object.fromEntries(
         Object.entries(keys).map(([name, found]) => [
            name,
            {
               unique: found === true,
               level: name.slice(0, name.indexOf(":")),
               ...(found === "unverified" ? { unverified: true } : {}),
            },
         ]),
      ),
   });
   const rows: {
      name: string;
      table: "a" | "p";
      entry: Record<string, unknown>;
      bound: KeyUniqueness | undefined;
   }[] = [
      {
         name: "a local run's verdict on the entry yields to the contract's",
         table: "a",
         entry: {
            keyUniqueness: {
               keyColumn: "flight_id",
               unique: true,
               parentKeys: { "flights@m": true },
               parentKeyColumns: { "flights@m": "id" },
            },
            contract: contract({
               "rollup:flight_id": false,
               "parent:flights@m.id": false,
            }),
         },
         bound: {
            keyColumn: "flight_id",
            unique: false,
            parentKeys: { "flights@m": false },
            parentKeyColumns: { "flights@m": "id" },
         },
      },
      {
         name: "a local run's verdict with no contract to back it binds nothing",
         table: "a",
         entry: { keyUniqueness: { keyColumn: "flight_id", unique: true } },
         bound: undefined,
      },
      {
         name: "a check under a primary key the model has since moved binds nothing",
         table: "a",
         entry: {
            keyUniqueness: { parentKeys: { "flights@m": true } },
            contract: contract({ "parent:flights@m.old_id": true }),
         },
         bound: undefined,
      },
      {
         name: "a host-built contract's verdict binds as a local build's does",
         table: "a",
         entry: {
            contract: contract(
               { "rollup:flight_id": true, "parent:flights@m.id": true },
               "a host's worker",
            ),
         },
         bound: {
            keyColumn: "flight_id",
            unique: true,
            parentKeys: { "flights@m": true },
            parentKeyColumns: { "flights@m": "id" },
         },
      },
      {
         name: "a pair table's leaf check binds as the leaf source's, through the plan",
         table: "p",
         entry: { contract: contract({ "leaf:segs.seg_id": true }) },
         bound: {
            parentKeys: { "segs@m": true },
            parentKeyColumns: { "segs@m": "seg_id" },
         },
      },
      {
         name: "a host-built pair table's leaf check binds too",
         table: "p",
         entry: {
            contract: contract({ "leaf:segs.seg_id": true }, "a host's worker"),
         },
         bound: {
            parentKeys: { "segs@m": true },
            parentKeyColumns: { "segs@m": "seg_id" },
         },
      },
      {
         name: "a leaf check under a key the leaf has since moved binds nothing",
         table: "p",
         entry: { contract: contract({ "leaf:segs.flight_id": true }) },
         bound: undefined,
      },
      {
         name: "a leaf check that could not run binds as not passed",
         table: "p",
         entry: { contract: contract({ "leaf:segs.seg_id": "unverified" }) },
         bound: {
            parentKeys: { "segs@m": false },
            parentKeyColumns: { "segs@m": "seg_id" },
         },
      },
   ];
   for (const row of rows) {
      it(row.name, () => {
         const materialization = declaring({
            flights: { sourceID: "flights@m", primaryKey: "id" },
            segs: { sourceID: "segs@m", primaryKey: "seg_id" },
         });
         // `flights_pairs` stores `flights` with its join_many `segs` inlined.
         materialization.plan.buildPlan = {
            sources: {
               "flights_pairs@m": {
                  name: "flights_pairs",
                  sourceEntityId: "p",
               },
            },
         } as never;
         materialization.plan.materializePlans = {
            "flights_pairs@m": {
               persist: {},
               pair: {
                  join: "segs",
                  parentKey: "id",
                  childKey: "seg_id",
                  childSourceID: "segs@m",
                  parentRowColumn: "__parent_row",
               },
            } as unknown as MaterializePlan,
         };
         materialization.bind({
            databaseAsGiven: true,
            database: { [row.table]: { tableName: "t", ...row.entry } },
         });
         expect(
            materialization.verdicts.tableCatalog().bound[row.table]
               ?.keyUniqueness,
         ).toEqual(row.bound);
      });
   }
});
