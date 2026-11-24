// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A pair table's leaf key check (known limitation 40): a `#(materialize)`
 * source that inlines a join_many is built whatever its leaf's key holds, and
 * every build then checks that key unique and never NULL over the leaf
 * source's rows and records what it found in the table's contract, under
 * the leaf's join path (`leaf:segs.seg_id`). A bind maps that key to the leaf
 * source's sourceID through the plan, under the primary key the leaf
 * declares now, and the served copy re-declares a count through the path
 * only where that check passed. Through real builds run after run: the
 * check describes the table the checking build wrote, rides on its entry
 * while that table stands, and reaches the table's serve binding.
 */

import {
   materializePlansIn,
   sourcesIn,
   targetsIn,
} from "../../../test_helpers/materialization_test_fixtures";
import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { DatabaseTier } from "./database_build_strategy";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { computeSourceEntityId } from "../plan/build_plan";
import { compilePackageBuildPlan } from "../plan/build_plan_compile";
import type { MaterializePlan } from "../domain/table_plans";
import {
   PackageMaterialization,
   type PackageMaterializationOwner,
} from "../package_materialization";
import type { Binding } from "../domain/binding";
import type { BuildInstruction, ManifestEntry } from "../domain/types";
import { parseContract } from "../domain/column_contract";
import { pairLeafKeyChecksOf, UniquenessLedger } from "./inline_uniqueness";
import { InstructedBuild } from "./instructed_build";
import { type BuildEnvironment } from "./build_upstreams";
import { buildSource } from "./source_build";
import { contractFor } from "../../../test_helpers/contract_fixture";

const packages = tempBuildPlanPackages();
let restore: () => void;
beforeEach(() => {
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true", PERSIST_STORAGE_MODE: "off" },
   }).restore;
});
afterEach(async () => {
   restore();
   await packages.cleanup();
});

const SEED = [
   "CREATE OR REPLACE TABLE flights_src AS SELECT * FROM (VALUES (1, 'AA'), (2, 'AA'), (3, 'BB')) AS t(id, carrier)",
   "CREATE OR REPLACE TABLE segs_src AS SELECT * FROM (VALUES (10, 1, 2.0), (11, 1, 3.0), (12, 3, 4.0)) AS t(seg_id, flight_id, miles)",
];

const MODEL = `source: segs is duckdb.table('segs_src') extend { primary_key: seg_id }

#(materialize) name="flights_pairs" inline=["segs"]
source: flights is duckdb.table('flights_src') extend {
  primary_key: id
  join_many: segs on segs.flight_id = id
  measure: seg_n is segs.count()
}
`;

async function scenario() {
   const pkg = await packages.create(MODEL);
   const duckdb = (await pkg.getMalloyConnection(
      "duckdb",
   )) as unknown as MalloyConnection;
   for (const sql of SEED) await duckdb.runSQL(sql);
   const compiled = await compilePackageBuildPlan(pkg);
   const [sourceID] = Object.keys(materializePlansIn(compiled) ?? {});
   const source = sourcesIn(compiled)[sourceID ?? ""];
   if (!sourceID || !source) throw new Error("no planned pair source");
   const address = computeSourceEntityId(
      source,
      compiled.connectionDigests,
      targetsIn(compiled)?.[sourceID],
   );
   const [leaf] = pairLeafKeyChecksOf(materializePlansIn(compiled)?.[sourceID]);
   if (!leaf) throw new Error("no leaf key check planned");
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
   const instruction = {
      sourceEntityId: address,
      materializedTableId: "mt-pairs",
      physicalTableName: "flights_pairs_t",
      realization: "COPY",
   } as BuildInstruction;
   let previous: Record<string, ManifestEntry> = {};
   let runs = 0;
   async function run(
      rebuild: boolean,
      orchestrated = false,
      seed?: Record<string, ManifestEntry>,
   ) {
      runs += 1;
      const ledger = new UniquenessLedger(undefined, `run-${runs}`);
      const result = await build.run({
         compiled,
         environment,
         instructions: rebuild
            ? [orchestrated ? { ...instruction, sourceID } : instruction]
            : [],
         seedEntries: rebuild ? {} : (seed ?? previous),
         signal: new AbortController().signal,
         priorRefusals: [],
         hostInstructed: orchestrated,
         uniqueness: ledger,
      });
      expect(result.failures).toEqual({});
      previous = result.entries;
      return result;
   }
   /** The leaf check `result`'s pair table records in its contract. */
   const leafCheck = (result: Awaited<ReturnType<typeof run>>) =>
      parseContract(result.entries[address]?.contract)?.keys?.[
         leaf.contractKey
      ];
   return { duckdb, compiled, sourceID, source, address, leaf, leafCheck, run };
}

describe("a pair table's leaf key check, on a build", () => {
   it(
      "is planned for the inlined join_many's leaf, and never fails the build",
      async () => {
         const s = await scenario();
         expect(s.leaf).toMatchObject({
            keyColumn: "seg_id",
            sourceID: expect.stringMatching(/^segs@/),
         });
         // A NULL key and a repeated one: the build still succeeds.
         await s.duckdb.runSQL(
            "INSERT INTO segs_src VALUES (NULL, 2, 9.0), (10, 2, 1.0)",
         );
         const first = await s.run(true);
         expect(first.entries[s.address]?.physicalTableName).toBe(
            "flights_pairs_t",
         );
         expect(s.leafCheck(first)).toEqual({ unique: false, level: "leaf" });
      },
      { timeout: 60000 },
   );

   it(
      "records a clean key, carries it with the table, and checks again on a rebuild",
      async () => {
         const s = await scenario();
         const passed = { unique: true, level: "leaf" };
         expect(s.leafCheck(await s.run(true))).toEqual(passed);
         // Carried: the table was not written again, and its contract rides
         // on its entry.
         expect(s.leafCheck(await s.run(false))).toEqual(passed);
         // A rebuild over a NULL key checks again.
         await s.duckdb.runSQL("INSERT INTO segs_src VALUES (NULL, 2, 9.0)");
         expect(s.leafCheck(await s.run(true))).toEqual({
            unique: false,
            level: "leaf",
         });
      },
      { timeout: 60000 },
   );

   it(
      "is written into the table's contract, under the leaf's join path",
      async () => {
         const s = await scenario();
         const result = await s.run(true);
         const entry = result.entries[s.address] as ManifestEntry;
         expect(s.leaf.contractKey).toBe("leaf:segs.seg_id");
         expect(parseContract(entry.contract)?.keys).toEqual({
            [s.leaf.contractKey]: { unique: true, level: "leaf" },
         });
      },
      { timeout: 60000 },
   );

   // A host instructs the build (an instruction naming the source's
   // sourceID): its worker records the check in the contract as an auto-run
   // does, and the host round-trips the contract whole, so the table serves
   // by it alike.
   it(
      "is recorded in the contract on an orchestrated build too",
      async () => {
         const s = await scenario();
         await s.duckdb.runSQL("INSERT INTO segs_src VALUES (NULL, 2, 9.0)");
         const result = await s.run(true, true);
         expect(result.entries[s.address]?.physicalTableName).toBe(
            "flights_pairs_t",
         );
         expect(s.leafCheck(result)).toEqual({ unique: false, level: "leaf" });
      },
      { timeout: 60000 },
   );

   // A run about to rewrite a database pair table forgets the key checks
   // bound off its contract on the bindings the models serve from, before
   // any table is written: a run that fails part-way, or a rewrite that
   // brings a NULL or repeated key, never leaves a pass bound over rows it
   // did not check.
   it("is forgotten on the models' database bindings when a run is about to write the table", () => {
      const model = {
         setServedCopyContext: () => {},
         setStringCapabilityReader: () => {},
         setScopeFactsRecorder: () => {},
         prepareServedCopy: async () => {},
         persistedViewBuilds: () => [],
         servedCopyProblems: () => [],
         gateLoadWarnings: () => [],
         storedColumnTypeMismatches: () => [],
         // `segs`, keyed on seg_id, as a bind reads the models' primary keys.
         getModelDef: () => ({
            contents: { segs: { sourceID: "segs@m", primaryKey: "seg_id" } },
         }),
      };
      const checked = (unique: boolean) => ({
         ...contractFor(),
         keys: { "parent:segs@m.seg_id": { unique, level: "parent" } },
      });
      const materialization = new PackageMaterialization({
         models: () => new Map([["m.malloy", model]]),
         packageName: () => "pkg",
         packagePath: () => "/nowhere",
         malloyConfig: () => undefined,
      } as unknown as PackageMaterializationOwner);
      materialization.plan.buildPlan = {
         sources: {
            "flights@m": {
               name: "flights",
               sourceEntityId: "a",
               connectionName: "duckdb",
               dialect: "duckdb",
            },
         },
      } as never;
      materialization.plan.materializePlans = {
         "flights@m": {
            persist: {},
            storedColumns: [{ name: "id", type: "number" }],
         } as unknown as MaterializePlan,
      };
      materialization.plan.databaseSourceEligibility = {
         eligibleEntityIds: new Set(["a"]),
      } as never;
      const bound = (): Binding | undefined =>
         materialization
            .snapshot()
            .bindings.find((b) => b.virtualHandle === "a");
      materialization.bind({
         databaseAsGiven: true,
         database: {
            a: {
               tableName: "flights_pairs",
               contract: checked(true),
               connectionName: "duckdb",
            },
         },
      });
      expect(bound()?.primaryKeys).toEqual({ "segs@m": true });
      materialization.forgetKeyUniqueness(["a"]);
      expect(bound()?.tablePath).toBe('"flights_pairs"');
      expect(bound()?.primaryKeys).toBeUndefined();
      // A later bind whose contract records a check again pushes it.
      materialization.bind({
         databaseAsGiven: true,
         database: {
            a: {
               tableName: "flights_pairs",
               contract: checked(false),
               connectionName: "duckdb",
            },
         },
      });
      expect(bound()?.primaryKeys).toEqual({ "segs@m": false });
   });
});

describe("a pair table's leaf key check that could not run", () => {
   /** Leaf key checks fail to run while `failing.on`; counts each try. */
   function flakyChecks() {
      const failing = { on: true, tries: 0 };
      const original = DatabaseTier.prototype.checkKey;
      const spy = spyOn(DatabaseTier.prototype, "checkKey").mockImplementation(
         function (this: DatabaseTier, ...args) {
            const check = args[1] as { contractKey?: string };
            if (check.contractKey?.startsWith("leaf:")) {
               failing.tries += 1;
               if (failing.on) {
                  return Promise.reject(new Error("leaf check unavailable"));
               }
            }
            return original.apply(this, args);
         },
      );
      return { failing, restore: () => spy.mockRestore() };
   }
   const unverified = {
      unique: false,
      level: "leaf",
      unverified: true,
   } as const;

   it(
      "is tried again once by the next standalone run, which records the check passed on the carried table",
      async () => {
         const s = await scenario();
         const flaky = flakyChecks();
         try {
            const first = await s.run(true);
            expect(s.leafCheck(first)).toEqual(unverified);
            flaky.failing.on = false;
            const tries = flaky.failing.tries;
            const second = await s.run(false);
            expect(flaky.failing.tries - tries).toBe(1);
            // Carried, not rebuilt: the same table, now checked.
            expect(second.entries[s.address]?.builtAt).toBe(
               first.entries[s.address]?.builtAt,
            );
            expect(s.leafCheck(second)).toEqual({
               unique: true,
               level: "leaf",
            });
            // Passed: nothing is tried again.
            await s.run(false);
            expect(flaky.failing.tries - tries).toBe(1);
         } finally {
            flaky.restore();
         }
      },
      { timeout: 60000 },
   );

   it(
      "stays unverified when it still cannot run, tried once per run and never in a loop",
      async () => {
         const s = await scenario();
         const flaky = flakyChecks();
         try {
            await s.run(true);
            for (const expected of [2, 3]) {
               expect(s.leafCheck(await s.run(false))).toEqual(unverified);
               expect(flaky.failing.tries).toBe(expected);
            }
         } finally {
            flaky.restore();
         }
      },
      { timeout: 60000 },
   );

   it(
      "records a key the retry finds repeated as failed",
      async () => {
         const s = await scenario();
         const flaky = flakyChecks();
         try {
            await s.run(true);
            flaky.failing.on = false;
            await s.duckdb.runSQL("INSERT INTO segs_src VALUES (10, 2, 1.0)");
            expect(s.leafCheck(await s.run(false))).toEqual({
               unique: false,
               level: "leaf",
            });
         } finally {
            flaky.restore();
         }
      },
      { timeout: 60000 },
   );

   it(
      "is not tried by a host's run, which carries what it was handed",
      async () => {
         const s = await scenario();
         const flaky = flakyChecks();
         try {
            const first = await s.run(true);
            flaky.failing.on = false;
            const tries = flaky.failing.tries;
            const carried = await s.run(false, true, first.entries);
            expect(flaky.failing.tries).toBe(tries);
            expect(s.leafCheck(carried)).toEqual(unverified);
         } finally {
            flaky.restore();
         }
      },
      { timeout: 60000 },
   );
});
