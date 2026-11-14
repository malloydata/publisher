// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A pair table's leaf key check (known limitation 40): a `#(materialize)`
 * source that inlines a join_many is built whatever its leaf's key holds, and
 * an auto-run then checks that key unique and never NULL over the leaf
 * source's rows and records what it found beside the table, under the leaf
 * source's sourceID. The served copy re-declares a count through the path
 * only where that record passed. Through real builds run after run, as the
 * rollup key check's spec drives them: the record describes the table the
 * checking run wrote, is carried while that table stands, and reaches the
 * colocated table's serve binding.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { computeSourceEntityId } from "../plan/build_plan";
import { compilePackageBuildPlan } from "../plan/build_plan_compile";
import type { MaterializePlan } from "../plan/build_file_generator";
import {
   PackageMaterialization,
   type PackageMaterializationOwner,
} from "../package_materialization";
import type { ServeBinding } from "../serve/materialization_serve_transform";
import type { BuildInstruction, ManifestEntry } from "../types";
import {
   INLINE_UNIQUENESS_KEY,
   keyUniquenessAt,
   pairLeafKeyChecksOf,
   rollupKeyRecordKey,
   rollupKeyRecordsIn,
   UniquenessLedger,
} from "./inline_uniqueness";
import { InstructedBuild } from "./instructed_build";
import type { BuildEnvironment } from "./materialization_run_support";
import { SourceBuilder } from "./source_build";
import { deriveColocatedServeBindings } from "../serve/colocated_bindings";

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

type Metadata = NonNullable<ReturnType<UniquenessLedger["toMetadata"]>>;

async function scenario() {
   const pkg = await packages.create(MODEL);
   const duckdb = (await pkg.getMalloyConnection(
      "duckdb",
   )) as unknown as MalloyConnection;
   for (const sql of SEED) await duckdb.runSQL(sql);
   const compiled = await compilePackageBuildPlan(pkg);
   const [sourceID] = Object.keys(compiled.materializePlans ?? {});
   const source = compiled.sources[sourceID ?? ""];
   if (!sourceID || !source) throw new Error("no planned pair source");
   const address = computeSourceEntityId(
      source,
      compiled.connectionDigests,
      compiled.targets?.[sourceID],
   );
   const [leaf] = pairLeafKeyChecksOf(compiled.materializePlans?.[sourceID]);
   if (!leaf) throw new Error("no leaf key check planned");
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
      materializedTableId: "mt-pairs",
      physicalTableName: "flights_pairs_t",
      realization: "COPY",
   } as BuildInstruction;
   let previous: {
      entries: Record<string, ManifestEntry>;
      metadata?: Metadata;
   } = { entries: {} };
   let runs = 0;
   async function run(rebuild: boolean, orchestrated = false) {
      runs += 1;
      const ledger = new UniquenessLedger(
         undefined,
         `run-${runs}`,
         previous.metadata
            ? rollupKeyRecordsIn({ [INLINE_UNIQUENESS_KEY]: previous.metadata })
            : undefined,
      );
      const result = await build.run(
         compiled,
         environment,
         rebuild
            ? [orchestrated ? { ...instruction, sourceID } : instruction]
            : [],
         rebuild ? {} : previous.entries,
         new AbortController().signal,
         false,
         undefined,
         undefined,
         undefined,
         [],
         undefined,
         orchestrated,
         "recompute",
         ledger,
      );
      expect(result.failures).toEqual({});
      previous = { entries: result.entries, metadata: result.uniquenessChecks };
      return result;
   }
   const key = rollupKeyRecordKey(address, leaf.sourceID);
   return { duckdb, compiled, sourceID, source, address, leaf, key, run };
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
         expect(first.uniquenessChecks?.rollupKeys?.[s.key]).toMatchObject({
            keyColumn: "seg_id",
            unique: false,
            sourceID: s.leaf.sourceID,
         });
      },
      { timeout: 60000 },
   );

   it(
      "records a clean key, carries it while the table stands, and checks again on a rebuild",
      async () => {
         const s = await scenario();
         const first = await s.run(true);
         expect(first.uniquenessChecks?.rollupKeys?.[s.key]).toMatchObject({
            unique: true,
            checkedInRun: "run-1",
         });
         // Carried: the table was not written again.
         const second = await s.run(false);
         expect(second.uniquenessChecks?.rollupKeys?.[s.key]).toMatchObject({
            unique: true,
            checkedInRun: "run-1",
         });
         // A rebuild over a NULL key checks again.
         await s.duckdb.runSQL("INSERT INTO segs_src VALUES (NULL, 2, 9.0)");
         const third = await s.run(true);
         expect(third.uniquenessChecks?.rollupKeys?.[s.key]).toMatchObject({
            unique: false,
            checkedInRun: "run-3",
         });
      },
      { timeout: 60000 },
   );

   it(
      "reaches the colocated table's serve binding",
      async () => {
         const s = await scenario();
         const result = await s.run(true);
         const entry = result.entries[s.address] as ManifestEntry;
         const keyUniqueness = keyUniquenessAt(
            result.uniquenessChecks?.rollupKeys,
            s.address,
            entry,
         );
         expect(keyUniqueness?.parentKeys).toEqual({ [s.leaf.sourceID]: true });
         const bindings = deriveColocatedServeBindings({
            entries: {
               [s.address]: {
                  tableName: entry.physicalTableName ?? "",
                  connectionName: entry.connectionName,
                  ...(keyUniqueness ? { keyUniqueness } : {}),
               },
            },
            planSources: {
               [s.sourceID]: {
                  name: s.source.name,
                  sourceEntityId: s.address,
                  connectionName: "duckdb",
               },
            },
            materializePlans: s.compiled.materializePlans ?? {},
            eligible: () => true,
         });
         expect(bindings.map((b) => b.primaryKeys)).toEqual([
            { [s.leaf.sourceID]: true },
         ]);
      },
      { timeout: 60000 },
   );

   // A host instructs the build (an instruction naming the source's
   // sourceID): the run never checks the leaf, so the table carries no
   // record and the served copy declines every leaf count, sum and average,
   // whatever the leaf's key holds (known limitation 40).
   it(
      "is never recorded on an orchestrated build",
      async () => {
         const s = await scenario();
         await s.duckdb.runSQL("INSERT INTO segs_src VALUES (NULL, 2, 9.0)");
         const result = await s.run(true, true);
         expect(result.entries[s.address]?.physicalTableName).toBe(
            "flights_pairs_t",
         );
         expect(result.uniquenessChecks?.rollupKeys?.[s.key]).toBeUndefined();
         expect(
            keyUniquenessAt(
               result.uniquenessChecks?.rollupKeys,
               s.address,
               result.entries[s.address] as ManifestEntry,
            ),
         ).toBeUndefined();
      },
      { timeout: 60000 },
   );

   // A run about to rewrite a colocated pair table forgets its leaf record on
   // the bindings the models serve from, before any table is written: a run
   // that fails part-way, or a rewrite that brings a NULL or repeated leaf
   // key, never leaves a pass bound over rows it did not check.
   it("is forgotten on the models' colocated bindings when a run is about to write the table", () => {
      const pushed: ServeBinding[][] = [];
      const model = {
         setServeBindings: (b: ServeBinding[]) => pushed.push(b),
      };
      const materialization = new PackageMaterialization({
         models: () => new Map([["m.malloy", model]]),
         packageName: () => "pkg",
         packagePath: () => "/nowhere",
         malloyConfig: () => undefined,
      } as unknown as PackageMaterializationOwner);
      materialization.buildPlan = {
         sources: {
            "flights@m": {
               name: "flights",
               sourceEntityId: "a",
               connectionName: "duckdb",
            },
         },
      } as never;
      materialization.materializePlans = {
         "flights@m": {
            persist: {},
            storedColumns: [{ name: "id", type: "number" }],
         } as unknown as MaterializePlan,
      };
      materialization.colocatedSourceEligibility = {
         eligibleEntityIds: new Set(["a"]),
      } as never;
      const bound = () =>
         pushed[pushed.length - 1]?.find((b) => b.virtualHandle === "a");
      materialization.recordManifestBinding({
         a: {
            tableName: "flights_pairs",
            connectionName: "duckdb",
            keyUniqueness: { parentKeys: { "segs@m": true } },
         },
      });
      expect(bound()?.primaryKeys).toEqual({ "segs@m": true });
      materialization.forgetKeyUniqueness(["a"]);
      expect(bound()?.tablePath).toBe("flights_pairs");
      expect(bound()?.primaryKeys).toBeUndefined();
      // A later bind that holds the record again pushes it.
      materialization.recordManifestBinding({
         a: {
            tableName: "flights_pairs",
            connectionName: "duckdb",
            keyUniqueness: { parentKeys: { "segs@m": false } },
         },
      });
      expect(bound()?.primaryKeys).toEqual({ "segs@m": false });
   });
});
