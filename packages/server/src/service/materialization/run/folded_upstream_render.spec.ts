// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A database build over an upstream whose address folds in its `name=`
 * (foldDistinctTableNames), through real compiles: `col_a` has a `name=`,
 * `twin_a` has the same SQL and none (a `storage=` twin), so `col_a`'s
 * address is no longer the BuildID the compiler looks its table up by.
 * `col_b`, a database source over `col_a`, must still render over `col_a`'s
 * table: under strict upstreams it once missed it
 * (`runtime-manifest-strict-miss`), and otherwise recomputed it from raw.
 * Self-instructed and host-instructed, and without the twin (no fold).
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { sourcesIn } from "../../../test_helpers/materialization_test_fixtures";
import { useMaterializationOn } from "../../../test_helpers/settings_env";
import { compilePackageBuildPlan } from "../plan/build_plan_compile";
import { compilerBuildId, planAddresses } from "../plan/addressing";
import type { ManifestEntry } from "../domain/types";
import { type BuildEnvironment } from "./build_upstreams";
import { InstructedBuild } from "./instructed_build";
import { deriveSelfInstructions } from "./run_instructions";
import { buildSource } from "./source_build";

const packages = tempBuildPlanPackages();
useMaterializationOn({ each: true });
afterEach(async () => {
   await packages.cleanup();
});

const SEED =
   "CREATE OR REPLACE TABLE ev_src AS SELECT * FROM (VALUES ('a', 1), ('a', 2), ('b', 3)) AS t(k, v)";

const COL_A = `#@ persist name="fa"
source: col_a is duckdb.sql("select k, sum(v) as total from ev_src group by 1")
`;
const TWIN_A = `#@ persist storage="lake"
source: twin_a is duckdb.sql("select k, sum(v) as total from ev_src group by 1")
`;
const COL_B = `#@ persist name="fb"
source: col_b is col_a -> { group_by: k; aggregate: grand is total.sum() }
`;

const WITH_TWIN = `##! experimental.persistence\n${COL_A}${TWIN_A}${COL_B}`;
/** The twin declared first, so it is first to claim the compiler's key. */
const TWIN_FIRST = `##! experimental.persistence\n${TWIN_A}${COL_A}${COL_B}`;
const WITHOUT_TWIN = `##! experimental.persistence\n${COL_A}${COL_B}`;

async function scenario(model: string) {
   const pkg = await packages.create(model);
   const duckdb = (await pkg.getMalloyConnection(
      "duckdb",
   )) as unknown as MalloyConnection;
   await duckdb.runSQL(SEED);
   const compiled = await compilePackageBuildPlan(pkg);
   const sources = sourcesIn(compiled);
   const idOf = (name: string) =>
      Object.entries(sources).find(([, s]) => s.name === name)![0];
   const addresses = planAddresses(compiled);
   const environment = {
      getApiConnection: () => ({ name: "duckdb", type: "duckdb" }),
      getStorageDestination: () => {
         throw new Error("no storage destination in this spec");
      },
      getEnvironmentName: () => "env",
      getEnvironmentPath: () => pkg.getPackagePath(),
   } as unknown as BuildEnvironment;
   // The storage tier, stood in for by the scenario's own database: the
   // twin's recompute SQL is written to a table of its own there, and the
   // table is entered in the run's manifest, as the real tier does.
   const storage: Parameters<typeof buildSource>[1] = async (request) => {
      const table = request.instruction.physicalTableName;
      await duckdb.runSQL(
         `CREATE OR REPLACE TABLE ${table} AS ${request.storedBuildSQL}`,
      );
      request.manifest.update(request.contentSourceEntityId ?? "", {
         tableName: table,
      });
      return {
         sourceEntityId: request.contentSourceEntityId ?? "",
         sourceName: request.persistSource.name,
         physicalTableName: table,
         connectionName: "duckdb",
         storageDestinationName: "lake",
      } as ManifestEntry;
   };
   // The statements each database build ran, by source.
   const ran: Record<string, string[]> = {};
   const build = new InstructedBuild({
      buildOneSource: (options) => {
         const statements = (ran[options.persistSource.name] ??= []);
         const connection = new Proxy(options.connection, {
            get(target, key, receiver) {
               const value = Reflect.get(target, key, receiver);
               if (key !== "runSQL") return value;
               return (sql: string, ...rest: unknown[]) => {
                  statements.push(sql);
                  return (value as (...a: unknown[]) => unknown).call(
                     target,
                     sql,
                     ...rest,
                  );
               };
            },
         });
         return buildSource({ ...options, connection }, storage);
      },
      tableNotOwned: async () => undefined,
      reclaimStorageTablesFromFailedRun: async () => {},
   });
   const { instructions } = deriveSelfInstructions(
      compiled,
      undefined,
      {},
      undefined,
      { environmentName: "env", packageName: "pkg" },
   );
   const run = (opts: { strict: boolean; hostInstructed: boolean }) =>
      build.run({
         compiled,
         environment,
         instructions,
         seedEntries: {},
         signal: new AbortController().signal,
         strict: opts.strict,
         priorRefusals: [],
         hostInstructed: opts.hostInstructed,
         warehouseReach: "recompute",
      });
   return {
      duckdb,
      run,
      address: (name: string) => addresses[idOf(name)] as string,
      compilerKey: (name: string) => compilerBuildId(sources[idOf(name)]),
      /** Whether `source`'s build read the table `table`. */
      read: (source: string, table: string) =>
         (ran[source] ?? []).some((sql) =>
            new RegExp(`\\b(FROM|JOIN)\\s+"?${table}"?(\\s|$)`, "i").test(sql),
         ),
   };
}

const MODES = [
   { label: "self-instructed", strict: true, hostInstructed: false },
   {
      label: "self-instructed, not strict",
      strict: false,
      hostInstructed: false,
   },
   { label: "host-instructed", strict: true, hostInstructed: true },
];

describe("a database build over an upstream whose address folds in its name=", () => {
   for (const [model, order] of [
      [WITH_TWIN, "declared after it"],
      [TWIN_FIRST, "declared first"],
   ]) {
      for (const mode of MODES) {
         it(
            `builds col_b over col_a's table beside a storage twin ${order} (${mode.label})`,
            async () => {
               const s = await scenario(model);
               // The fold this spec is about: col_a's address is not the
               // compiler's key for it.
               expect(s.address("col_a")).not.toBe(s.compilerKey("col_a"));
               const result = await s.run(mode);
               expect(result.failures).toEqual({});
               const colB = result.entries[s.address("col_b")];
               expect(colB).toMatchObject({ sourceName: "col_b" });
               // Read from col_a's table, not recomputed from raw.
               expect(colB.upstreamReuse).toBe("reused");
               expect(s.read("col_b", "fa")).toBe(true);
               expect(s.read("col_b", "ev_src")).toBe(false);
               expect(
                  (
                     await s.duckdb.runSQL(
                        "SELECT k, grand::INTEGER AS grand FROM fb ORDER BY k",
                     )
                  ).rows,
               ).toEqual([
                  { k: "a", grand: 3 },
                  { k: "b", grand: 3 },
               ]);
            },
            { timeout: 60000 },
         );
      }
   }

   for (const mode of MODES) {
      it(
         `builds col_b over col_a's table with no twin (${mode.label})`,
         async () => {
            const s = await scenario(WITHOUT_TWIN);
            expect(s.address("col_a")).toBe(s.compilerKey("col_a"));
            const result = await s.run(mode);
            expect(result.failures).toEqual({});
            const colB = result.entries[s.address("col_b")];
            expect(colB.upstreamReuse).toBe("reused");
            expect(s.read("col_b", "fa")).toBe(true);
            expect(s.read("col_b", "ev_src")).toBe(false);
         },
         { timeout: 60000 },
      );
   }
});
