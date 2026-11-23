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
/** A storage source over the twin, and a database source over it. */
const TWIN_B = `#@ persist name="tb" storage="lake"
source: twin_b is twin_a -> { group_by: k; aggregate: grand is total.sum() }
`;
const DB_C = `#@ persist name="fc"
source: db_c is twin_a -> { group_by: k; aggregate: grand is total.sum() }
`;
const OVER_TWIN = `##! experimental.persistence\n${COL_A}${TWIN_A}${TWIN_B}`;
const DB_OVER_TWIN = `##! experimental.persistence\n${COL_A}${TWIN_A}${DB_C}`;
/** A second `name=` twin of col_a, and a source reading both through a join. */
const TWIN2 = `#@ persist name="fa2"
source: twin2 is duckdb.sql("select k, sum(v) as total from ev_src group by 1")
`;
const joinBoth = (base: string, joined: string) =>
   `##! experimental.persistence\n${COL_A}${TWIN2}` +
   `source: j is ${base} extend { join_one: t is ${joined} on k = t.k }
#@ persist name="fboth"
source: both is j -> { group_by: k; aggregate: grand is total.sum(), g2 is t.total.sum() }
`;
/** A storage source two levels over the twin. */
const C3 = `#@ persist name="tc" storage="lake"
source: c3 is twin_b -> { group_by: k; aggregate: g3 is grand.sum() }
`;
const TWO_OVER_TWIN = `##! experimental.persistence\n${COL_A}${TWIN_A}${TWIN_B}${C3}`;
const WITHOUT_TWIN = `##! experimental.persistence\n${COL_A}${COL_B}`;

async function scenario(model: string) {
   // The statements each build ran, by source: a database build's, and the
   // recompute a storage build copies.
   const ran: Record<string, string[]> = {};
   /** Whether each storage build would stack on a stored parent. */
   const stacks: Record<string, boolean> = {};
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
      (ran[request.persistSource.name] ??= []).push(request.storedBuildSQL);
      stacks[request.persistSource.name] = request.dependsOnStorageUpstream;
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
   const run = (
      opts: { strict: boolean; hostInstructed: boolean },
      /** Build only these sources, over `seedEntries` (a later run). */
      later?: { only: string[]; seedEntries: Record<string, ManifestEntry> },
   ) => {
      for (const name of later?.only ?? Object.keys(ran)) delete ran[name];
      return build.run({
         compiled,
         environment,
         instructions: later
            ? instructions.filter((i) =>
                 later.only.some(
                    (name) => addresses[idOf(name)] === i.sourceEntityId,
                 ),
              )
            : instructions,
         seedEntries: later?.seedEntries ?? {},
         signal: new AbortController().signal,
         strict: opts.strict,
         priorRefusals: [],
         hostInstructed: opts.hostInstructed,
      });
   };
   return {
      duckdb,
      run,
      address: (name: string) => addresses[idOf(name)] as string,
      compilerKey: (name: string) => compilerBuildId(sources[idOf(name)]),
      stacks,
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

   // The key col_a and twin_a share belongs to twin_a for a source that reads
   // twin_a: col_a's table, in another tier and possibly of another age, is
   // never read in its place.
   for (const mode of MODES) {
      it(
         `builds a storage source over the twin from the twin's rows, not col_a's older table (${mode.label})`,
         async () => {
            const s = await scenario(OVER_TWIN);
            const first = await s.run(mode);
            expect(first.failures).toEqual({});
            // The warehouse changes; twin_a alone is rebuilt, col_a is stale.
            await s.duckdb.runSQL("INSERT INTO ev_src VALUES ('a', 10)");
            const without = (
               entries: Record<string, ManifestEntry>,
               name: string,
            ) =>
               Object.fromEntries(
                  Object.entries(entries).filter(
                     ([address]) => address !== s.address(name),
                  ),
               );
            const second = await s.run(mode, {
               only: ["twin_a"],
               seedEntries: without(first.entries, "twin_a"),
            });
            expect(second.failures).toEqual({});
            // twin_b, with both col_a's and twin_a's tables supplied.
            const third = await s.run(mode, {
               only: ["twin_b"],
               seedEntries: without(second.entries, "twin_b"),
            });
            expect(third.failures).toEqual({});
            // It inlined twin_a, so the real tier stacks on twin_a's table.
            expect(s.read("twin_b", "fa")).toBe(false);
            expect(s.stacks.twin_b).toBe(true);
            expect(
               (
                  await s.duckdb.runSQL(
                     "SELECT k, grand::INTEGER AS grand FROM tb ORDER BY k",
                  )
               ).rows,
            ).toEqual([
               { k: "a", grand: 13 },
               { k: "b", grand: 3 },
            ]);
         },
         { timeout: 60000 },
      );
   }

   it(
      "never reads col_a's table for a database source over the twin, which is refused at plan",
      async () => {
         const s = await scenario(DB_OVER_TWIN);
         const result = await s.run({ strict: false, hostInstructed: false });
         expect(result.failures).toEqual({});
         // A warehouse build cannot read the twin's `storage=` table, and
         // never recomputes it from raw: refused at plan, so never built.
         expect(s.read("db_c", "fa")).toBe(false);
         expect(s.read("db_c", "ev_src")).toBe(false);
         expect(
            Object.values(result.entries).map((e) => e.sourceName),
         ).not.toContain("db_c");
      },
      { timeout: 60000 },
   );

   // Both twins are read (one through a join) and only one is built: the
   // key they share holds the built one's table, whichever is reached first.
   for (const [base, joined] of [
      ["col_a", "twin2"],
      ["twin2", "col_a"],
   ]) {
      for (const [built, table] of [
         ["col_a", "fa"],
         ["twin2", "fa2"],
      ]) {
         for (const strict of [true, false]) {
            it(
               `builds over the one twin built when ${base} joins ${joined} and only ${built} is built (strict: ${strict})`,
               async () => {
                  const s = await scenario(joinBoth(base, joined));
                  expect(s.compilerKey("col_a")).toBe(s.compilerKey("twin2"));
                  expect(s.address("col_a")).not.toBe(s.address("twin2"));
                  const result = await s.run(
                     { strict, hostInstructed: false },
                     { only: [built, "both"], seedEntries: {} },
                  );
                  expect(result.failures).toEqual({});
                  expect(result.entries[s.address("both")].upstreamReuse).toBe(
                     "reused",
                  );
                  expect(s.read("both", table)).toBe(true);
                  expect(s.read("both", "ev_src")).toBe(false);
               },
               { timeout: 60000 },
            );
         }
      }
   }

   // c3 over twin_b over twin_a: with twin_b's table absent the render
   // inlines twin_b, and the twin key it then looks up is twin_a's, never
   // col_a's older table.
   for (const strict of [true, false]) {
      for (const twinBSupplied of [true, false]) {
         if (strict && !twinBSupplied) continue;
         it(
            `builds a storage source two levels over the twin from the twin's rows (strict: ${strict}, twin_b supplied: ${twinBSupplied})`,
            async () => {
               const s = await scenario(TWO_OVER_TWIN);
               const mode = { strict, hostInstructed: true };
               const first = await s.run(mode);
               expect(first.failures).toEqual({});
               await s.duckdb.runSQL("INSERT INTO ev_src VALUES ('a', 10)");
               const without = (
                  entries: Record<string, ManifestEntry>,
                  ...names: string[]
               ) =>
                  Object.fromEntries(
                     Object.entries(entries).filter(
                        ([address]) =>
                           !names.some((n) => address === s.address(n)),
                     ),
                  );
               const second = await s.run(mode, {
                  only: ["twin_a", "twin_b"],
                  seedEntries: without(first.entries, "twin_a", "twin_b"),
               });
               expect(second.failures).toEqual({});
               const third = await s.run(mode, {
                  only: ["c3"],
                  seedEntries: twinBSupplied
                     ? without(second.entries, "c3")
                     : without(second.entries, "c3", "twin_b"),
               });
               expect(third.failures).toEqual({});
               expect(s.read("c3", "fa")).toBe(false);
               expect(s.stacks.c3).toBe(true);
               expect(
                  (
                     await s.duckdb.runSQL(
                        "SELECT k, g3::INTEGER AS g3 FROM tc ORDER BY k",
                     )
                  ).rows,
               ).toEqual([
                  { k: "a", g3: 13 },
                  { k: "b", g3: 3 },
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
