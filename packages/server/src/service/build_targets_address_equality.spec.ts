// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The address-equality guard: the compiler's artifact id must equal ours.
//
// `Runtime.getBuildTargets()` supersedes the deprecated `Model.getBuildPlan()`,
// and reports TABLES where the old API reported SOURCES. The port is mechanical
// only if the two agree about a table's ADDRESS, because that address is not an
// internal detail — it is the manifest key, the serve binding's `virtualHandle`,
// and the key an incremental refresh's covered_through ledger is stored under. A
// port that moved addresses would orphan every built table and every boundary at
// once, silently: the tables still exist, nothing looks up their new keys, and
// each source re-seeds forever.
//
// So, for every target `getBuildTargets` returns and every unpartitioned source
// that mapped onto it:
//
//     target.buildId === computeSourceEntityId(source, connectionDigests)
//
// `BuildTarget.buildId` is documented as "a hash of the connection digest and
// `sql`" over a fully-inlined, substitution-free `sql`; `computeSourceEntityId`
// is `source.makeBuildId(digest, source.getSQL())` under the same no-options
// invariant. This asserts they are in fact the same number. A `partition=`
// layout is folded into the address alone, so for a partitioned source it is
// `compilerBuildId` that equals the target's, and the builder addresses each
// source itself.
//
// Kept permanently rather than run once and deleted. It is cheap, and a later
// compiler bump is exactly the event that could move an address without anything
// else failing — which is the failure this file exists to make loud.
//
// It also pins the multi-source case, which is the whole reason the new API
// exists: `#@ persist` is an annotation, so an `extend` inherits it while
// leaving the SQL alone, and several sources therefore land on ONE table. The
// old API left each builder to rediscover that by hashing; the new one hands it
// over in `target.sources`. That mapping is what a rollup's serve binding should
// be keyed on, so its correctness is a prerequisite for keying anything on it.
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { FixedConnectionMap } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { makeDigest } from "@malloydata/malloy";
import {
   addressSQL,
   buildTargetOf,
   compilerBuildId,
   computeSourceEntityId,
   plannedSources,
} from "./materialization/plan/build_plan";
import {
   duckdbTestConnections,
   loadTestRuntime,
} from "../test_helpers/incremental_test_harness";

let connections: FixedConnectionMap;
let duckdb: DuckDBConnection;
let connectionDigests: Record<string, string>;

beforeAll(async () => {
   ({ duckdb, connections } = duckdbTestConnections());
   connectionDigests = { duckdb: await duckdb.getDigest() };
});

const BASE_ROWS = `SELECT * FROM (VALUES
    (10, 'A'),
    (20, 'B'),
    (30, 'A')
  ) AS t(amount, category)`;

/**
 * Three shapes in one model, chosen so the walk is not trivial:
 *
 *  - `rollup`, an ordinary persisted root;
 *  - `rollup_extended`, which inherits `#@ persist` through `extend` while
 *    leaving the SQL untouched — so it must land on `rollup`'s target, not on a
 *    second one;
 *  - `stacked`, persisted and dependent on `rollup`, so `dependsOn` is exercised
 *    rather than assumed.
 */
const MODEL = `##! experimental.persistence
source: base is duckdb.sql("""${BASE_ROWS}""")

#@ persist
source: rollup is base -> {
  group_by: category
  aggregate: total is amount.sum()
}

source: rollup_extended is rollup extend {
  dimension: label is concat('c-', category)
}

#@ persist
source: stacked is rollup -> {
  group_by: category
  aggregate: grand is total.sum()
}
`;

/** Model with no `##! experimental.persistence`, for the guard question. */
const MODEL_NO_FLAG = `source: base is duckdb.sql("""${BASE_ROWS}""")
`;

async function buildTargets(model = MODEL) {
   const { runtime, materializer } = loadTestRuntime(connections, model);
   const targets = await runtime.getBuildTargets(await materializer.getModel());
   // As a plan does: record each source's target, which addressing reads.
   plannedSources(targets);
   return targets;
}

describe("address equality: getBuildTargets vs computeSourceEntityId", () => {
   it("THE GUARD: every source's entity id equals its target's buildId", async () => {
      const { connections: builds } = await buildTargets();
      const checked: string[] = [];
      for (const build of builds) {
         for (const target of build.targets) {
            for (const source of target.sources) {
               expect(computeSourceEntityId(source, connectionDigests)).toBe(
                  target.buildId,
               );
               checked.push(source.name);
            }
         }
      }
      // The assertion above is vacuous if the walk returned nothing, which is
      // exactly what a compiler change that stopped reporting targets would look
      // like. Pin that something was actually examined.
      expect(checked.sort()).toEqual(["rollup", "rollup_extended", "stacked"]);
   });

   it("compilerBuildId is the target's buildId, equal to Publisher's digest over its SQL", async () => {
      // compilerBuildId reads the target's buildId rather than re-hashing; the
      // two are the same number only because Publisher and the runtime digest
      // the same connections. Pinned so a divergence is loud.
      const { connections: builds } = await buildTargets();
      for (const build of builds) {
         for (const target of build.targets) {
            for (const source of target.sources) {
               expect(compilerBuildId(source, connectionDigests)).toBe(
                  target.buildId,
               );
               expect(target.buildId).toBe(
                  makeDigest(
                     connectionDigests[source.connectionName],
                     addressSQL(source),
                  ),
               );
            }
         }
      }
   });

   it("names why a source has no target: unkeyable, or never planned", async () => {
      const { connections: builds } = await buildTargets();
      const [source] = builds.flatMap((b) => b.targets)[0].sources;
      // A source object no plan ever paired: a bug in the caller, said so.
      const stranger = Object.create(source) as typeof source;
      expect(() => buildTargetOf(stranger)).toThrow(/never planned/);
      // An explicit target addresses it anyway.
      const [target] = builds.flatMap((b) => b.targets);
      expect(buildTargetOf(stranger, target)).toBe(target);
   });

   it("several sources map onto ONE target when extend leaves the SQL alone", async () => {
      const { connections: builds } = await buildTargets();
      const targets = builds.flatMap((b) => b.targets);
      const shared = targets.find((t) => t.sources.length > 1);
      expect(shared).toBeDefined();
      expect(shared!.sources.map((s) => s.name).sort()).toEqual([
         "rollup",
         "rollup_extended",
      ]);
      // The merge is the point: two sources, one table, one manifest entry.
      expect(targets).toHaveLength(2);
   });

   it("targets come back in dependency order, dependents last", async () => {
      const { connections: builds } = await buildTargets();
      const targets = builds.flatMap((b) => b.targets);
      const names = targets.map((t) => t.sources.map((s) => s.name));
      // `stacked` reads `rollup`, so it cannot come first.
      const stackedAt = names.findIndex((n) => n.includes("stacked"));
      const rollupAt = names.findIndex((n) => n.includes("rollup"));
      expect(rollupAt).toBeLessThan(stackedAt);
      expect(targets[stackedAt].dependsOn.map((d) => d.buildId)).toContain(
         targets[rollupAt].buildId,
      );
   });

   it("the addresses match getBuildPlan's, so the port moves nothing", async () => {
      // The sequencing question this file was written to answer: if the two APIs
      // disagree, work keyed on these addresses has to wait for the port.
      const { runtime, materializer } = loadTestRuntime(connections, MODEL);
      const model = await materializer.getModel();
      // The deprecated API's address, spelled out: getBuildPlan's sources have
      // no build target to read.
      const fromPlan = new Set(
         Object.values(model.getBuildPlan().sources).map((s) =>
            s.makeBuildId(connectionDigests[s.connectionName], s.getSQL()),
         ),
      );
      const fromTargets = new Set(
         (await runtime.getBuildTargets(model)).connections
            .flatMap((b) => b.targets)
            .map((t) => t.buildId),
      );
      expect([...fromTargets].sort()).toEqual([...fromPlan].sort());
   });

   it("a partition= layout moves the address but not the buildId", async () => {
      // `getSQL()` carries no annotation bytes, so the compiler keys a
      // partitioned source's table exactly as the unpartitioned one. The
      // address folds the layout in, so a layout change rebuilds the table.
      // The two keys therefore differ here, and a builder must address each
      // source itself rather than adopt `target.buildId`.
      const { connections: builds } =
         await buildTargets(`##! experimental.persistence
source: base is duckdb.sql("""${BASE_ROWS}""")

#@ persist storage=lake partition="category"
source: by_category is base -> {
  group_by: category
  aggregate: total is amount.sum()
}
`);
      const [target] = builds.flatMap((b) => b.targets);
      const [source] = target.sources;
      expect(computeSourceEntityId(source, connectionDigests)).not.toBe(
         target.buildId,
      );
      expect(compilerBuildId(source, connectionDigests)).toBe(target.buildId);
   });

   it("throws without ##! experimental.persistence, exactly as getBuildPlan does", async () => {
      // `getBuildPlan()` throws on a model lacking the flag, and the publisher
      // carries a guard mirroring that throw. Whether `getBuildTargets` behaves
      // the same decides whether that guard can retire with the port rather than
      // being reimplemented beside it — so it is pinned rather than assumed.
      await expect(buildTargets(MODEL_NO_FLAG)).rejects.toThrow(
         /##! experimental\.persistence/,
      );
   });
});
