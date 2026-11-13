// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The inline uniqueness check (P8) skipped when its answer cannot have
 * changed: through real builds of a `#(materialize)` source that inlines a
 * join_one, run after run as auto-run drives them (the previous run's entries
 * carried, its recorded passes relied on). The check is skipped only when the
 * joined source is a persisted table the parent's build reads and that table
 * was not written since a check over it passed; a joined warehouse table is
 * checked on every build, since its rows change under a stable address.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import type { Materialization } from "../../../storage/DatabaseInterface";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { createMocks } from "../../../test_helpers/materialization_service_harness";
import { makeMaterialization } from "../../../test_helpers/materialization_test_fixtures";
import { installSettings } from "../../../test_helpers/settings_env";
import { computeSourceEntityId } from "../plan/build_plan";
import { compilePackageBuildPlan } from "../plan/build_plan_compile";
import type { BuildInstruction, ManifestEntry } from "../types";
import {
   INLINE_UNIQUENESS_KEY,
   UniquenessLedger,
   uniquenessPassesIn,
} from "./inline_uniqueness";
import { InstructedBuild } from "./instructed_build";
import type { BuildEnvironment } from "./materialization_run_support";
import { SourceBuilder } from "./source_build";

const packages = tempBuildPlanPackages();
let restore: () => void;
beforeEach(() => {
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true" },
   }).restore;
});
afterEach(async () => {
   restore();
   await packages.cleanup();
});

const SEED = [
   "CREATE OR REPLACE TABLE regions_src AS SELECT * FROM (VALUES ('east', 'East'), ('west', 'West')) AS t(code, label)",
   "CREATE OR REPLACE TABLE orders_src AS SELECT * FROM (VALUES (1, 'east', 10.0), (2, 'west', 20.0), (3, 'east', 5.0)) AS t(id, region, amount)",
];

/** `regions` persisted, so `orders`' build reads its table. */
const PERSISTED_JOIN = `##! experimental.persistence
source: regions_raw is duckdb.table('regions_src')
#@ persist name="regions_t"
source: regions is regions_raw -> { select: code, label } extend {
  primary_key: code
}
#(materialize) name="orders_t" inline=["regions"]
source: orders is duckdb.table('orders_src') extend {
  join_one: regions with region
}
`;

/** `regions` is the warehouse table itself, read live by every build. */
const RAW_JOIN = `source: regions is duckdb.table('regions_src') extend {
  primary_key: code
}
#(materialize) name="orders_t" inline=["regions"]
source: orders is duckdb.table('orders_src') extend {
  join_one: regions with region
}
`;

/**
 * `regions` is `#(materialize)` too, but the parent's build reads its
 * definition, not its table (only a `#@ persist` source is substituted), so
 * its table says nothing about what `orders` stores.
 */
const MATERIALIZED_JOIN = `source: regions is duckdb.table('regions_src') extend {
  primary_key: code
}
#(materialize) name="regions_view"
source: regions_m is regions extend {}
#(materialize) name="orders_t" inline=["regions_m"]
source: orders is duckdb.table('orders_src') extend {
  join_one: regions_m with region
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
   const addresses = new Map(
      Object.entries(compiled.sources).map(([id, s]) => [
         s.name,
         computeSourceEntityId(
            s,
            compiled.connectionDigests,
            compiled.targets?.[id],
         ),
      ]),
   );
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
   const instruction = (name: string): BuildInstruction =>
      ({
         sourceEntityId: addresses.get(name),
         materializedTableId: `mt-${name}`,
         physicalTableName: `${name}_t`,
         realization: "COPY",
      }) as BuildInstruction;

   let previous: {
      entries: Record<string, ManifestEntry>;
      metadata?: Metadata;
   } = { entries: {} };
   let runs = 0;
   /**
    * One auto-run: `rebuild` names the sources built, every other entry of
    * the run before is carried, and the run before's passes are relied on
    * unless `trustPrevious` is false.
    */
   async function run(rebuild: string[], trustPrevious = true) {
      runs += 1;
      const carried = Object.fromEntries(
         Object.entries(previous.entries).filter(
            ([address]) =>
               !rebuild.some((name) => addresses.get(name) === address),
         ),
      );
      const ledger = new UniquenessLedger(
         trustPrevious
            ? uniquenessPassesIn({
                 [INLINE_UNIQUENESS_KEY]: previous.metadata,
              })
            : undefined,
         `run-${runs}`,
      );
      const result = await build.run(
         compiled,
         environment,
         rebuild.map(instruction),
         carried,
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
      if (Object.keys(result.failures).length === 0) {
         previous = {
            entries: result.entries,
            metadata: result.uniquenessChecks,
         };
      }
      return result;
   }
   const ordersRows = async () =>
      (
         await duckdb.runSQL(
            "SELECT id, regions__label AS label FROM orders_t ORDER BY id",
         )
      ).rows;
   return { duckdb, run, addresses, ordersRows };
}

const outcomes = (m: Metadata | undefined) =>
   (m?.checks ?? []).map((c) => `${c.sourceName}.${c.join}:${c.outcome}`);

describe("the inline uniqueness check, skipped only when its table is unchanged", () => {
   it(
      "skips over an unchanged persisted joined table, and catches a duplicate on its next generation",
      async () => {
         const s = await scenario(PERSISTED_JOIN);

         // First build: no pass to rely on, so the check runs (over the
         // stored regions table) and its pass is recorded.
         const first = await s.run(["regions", "orders"]);
         expect(first.failures).toEqual({});
         expect(outcomes(first.uniquenessChecks)).toEqual([
            "orders.regions:ran",
         ]);
         const [pass] = Object.values(first.uniquenessChecks?.passes ?? {});
         expect(pass).toMatchObject({
            joinedAddress: s.addresses.get("regions"),
            checkedInRun: "run-1",
         });

         // `orders` rebuilt over the carried regions table: skipped, and the
         // pass carried with the run that actually checked it.
         const second = await s.run(["orders"]);
         expect(second.failures).toEqual({});
         expect(outcomes(second.uniquenessChecks)).toEqual([
            "orders.regions:skipped",
         ]);
         expect(Object.values(second.uniquenessChecks?.passes ?? {})).toEqual([
            pass,
         ]);

         // A duplicate in the warehouse does not reach the stored regions
         // table until it is rebuilt, so the skip still holds and `orders`
         // still stores each order once.
         await s.duckdb.runSQL(
            "INSERT INTO regions_src VALUES ('east', 'Also east')",
         );
         const third = await s.run(["orders"]);
         expect(outcomes(third.uniquenessChecks)).toEqual([
            "orders.regions:skipped",
         ]);
         expect(await s.ordersRows()).toEqual([
            { id: 1, label: "East" },
            { id: 2, label: "West" },
            { id: 3, label: "East" },
         ]);

         // The next generation of regions holds the duplicate: the check
         // runs and fails `orders` before it writes anything.
         const fourth = await s.run(["regions", "orders"]);
         const failed = Object.values(fourth.failures);
         expect(failed).toHaveLength(1);
         expect(failed[0].sourceName).toBe("orders");
         expect(failed[0].reason).toMatch(/inlines the join_one 'regions'/);
         expect(outcomes(fourth.uniquenessChecks)).toEqual([]);
         expect(fourth.uniquenessChecks?.passes ?? {}).toEqual({});
      },
      { timeout: 60000 },
   );

   it(
      "runs after a failed check, and when the run before cannot be relied on",
      async () => {
         const s = await scenario(PERSISTED_JOIN);
         await s.duckdb.runSQL(
            "INSERT INTO regions_src VALUES ('east', 'Also east')",
         );
         // Fails: no pass is recorded.
         const failed = await s.run(["regions", "orders"]);
         expect(Object.keys(failed.failures)).toHaveLength(1);
         expect(failed.uniquenessChecks?.passes ?? {}).toEqual({});

         // Fixed and rebuilt: runs, passes.
         await s.duckdb.runSQL(
            "DELETE FROM regions_src WHERE label = 'Also east'",
         );
         const fixed = await s.run(["regions", "orders"]);
         expect(fixed.failures).toEqual({});
         expect(outcomes(fixed.uniquenessChecks)).toEqual([
            "orders.regions:ran",
         ]);

         // The same table, but the run before is not one to rely on.
         const untrusted = await s.run(["orders"], false);
         expect(outcomes(untrusted.uniquenessChecks)).toEqual([
            "orders.regions:ran",
         ]);
      },
      { timeout: 60000 },
   );

   it(
      "runs on every build over a joined warehouse table, whose rows change under one address",
      async () => {
         const s = await scenario(RAW_JOIN);
         const first = await s.run(["orders"]);
         expect(first.failures).toEqual({});
         expect(outcomes(first.uniquenessChecks)).toEqual([
            "orders.regions:ran",
         ]);
         // Nothing to rely on: a pass over a live read is never recorded.
         expect(first.uniquenessChecks?.passes ?? {}).toEqual({});

         await s.duckdb.runSQL(
            "INSERT INTO regions_src VALUES ('east', 'Also east')",
         );
         // The run's only source fails, so the run does.
         await expect(s.run(["orders"])).rejects.toThrow(
            /inlines the join_one 'regions'/,
         );
      },
      { timeout: 60000 },
   );

   it(
      "runs on every build over a joined source whose table the parent's build does not read",
      async () => {
         const s = await scenario(MATERIALIZED_JOIN);
         const first = await s.run(["regions_m", "orders"]);
         expect(first.failures).toEqual({});
         expect(outcomes(first.uniquenessChecks)).toEqual([
            "orders.regions_m:ran",
         ]);
         expect(first.uniquenessChecks?.passes ?? {}).toEqual({});
         // regions_m's table is carried, but orders' build reads the
         // warehouse: the check runs, and no pass is recorded.
         const second = await s.run(["orders"]);
         expect(second.failures).toEqual({});
         expect(outcomes(second.uniquenessChecks)).toEqual([
            "orders.regions_m:ran",
         ]);
         expect(second.uniquenessChecks?.passes ?? {}).toEqual({});
         // The warehouse now holds a duplicate: caught.
         await s.duckdb.runSQL(
            "INSERT INTO regions_src VALUES ('east', 'Also east')",
         );
         await expect(s.run(["orders"])).rejects.toThrow(
            /inlines the join_one 'regions_m'/,
         );
      },
      { timeout: 60000 },
   );

   it("refuses a pass for another table or address, or one written this run", () => {
      const pass = { joinedAddress: "a1", joinedTable: "t1" };
      const stored = { sql: "select 1", address: "a1", table: "t1" };
      const ledger = new UniquenessLedger({ d: pass });
      expect(ledger.heldPass("d", stored)).toEqual(pass);
      expect(ledger.heldPass("other", stored)).toBeUndefined();
      expect(
         ledger.heldPass("d", { ...stored, address: "a2" }),
      ).toBeUndefined();
      expect(ledger.heldPass("d", { ...stored, table: "t2" })).toBeUndefined();
      ledger.markWritten("a1", "elsewhere");
      expect(ledger.heldPass("d", stored)).toBeUndefined();
      const byTable = new UniquenessLedger({ d: pass });
      byTable.markWritten("a9", "t1");
      expect(byTable.heldPass("d", stored)).toBeUndefined();
      // A pass this run recorded is lost when its table is written after.
      const after = new UniquenessLedger(undefined);
      after.record("d", pass);
      after.markWritten("a1", "t1");
      expect(after.toMetadata()?.passes ?? {}).toEqual({});
   });

   it("reads only the passes this Publisher writes", () => {
      expect(uniquenessPassesIn(null)).toBeUndefined();
      expect(
         uniquenessPassesIn({ [INLINE_UNIQUENESS_KEY]: { passes: [] } }),
      ).toBeUndefined();
      expect(
         uniquenessPassesIn({
            [INLINE_UNIQUENESS_KEY]: {
               passes: {
                  good: { joinedAddress: "a", joinedTable: "t" },
                  bad: { joinedAddress: 1 },
               },
            },
         }),
      ).toEqual({ good: { joinedAddress: "a", joinedTable: "t" } });
   });
});

describe("which run's passes an auto-run relies on", () => {
   const PASSES = {
      [INLINE_UNIQUENESS_KEY]: {
         passes: { d: { joinedAddress: "a", joinedTable: "t" } },
         checks: [],
      },
   };
   const at = (minute: number) => new Date(Date.UTC(2026, 10, 12, 0, minute));
   const previous = (over: Partial<Materialization> = {}) =>
      makeMaterialization({
         id: "prev",
         status: "MANIFEST_FILE_READY",
         metadata: { mode: "auto", ...PASSES },
         createdAt: at(10),
         startedAt: at(10),
         completedAt: at(12),
         updatedAt: at(12),
         ...over,
      });
   const older = (over: Partial<Materialization> = {}) =>
      makeMaterialization({
         id: "older",
         status: "MANIFEST_FILE_READY",
         metadata: { mode: "auto" },
         createdAt: at(1),
         startedAt: at(1),
         completedAt: at(5),
         updatedAt: at(5),
         ...over,
      });
   const current = makeMaterialization({ id: "now", createdAt: at(20) });

   async function relied(runs: Materialization[]) {
      const ctx = createMocks();
      ctx.repository.listMaterializations.resolves(runs);
      return (
         ctx.service as unknown as {
            priorUniquenessPasses(
               env: string,
               pkg: string,
               exclude: string,
            ): Promise<unknown>;
         }
      ).priorUniquenessPasses("env-1", "pkg", "now");
   }

   it("relies on the newest run when every other finished before it began", async () => {
      expect(await relied([current, previous(), older()])).toEqual(
         PASSES[INLINE_UNIQUENESS_KEY].passes,
      );
   });

   it("relies on nothing when the newest run failed, was orchestrated, or is still going", async () => {
      expect(
         await relied([current, previous({ status: "FAILED" }), older()]),
      ).toBeUndefined();
      expect(
         await relied([
            current,
            previous({ metadata: { mode: "orchestrated", ...PASSES } }),
         ]),
      ).toBeUndefined();
      expect(
         await relied([current, previous({ status: "PENDING" })]),
      ).toBeUndefined();
   });

   it("relies on nothing when another run overlapped it or is still active", async () => {
      // An older run that ended after the newest began may have written a
      // table after that run's check.
      expect(
         await relied([
            current,
            previous(),
            older({ completedAt: at(11), updatedAt: at(11) }),
         ]),
      ).toBeUndefined();
      expect(
         await relied([
            current,
            previous(),
            older({ status: "MANIFEST_ROWS_READY", completedAt: null }),
         ]),
      ).toBeUndefined();
   });

   it("relies on nothing when the listing fails", async () => {
      const ctx = createMocks();
      ctx.repository.listMaterializations.rejects(new Error("down"));
      expect(
         await (
            ctx.service as unknown as {
               priorUniquenessPasses(
                  env: string,
                  pkg: string,
                  exclude: string,
               ): Promise<unknown>;
            }
         ).priorUniquenessPasses("env-1", "pkg", "now"),
      ).toBeUndefined();
   });
});
