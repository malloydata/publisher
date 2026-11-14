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
import type { BuildInstruction, ManifestEntry } from "../domain/types";
import {
   INLINE_UNIQUENESS_KEY,
   UniquenessLedger,
   uniquenessPassesIn,
   withoutUniquenessPasses,
} from "./inline_uniqueness";
import { InstructedBuild } from "./instructed_build";
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
#(materialize) storage="lake" name="orders_t" inline=["regions"]
source: orders is duckdb.table('orders_src') extend {
  join_one: regions with region
}
`;

/** `regions` is the warehouse table itself, read live by every build. */
const RAW_JOIN = `source: regions is duckdb.table('regions_src') extend {
  primary_key: code
}
#(materialize) storage="lake" name="orders_t" inline=["regions"]
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
#(materialize) storage="lake" name="regions_view"
source: regions_m is regions extend {}
#(materialize) storage="lake" name="orders_t" inline=["regions_m"]
source: orders is duckdb.table('orders_src') extend {
  join_one: regions_m with region
}
`;

/** A one-hop pair table: its build checks its own primary key first. */
const PAIR = `source: regions is duckdb.table('regions_src') extend {
  primary_key: code
}
#(materialize) storage="lake" name="orders_t" inline=["regions"]
source: orders is duckdb.table('orders_src') extend {
  primary_key: id
  join_many: regions on regions.code = region
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
   // The storage tier, stood in for by the scenario's own database: the
   // build SQL it is handed (the warehouse recompute a `storage=` build
   // copies) is written to the table there, so the stored rows can be read.
   const builder = new SourceBuilder(async (request) => {
      const table = request.instruction.physicalTableName;
      await duckdb.runSQL(
         `CREATE OR REPLACE TABLE ${table} AS ${request.publicBuildSQL}`,
      );
      return {
         sourceEntityId: request.contentSourceEntityId ?? "",
         sourceName: request.persistSource.name,
         physicalTableName: table,
         connectionName: "duckdb",
         storageDestinationName: "lake",
      } as ManifestEntry;
   });
   const build = new InstructedBuild({
      buildOneSource: (...a) => builder.build(...a),
      reclaimStorageTablesFromFailedRun: async () => {},
   });
   const sourceIdOf = (name: string) =>
      Object.entries(compiled.sources).find(([, s]) => s.name === name)?.[0] ??
      "";
   const instruction = (name: string): BuildInstruction =>
      ({
         sourceEntityId: addresses.get(name),
         materializedTableId: `mt-${name}`,
         physicalTableName: `${name}_t`,
         realization: "COPY",
         // A `#(materialize)` source is built only into a destination.
         ...(compiled.materializePlans?.[sourceIdOf(name)]
            ? { destination: "lake" }
            : {}),
      }) as BuildInstruction;

   let previous: {
      entries: Record<string, ManifestEntry>;
      metadata?: Metadata;
   } = { entries: {} };
   let runs = 0;
   /**
    * One auto-run: `rebuild` names the sources built, every other entry of
    * the run before is carried, and the run before's passes are relied on
    * unless `trustPrevious` is false. A metadata value in its place is the
    * one relied on instead, as when the run before's record was deleted.
    */
   async function run(
      rebuild: string[],
      trustPrevious: boolean | Metadata = true,
   ) {
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
                 [INLINE_UNIQUENESS_KEY]:
                    trustPrevious === true ? previous.metadata : trustPrevious,
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

   it(
      "checks a one-hop pair table's parent key on every build, never relying on its own table",
      async () => {
         const s = await scenario(PAIR);
         const first = await s.run(["orders"]);
         expect(first.failures).toEqual({});
         expect(outcomes(first.uniquenessChecks)).toEqual(["orders.:ran"]);
         expect(first.uniquenessChecks?.passes ?? {}).toEqual({});
         // Its own table is carried, but that is what the build writes, not
         // what it reads: the check runs again.
         const second = await s.run(["orders"]);
         expect(second.failures).toEqual({});
         expect(outcomes(second.uniquenessChecks)).toEqual(["orders.:ran"]);
         // A parent key held twice: caught before the table is written.
         await s.duckdb.runSQL(
            "INSERT INTO orders_src VALUES (3, 'west', 7.0)",
         );
         await expect(s.run(["orders"])).rejects.toThrow(
            /keys one row flag on 'id' of 'orders' itself/,
         );
      },
      { timeout: 60000 },
   );

   it(
      "checks again once a newer run's record is deleted, rather than rely on the pass it hid",
      async () => {
         const s = await scenario(PERSISTED_JOIN);
         const first = await s.run(["regions", "orders"]);
         expect(outcomes(first.uniquenessChecks)).toEqual([
            "orders.regions:ran",
         ]);
         // A later run rebuilds regions alone, over a duplicate: same
         // address, same table, new rows. It reaches no check, so it records
         // no pass.
         await s.duckdb.runSQL(
            "INSERT INTO regions_src VALUES ('east', 'Also east')",
         );
         const second = await s.run(["regions"]);
         expect(second.uniquenessChecks).toBeUndefined();
         // Its record deleted, the first run is the newest again. Relying on
         // its pass would skip the check and store each east order twice;
         // the delete forgets it, so the check runs and refuses orders.
         const remaining = withoutUniquenessPasses({
            mode: "auto",
            [INLINE_UNIQUENESS_KEY]: first.uniquenessChecks,
         });
         expect(remaining?.[INLINE_UNIQUENESS_KEY]).toEqual({
            passes: {},
            checks: first.uniquenessChecks?.checks,
         });
         // (Its only source fails, so the run does.)
         await expect(
            s.run(["orders"], remaining?.[INLINE_UNIQUENESS_KEY] as Metadata),
         ).rejects.toThrow(/inlines the join_one 'regions'/);
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
      const prior = await (
         ctx.service as unknown as {
            priorUniquenessMetadata(
               env: string,
               pkg: string,
               exclude: string,
            ): Promise<Record<string, unknown> | undefined>;
         }
      ).priorUniquenessMetadata("env-1", "pkg", "now");
      return prior ? uniquenessPassesIn(prior) : undefined;
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

   it("forgets every other run's passes before a run's record is deleted", async () => {
      const ctx = createMocks();
      const kept = previous();
      const plain = older();
      ctx.repository.getMaterializationById.resolves(
         makeMaterialization({ id: "gone", status: "MANIFEST_FILE_READY" }),
      );
      ctx.repository.listMaterializations.resolves([kept, plain]);
      const order: string[] = [];
      ctx.repository.updateMaterialization.callsFake(async (id: string) => {
         order.push(`update:${id}`);
         return kept;
      });
      ctx.repository.deleteMaterialization.callsFake(async (id: string) => {
         order.push(`delete:${id}`);
      });
      await ctx.service.deleteMaterialization("my-env", "pkg", "gone");
      // Only a run that records a pass is rewritten, and before the delete.
      expect(order).toEqual(["update:prev", "delete:gone"]);
      const metadata = ctx.repository.updateMaterialization.firstCall.args[1]
         .metadata as Record<string, unknown>;
      expect(metadata.mode).toBe("auto");
      expect(uniquenessPassesIn(metadata)).toEqual({});
   });

   it("relies on nothing when the listing fails", async () => {
      const ctx = createMocks();
      ctx.repository.listMaterializations.rejects(new Error("down"));
      expect(
         await (
            ctx.service as unknown as {
               priorUniquenessMetadata(
                  env: string,
                  pkg: string,
                  exclude: string,
               ): Promise<unknown>;
            }
         ).priorUniquenessMetadata("env-1", "pkg", "now"),
      ).toBeUndefined();
   });
});
