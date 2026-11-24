// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A standalone run holds each table it may carry to the column contract its
 * build recorded, as the table is now (`holdCarriedToContracts`): one
 * altered since the bind last read it (an out-of-band `ALTER … TYPE
 * DECIMAL(10,0)`) goes to the drift handler, so `incompatibleTables` names
 * it and the run rebuilds it in full rather than carry it. Before, the run
 * consulted only what the bind had seen, and a package loaded again after
 * the alter, which had bound nothing, carried the altered table.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import {
   compiledWith,
   fakeSource,
   makePlanOutcome,
   okWorkerPlan,
} from "../../test_helpers/materialization_test_fixtures";
import { contractFor } from "../../test_helpers/contract_fixture";
import type { ManifestEntry } from "./domain/types";
import { PackageMaterialization } from "./package_materialization";
import { deriveHoldingCarried } from "./run/carried_hold";
import { deriveSelfInstructions } from "./run/run_instructions";

let warehouse: DuckDBConnection;

beforeAll(async () => {
   warehouse = new DuckDBConnection("warehouse", ":memory:");
   await warehouse.runSQL(
      "CREATE TABLE altered AS SELECT 'a' AS cat, CAST(2.4 AS DECIMAL(10,1)) AS amt",
   );
   await warehouse.runSQL("ALTER TABLE altered ALTER amt TYPE DECIMAL(10,0)");
   await warehouse.runSQL(
      "CREATE TABLE intact AS SELECT 'a' AS cat, CAST(2.4 AS DECIMAL(10,1)) AS amt",
   );
   for (const t of ["elsewhere", "unrecorded"]) {
      await warehouse.runSQL(
         `CREATE TABLE ${t} AS SELECT 'a' AS cat, CAST(2.4 AS DECIMAL(10,0)) AS amt`,
      );
   }
});

afterAll(async () => {
   await warehouse?.close();
});

type Warehouse = Pick<DuckDBConnection, "runSQL" | "dialectName">;

/** The warehouse, with each statement it is sent recorded. */
function spied(): { connection: Warehouse; statements: string[] } {
   const statements: string[] = [];
   return {
      statements,
      connection: {
         dialectName: warehouse.dialectName,
         runSQL: (sql: string, options?: unknown) => {
            statements.push(sql);
            return warehouse.runSQL(sql, options as never);
         },
      },
   };
}

async function facade(
   connection: Warehouse = warehouse,
): Promise<PackageMaterialization> {
   const m = new PackageMaterialization({
      packageName: () => "pkg",
      versionId: () => undefined,
      metadata: () => ({}) as never,
      models: () => new Map(),
      modelPolicy: () => ({ setServingResolvers: () => {} }) as never,
      packagePath: () => "/pkg",
      malloyConfig: () => ({}) as never,
      connection: async () => connection as never,
      buildPlanPackage: {} as never,
      publishedSurface: () => undefined,
   });
   await m.applyLoadBuildPlan(okWorkerPlan(makePlanOutcome({})));
   return m;
}

/** A database entry as its build recorded it: `amt` built as DECIMAL(10,1). */
const built = (table: string): ManifestEntry =>
   ({
      sourceEntityId: table,
      physicalTableName: table,
      connectionName: "warehouse",
      builtAt: "2026-11-01T00:00:00.000Z",
      contract: contractFor([
         { name: "cat", type: "VARCHAR" },
         { name: "amt", type: "DECIMAL(10,1)" },
      ]),
   }) as ManifestEntry;

describe("a standalone run holds the tables it would carry to their contracts", () => {
   it("names a table retyped since its build, so the run rebuilds it rather than carry it", async () => {
      const m = await facade();
      const prior = { altered: built("altered"), intact: built("intact") };
      // Nothing bound: the bind never saw the alter.
      expect([...m.incompatibleTables(prior)]).toEqual([]);
      const found = await m.holdCarriedToContracts(prior);
      expect([...found.moved]).toEqual(["altered"]);
      expect([...found.unread]).toEqual([]);
      expect([...m.incompatibleTables(prior)]).toEqual(["altered"]);
      m.dispose();
   });

   it("never judges a table it cannot read, or one with no contract, moved", async () => {
      const m = await facade();
      const gone = built("no_such_table");
      const uncontracted = { ...built("altered"), contract: undefined };
      const found = await m.holdCarriedToContracts({
         no_such_table: gone,
         altered: uncontracted as ManifestEntry,
      });
      expect([...found.moved]).toEqual([]);
      // Unread: the caller decides (an advanced incremental table reseeds).
      expect([...found.unread]).toEqual(["no_such_table"]);
      m.dispose();
   });
});

describe("a standalone run reads only the tables it would carry", () => {
   // Four persisted sources, each over its own table: `altered` and
   // `intact` carried, `elsewhere` outside a run of `altered` and `intact`,
   // `unrecorded` rebuilt (no contract). Every table but `intact` reads as
   // moved.
   const names = ["altered", "intact", "elsewhere", "unrecorded"];
   const compiled = compiledWith(
      Object.fromEntries(
         names.map((n) => [n, fakeSource({ name: n, sourceEntityId: n })]),
      ),
      [names],
   );
   const prior: Record<string, ManifestEntry> = {
      altered: built("altered"),
      intact: built("intact"),
      elsewhere: built("elsewhere"),
      unrecorded: { ...built("unrecorded"), contract: undefined },
   };

   /** A run of `sourceNames` over `m`, with its pre-run hold spied on. */
   async function run(
      m: PackageMaterialization,
      sourceNames?: string[],
      hold = (carried: Readonly<Record<string, ManifestEntry>>) =>
         m.holdCarriedToContracts(carried),
   ) {
      const held: string[][] = [];
      const derived = await deriveHoldingCarried(
         (incompatible) =>
            deriveSelfInstructions(
               compiled,
               sourceNames,
               prior,
               undefined,
               undefined,
               { incompatible },
            ),
         () => m.incompatibleTables(prior),
         (carried) => {
            held.push(Object.keys(carried).sort());
            return hold(carried);
         },
      );
      return {
         held,
         built: derived.instructions.map((i) => i.sourceEntityId).sort(),
         carried: Object.keys(derived.carried).sort(),
      };
   }

   it("reads the carried tables alone, and rebuilds the one that moved", async () => {
      const { connection, statements } = spied();
      const m = await facade(connection);
      const outcome = await run(m, ["altered", "intact"]);
      // `elsewhere` is outside the run, `unrecorded` rebuilds anyway.
      expect(outcome.held).toEqual([["altered", "intact"]]);
      expect(outcome.built).toEqual(["altered"]);
      expect(outcome.carried).toEqual(["intact"]);
      const read = statements.join("\n");
      expect(read).toContain("altered");
      expect(read).toContain("intact");
      expect(read).not.toContain("elsewhere");
      expect(read).not.toContain("unrecorded");
      m.dispose();
   });

   it("never reports a moved table the run does not carry, run after run", async () => {
      const m = await facade(spied().connection);
      const reported = spyOn(m, "reportIncompatible");
      for (let i = 0; i < 3; i++) {
         const outcome = await run(m, ["intact"]);
         expect(outcome.held).toEqual([["intact"]]);
         expect(outcome.carried).toEqual(["intact"]);
      }
      // `elsewhere` and `altered` read as moved, but no run of `intact`
      // carries them: nothing goes to the drift handler, so the package is
      // not read and planned again on every run.
      expect(reported).not.toHaveBeenCalled();
      m.dispose();
   });

   it("carries a table its read did not reach within the budget, and starts within it", async () => {
      const stalled: Warehouse = {
         dialectName: warehouse.dialectName,
         runSQL: () => new Promise(() => undefined),
      };
      const m = await facade(stalled);
      const reported = spyOn(m, "reportIncompatible");
      const started = Date.now();
      const outcome = await run(m, undefined, (carried) =>
         m.holdCarriedToContracts(carried, undefined, { budgetMs: 50 }),
      );
      expect(Date.now() - started).toBeLessThan(5_000);
      // Unread is never moved: carried as before the read existed.
      expect(outcome.carried).toEqual(["altered", "elsewhere", "intact"]);
      expect(outcome.built).toEqual(["unrecorded"]);
      expect(reported).not.toHaveBeenCalled();
      m.dispose();
   });

   /** Incremental sources over the warehouse's tables, each a delta advances. */
   const incremental = (ids: string[], carriedToo: string[] = []) => {
      const declarations = Object.fromEntries(
         ids.map((id) => [
            id,
            {
               refresh: "incremental",
               incremental: true,
               declaredWatermark: true,
               declaredMergeKey: false,
               watermark: {
                  name: "amt",
                  kind: "dimension",
                  malloyType: "number",
               },
               watermarkOrderable: true,
               mergeKeys: [],
               scopeColumns: [],
               scopeIncomplete: false,
               watermarkInMergeKeys: false,
               strategy: "range_replace",
               malformed: [],
               unknownKeys: [],
               calculateFields: [],
               outputColumns: ["cat", "amt"],
            },
         ]),
      );
      const plan = compiledWith(
         Object.fromEntries(
            [...ids, ...carriedToo].map((id) => [
               id,
               fakeSource({
                  name: id,
                  sourceEntityId: id,
                  connectionName: "warehouse",
                  // A delta reads its source range on Postgres, not DuckDB;
                  // the tables are read on the warehouse connection.
                  dialectName: "postgres",
                  ...(ids.includes(id)
                     ? {
                          annotationFields: {
                             refresh: "incremental",
                             watermark: "amt",
                          },
                       }
                     : {}),
               }),
            ]),
         ),
         [[...ids, ...carriedToo]],
      );
      const context = {
         environmentId: "env-1",
         packageName: "pkg",
         materializationId: "run-1",
         forceRefresh: false,
         now: new Date("2026-11-23T00:00:00Z"),
         declarations,
         ledger: {},
      } as never;
      return { plan, context };
   };

   async function deltaRun(
      m: PackageMaterialization,
      ids: string[],
      carriedToo: string[],
      budgetMs?: number,
   ) {
      const { plan, context } = incremental(ids, carriedToo);
      const prior = Object.fromEntries(
         [...ids, ...carriedToo].map((id) => [id, built(id)]),
      );
      const derive = (incompatible: ReadonlySet<string>) =>
         deriveSelfInstructions(plan, undefined, prior, context, undefined, {
            incompatible,
         });
      // The source is incremental: a delta would advance it in place.
      for (const id of ids) {
         expect(derive(new Set()).advanced[id]).toBeDefined();
      }
      return deriveHoldingCarried(
         derive,
         () => m.incompatibleTables(prior),
         (kept) =>
            m.holdCarriedToContracts(
               kept,
               undefined,
               budgetMs !== undefined ? { budgetMs } : {},
            ),
         "pkg",
      );
   }

   it("reads an incremental table a delta would advance, and reseeds it when it moved", async () => {
      const m = await facade();
      const derived = await deltaRun(m, ["altered", "intact"], []);
      const reseed = Object.fromEntries(
         derived.instructions.map((i) => [i.sourceEntityId, i.reseed === true]),
      );
      expect(reseed).toEqual({ altered: true, intact: false });
      expect(Object.keys(derived.advanced)).toEqual(["intact"]);
      m.dispose();
   });

   it("reseeds an advanced table it could not read within the budget, and carries a carried one", async () => {
      const m = await facade({
         dialectName: warehouse.dialectName,
         runSQL: () => new Promise(() => undefined),
      });
      const reported = spyOn(m, "reportIncompatible");
      const derived = await deltaRun(m, ["intact"], ["elsewhere"], 50);
      // Never judged moved: no drift handler.
      expect(reported).not.toHaveBeenCalled();
      // Unverified, so not advanced: a delta would write into a table no
      // check holds. The carried table stays carried.
      expect(derived.instructions).toHaveLength(1);
      expect(derived.instructions[0].sourceEntityId).toBe("intact");
      expect(derived.instructions[0].reseed).toBe(true);
      expect(derived.advanced).toEqual({});
      expect(Object.keys(derived.carried)).toEqual(["elsewhere"]);
      m.dispose();
   });

   it("holds an advanced table whose entry an earlier run recorded, not the last one", async () => {
      // The last run was of other named sources, so it recorded no entry
      // for these: each delta's table is known only from an earlier run.
      const { plan, context } = incremental([
         "altered",
         "intact",
         "unrecorded",
      ]);
      const incrementalPriors = {
         altered: built("altered"),
         intact: built("intact"),
         unrecorded: { ...built("unrecorded"), contract: undefined },
      };
      const { connection, statements } = spied();
      const m = await facade(connection);
      const derived = await deriveHoldingCarried(
         (incompatible) =>
            deriveSelfInstructions(plan, undefined, {}, context, undefined, {
               incompatible,
               incrementalPriors,
            }),
         () => m.incompatibleTables({}),
         (kept) => m.holdCarriedToContracts(kept),
         "pkg",
      );
      const reseed = Object.fromEntries(
         derived.instructions.map((i) => [i.sourceEntityId, i.reseed === true]),
      );
      // Moved: reseeded. No contract: reseeded unread. Intact: advanced.
      expect(reseed).toEqual({
         altered: true,
         intact: false,
         unrecorded: true,
      });
      expect(Object.keys(derived.advanced)).toEqual(["intact"]);
      const read = statements.join("\n");
      expect(read).toContain("altered");
      expect(read).not.toContain("unrecorded");
      m.dispose();
   });

   it("reseeds an advanced table from an earlier run's entry it could not read", async () => {
      const { plan, context } = incremental(["intact"]);
      const m = await facade({
         dialectName: warehouse.dialectName,
         runSQL: () => new Promise(() => undefined),
      });
      const derived = await deriveHoldingCarried(
         (incompatible) =>
            deriveSelfInstructions(plan, undefined, {}, context, undefined, {
               incompatible,
               incrementalPriors: { intact: built("intact") },
            }),
         () => m.incompatibleTables({}),
         (kept) => m.holdCarriedToContracts(kept, undefined, { budgetMs: 50 }),
         "pkg",
      );
      expect(derived.instructions[0].reseed).toBe(true);
      expect(derived.advanced).toEqual({});
      m.dispose();
   });

   it("stops reading at the run's cancel, judging nothing", async () => {
      const m = await facade({
         dialectName: warehouse.dialectName,
         runSQL: () => new Promise(() => undefined),
      });
      const cancel = new AbortController();
      const found = m.holdCarriedToContracts(
         { altered: built("altered") },
         undefined,
         { signal: cancel.signal },
      );
      cancel.abort();
      const held = await found;
      expect([...held.moved]).toEqual([]);
      expect([...held.unread]).toEqual(["altered"]);
      m.dispose();
   });
});
