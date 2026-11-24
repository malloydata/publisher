// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { logger } from "../../../logger";
import {
   persistedResultFixture,
   type PersistedResultFixture,
   pinnedOf,
} from "../../../test_helpers/persisted_result_fixture";
import type { Binding } from "../domain/binding";
import {
   contractWithoutPassedKeys,
   keyUniquenessOfContract,
} from "../domain/column_contract";
import { LegDecline } from "../domain/leg_decline";
import { splitSourceID } from "../domain/table_plans";
import { rollupCompanionURLOf } from "../text/read_text";

/**
 * The rollup leg against a real compile: which queries a rollup reproduces
 * exactly and answers from its table, and each way it declines (grain,
 * stored measures, an aggregate over its rows, a time zone, an extension that
 * filters, an unbuilt or stale table) or does not apply. No query runs; an
 * answer is checked by the SQL it compiles.
 */

const MODEL = `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A', 'east'), (20, 'B', 'west')) AS t(amount, category, region)
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
  measure: n is count()
  view: by_cat is { group_by: category; aggregate: total }
}

source: orders_x is orders extend { dimension: cat2 is concat(category, '!') }
source: orders_w is orders extend { where: region = 'east' }
source: plain is duckdb.sql("select 1 as a")
`;

const TABLE = "stored_rollup_t";

async function outcomeOf(
   promise: Promise<unknown>,
): Promise<{ name: string; code?: string; detail?: string; shape?: boolean }> {
   const err = await promise.then(
      () => undefined,
      (e: unknown) => e,
   );
   expect(err).toBeInstanceOf(Error);
   const e = err as Error;
   return {
      name: e.constructor.name,
      ...(e instanceof LegDecline
         ? { code: e.code, detail: e.callerDetail, shape: e.shapeCheck }
         : { detail: e.message }),
   };
}

describe("RollupVariantServe (database)", () => {
   let f: PersistedResultFixture;
   let tableId: string;
   beforeAll(async () => {
      f = await persistedResultFixture(MODEL);
      const planned = [...f.catalog.planned.values()].find(
         (t) => t.origin === "rollup",
      );
      if (!planned) throw new Error("no rollup planned");
      tableId = planned.sourceEntityId;
   });
   afterAll(async () => {
      await f?.cleanup();
   });

   function attempt(
      queryString: string,
      entries: Record<string, unknown> = { [tableId]: { tableName: TABLE } },
   ) {
      return f.rollups.attempt({
         queryString,
         givens: undefined,
         authorRunnable: f.materializer.loadQuery(queryString),
         pinned: pinnedOf([], entries as never),
      });
   }

   async function sqlOf(queryString: string): Promise<string> {
      const answer = await attempt(queryString);
      const { sql } = await answer.runnable.getPreparedResult({
         buildManifest: pinnedOf([], { [tableId]: { tableName: TABLE } })
            .compilerManifest,
      } as never);
      return sql;
   }

   it("lists the model's rollups with their bases' names", () => {
      expect(f.rollups.hasRollups()).toBe(true);
      const [rollup, ...more] = f.rollups.rollups();
      expect(more).toEqual([]);
      expect(rollup.baseSourceName).toBe("orders");
      expect(rollup.rollupSourceName).toStartWith("orders__rollup__category__");
      expect(rollup.baseSourceID).toEndWith("/m.malloy");
   });

   it("answers a query at the grain over a stored measure from the rollup's table", async () => {
      const answer = await attempt(
         "run: orders -> { group_by: category; aggregate: total }",
      );
      expect(answer.rollupSourceName).toStartWith("orders__rollup__category__");
      expect(answer.virtualMap).toBeUndefined();
      expect(answer.binding).toBeUndefined();
      expect(answer.graftScope?.cacheScope).toStartWith("rollup\u0000");
      expect(
         await sqlOf("run: orders -> { group_by: category; aggregate: total }"),
      ).toContain(TABLE);
   });

   it("answers a named view, a filter on the grain, and a having on a stored measure", async () => {
      for (const query of [
         "run: orders -> by_cat",
         "run: orders -> { where: category = 'A'; aggregate: total }",
         "run: orders -> { group_by: category; aggregate: total; having: total > 1 }",
      ]) {
         expect(await sqlOf(query)).toContain(TABLE);
      }
   });

   it("answers a query on an extension that adds only dimensions over the grain", async () => {
      expect(
         await sqlOf("run: orders_x -> { group_by: cat2; aggregate: total }"),
      ).toContain(TABLE);
   });

   it("declines a field the grain does not keep, as a shape check", async () => {
      expect(
         await outcomeOf(
            attempt("run: orders -> { group_by: region; aggregate: total }"),
         ),
      ).toEqual({
         name: "LegDecline",
         code: "rollup_grain_too_coarse",
         detail:
            "the query reads `region`, which the rollup's grain does not keep",
         shape: true,
      });
   });

   it("declines a measure the rollup does not store, as a shape check", async () => {
      expect(
         await outcomeOf(
            attempt("run: orders -> { group_by: category; aggregate: n }"),
         ),
      ).toEqual({
         name: "LegDecline",
         code: "rollup_missing_field",
         detail: "the query aggregates `n`, which the rollup does not store",
         shape: true,
      });
   });

   it("declines an aggregate written in the query, and a select: stage, as reading the rollup's rows", async () => {
      expect(
         await outcomeOf(
            attempt(
               "run: orders -> { group_by: category; aggregate: c is count() }",
            ),
         ),
      ).toEqual({
         name: "LegDecline",
         code: "rollup_count_semantics",
         detail: "`c` aggregates the rollup's rows, not a stored measure",
         shape: false,
      });
      expect(
         await outcomeOf(attempt("run: orders -> { select: category }")),
      ).toEqual({
         name: "LegDecline",
         code: "rollup_count_semantics",
         detail:
            "a select: stage reads the source's rows, which the rollup does not keep",
         shape: false,
      });
   });

   it("answers a query that sets its own time zone and reads no time (A5)", async () => {
      // Nothing it reads depends on a zone; one reading a stored truncation
      // is declined `rollup_time_zone` (r4/rollup_gaps).
      const answer = await attempt(
         "run: orders -> { timezone: 'America/New_York'; group_by: category; aggregate: total }",
      );
      expect(answer.rollupSourceName).toBeDefined();
   });

   it("declines an extension that filters the rolled-up source's rows", async () => {
      expect(
         await outcomeOf(
            attempt(
               "run: orders_w -> { group_by: category; aggregate: total }",
            ),
         ),
      ).toEqual({
         name: "LegDecline",
         code: "rollup_grain_too_coarse",
         detail:
            "the query's source filters its rows (`where:`) beyond the source the rollup aggregates",
         shape: true,
      });
   });

   it("does not apply to a source with no rollup, with no LegDecline", async () => {
      expect(await outcomeOf(attempt("run: plain -> { group_by: a }"))).toEqual(
         {
            name: "NoRollupForQuery",
            detail: "the query's source declares no rollup",
         },
      );
   });

   it("declines an unbuilt table as not built, and one past its window as stale", async () => {
      const query = "run: orders -> { group_by: category; aggregate: total }";
      expect(await outcomeOf(attempt(query, {}))).toMatchObject({
         code: "rollup_not_built",
         detail: "the rollup has no fresh table",
      });
      expect(
         await outcomeOf(
            attempt(query, {
               [tableId]: {
                  tableName: TABLE,
                  dataAsOf: new Date(Date.now() - 3_600_000).toISOString(),
                  freshnessWindowSeconds: 60,
                  freshnessFallback: "live",
               },
            }),
         ),
      ).toMatchObject({
         code: "rollup_stale",
         detail: "the rollup's table is past its freshness window",
      });
   });
});

const STORAGE_MODEL = `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A', 'east'), (20, 'B', 'west')) AS t(amount, category, region)
""") extend {
  #(rollup) grain=["category"] storage="lake"
  measure: total is amount.sum()
}
`;

describe("RollupVariantServe (storage=)", () => {
   let f: PersistedResultFixture;
   let noDestination: PersistedResultFixture;
   const query = "run: orders -> { group_by: category; aggregate: total }";

   function bindingFor(
      fixture: PersistedResultFixture,
      over: Partial<Binding> = {},
   ): Binding {
      const [rollup] = fixture.rollups.rollups();
      const declared = splitSourceID(rollup.baseSourceID);
      const companion = rollupCompanionURLOf(declared!.url);
      return {
         kind: "rollup",
         tier: "storage",
         sourceName: rollup.rollupSourceName,
         sourceIDs: [`${rollup.rollupSourceName}@${companion!.href}`],
         destinationName: "lake",
         virtualHandle: "vh_rollup",
         tablePath: "main.rollup_t",
         schema: [
            { name: "category", type: "VARCHAR" },
            { name: "total__partial", type: "DOUBLE" },
         ],
         ...over,
      };
   }

   beforeAll(async () => {
      f = await persistedResultFixture(STORAGE_MODEL, {
         noBuildPlan: true,
         destinations: { lake: new DuckDBConnection("lake", ":memory:") },
      });
      noDestination = await persistedResultFixture(STORAGE_MODEL, {
         noBuildPlan: true,
      });
   });
   afterAll(async () => {
      await f?.cleanup();
      await noDestination?.cleanup();
   });

   it("lists only the storage rollup bindings of a pin", () => {
      const mine = bindingFor(f);
      const pinned = pinnedOf([
         mine,
         { ...mine, kind: "persist" },
         { ...mine, tier: "database" },
      ]);
      expect(f.rollups.rollupBindings(pinned, "bound")).toEqual([mine]);
      expect(f.rollups.rollupBindings(pinned, "fresh")).toEqual([mine]);
   });

   it("answers from the stored table through its destination", async () => {
      const binding = bindingFor(f);
      const answer = await f.rollups.attempt({
         queryString: query,
         givens: undefined,
         authorRunnable: f.materializer.loadQuery(query),
         pinned: pinnedOf([binding]),
      });
      expect(answer.binding).toBe(binding);
      const { sql } = await answer.runnable.getPreparedResult({
         virtualMap: answer.virtualMap,
      } as never);
      expect(sql).toContain('"main"."rollup_t"');
   });

   // P3-06..09: a table a Snowflake session stored with its aliases folded
   // upper-case is matched by a unique case fold, as the served copy matches.
   it("P3-07: answers from a stored table whose columns a warehouse upper-cased", async () => {
      const binding = bindingFor(f, {
         schema: [
            { name: "CATEGORY", type: "VARCHAR" },
            { name: "TOTAL__PARTIAL", type: "DOUBLE" },
         ],
      });
      const answer = await f.rollups.attempt({
         queryString: query,
         givens: undefined,
         authorRunnable: f.materializer.loadQuery(query),
         pinned: pinnedOf([binding]),
      });
      expect(answer.binding).toBe(binding);
   });

   it("declines a fresh binding with no destination to read it as not built", async () => {
      expect(
         await outcomeOf(
            noDestination.rollups.attempt({
               queryString: query,
               givens: undefined,
               authorRunnable: noDestination.materializer.loadQuery(query),
               pinned: pinnedOf([bindingFor(noDestination)]),
            }),
         ),
      ).toMatchObject({ code: "rollup_not_built" });
   });

   it("refuses, as a stored column live's type cannot read, a stored partial whose type the measure cannot merge, without a warning", async () => {
      const warn = sinon.stub(logger, "warn");
      try {
         const binding = bindingFor(f, {
            virtualHandle: "vh_text",
            schema: [
               { name: "category", type: "VARCHAR" },
               { name: "total__partial", type: "VARCHAR" },
            ],
         });
         expect(
            await outcomeOf(
               f.rollups.attempt({
                  queryString: query,
                  givens: undefined,
                  authorRunnable: f.materializer.loadQuery(query),
                  pinned: pinnedOf([binding]),
               }),
            ),
         ).toMatchObject({
            code: "rollup_missing_field",
            detail:
               "the stored rollup cannot answer with live's columns: the stored `total` (VARCHAR) cannot be read as live's number",
         });
         expect(warn.called).toBe(false);
      } finally {
         warn.restore();
      }
   });

   it("warns once for a variant that does not compile, however many queries it declines", async () => {
      const warn = sinon.stub(logger, "warn");
      try {
         const binding = bindingFor(f, {
            virtualHandle: "vh_missing_partial",
            schema: [{ name: "category", type: "VARCHAR" }],
         });
         // One snapshot of the bindings, as every query until they change reads.
         const pinned = pinnedOf([binding]);
         for (let i = 0; i < 3; i++) {
            expect(
               await outcomeOf(
                  f.rollups.attempt({
                     queryString: query,
                     givens: undefined,
                     authorRunnable: f.materializer.loadQuery(query),
                     pinned,
                  }),
               ),
            ).toMatchObject({
               code: "rollup_missing_field",
               detail: "the query does not compile against the rollup",
            });
         }
         const compileWarnings = warn
            .getCalls()
            .filter(
               (call) =>
                  (call.args as unknown as [string])[0] ===
                  "A #(rollup) rollup variant did not compile",
            );
         expect(compileWarnings.length).toBe(1);
      } finally {
         warn.restore();
      }
   });
});

const JOIN_MODEL = `source: segments is duckdb.sql("""
  select * from (values (10, 1, 'p', 2.0), (11, 1, 'q', 3.0)) as t(seg_id, flight_id, kind, miles)
""") extend {
  primary_key: seg_id
  #(rollup) grain=["flight_id"]
  measure: seg_miles is miles.sum()
}

source: crew is duckdb.sql("select * from (values (1, 3)) as t(flight_id, size)") extend {
  primary_key: flight_id
  #(rollup) grain=["flight_id"]
  measure: crew_size is size.sum()
}

source: flights is duckdb.sql("select * from (values (1, 'AA', 100)) as t(id, carrier, distance)") extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  join_one: crew with id
  measure: flight_count is count()
}
`;

/**
 * P4-03 (F-14): a rollup with no `storage=` that a host instructed into a
 * storage destination (a database origin) is bound as a storage table, and
 * the binding decides the tier: it is read from the destination exactly as a
 * `storage=` rollup's is, never looked for in the database manifest.
 */
describe("RollupVariantServe (a host-chosen destination, no storage=)", () => {
   let f: PersistedResultFixture;
   const query = "run: orders -> { group_by: category; aggregate: total }";
   beforeAll(async () => {
      f = await persistedResultFixture(
         STORAGE_MODEL.replace(' storage="lake"', ""),
         {
            noBuildPlan: true,
            destinations: {
               lake: new DuckDBConnection("lake_f14r", ":memory:"),
            },
         },
      );
   });
   afterAll(async () => {
      await f?.cleanup();
   });

   it("P4-03: answers from the storage binding through its destination", async () => {
      const [rollup] = f.rollups.rollups();
      const declared = splitSourceID(rollup.baseSourceID);
      const companion = rollupCompanionURLOf(declared!.url);
      const binding: Binding = {
         kind: "rollup",
         tier: "storage",
         sourceName: rollup.rollupSourceName,
         sourceIDs: [`${rollup.rollupSourceName}@${companion!.href}`],
         destinationName: "lake",
         virtualHandle: "vh_rollup",
         tablePath: "main.rollup_t",
         schema: [
            { name: "category", type: "VARCHAR" },
            { name: "total__partial", type: "DOUBLE" },
         ],
      };
      const answer = await f.rollups.attempt({
         queryString: query,
         givens: undefined,
         authorRunnable: f.materializer.loadQuery(query),
         pinned: pinnedOf([binding]),
      });
      expect(answer.binding).toBe(binding);
      expect(answer.virtualMap).toBeDefined();
      // The leg reports this binding as the table it read, so the gate
      // policy judges the answer as a storage= rollup's (F-14).
      expect(answer.binding?.tier).toBe("storage");
      const { sql } = await answer.runnable.getPreparedResult({
         virtualMap: answer.virtualMap,
      } as never);
      expect(sql).toContain('"main"."rollup_t"');
   });
});

describe("RollupVariantServe (a joined source's rollup, read through the join)", () => {
   let f: PersistedResultFixture;
   let segmentsTable: string;
   let crewTable: string;
   let flightsID: string;
   let segmentsID: string;
   beforeAll(async () => {
      f = await persistedResultFixture(JOIN_MODEL);
      const idOf = (prefix: string) => {
         const t = [...f.catalog.planned.values()].find((p) =>
            p.name.startsWith(prefix),
         );
         if (!t) throw new Error(`no rollup of ${prefix}`);
         return t.sourceEntityId;
      };
      segmentsTable = idOf("segments__rollup__");
      crewTable = idOf("crew__rollup__");
      const sourceIDOf = (name: string) =>
         (f.modelDef.contents[name] as unknown as { sourceID: string })
            .sourceID;
      flightsID = sourceIDOf("flights");
      segmentsID = sourceIDOf("segments");
   });
   afterAll(async () => {
      await f?.cleanup();
   });

   /** Both rollups built, each entry with the key checks `checks` gives it. */
   function attempt(
      queryString: string,
      checks: { segments?: object; crew?: object } = {},
   ) {
      return f.rollups.attempt({
         queryString,
         givens: undefined,
         authorRunnable: f.materializer.loadQuery(queryString),
         pinned: pinnedOf([], {
            [segmentsTable]: {
               tableName: "seg_rollup_t",
               ...(checks.segments ? { keyUniqueness: checks.segments } : {}),
            },
            [crewTable]: {
               tableName: "crew_rollup_t",
               ...(checks.crew ? { keyUniqueness: checks.crew } : {}),
            },
         } as never),
      });
   }

   it("answers a join_one's measure from its rollup, the join re-pointed", async () => {
      const answer = await attempt(
         "run: flights -> { group_by: carrier; aggregate: crew.crew_size }",
      );
      expect(answer.rollupSourceName).toStartWith("crew__rollup__");
      expect(answer.graftScope).toBeUndefined();
   });

   it("serves an aggregate outside a join_one only when the rollup's build found its key unique", async () => {
      const query =
         "run: flights -> { group_by: carrier; aggregate: crew.crew_size, flight_count }";
      expect(await outcomeOf(attempt(query))).toMatchObject({
         code: "rollup_count_semantics",
         detail:
            "count() aggregates rows outside the join_one `crew`, which live repeats for each of its rows, and its key was not checked unique when its rollup was built; its rollup holds one row per key",
      });
      expect(
         await outcomeOf(
            attempt(query, {
               crew: { keyColumn: "flight_id", unique: false },
            }),
         ),
      ).toMatchObject({
         code: "rollup_count_semantics",
         detail:
            "count() aggregates rows outside the join_one `crew`, whose key the data holds more than once, so live repeats those rows; its rollup holds one row per key",
      });
      const answer = await attempt(query, {
         crew: { keyColumn: "flight_id", unique: true },
      });
      expect(answer.rollupSourceName).toStartWith("crew__rollup__");
   });

   it("reads a join_many through its rollup only when both keys were checked unique", async () => {
      const query =
         "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }";
      expect(await outcomeOf(attempt(query))).toMatchObject({
         code: "rollup_count_semantics",
         detail:
            "the query source's primary key `id` was not checked unique and never NULL when the rollup of `segments` was built, so a row of it could be read once per row that shares it",
      });
      expect(
         await outcomeOf(
            attempt(query, {
               segments: {
                  parentKeys: { [flightsID]: false },
                  parentKeyColumns: { [flightsID]: "id" },
               },
            }),
         ),
      ).toMatchObject({
         code: "rollup_count_semantics",
         detail:
            "the query source holds a value of its primary key `id` more than once or NULL, so a row of the rollup of `segments` would be read once per row that shares it",
      });
      // A check of the key `flights` declared at build, not now, says nothing.
      expect(
         await outcomeOf(
            attempt(query, {
               segments: {
                  parentKeys: { [flightsID]: true },
                  parentKeyColumns: { [flightsID]: "old_id" },
               },
            }),
         ),
      ).toMatchObject({
         code: "rollup_count_semantics",
         detail:
            "the query source's primary key `id` was not checked unique and never NULL when the rollup of `segments` was built, so a row of it could be read once per row that shares it",
      });
      expect(
         await outcomeOf(
            attempt(query, {
               segments: {
                  parentKeys: { [flightsID]: true },
                  parentKeyColumns: { [flightsID]: "id" },
               },
            }),
         ),
      ).toMatchObject({
         code: "rollup_count_semantics",
         detail:
            "`segments`'s primary key `seg_id` was not checked unique and never NULL when its rollup was built, and live counts and sums `segments.seg_miles` once per non-NULL key where its rollup reads every row",
      });
      const answer = await attempt(query, {
         segments: {
            parentKeys: { [flightsID]: true, [segmentsID]: true },
            parentKeyColumns: { [flightsID]: "id", [segmentsID]: "seg_id" },
         },
      });
      expect(answer.rollupSourceName).toStartWith("segments__rollup__");
   });

   it("declines a join_many read beside a join_one whose rollup's build found its key repeated, also while a run is active", async () => {
      const query =
         "run: flights -> { group_by: crew.size; aggregate: segments.seg_miles }";
      const segments = {
         parentKeys: { [flightsID]: true, [segmentsID]: true },
         parentKeyColumns: { [flightsID]: "id", [segmentsID]: "seg_id" },
      };
      // `crew`'s contract as the store holds it, and as a bind reads it while
      // a run of the package is active: only a passed check is stripped, so
      // the repeat is still known.
      const crewContract = {
         v: 1,
         dialect: "duckdb",
         columns: [],
         keys: { "rollup:flight_id": { unique: false, level: "rollup" } },
      };
      const whileRunning = keyUniquenessOfContract(
         contractWithoutPassedKeys(crewContract),
         [],
      );
      expect(whileRunning).toEqual({ keyColumn: "flight_id", unique: false });
      expect(
         await outcomeOf(attempt(query, { segments, crew: whileRunning })),
      ).toMatchObject({
         code: "rollup_count_semantics",
         detail:
            "the query reads the join_one `crew`, whose key the data holds more than once, so live repeats rows through it",
      });
      // Unknown is served (known limitation 29): what stripping every check
      // would have done.
      const answer = await attempt(query, { segments });
      expect(answer.rollupSourceName).toStartWith("segments__rollup__");
   });

   it("declines a field through the join the rollup's grain does not keep", async () => {
      expect(
         await outcomeOf(
            attempt(
               "run: flights -> { group_by: segments.kind; aggregate: segments.seg_miles }",
            ),
         ),
      ).toMatchObject({
         code: "rollup_grain_too_coarse",
         detail:
            "the query reads `segments.kind`, which the rollup's grain does not keep",
      });
   });

   it("does not apply to a query that reads nothing through a join with a rollup", async () => {
      expect(
         await outcomeOf(
            attempt(
               "run: flights -> { group_by: carrier; aggregate: flight_count }",
            ),
         ),
      ).toEqual({ name: "NoRollupForQuery", detail: "no rollup" });
   });
});

const FOLDED_MODEL = `##! experimental { persistence composite_sources }
source: trips_named is duckdb.sql("select * from (values (1, 1)) as t(trip_id, station_id)") extend {
  primary_key: trip_id
  #(rollup) grain=["station_id"] name="scratch.trips_by_station"
  measure: trip_count is count()
}

source: trips_default is duckdb.sql("select * from (values (1, 1)) as t(trip_id, station_id)") extend {
  primary_key: trip_id
  #(rollup) grain=["station_id"]
  measure: trip_count is count()
}
`;

/**
 * Campaign R-01: two rollups of one build placed apart are two addresses that
 * Malloy files under one key. The moved one is read under that key, also
 * when the manifest holds no entry at the key (its twin stored elsewhere, or
 * not built), where before its compile recomputed and it read as NOT_BUILT.
 */
describe("RollupVariantServe (a rollup whose address is not Malloy's key)", () => {
   let f: PersistedResultFixture;
   let named: { sourceEntityId: string; compilerKey?: string };
   beforeAll(async () => {
      f = await persistedResultFixture(FOLDED_MODEL);
      const found = [...f.catalog.planned.values()].find((t) =>
         t.name.startsWith("trips_named__rollup__"),
      );
      if (!found) throw new Error("no rollup of trips_named planned");
      named = found;
   });
   afterAll(async () => {
      await f?.cleanup();
   });

   it("answers from its own table with only its own entry bound", async () => {
      expect(named.compilerKey).toBeDefined();
      expect(named.compilerKey).not.toBe(named.sourceEntityId);
      const query =
         "run: trips_named -> { group_by: station_id; aggregate: trip_count }";
      const entries = {
         [named.sourceEntityId]: { tableName: "named_rollup_t" },
      };
      const answer = await f.rollups.attempt({
         queryString: query,
         givens: undefined,
         authorRunnable: f.materializer.loadQuery(query),
         pinned: pinnedOf([], entries as never),
      });
      expect(answer.databaseTablePath).toBe("named_rollup_t");
      expect(answer.keyedTable).toEqual({
         tableId: named.sourceEntityId,
         compilerKey: named.compilerKey!,
      });
   });
});
