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
   storedViewBinding,
} from "../../../test_helpers/persisted_result_fixture";
import { gateSwitches } from "../../../config";
import { LegDecline } from "../domain/leg_decline";
import { makeLegs } from "./leg";
import { collationProbe, ReadAdmission } from "./read_admission";
import { PersistedViewServe } from "./view_variants";

/**
 * The persisted view leg against a real compile: which query texts call a
 * persisted view, which table it would read (database or `storage=`, built,
 * stale or fresh), and each way an attempt declines or answers from the
 * stored result. No query runs; an answer is checked by the SQL it compiles.
 */

const MODEL = `##! experimental { persistence }
source: orders is duckdb.sql("""
  select * from (values (1, 'east', 10.0), (2, 'west', 20.0)) as t(id, region, amount)
""") extend {
  measure: total is amount.sum()
  #@ persist
  view: by_region is { group_by: region; aggregate: total; order_by: region }
  #@ persist
  view: grand is { aggregate: total }
  view: plain is { group_by: region }
}

source: orders_ext is orders extend { dimension: big is amount > 15 }
`;

const STORED = "stored_by_region_t";

async function attemptOf(
   f: PersistedResultFixture,
   queryString: string,
   entries: Record<string, { tableName: string }>,
   views: PersistedViewServe = f.views,
) {
   const pinned = pinnedOf([], entries);
   return views.attempt({
      queryString,
      givens: undefined,
      authorRunnable: f.materializer.loadQuery(queryString),
      pinned,
      buildManifest: pinned.compilerManifest,
   });
}

async function declineOf(promise: Promise<unknown>): Promise<LegDecline> {
   const err = await promise.then(
      () => undefined,
      (e: unknown) => e,
   );
   expect(err).toBeInstanceOf(LegDecline);
   return err as LegDecline;
}

describe("PersistedViewServe (database)", () => {
   let f: PersistedResultFixture;
   let tableId: string;
   let built: Record<string, { tableName: string }>;
   beforeAll(async () => {
      f = await persistedResultFixture(MODEL);
      tableId = f.tableIdOf("orders__by_region__view");
      built = { [tableId]: { tableName: STORED } };
   });
   afterAll(async () => {
      await f?.cleanup();
   });

   it("plans the model's persisted views, and only those", () => {
      expect(f.views.hasViews()).toBe(true);
      expect(
         f.views
            .plans()
            .map((p) => p.viewName)
            .sort(),
      ).toEqual(["by_region", "grand"]);
   });

   it("names the view a call reads, and nothing for a text that is not a call of one", () => {
      expect(
         f.views.calledView("run: orders -> by_region")?.plan.viewName,
      ).toBe("by_region");
      expect(
         f.views.calledView("run: orders -> by_region -> { select: region }")
            ?.call.laterStages,
      ).toBe(1);
      expect(f.views.calledView("run: orders -> plain")).toBeUndefined();
      expect(
         f.views.calledView("run: orders -> { group_by: region }"),
      ).toBeUndefined();
      // An extension is another source: its sourceID is not the view's.
      expect(
         f.views.calledView("run: orders_ext -> by_region"),
      ).toBeUndefined();
      expect(f.views.calledView("run: nowhere -> by_region")).toBeUndefined();
   });

   it("reports the table a call would read: unbuilt, stale past the pin, or fresh", () => {
      const call = "run: orders -> by_region";
      expect(f.views.reportedTable(call, pinnedOf())).toEqual({
         tableId,
         tier: "database",
         built: false,
         stale: false,
      });
      const old = new Date(Date.now() - 3_600_000).toISOString();
      expect(
         f.views.reportedTable(
            call,
            pinnedOf([], {
               [tableId]: {
                  tableName: STORED,
                  dataAsOf: old,
                  freshnessWindowSeconds: 60,
                  freshnessFallback: "live",
               },
            }),
         ),
      ).toEqual({
         tableId,
         tier: "database",
         built: true,
         stale: true,
         dataAsOf: old,
      });
      expect(f.views.reportedTable(call, pinnedOf([], built))).toEqual({
         tableId,
         tier: "database",
         built: true,
         stale: false,
      });
      expect(
         f.views.reportedTable("run: orders -> plain", pinnedOf([], built)),
      ).toBeUndefined();
   });

   it("answers an exact call from the stored table, its SQL reading it", async () => {
      const answer = await attemptOf(f, "run: orders -> by_region", built);
      expect(answer.table).toEqual({
         tableId,
         tier: "database",
         built: true,
         stale: false,
      });
      expect(answer.virtualMap).toBeUndefined();
      expect(answer.binding).toBeUndefined();
      expect(answer.graftScope.cacheScope).toStartWith("persisted_view\u0000");
      const pinned = pinnedOf([], built);
      const { sql } = await answer.runnable.getPreparedResult({
         buildManifest: pinned.compilerManifest,
      } as never);
      expect(sql).toContain(STORED);
   });

   it("answers a query that goes on past the view from the stored table", async () => {
      const answer = await attemptOf(
         f,
         "run: orders -> by_region -> { select: region }",
         built,
      );
      expect(answer.table.built).toBe(true);
   });

   it("declines a refinement of the view", async () => {
      const decline = await declineOf(
         attemptOf(f, "run: orders -> by_region + { limit: 1 }", built),
      );
      expect(decline.code).toBe("view_shape_mismatch");
      expect(decline.reason).toBe("SHAPE_MISMATCH");
      expect(decline.callerDetail).toContain("refines `by_region`");
   });

   it("declines a text that calls no persisted view, with no detail", async () => {
      const decline = await declineOf(
         attemptOf(f, "run: orders_ext -> by_region", built),
      );
      expect(decline.code).toBe("view_shape_mismatch");
      expect(decline.callerDetail).toBeUndefined();
   });

   it("declines an unbuilt view as not built, and one bound but past its pin as stale", async () => {
      expect(
         (await declineOf(attemptOf(f, "run: orders -> by_region", {}))).code,
      ).toBe("view_not_built");
      const pinned = pinnedOf([], built);
      const stale = await declineOf(
         f.views.attempt({
            queryString: "run: orders -> by_region",
            givens: undefined,
            authorRunnable: f.materializer.loadQuery(
               "run: orders -> by_region",
            ),
            pinned,
            // The manifest the query compiles with does not hold the table.
            buildManifest: { entries: {}, strict: false },
         }),
      );
      expect(stale.code).toBe("view_stale");
   });

   it("declines a view whose stored rows cannot be regrouped into live's, saying why", async () => {
      const grandId = f.tableIdOf("orders__grand__view");
      const decline = await declineOf(
         attemptOf(f, "run: orders -> grand", {
            [grandId]: { tableName: "stored_grand_t" },
         }),
      );
      expect(decline.code).toBe("view_missing_field");
      // A column the stored result cannot give, not a query of another shape.
      expect(decline.reason).toBe("MISSING_FIELD");
      expect(decline.callerDetail).toBe(
         "the stored result cannot answer `grand` with live's columns: it groups by no dimension, so an empty result would come back as one row",
      );
   });

   it("declines, and logs, a variant compiled with no package context", async () => {
      const bare = new PersistedViewServe({
         modelPath: () => "m.malloy",
         modelDef: () => f.modelDef,
         modelMaterializer: () => f.materializer,
         serveDestinationConfig: () => undefined,
         catalog: () => f.catalog,
         planModelPersistedViews: (m) =>
            f.views.plans().length > 0 && m
               ? { plans: f.views.plans() }
               : { plans: [] },
         planModelPersistedQueries: () => ({ plans: [] }),
      });
      const warn = sinon.stub(logger, "warn");
      try {
         const decline = await declineOf(
            attemptOf(f, "run: orders -> by_region", built, bare),
         );
         expect(decline.code).toBe("view_missing_field");
         expect(decline.callerDetail).toBe(
            "the query does not compile against the stored view",
         );
         const [message, meta] = warn.firstCall.args as unknown as [
            string,
            Record<string, unknown>,
         ];
         expect(message).toBe("A persisted-view variant did not compile");
         // The compile's text never reaches the log.
         expect(meta).not.toHaveProperty("error");
      } finally {
         warn.restore();
      }
   });

   it("marks a table stored as of its build, so read admission declines an access decision over it", async () => {
      // The other checks keep access decisions off this leg today; this
      // backstop must decline one on its own, so it has to see the flag. A
      // database view's table has no binding: its manifest entry carries it.
      const pinned = pinnedOf([], {
         [tableId]: { tableName: STORED, asOfBuild: true },
      });
      const query = {
         queryString: "run: orders -> by_region",
         givens: undefined,
         authorRunnable: f.materializer.loadQuery("run: orders -> by_region"),
      };
      const [leg] = makeLegs(pinned, { persistedViews: f.views });
      const answer = await leg.attempt(query, pinned);
      expect(answer.resultTableRead).toMatchObject({
         tier: "database",
         tablePath: STORED,
         asOfBuild: true,
      });
      // The path report does not carry it.
      expect(answer.viewTable).not.toHaveProperty("asOfBuild");
      const admission = new ReadAdmission(
         query,
         gateSwitches(),
         collationProbe,
      );
      // Past an entry-point gate: an access decision, declined.
      expect((await admission.decide("view", answer, true)).verdict).toEqual({
         admit: false,
         reason: "as_of_build_access",
         detail: expect.any(String),
      });
      // The view's ordering by a string is applied again to the stored rows,
      // so the table's collation is read: one the catalog does not list is
      // unknown and answers live.
      expect(
         (await admission.decide("view", answer, false)).verdict,
      ).toMatchObject({ admit: false, reason: "result_order_comparison" });
      const runtime = (
         answer.runnable as unknown as {
            runtime: {
               connections: {
                  lookupConnection(n: string): Promise<{
                     runSQL(sql: string): Promise<unknown>;
                  }>;
               };
            };
         }
      ).runtime;
      await (
         await runtime.connections.lookupConnection("duckdb")
      ).runSQL(
         `CREATE TABLE ${STORED} AS SELECT 'east' AS region, 10.0 AS total`,
      );
      // The query reads no given and passes no gate: it reads the table.
      const listed = new ReadAdmission(query, gateSwitches(), collationProbe);
      expect((await listed.decide("view", answer, false)).verdict).toEqual({
         admit: true,
         pastEntryPointGate: false,
      });
      // Without the flag the same answer is not an as-of-build read.
      const plain = await leg.attempt(query, pinnedOf([], built));
      expect(plain.resultTableRead).not.toHaveProperty("asOfBuild");
   });

   it("names the compiler key of a planned table, and none for an unplanned one", () => {
      const id = f.buildSourceIDOf("orders__by_region__view");
      // The table's own address is the compiler's key here.
      expect(f.views.compilerKeyOf(id)).toBeUndefined();
      expect(f.views.compilerKeyOf("nothing@file:///x")).toBeUndefined();
   });
});

const STORAGE_MODEL = `##! experimental { persistence }
source: orders is duckdb.sql("""
  select * from (values (1, 'east', 10.0), (2, 'west', 20.0)) as t(id, region, amount)
""") extend {
  measure: total is amount.sum()
  #@ persist storage="lake"
  view: by_region is { group_by: region; aggregate: total; order_by: region }
}
`;

describe("PersistedViewServe (storage=)", () => {
   let f: PersistedResultFixture;
   let noDestination: PersistedResultFixture;
   let buildSourceID: string;
   const schema = [
      { name: "region", type: "VARCHAR" },
      { name: "total", type: "DOUBLE" },
   ];
   beforeAll(async () => {
      f = await persistedResultFixture(STORAGE_MODEL, {
         destinations: { lake: new DuckDBConnection("lake", ":memory:") },
         noBuildPlan: true,
      });
      noDestination = await persistedResultFixture(STORAGE_MODEL, {
         noBuildPlan: true,
      });
      buildSourceID = f.buildSourceIDOf("orders__by_region__view");
   });
   afterAll(async () => {
      await f?.cleanup();
      await noDestination?.cleanup();
   });

   it("keeps only the view-kind storage bindings of a view this model sees", () => {
      const mine = storedViewBinding(buildSourceID, { schema });
      const pinned = pinnedOf([
         mine,
         storedViewBinding("other@file:///x.malloy"),
         { ...mine, kind: "persist" },
         { ...mine, tier: "database" },
      ]);
      expect(f.views.viewBindings(pinned, "bound")).toEqual([mine]);
      expect(f.views.viewBindings(pinned, "fresh")).toEqual([mine]);
   });

   it("reads a fresh binding through virtual() on its destination", () => {
      const binding = storedViewBinding(buildSourceID, {
         schema,
         freshAsOf: "2026-01-01T00:00:00.000Z",
      });
      const table = f.views.tableOf(
         { buildSourceID, storage: "lake" },
         pinnedOf([binding]),
         undefined,
      );
      expect(table.tier).toBe("storage");
      expect([table.built, table.stale]).toEqual([true, false]);
      expect(table.dataAsOf).toBe("2026-01-01T00:00:00.000Z");
      expect(table.quotedPath).toBe('"main"."stored_view_t"');
      expect([...(table.virtualMap?.values() ?? [])]).toEqual([
         new Map([["vh_1", '"main"."stored_view_t"']]),
      ]);
   });

   // P3-06..09: a table a Snowflake session stored with its aliases folded
   // upper-case is matched by a unique case fold, as the served copy matches.
   it("P3-08: answers from a stored table whose columns a warehouse upper-cased", async () => {
      const binding = storedViewBinding(buildSourceID, {
         schema: [
            { name: "REGION", type: "VARCHAR" },
            { name: "TOTAL", type: "DOUBLE" },
         ],
      });
      const pinned = pinnedOf([binding]);
      const query = "run: orders -> by_region";
      const answer = await f.views.attempt({
         queryString: query,
         givens: undefined,
         authorRunnable: f.materializer.loadQuery(query),
         pinned,
         buildManifest: pinned.compilerManifest,
      });
      expect(answer.binding).toBe(binding);
   });

   it("reports a binding stale past its window, and any binding not served with no destination to read it", () => {
      const old = storedViewBinding(buildSourceID, {
         schema,
         freshAsOf: new Date(Date.now() - 3_600_000).toISOString(),
         freshnessWindowSeconds: 60,
         freshnessFallback: "live",
      });
      expect(
         f.views.tableOf(
            { buildSourceID, storage: "lake" },
            pinnedOf([old]),
            undefined,
         ),
      ).toMatchObject({ built: true, stale: true });
      expect(
         noDestination.views.tableOf(
            { buildSourceID, storage: "lake" },
            pinnedOf([storedViewBinding(buildSourceID, { schema })]),
            undefined,
         ),
      ).toEqual({
         tier: "storage",
         built: true,
         stale: false,
         storageNotServed: true,
      });
      expect(
         f.views.tableOf(
            { buildSourceID, storage: "lake" },
            pinnedOf(),
            undefined,
         ),
      ).toEqual({ tier: "storage", built: false, stale: false });
   });

   it("answers an exact call from the stored table, its SQL reading it through the destination", async () => {
      const binding = storedViewBinding(buildSourceID, { schema });
      const pinned = pinnedOf([binding]);
      const query = "run: orders -> by_region";
      const answer = await f.views.attempt({
         queryString: query,
         givens: undefined,
         authorRunnable: f.materializer.loadQuery(query),
         pinned,
         buildManifest: pinned.compilerManifest,
      });
      expect(answer.binding).toBe(binding);
      expect(answer.table).toEqual({
         tier: "storage",
         built: true,
         stale: false,
      });
      const { sql } = await answer.runnable.getPreparedResult({
         virtualMap: answer.virtualMap,
      } as never);
      expect(sql).toContain('"main"."stored_view_t"');
   });

   it("declines a stored column that no cast reads as live's type", async () => {
      const binding = storedViewBinding(buildSourceID, {
         schema: [
            { name: "region", type: "VARCHAR" },
            { name: "total", type: "VARCHAR" },
         ],
         virtualHandle: "vh_text",
      });
      const pinned = pinnedOf([binding]);
      const query = "run: orders -> by_region";
      const decline = await declineOf(
         f.views.attempt({
            queryString: query,
            givens: undefined,
            authorRunnable: f.materializer.loadQuery(query),
            pinned,
            buildManifest: pinned.compilerManifest,
         }),
      );
      expect(decline.callerDetail).toBe(
         "the stored result cannot answer `by_region` with live's columns: the stored `total` (VARCHAR) cannot be read as live's number",
      );
   });
});

/**
 * P4-03/04/05 (F-14): a host may instruct a view with no `storage=` into a
 * storage destination (a database origin whose destination the control plane
 * chose), and the run's manifest binds it as a storage table. The binding
 * decides the tier, not the annotation: the view is read from it exactly as a
 * `storage=` view's is, rather than looked for in the database manifest.
 */
describe("PersistedViewServe (a host-chosen destination, no storage=)", () => {
   let f: PersistedResultFixture;
   let buildSourceID: string;
   const schema = [
      { name: "region", type: "VARCHAR" },
      { name: "total", type: "DOUBLE" },
   ];
   beforeAll(async () => {
      f = await persistedResultFixture(MODEL, {
         destinations: { lake: new DuckDBConnection("lake_f14", ":memory:") },
         noBuildPlan: true,
      });
      buildSourceID = f.buildSourceIDOf("orders__by_region__view");
   });
   afterAll(async () => {
      await f?.cleanup();
   });

   it("P4-04: reads a fresh storage binding through virtual() on its destination", () => {
      const table = f.views.tableOf(
         { buildSourceID },
         pinnedOf([storedViewBinding(buildSourceID, { schema })]),
         undefined,
      );
      expect(table.tier).toBe("storage");
      expect([table.built, table.stale]).toEqual([true, false]);
      expect(table.quotedPath).toBe('"main"."stored_view_t"');
   });

   it("P4-04: answers an exact call from the storage binding", async () => {
      const binding = storedViewBinding(buildSourceID, { schema });
      const pinned = pinnedOf([binding]);
      const query = "run: orders -> by_region";
      const answer = await f.views.attempt({
         queryString: query,
         givens: undefined,
         authorRunnable: f.materializer.loadQuery(query),
         pinned,
         buildManifest: pinned.compilerManifest,
      });
      expect(answer.binding).toBe(binding);
      expect(answer.table.tier).toBe("storage");
      const { sql } = await answer.runnable.getPreparedResult({
         virtualMap: answer.virtualMap,
      } as never);
      expect(sql).toContain('"main"."stored_view_t"');
   });

   it("still reads the database manifest when no storage binding stores it", () => {
      expect(
         f.views.tableOf({ buildSourceID }, pinnedOf(), undefined).tier,
      ).toBe("database");
   });
});
