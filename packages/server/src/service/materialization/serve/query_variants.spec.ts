// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   persistedResultFixture,
   type PersistedResultFixture,
   pinnedOf,
   storedViewBinding,
} from "../../../test_helpers/persisted_result_fixture";
import { LegDecline } from "../domain/leg_decline";

/**
 * The persisted named-query leg against a real compile: which texts call a
 * persisted query (by the name its file gives it), the table it reads through
 * the view leg's lookup, and each way an attempt declines or answers from the
 * stored result. No query runs; an answer is checked by the SQL it compiles.
 */

const MODEL = `##! experimental { persistence }
source: orders is duckdb.sql("""
  select * from (values (1, 'east', 10.0), (2, 'west', 20.0)) as t(id, region, amount)
""") extend {
  measure: total is amount.sum()
}

#@ persist
query: q_region is orders -> { group_by: region; aggregate: total; order_by: region }

#@ persist
query: q_grand is orders -> { aggregate: total }

#@ persist
query: q_inline is duckdb.sql("select 'a' as k, 1 as v") -> { group_by: k; aggregate: s is v.sum() }

query: q_plain is orders -> { group_by: region }
`;

const STORED = "stored_q_t";

async function declineOf(promise: Promise<unknown>): Promise<LegDecline> {
   const err = await promise.then(
      () => undefined,
      (e: unknown) => e,
   );
   expect(err).toBeInstanceOf(LegDecline);
   return err as LegDecline;
}

describe("PersistedQueryServe (colocated)", () => {
   let f: PersistedResultFixture;
   let tableId: string;
   let built: Record<string, { tableName: string }>;
   beforeAll(async () => {
      f = await persistedResultFixture(MODEL);
      tableId = f.tableIdOf("q_region__query");
      built = { [tableId]: { tableName: STORED } };
   });
   afterAll(async () => {
      await f?.cleanup();
   });

   function attempt(
      queryString: string,
      entries: Record<string, { tableName: string }>,
   ) {
      const pinned = pinnedOf([], entries);
      return f.queries.attempt({
         queryString,
         givens: undefined,
         authorRunnable: f.materializer.loadQuery(queryString),
         pinned,
         buildManifest: pinned.compilerManifest,
      });
   }

   it("plans the model's persisted queries, and only those", () => {
      expect(f.queries.hasQueries()).toBe(true);
      expect(
         f.queries
            .plans()
            .map((p) => p.queryName)
            .sort(),
      ).toEqual(["q_grand", "q_inline", "q_region"]);
      const inline = f.queries.plans().find((p) => p.queryName === "q_inline");
      expect(inline?.sourceID).toBeUndefined();
   });

   it("names the query a call reads, and nothing for any other text", () => {
      expect(f.queries.calledQuery("run: q_region")?.plan.queryName).toBe(
         "q_region",
      );
      expect(
         f.queries.calledQuery("run: q_region -> { select: region }")?.call
            .laterStages,
      ).toBe(1);
      expect(f.queries.calledQuery("run: q_plain")).toBeUndefined();
      // A source of the model is not a query.
      expect(
         f.queries.calledQuery("run: orders -> { group_by: region }"),
      ).toBeUndefined();
      expect(f.queries.calledQuery("run: nothing")).toBeUndefined();
   });

   it("reports the table a call would read, as a query's", () => {
      expect(
         f.queries.reportedTable("run: q_region", pinnedOf([], built)),
      ).toEqual({
         tableId,
         tier: "colocated",
         built: true,
         stale: false,
         kind: "query",
      });
      expect(
         f.queries.reportedTable("run: q_plain", pinnedOf([], built)),
      ).toBeUndefined();
   });

   it("answers `run: q` from the stored table, its SQL reading it", async () => {
      const answer = await attempt("run: q_region", built);
      expect(answer.table).toEqual({
         tableId,
         tier: "colocated",
         built: true,
         stale: false,
      });
      expect(answer.graftScope.cacheScope).toStartWith("persisted_query\u0000");
      expect(answer.graftScope.cacheScope).toEndWith("\u0000orders");
      const { sql } = await answer.runnable.getPreparedResult({
         buildManifest: pinnedOf([], built).compilerManifest,
      } as never);
      expect(sql).toContain(STORED);
   });

   it("answers a query that goes on past `q` from the stored table", async () => {
      const answer = await attempt(
         "run: q_region -> { select: region }",
         built,
      );
      expect(answer.table.built).toBe(true);
   });

   it("answers a query whose run target is written inline, under no run target", async () => {
      const inlineId = f.tableIdOf("q_inline__query");
      const answer = await attempt("run: q_inline", {
         [inlineId]: { tableName: "stored_inline_t" },
      });
      expect(answer.graftScope.cacheScope).toEndWith("\u0000");
   });

   it("declines a refinement of the query", async () => {
      const decline = await declineOf(
         attempt("run: q_region + { limit: 1 }", built),
      );
      expect(decline.code).toBe("query_missing_field");
      expect(decline.callerDetail).toBe(
         "the query refines `q_region` (`+ { … }`), and the stored result holds only the query as written",
      );
   });

   it("declines a text that names the run target beside the call", async () => {
      const decline = await declineOf(
         attempt(
            "run: q_region -> { extend: { join_one: o is orders on o.region = region } select: region }",
            built,
         ),
      );
      expect(decline.callerDetail).toContain("names `orders` beside the call");
   });

   it("declines a text that calls no persisted query, with no detail", async () => {
      const decline = await declineOf(attempt("run: q_plain", built));
      expect(decline.callerDetail).toBeUndefined();
   });

   it("declines an unbuilt query as not built, and one missing from the query's manifest as stale", async () => {
      expect((await declineOf(attempt("run: q_region", {}))).code).toBe(
         "query_not_built",
      );
      const pinned = pinnedOf([], built);
      const decline = await declineOf(
         f.queries.attempt({
            queryString: "run: q_region",
            givens: undefined,
            authorRunnable: f.materializer.loadQuery("run: q_region"),
            pinned,
            buildManifest: { entries: {}, strict: false },
         }),
      );
      expect(decline.code).toBe("query_stale");
   });

   it("declines a result whose stored rows cannot be regrouped into live's", async () => {
      const grandId = f.tableIdOf("q_grand__query");
      const decline = await declineOf(
         attempt("run: q_grand", { [grandId]: { tableName: "stored_grand" } }),
      );
      expect(decline.callerDetail).toBe(
         "the stored result cannot answer `q_grand` with live's columns: it groups by no dimension, so an empty result would come back as one row",
      );
   });
});

const STORAGE_MODEL = `##! experimental { persistence }
source: orders is duckdb.sql("""
  select * from (values (1, 'east', 10.0), (2, 'west', 20.0)) as t(id, region, amount)
""") extend {
  measure: total is amount.sum()
}

#@ persist storage="lake"
query: q_region is orders -> { group_by: region; aggregate: total; order_by: region }
`;

describe("PersistedQueryServe (storage=)", () => {
   let f: PersistedResultFixture;
   let buildSourceID: string;
   beforeAll(async () => {
      f = await persistedResultFixture(STORAGE_MODEL, {
         destinations: { lake: new DuckDBConnection("lake", ":memory:") },
         noBuildPlan: true,
      });
      buildSourceID = f.buildSourceIDOf("q_region__query");
   });
   afterAll(async () => {
      await f?.cleanup();
   });

   it("keeps only the storage bindings of a query this model sees", () => {
      const mine = storedViewBinding(buildSourceID);
      const pinned = pinnedOf([
         mine,
         storedViewBinding("other@file:///x.malloy"),
         { ...mine, tier: "colocated" },
      ]);
      expect(f.queries.queryBindings(pinned, "bound")).toEqual([mine]);
      expect(f.queries.queryBindings(pinned, "fresh")).toEqual([mine]);
   });

   it("answers `run: q` from the stored table through the destination", async () => {
      const binding = storedViewBinding(buildSourceID, {
         schema: [
            { name: "region", type: "VARCHAR" },
            { name: "total", type: "DOUBLE" },
         ],
      });
      const pinned = pinnedOf([binding]);
      const answer = await f.queries.attempt({
         queryString: "run: q_region",
         givens: undefined,
         authorRunnable: f.materializer.loadQuery("run: q_region"),
         pinned,
         buildManifest: pinned.compilerManifest,
      });
      expect(answer.binding).toBe(binding);
      const { sql } = await answer.runnable.getPreparedResult({
         virtualMap: answer.virtualMap,
      } as never);
      expect(sql).toContain('"main"."stored_view_t"');
   });

   it("declines a stored column no cast reads as live's type", async () => {
      const binding = storedViewBinding(buildSourceID, {
         schema: [
            { name: "region", type: "VARCHAR" },
            { name: "total", type: "VARCHAR" },
         ],
         virtualHandle: "vh_text",
      });
      const pinned = pinnedOf([binding]);
      const decline = await declineOf(
         f.queries.attempt({
            queryString: "run: q_region",
            givens: undefined,
            authorRunnable: f.materializer.loadQuery("run: q_region"),
            pinned,
            buildManifest: pinned.compilerManifest,
         }),
      );
      expect(decline.callerDetail).toBe(
         "the stored result cannot answer `q_region` with live's columns: the stored `total` (VARCHAR) cannot be read as live's number",
      );
   });
});

/**
 * P4-05 (F-14): a persisted query with no `storage=` that a host instructed
 * into a storage destination is read from its storage binding, which decides
 * the tier, as a `storage=` query's is.
 */
describe("PersistedQueryServe (a host-chosen destination, no storage=)", () => {
   let f: PersistedResultFixture;
   let buildSourceID: string;
   beforeAll(async () => {
      f = await persistedResultFixture(
         STORAGE_MODEL.replace(' storage="lake"', ""),
         {
            destinations: {
               lake: new DuckDBConnection("lake_f14q", ":memory:"),
            },
            noBuildPlan: true,
         },
      );
      buildSourceID = f.buildSourceIDOf("q_region__query");
   });
   afterAll(async () => {
      await f?.cleanup();
   });

   it("P4-05: answers `run: q` from the storage binding through the destination", async () => {
      const binding = storedViewBinding(buildSourceID, {
         schema: [
            { name: "region", type: "VARCHAR" },
            { name: "total", type: "DOUBLE" },
         ],
      });
      const pinned = pinnedOf([binding]);
      const answer = await f.queries.attempt({
         queryString: "run: q_region",
         givens: undefined,
         authorRunnable: f.materializer.loadQuery("run: q_region"),
         pinned,
         buildManifest: pinned.compilerManifest,
      });
      expect(answer.binding).toBe(binding);
      const { sql } = await answer.runnable.getPreparedResult({
         virtualMap: answer.virtualMap,
      } as never);
      expect(sql).toContain('"main"."stored_view_t"');
   });
});
