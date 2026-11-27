// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   persistedResultFixture,
   type PersistedResultFixture,
   pinnedOf,
} from "../../../test_helpers/persisted_result_fixture";
import { LegDecline } from "../domain/leg_decline";

/**
 * #2048: the rollup leg over a path grain against a real compile: the
 * variant declares the join as a record of its stored path columns, so a
 * query that writes the path compiles against it and reads the table; each
 * way a query on the path declines keeps today's code. No query runs; an
 * answer is checked by the SQL it compiles (the R4 spec compares values).
 */

const MODEL = `source: accounts is duckdb.sql("""select * from (values (1,'EU','SE')) t(account_id, region, country)""") extend {
  primary_key: account_id
  #(doc) The sales region.
  dimension: sales_region is region
}
source: items is duckdb.sql("""select 1 as item_id, 10 as sub_id, 'A' as sku""") extend { primary_key: item_id }
source: subs is duckdb.sql("""select * from (values (10,1,DATE '2026-01-01',100.0)) t(sub_id, account_id, period, amount)""") extend {
  primary_key: sub_id
  join_one: account is accounts with account_id
  join_many: items on items.sub_id = sub_id
  dimension: period_month is period.month
  #(rollup) grain=["account.region", "account.sales_region", "items.sku", "period_month"]
  measure: mrr is amount.sum()
}
`;

const TABLE = "stored_path_rollup_t";

async function declined(promise: Promise<unknown>) {
   const err = await promise.then(
      () => undefined,
      (e: unknown) => e,
   );
   expect(err).toBeInstanceOf(LegDecline);
   const e = err as LegDecline;
   return { code: e.code, detail: e.callerDetail };
}

describe("RollupVariantServe over a path grain (#2048)", () => {
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

   function attempt(queryString: string) {
      return f.rollups.attempt({
         queryString,
         givens: undefined,
         authorRunnable: f.materializer.loadQuery(queryString),
         pinned: pinnedOf([], { [tableId]: { tableName: TABLE } } as never),
      });
   }

   it("answers a query grouped by the path from the table, reading the stored column", async () => {
      const answer = await attempt(
         "run: subs -> { group_by: items.sku, account.region; aggregate: mrr }",
      );
      const { sql } = await answer.runnable.getPreparedResult({
         buildManifest: pinnedOf([], { [tableId]: { tableName: TABLE } })
            .compilerManifest,
      } as never);
      expect(sql).toContain(TABLE);
      expect(sql).toContain("account__region");
      expect(sql).not.toContain("JOIN");
   });

   for (const [label, query, code, detail] of [
      [
         "another field of the join",
         "run: subs -> { group_by: items.sku, account.country; aggregate: mrr }",
         "rollup_grain_too_coarse",
         "the query reads `account.country`, which the rollup's grain does not keep",
      ],
      [
         "an aggregate through the join",
         "run: subs -> { group_by: items.sku, account.region; aggregate: c is account.count() }",
         "rollup_count_semantics",
         "`c` aggregates the rollup's rows, not a stored measure",
      ],
      [
         "a query that drops the join_many path",
         "run: subs -> { group_by: account.region; aggregate: mrr }",
         "rollup_grain_too_coarse",
         "the query does not group by or fix `items.sku`, a fan-out dimension of the rollup's grain, so its measures would be counted once per value",
      ],
      [
         "a filter on the join_many path without grouping by it",
         "run: subs -> { where: items.sku = 'A'; group_by: account.region; aggregate: mrr }",
         "rollup_grain_too_coarse",
         "the query does not group by or fix `items.sku`, a fan-out dimension of the rollup's grain, so its measures would be counted once per value",
      ],
      [
         "an annotated joined field returned as a column",
         "run: subs -> { group_by: items.sku, account.sales_region; aggregate: mrr }",
         "rollup_missing_field",
         "the query returns `account.sales_region`, whose annotations the rollup's record of stored columns cannot carry; a field under a name of its own (`group_by: r is account.sales_region`), which carries none live either, is served from the rollup",
      ],
      [
         "a query that reads nothing through the join_one the grain reads",
         "run: subs -> { group_by: items.sku; aggregate: mrr }",
         "rollup_count_semantics",
         "the query reads nothing through `account`, a join_one the rollup's grain reads (`account.region`, `account.sales_region`); the rollup was built over that join, so a key it repeats would inflate a coarser answer that live, not joining it, does not",
      ],
   ] as const) {
      it(`declines ${label} with today's code`, async () => {
         expect(await declined(attempt(query))).toEqual({ code, detail });
      });
   }
});
