// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { renderHook } from "@testing-library/react";
import { afterAll, describe, expect, it, setDefaultTimeout } from "bun:test";
import { drillClickRelay } from "../drill/drillClickRelay";
import type { DrillRowsRequest } from "../drill/resolveDrill";
import { useDrill } from "../drill/useDrill";
import {
   rowsQuery,
   rowsRefusal,
   rowsRequestFor,
   rowsTitleParts,
} from "./RowsDialog";

/**
 * "Show the rows" end to end, with nothing standing in for Malloy: a tile's
 * query runs on DuckDB, the real renderer draws it, every body cell is
 * clicked, and the rows query each click produces is compiled and run.
 *
 * The oracle is the tile's own `sale_count`: the rows behind a row number
 * exactly that many, so a drill that drops a dimension (too many rows), spells
 * a value wrong (none), or fails to compile is caught by the same assertion.
 */

setDefaultTimeout(60_000);

const MODEL = `
##! experimental.givens
source: quarterly_sales is duckdb.sql("""
  select * from (values
    (DATE '2026-01-15', TIMESTAMP '2026-01-15 10:30:15', 'West', 'Ben & Jerry''s', 10.0, true),
    (DATE '2026-01-15', TIMESTAMP '2026-01-15 10:30:15', 'West', 'Ben & Jerry''s', 4.0, true),
    (DATE '2026-02-03', TIMESTAMP '2026-02-03 23:59:00', 'East', 'O"Brien', 20.0, false),
    (DATE '2026-04-10', TIMESTAMP '2026-04-10 01:00:00', 'West', NULL, 5.0, NULL),
    (DATE '2026-01-20', TIMESTAMP '2026-01-20 12:00:00', NULL, 'back\\slash', 7.0, false),
    (DATE '2026-12-31', TIMESTAMP '2026-12-31 20:15:00', 'East', 'Plain', 3.5, true)
  ) t(sale_date, sold_at, region, product, amount, is_online)
""") extend {
  dimension: sales_quarter is sale_date.quarter
  measure: total_revenue is amount.sum()
  measure: sale_count is count()
}
given: REGION :: filter<string> is f''
source: sales_tiles is quarterly_sales extend {
  view: quarter_region_table is {
    where: region ~ $REGION
    group_by: sales_quarter, region
    aggregate: total_revenue, sale_count
    order_by: sales_quarter, total_revenue desc
  }
  view: by_region is {
    group_by: region
    aggregate: sale_count
  }
  view: region_hours is {
    group_by: region
    aggregate: sale_count
    nest: by_hour is { group_by: sold_hour is sold_at.hour; aggregate: sale_count }
  }
  view: truncations is {
    group_by:
      y is sold_at.year, q is sold_at.quarter, mo is sold_at.month,
      wk is sold_at.week, d is sold_at.day, h is sold_at.hour,
      mi is sold_at.minute, s is sold_at, sd is sale_date,
      sd_month is sale_date.month, sd_week is sale_date.week
    aggregate: sale_count
  }
  view: tokyo_hours is {
    timezone: 'Asia/Tokyo'
    group_by: tokyo_day is sold_at.day, tokyo_hour is sold_at.hour
    aggregate: sale_count
  }
  view: products is {
    group_by: product, is_online, amount
    aggregate: sale_count
  }
}
`;

type Row = Record<string, unknown>;

const { SingleConnectionRuntime, API } = await import("@malloydata/malloy");
const { DuckDBConnection } = await import("@malloydata/db-duckdb");
// Past any `mock.module("@malloydata/render")` another spec in the run has
// registered: the query makes this a module of its own.
const { MalloyRenderer } = (await import(
   `${Bun.resolveSync("@malloydata/render", import.meta.dir)}?real`
)) as typeof import("@malloydata/render");
const connection = new DuckDBConnection("duckdb", ":memory:");
const model = new SingleConnectionRuntime({ connection } as never).loadModel(
   MODEL,
);
afterAll(() => connection.close());

async function run(query: string) {
   return model.loadQuery(query).run();
}

/**
 * Every body cell of `view` clicked, through the same relay and `useDrill`
 * the dashboard uses, with the tile expression the dashboard sets as context.
 */
async function clickEveryCell(view: string) {
   const tile = `sales_tiles -> ${view}`;
   const result = await run(`run: ${tile}`);
   const requests: DrillRowsRequest[] = [];
   const { result: hook } = renderHook(() =>
      useDrill({ onRows: (request) => requests.push(request) }),
   );
   const relay = drillClickRelay((payload) =>
      hook.current.drill.onClick({ ...payload, context: tile }),
   );
   const viz = new MalloyRenderer({
      onClick: relay.onClick as never,
      onDrill: relay.onDrill,
      // happy-dom has no layout, so a virtualized table renders no body rows.
      tableConfig: { enableDrill: true, disableVirtualization: true },
   }).createViz();
   viz.setResult(API.util.wrapResult(result));
   const element = document.createElement("div");
   document.body.appendChild(element);
   viz.render(element);
   await new Promise((resolve) => setTimeout(resolve, 300));
   const cells = Array.from(
      element.querySelectorAll<HTMLElement>(".cell-content"),
   );
   for (const cell of cells) {
      cell.click();
      await Promise.resolve();
   }
   viz.remove();
   element.remove();
   return { rows: result.data.toObject() as Row[], requests };
}

/**
 * What each clicked row asks for, one entry per distinct row in table order:
 * the rows query, or the dialog's refusal when there is none.
 */
function perRow(requests: DrillRowsRequest[]) {
   const seen = new Map<string, { query?: string; refusal?: string }>();
   for (const request of requests) {
      expect(request.drillClauses).toBeDefined();
      const key = JSON.stringify(request.drillClauses);
      if (seen.has(key)) continue;
      const rows = rowsRequestFor(request)!;
      const query = rowsQuery(rows);
      seen.set(key, query ? { query } : { refusal: rowsRefusal(rows) });
   }
   return [...seen.values()];
}

/**
 * Each row's outcome against the tile's own count: a query returns exactly
 * that many rows, and a row with a null dimension is refused by name.
 */
async function expectRowsMatch(
   outcomes: { query?: string; refusal?: string }[],
   expected: { count: unknown; nulls: string[] }[],
) {
   expect(outcomes).toHaveLength(expected.length);
   for (const [index, outcome] of outcomes.entries()) {
      const { count, nulls } = expected[index];
      if (nulls.length > 0) {
         expect(outcome.query).toBeUndefined();
         for (const name of nulls) expect(outcome.refusal).toContain(name);
         continue;
      }
      expect(outcome.query).toBeDefined();
      const rows = (await run(outcome.query!)).data.toObject();
      expect({ query: outcome.query, count: rows.length }).toEqual({
         query: outcome.query,
         count: count as number,
      });
   }
}

const nullsOf = (row: Row, dims: string[]) =>
   dims.filter((dim) => row[dim] === null);

describe("the rows behind a clicked table row, run on DuckDB", () => {
   it("drills a two-dimension table by both dimensions", async () => {
      const { rows, requests } = await clickEveryCell("quarter_region_table");
      const outcomes = perRow(requests);
      // The quarter as its first day: `drill:` refuses `@2026-Q1`.
      expect(outcomes[0].query).toBe(
         "run: sales_tiles -> { drill: quarter_region_table.sales_quarter = @2026-01-01, quarter_region_table.region = 'East'; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.map((r) => ({
            count: r.sale_count,
            nulls: nullsOf(r, ["sales_quarter", "region"]),
         })),
      );
      // Two dimension cells per row, and no request from a measure or header.
      expect(requests).toHaveLength(rows.length * 2);
      expect(rowsTitleParts(rowsRequestFor(requests[0])!)).toEqual([
         { field: "sales_quarter", label: "2026-Q1" },
         { field: "region", label: "East" },
      ]);
   });

   it("still drills a one-dimension table by its one value", async () => {
      const { rows, requests } = await clickEveryCell("by_region");
      const outcomes = perRow(requests);
      expect(outcomes[0].query).toBe(
         "run: sales_tiles -> { drill: by_region.region = 'West'; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.map((r) => ({
            count: r.sale_count,
            nulls: nullsOf(r, ["region"]),
         })),
      );
   });

   it("drills a nested row by its own dimension and every enclosing one", async () => {
      const { rows, requests } = await clickEveryCell("region_hours");
      const outcomes = perRow(requests);
      // The hour from the value's own digits: the renderer's text says 00.
      expect(outcomes.map((o) => o.query)).toContain(
         "run: sales_tiles -> { drill: region_hours.region = 'East', region_hours.by_hour.sold_hour = @2026-02-03 23:00:00; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.flatMap((r) => [
            { count: r.sale_count, nulls: nullsOf(r, ["region"]) },
            ...(r.by_hour as Row[]).map((h) => ({
               count: h.sale_count,
               nulls: [...nullsOf(r, ["region"]), ...nullsOf(h, ["sold_hour"])],
            })),
         ]),
      );
   });

   it("writes every date and timestamp truncation so its rows are found", async () => {
      const { rows, requests } = await clickEveryCell("truncations");
      const outcomes = perRow(requests);
      expect(outcomes.map((o) => o.query)).toContain(
         "run: sales_tiles -> { drill: truncations.y = @2026-01-01 00:00:00, truncations.q = @2026-01-01 00:00:00, truncations.mo = @2026-01-01 00:00:00, truncations.wk = @2026-01-11 00:00:00, truncations.d = @2026-01-15 00:00:00, truncations.h = @2026-01-15 10:00:00, truncations.mi = @2026-01-15 10:30:00, truncations.s = @2026-01-15 10:30:15, truncations.sd = @2026-01-15, truncations.sd_month = @2026-01-01, truncations.sd_week = @2026-01-11; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.map((r) => ({ count: r.sale_count, nulls: [] })),
      );
      const titled = requests.find((r) =>
         JSON.stringify(r.drillClauses).includes("2026-01-15T10:30:15"),
      )!;
      expect(rowsTitleParts(rowsRequestFor(titled)!)).toEqual([
         { field: "y", label: "2026" },
         { field: "q", label: "2026-Q1" },
         { field: "mo", label: "2026-01" },
         { field: "wk", label: "2026-01-11" },
         { field: "d", label: "2026-01-15" },
         { field: "h", label: "2026-01-15 10" },
         { field: "mi", label: "2026-01-15 10:30" },
         { field: "s", label: "2026-01-15 10:30:15" },
         { field: "sd", label: "2026-01-15" },
         { field: "sd_month", label: "2026-01" },
         { field: "sd_week", label: "2026-01-11" },
      ]);
   });

   it("drills a tile with its own timezone in that zone", async () => {
      const { rows, requests } = await clickEveryCell("tokyo_hours");
      const outcomes = perRow(requests);
      // 2026-02-03 23:59 UTC is 08:59 the next morning in Tokyo.
      expect(outcomes.map((o) => o.query)).toContain(
         "run: sales_tiles -> { timezone: 'Asia/Tokyo'; drill: tokyo_hours.tokyo_day = @2026-02-04 00:00:00, tokyo_hours.tokyo_hour = @2026-02-04 08:00:00; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.map((r) => ({ count: r.sale_count, nulls: [] })),
      );
   });

   it("quotes strings, and writes numbers and booleans", async () => {
      const { rows, requests } = await clickEveryCell("products");
      const outcomes = perRow(requests);
      const queries = outcomes.map((o) => o.query ?? "");
      expect(queries).toContain(
         "run: sales_tiles -> { drill: products.product = 'Ben & Jerry\\'s', products.is_online = true, products.amount = 10; select: *; limit: 200 }",
      );
      expect(queries).toContain(
         "run: sales_tiles -> { drill: products.product = 'O\"Brien', products.is_online = false, products.amount = 20; select: *; limit: 200 }",
      );
      expect(queries).toContain(
         "run: sales_tiles -> { drill: products.product = 'back\\\\slash', products.is_online = false, products.amount = 7; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.map((r) => ({
            count: r.sale_count,
            nulls: nullsOf(r, ["product", "is_online", "amount"]),
         })),
      );
   });
});
