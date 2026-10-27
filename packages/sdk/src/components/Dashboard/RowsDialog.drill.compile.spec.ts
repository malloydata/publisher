// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, fireEvent, render, screen } from "@testing-library/react";
import { createElement, Fragment } from "react";
import { afterAll, describe, expect, it, setDefaultTimeout } from "bun:test";
import {
   drillClickRelay,
   reportRowsWithoutFilters,
} from "../drill/drillClickRelay";
import type { DrillField, DrillRowsRequest } from "../drill/resolveDrill";
import {
   useDrill,
   type UseDrillOptions,
   type UseDrillResult,
} from "../drill/useDrill";
import {
   rowsQuery,
   ROWS_REFUSAL,
   rowsRequestFor,
   rowsTitleParts,
} from "./RowsDialog";
import { withPreamble } from "./textSource";
import { correctDrillMetadata } from "../../../../server/src/service/drill_metadata";

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
  join_one: owners is duckdb.sql("""
    select * from (values ('West', 'Wanda'), ('East', 'Ed')) t(region_key, owner)
  """) on region = owners.region_key
  view: by_owner is {
    group_by: owners.owner
    aggregate: sale_count
  }
  view: two_stage is {
    group_by: region
    aggregate: sale_count
  } -> { select: region, sale_count }
  join_one: targets is duckdb.sql("""
    select * from (values
      ('West', 60.0, TIMESTAMP '2025-03-01 00:00:00'),
      ('East', 1.0, TIMESTAMP '2024-07-01 00:00:00')
    ) t(region_key, amount, sold_at)
  """) extend {
    dimension: target_big is amount > 50
    dimension: target_year is sold_at.year
  } on region = targets.region_key
  view: by_target is { group_by: targets.target_big, targets.target_year; aggregate: sale_count }
  view: by_month is { group_by: sold_at.month; aggregate: sale_count }
  view: by_date is { group_by: sale_date; aggregate: sale_count }
  view: staged_nest is {
    group_by: region
    aggregate: sale_count
    nest: online is { group_by: is_online; aggregate: sale_count } -> { select: is_online }
  }
}
query: online_staged is sales_tiles -> {
  where: is_online = true
  group_by: region
  aggregate: sale_count
} -> { group_by: region; aggregate: n is count() }
source: edge_cases is duckdb.sql("""
  select * from (values
    (9007199254740993::BIGINT, TIMESTAMP '2026-01-15 10:30:15.250', 'East', 'line one
line two', '00000000-0000-0000-0000-000000000001'::UUID),
    (9007199254740992::BIGINT, TIMESTAMP '2026-01-15 10:30:15.750', 'West', 'plain', '00000000-0000-0000-0000-000000000002'::UUID),
    (1::BIGINT, TIMESTAMP '2026-01-16 23:30:00', 'North', 'plain', '00000000-0000-0000-0000-000000000003'::UUID)
  ) t(big_id, seen_at, region, note, uid)
""") extend {
  dimension: coastal is region = 'East' or region = 'West'
  measure: sale_count is count()
  view: by_id is { group_by: big_id; aggregate: sale_count }
  view: by_at is { group_by: seen_at; aggregate: sale_count }
  view: by_coastal is { group_by: coastal; aggregate: sale_count }
  view: by_note is { group_by: note; aggregate: sale_count }
  view: by_uid is { group_by: uid, region; aggregate: sale_count }
  view: mixed_zone is {
    group_by: d is seen_at.day
    aggregate: sale_count
    nest: tokyo is { timezone: 'Asia/Tokyo'; group_by: h is seen_at.hour; aggregate: sale_count }
  }
}
source: \`edge sales\` is edge_cases
query: online_quarters is sales_tiles -> quarter_region_table + { where: is_online = true }
query: given_regions is sales_tiles -> by_region + { where: region ~ $REGION }
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

/** `useDrill` mounted with its menu, as a surface renders it. */
function mountDrill(options: UseDrillOptions): {
   current: UseDrillResult;
   unmount: () => void;
} {
   const ref = {} as { current: UseDrillResult; unmount: () => void };
   function Host() {
      ref.current = useDrill(options);
      return createElement(Fragment, null, ref.current.drillMenu);
   }
   ref.unmount = render(createElement(Host)).unmount;
   return ref;
}

/** A query's result as Publisher's server sends it: its drill metadata corrected. */
async function run(query: string) {
   const result = await model.loadQuery(query).run();
   correctDrillMetadata(result);
   return result;
}

/**
 * Every body cell of `tile` clicked, through the same relay and the same
 * per-tile binding the dashboard uses (`bindingFor`), run
 * after `preamble` as a text-source report runs it. Also returns the fields
 * the binding marks clickable, which is what the affordance paints.
 */
async function clickEveryCell(tile: string, preamble = "") {
   const result = await run(withPreamble(preamble, `run: ${tile}`));
   const requests: DrillRowsRequest[] = [];
   // Mounted with its menu: every click opens it, and the rows are what the
   // reader gets by picking "Show rows" there.
   const hook = mountDrill({ onRows: (request) => requests.push(request) });
   const binding = hook.current.bindingFor(tile, true);
   const relay = drillClickRelay(binding.onClick);
   const viz = new MalloyRenderer({
      onClick: relay.onClick as never,
      onDrill: relay.onDrill,
      // happy-dom has no layout, so a virtualized table renders no body rows.
      tableConfig: { enableDrill: true, disableVirtualization: true },
   }).createViz();
   viz.setResult(API.util.wrapResult(result));
   // As `RenderedResult` does for a surface with drill wired.
   reportRowsWithoutFilters(viz.getMetadata()?.getRootField() as never);
   const element = document.createElement("div");
   document.body.appendChild(element);
   viz.render(element);
   await new Promise((resolve) => setTimeout(resolve, 300));
   const marked = (viz.getMetadata()?.getAllFields() as unknown as DrillField[])
      .filter((field) => binding.canDrill(field))
      .map((field) => field.name);
   const cells = Array.from(
      element.querySelectorAll<HTMLElement>(".cell-content"),
   );
   for (const cell of cells) {
      await act(async () => {
         cell.click();
         await Promise.resolve();
      });
      const showRows = screen.queryByRole("menuitem", {
         name: /^Show rows \(/,
      });
      if (showRows) {
         act(() => {
            fireEvent.click(showRows);
         });
      }
   }
   hook.unmount();
   viz.remove();
   element.remove();
   return {
      rows: result.data.toObject() as Row[],
      requests,
      marked: [...new Set(marked)],
      tile,
   };
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
      seen.set(key, query ? { query } : { refusal: ROWS_REFUSAL });
   }
   return [...seen.values()];
}

/**
 * Each row's outcome against the tile's own count: its query returns exactly
 * that many rows, a row holding a null included (its filter is `is null`).
 */
async function expectRowsMatch(
   outcomes: { query?: string; refusal?: string }[],
   counts: unknown[],
) {
   expect(outcomes).toHaveLength(counts.length);
   for (const [index, outcome] of outcomes.entries()) {
      expect(outcome.query).toBeDefined();
      const rows = (await run(outcome.query!)).data.toObject();
      expect({ query: outcome.query, count: rows.length }).toEqual({
         query: outcome.query,
         count: counts[index] as number,
      });
   }
}

/** The title's field and value pairs, without the expressions they match by. */
const titleText = (request: Parameters<typeof rowsTitleParts>[0]) =>
   rowsTitleParts(request).map(({ field, label }) => ({ field, label }));

describe("the rows behind a clicked table row, run on DuckDB", () => {
   it("drills a two-dimension table by both dimensions", async () => {
      const { rows, requests } = await clickEveryCell(
         "sales_tiles -> quarter_region_table",
      );
      const outcomes = perRow(requests);
      // The view's own `where:` (a given) and each dimension's expression,
      // the quarter as its first day.
      expect(outcomes[0].query).toBe(
         "run: sales_tiles -> { where: region ~ $REGION, sale_date.quarter = @2026-01-01, region = 'East'; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.map((r) => r.sale_count),
      );
      // A row with a null region is the rows where it is null.
      expect(outcomes.some((o) => o.query?.includes("region is null"))).toBe(
         true,
      );
      // Two dimension cells per row, and no request from a measure or header.
      expect(requests).toHaveLength(rows.length * 2);
      expect(titleText(rowsRequestFor(requests[0])!)).toEqual([
         { field: "sales_quarter", label: "2026-Q1" },
         { field: "region", label: "East" },
      ]);
   });

   it("still drills a one-dimension table by its one value", async () => {
      const { rows, requests } = await clickEveryCell(
         "sales_tiles -> by_region",
      );
      const outcomes = perRow(requests);
      expect(outcomes[0].query).toBe(
         "run: sales_tiles -> { where: region = 'West'; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.map((r) => r.sale_count),
      );
   });

   it("drills a nested row by its own dimension and every enclosing one", async () => {
      const { rows, requests } = await clickEveryCell(
         "sales_tiles -> region_hours",
      );
      const outcomes = perRow(requests);
      // The hour from the value's own digits: the renderer's text says 00.
      expect(outcomes.map((o) => o.query)).toContain(
         "run: sales_tiles -> { where: region = 'East', sold_at.hour = @2026-02-03 23:00:00; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.flatMap((r) => [
            r.sale_count,
            ...(r.by_hour as Row[]).map((h) => h.sale_count),
         ]),
      );
   });

   it("writes every date and timestamp truncation so its rows are found", async () => {
      const { rows, requests } = await clickEveryCell(
         "sales_tiles -> truncations",
      );
      const outcomes = perRow(requests);
      expect(outcomes.map((o) => o.query)).toContain(
         "run: sales_tiles -> { where: sold_at.year = @2026-01-01 00:00:00, sold_at.quarter = @2026-01-01 00:00:00, sold_at.month = @2026-01-01 00:00:00, sold_at.week = @2026-01-11 00:00:00, sold_at.day = @2026-01-15 00:00:00, sold_at.hour = @2026-01-15 10:00:00, sold_at.minute = @2026-01-15 10:30:00, sold_at = @2026-01-15 10:30:15, sale_date = @2026-01-15, sale_date.month = @2026-01-01, sale_date.week = @2026-01-11; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.map((r) => r.sale_count),
      );
      const titled = requests.find((r) =>
         JSON.stringify(r.drillClauses).includes("2026-01-15T10:30:15"),
      )!;
      expect(titleText(rowsRequestFor(titled)!)).toEqual([
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
      const { rows, requests } = await clickEveryCell(
         "sales_tiles -> tokyo_hours",
      );
      const outcomes = perRow(requests);
      // 2026-02-03 23:59 UTC is 08:59 the next morning in Tokyo.
      expect(outcomes.map((o) => o.query)).toContain(
         "run: sales_tiles -> { timezone: 'Asia/Tokyo'; where: sold_at.day = @2026-02-04 00:00:00, sold_at.hour = @2026-02-04 08:00:00; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.map((r) => r.sale_count),
      );
   });

   it("quotes strings, and writes numbers and booleans", async () => {
      const { rows, requests } = await clickEveryCell(
         "sales_tiles -> products",
      );
      const outcomes = perRow(requests);
      const queries = outcomes.map((o) => o.query ?? "");
      expect(queries).toContain(
         "run: sales_tiles -> { where: product = 'Ben & Jerry\\'s', is_online = true, amount = 10; select: *; limit: 200 }",
      );
      expect(queries).toContain(
         "run: sales_tiles -> { where: product = 'O\"Brien', is_online = false, amount = 20; select: *; limit: 200 }",
      );
      expect(queries).toContain(
         "run: sales_tiles -> { where: product = 'back\\\\slash', is_online = false, amount = 7; select: *; limit: 200 }",
      );
      await expectRowsMatch(
         outcomes,
         rows.map((r) => r.sale_count),
      );
   });
});

/** Every row's query, one per line, for a shape asserted in any row. */
const queriesOf = (outcomes: { query?: string }[]) =>
   outcomes.map((o) => o.query ?? "").join("\n");

/** One request per distinct row, each run against the tile's own count. */
async function expectEveryRowMatches(tile: string, preamble = "") {
   const { rows, requests, marked } = await clickEveryCell(tile, preamble);
   const outcomes = perRow(requests).map((o) =>
      o.query ? { query: withPreamble(preamble, o.query) } : o,
   );
   await expectRowsMatch(
      outcomes,
      rows.map((r) => r.sale_count),
   );
   return { outcomes, marked };
}

describe("which tile shapes open the rows behind a value", () => {
   it("a refined tile, its refinement's filter included", async () => {
      const { outcomes, marked } = await expectEveryRowMatches(
         "sales_tiles -> by_region + { where: is_online = true }",
      );
      expect(marked).toEqual(["region"]);
      expect(queriesOf(outcomes)).toContain(
         "where: is_online = true, region = ",
      );
   });

   it("a tile naming a query, as the report builder writes a filtered tile", async () => {
      const { outcomes } = await expectEveryRowMatches("online_quarters");
      // The view's own filter (a given), then the query's refinement.
      expect(queriesOf(outcomes)).toContain(
         "where: region ~ $REGION, is_online = true, sale_date.quarter = ",
      );
   });

   it("a tile naming a query whose filter reads a given", async () => {
      const { outcomes } = await expectEveryRowMatches("given_regions");
      expect(queriesOf(outcomes)).toContain(
         "where: region ~ $REGION, region = ",
      );
   });

   it("an inline tile", async () => {
      await expectEveryRowMatches(
         "sales_tiles -> { group_by: region; aggregate: sale_count }",
      );
   });

   it("a view grouped by a joined field (malloydata/malloy#3158)", async () => {
      const { outcomes } = await expectEveryRowMatches(
         "sales_tiles -> by_owner",
      );
      expect(outcomes.map((o) => o.query)).toContain(
         "run: sales_tiles -> { where: owners.owner = 'Wanda'; select: *; limit: 200 }",
      );
   });

   it("a text-source report's tile, its definitions sent ahead of the rows query", async () => {
      const preamble = `source: doc_tiles is sales_tiles extend {
  view: by_product is { where: amount > 4; group_by: product; aggregate: sale_count }
}`;
      const { outcomes, marked } = await expectEveryRowMatches(
         "doc_tiles -> by_product",
         preamble,
      );
      expect(marked).toEqual(["product"]);
      expect(queriesOf(outcomes)).toContain(
         "run: doc_tiles -> { where: amount > 4, product = ",
      );
   });

   it("marks nothing for rows on a view of more than one stage", async () => {
      // Its later stage's dimensions are earlier outputs the source cannot be
      // filtered by, so a rows query would fail rather than answer.
      const { requests, marked } = await clickEveryCell(
         "sales_tiles -> two_stage",
      );
      expect(marked).toEqual([]);
      expect(requests).toEqual([]);
   });

   it("marks nothing for rows on an inline pipeline of more than one stage", async () => {
      // Its first stage's `where:` is not in the result's filters, so a rows
      // query would drop it.
      const { requests, marked } = await clickEveryCell(
         "sales_tiles -> { where: is_online = true; group_by: region; aggregate: sale_count } -> { group_by: region; aggregate: n is count() }",
      );
      expect(marked).toEqual([]);
      expect(requests).toEqual([]);
   });

   it("marks nothing for rows on a named query of more than one stage", async () => {
      const { requests, marked } = await clickEveryCell("online_staged");
      expect(marked).toEqual([]);
      expect(requests).toEqual([]);
   });

   it("marks the outer rows of a view whose nest runs two stages, and not the nest's", async () => {
      const { rows, requests, marked } = await clickEveryCell(
         "sales_tiles -> staged_nest",
      );
      expect(marked).toEqual(["region"]);
      await expectRowsMatch(
         perRow(requests),
         rows.map((r) => r.sale_count),
      );
   });

   it("drills a computed dimension declared in a joined source by its joined name", async () => {
      // Its code is `amount > 50`, which the root source would read as its
      // own `amount`; the server writes `targets.target_big` instead.
      const { outcomes, marked } = await expectEveryRowMatches(
         "sales_tiles -> by_target",
      );
      expect(marked).toEqual(["target_big", "target_year"]);
      expect(queriesOf(outcomes)).toContain(
         "where: targets.target_big = true, targets.target_year = @2025-01-01 00:00:00;",
      );
   });

   it("drills an unnamed truncation where the result still says which it was", async () => {
      const own = await expectEveryRowMatches(
         "sales_tiles -> { group_by: sold_at.month; aggregate: sale_count }",
      );
      expect(queriesOf(own.outcomes)).toContain(
         "where: sold_at.month = @2026-01-01 00:00:00;",
      );
      const joined = await expectEveryRowMatches(
         "sales_tiles -> { group_by: targets.sold_at.year; aggregate: sale_count }",
      );
      expect(queriesOf(joined.outcomes)).toContain(
         "where: targets.sold_at.year = @2025-01-01 00:00:00;",
      );
   });

   it("marks nothing for an unnamed truncation in a view, whose truncation the result has lost", async () => {
      const { requests, marked } = await clickEveryCell(
         "sales_tiles -> by_month",
      );
      expect(marked).toEqual([]);
      expect(requests).toEqual([]);
   });

   it("still drills a plain date column grouped in a view", async () => {
      const { marked } = await expectEveryRowMatches("sales_tiles -> by_date");
      expect(marked).toEqual(["sale_date"]);
   });

   it("marks nothing for rows when the host turns rows off for the tile", async () => {
      const result = await run("run: sales_tiles -> by_region");
      const requests: DrillRowsRequest[] = [];
      const hook = mountDrill({ onRows: (request) => requests.push(request) });
      const binding = hook.current.bindingFor(
         "sales_tiles -> by_region",
         false,
      );
      const viz = new MalloyRenderer({}).createViz();
      viz.setResult(API.util.wrapResult(result));
      const element = document.createElement("div");
      document.body.appendChild(element);
      viz.render(element);
      await new Promise((resolve) => setTimeout(resolve, 300));
      const fields = viz
         .getMetadata()!
         .getAllFields() as unknown as DrillField[];
      expect(fields.filter((field) => binding.canDrill(field))).toEqual([]);
      binding.onClick({
         field: fields.find((f) => f.name === "region")!,
         value: "West",
      });
      await Promise.resolve();
      expect(screen.queryAllByRole("menuitem")).toEqual([]);
      expect(requests).toEqual([]);
      hook.unmount();
      viz.remove();
      element.remove();
   });
});

describe("values and shapes a rows query must spell exactly", () => {
   it("writes a bigint id with its own digits, not a rounded number", async () => {
      const { outcomes } = await expectEveryRowMatches("edge_cases -> by_id");
      expect(queriesOf(outcomes)).toContain("big_id = 9007199254740993;");
      expect(queriesOf(outcomes)).toContain("big_id = 9007199254740992;");
   });

   it("writes a raw timestamp to its fraction of a second", async () => {
      const { outcomes } = await expectEveryRowMatches("edge_cases -> by_at");
      expect(queriesOf(outcomes)).toContain(
         "seen_at = @2026-01-15 10:30:15.25;",
      );
      expect(queriesOf(outcomes)).toContain(
         "seen_at = @2026-01-15 10:30:15.75;",
      );
   });

   it("parenthesizes a dimension whose code is an expression", async () => {
      const { outcomes } = await expectEveryRowMatches(
         "edge_cases -> by_coastal",
      );
      expect(queriesOf(outcomes)).toContain(
         "where: (region = 'East' or region = 'West') = false;",
      );
   });

   it("escapes a line break inside a string value", async () => {
      const { outcomes } = await expectEveryRowMatches("edge_cases -> by_note");
      expect(queriesOf(outcomes)).toContain("note = 'line one\\nline two';");
   });

   it("back-quotes a source name that cannot stand bare", async () => {
      const { outcomes } = await expectEveryRowMatches("`edge sales` -> by_id");
      expect(queriesOf(outcomes)).toContain("run: `edge sales` -> {");
   });

   it("marks no column of a row holding a value a filter cannot spell", async () => {
      // The renderer reports no value for a uuid, so no row can be named.
      const { requests, marked } = await clickEveryCell("edge_cases -> by_uid");
      expect(marked).toEqual([]);
      expect(requests).toEqual([]);
   });

   it("marks no column of a row the renderer reports no clauses for", async () => {
      // An inline expression has no stable expression, so the renderer
      // gives up on the whole row: its cells must not read as clickable.
      const { requests, marked } = await clickEveryCell(
         "sales_tiles -> { group_by: big_amt is amount > 6, region; aggregate: sale_count }",
      );
      expect(marked).toEqual([]);
      expect(requests).toEqual([]);
   });

   it("marks a nested value only when its row's times share one zone", async () => {
      const { rows, requests, marked } = await clickEveryCell(
         "edge_cases -> mixed_zone",
      );
      // The outer day is in UTC and the nested hour in Tokyo: one rows query
      // has one zone, so the nested row is not offered.
      expect(marked).toEqual(["d"]);
      await expectRowsMatch(
         perRow(requests),
         rows.map((r) => r.sale_count),
      );
   });
});
