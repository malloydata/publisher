// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for the pair-table layouts past one join_many (materialization v2,
 * known-limitations row 9): a path of join_manys (`inline=["items.opts"]`),
 * stored as one table with a row flag per level above the leaf, and more than
 * one inlined join_many on one source, stored as blocks of one table, one
 * block per path, read one block at a time.
 *
 * Every shape the served copy answers equals live row for row: each aggregate
 * (count, sum, avg, min/max, a distinct count, measures of the parent over
 * each level), grouped and filtered by each level, in nests, over childless
 * rows at every level. Every shape it cannot reproduce is answered live and
 * equals live there too: a middle level aggregated beside the leaf (live
 * aggregates it by its key, symmetric), a parent or middle level grouped or
 * filtered by a level below it, an aggregate written in the query above the
 * leaf, the joined sources' own measures, a query reading two blocks, and a
 * query that reads nothing through the path (the parent-only rule).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir } from "os";
import path from "path";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   rerootDeclaration,
   typeDeclaration,
   type PairReroot,
} from "../../../src/service/materialization/serve/served_copy";
import {
   flatPairQueryRefusal,
   pairConditionReads,
} from "../../../src/service/governance/ir/pair_flat_query";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
   expectPairServedVia,
} from "./harness";

/** Measures that count through the inlined path (see {@link expectPairServedVia}). */
const PATH_COUNTS = [
   "item_n",
   "opt_n",
   "pay_n",
   "big_opts",
   "pricey_items",
   "by_color",
];

// Orders 1–7. Order 4 and 6 have no item; item 12, 14 and 16 have no option;
// item 13 has three options, item 11 two. Payments: order 2 has two, 4 one,
// the rest none (orders 4 and 6 are childless in one block and not the other).
const seedSql = [
   `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'a', 10.0), (2, 'a', 20.0), (3, 'b', 30.0), (4, 'b', 40.0),
      (5, 'c', 5.0), (6, 'c', 7.5), (7, 'a', 12.5)
    ) AS t(id, cat, amt)`,
   `CREATE TABLE items AS SELECT * FROM (VALUES
      (11, 1, 1.0, 'red'), (12, 1, 2.0, 'blue'), (13, 2, 4.0, 'red'),
      (14, 3, 8.0, 'red'), (15, 5, 3.0, 'blue'), (16, 7, 6.0, 'green'),
      (17, 7, 0.5, 'red')
    ) AS t(item_id, order_id, price, color)`,
   `CREATE TABLE opts AS SELECT * FROM (VALUES
      (101, 11, 'x', 0.5), (102, 11, 'y', 0.25), (103, 13, 'x', 0.125),
      (104, 13, 'y', 2.0), (105, 13, 'z', 1.5), (106, 15, 'y', 1.0),
      (107, 17, 'x', 3.0)
    ) AS t(opt_id, item_id, kind, extra)`,
   `CREATE TABLE pays AS SELECT * FROM (VALUES
      (21, 1, 'card', 5.0), (22, 2, 'cash', 7.0), (23, 2, 'card', 9.0),
      (24, 4, 'card', 1.0), (25, 5, 'cash', 2.5), (26, 7, 'card', 4.0)
    ) AS t(pay_id, order_id, method, paid)`,
];

const children = `source: opts is duckdb.table('opts') extend {
  primary_key: opt_id
  dimension: big_extra is extra > 1
  measure: extra_total is extra.sum()
}

source: items is duckdb.table('items') extend {
  primary_key: item_id
  join_many: opts on opts.item_id = item_id
  dimension: pricey is price > 2
  dimension: pricey_n is pick 1 when price > 2 else 0
  measure:
    item_cnt is count()
    price_total is price.sum()
}

source: pays is duckdb.table('pays') extend {
  primary_key: pay_id
  measure: paid_total is paid.sum()
}
`;

const measures = `  measure:
    order_n is count()
    amt_sum is amt.sum()
    amt_avg is amt.avg()
    amt_max is amt.max()
    cat_n is count(cat)
    amt_per_order is amt_sum / order_n
    item_n is items.count()
    price_sum is items.price.sum()
    price_avg is items.price.avg()
    price_max is items.price.max()
    color_n is count(items.color)
    pricey_items is item_n { where: items.pricey }
    pricey_avg is items.pricey_n.avg()
    price_per_order is price_sum / order_n
    opt_n is items.opts.count()
    extra_sum is items.opts.extra.sum()
    extra_avg is items.opts.extra.avg()
    extra_min is items.opts.extra.min()
    kind_n is count(items.opts.kind)
    big_opts is opt_n { where: items.opts.big_extra }
    extra_per_order is extra_sum / order_n`;

const model = `${children}
// One path of two join_manys, its start named too: one table.
#(materialize) storage="lake" inline=["items", "items.opts"]
source: orders_hop is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
  dimension: big_order is amt > 15
${measures}
  view: by_color is {
    group_by: items.color
    aggregate: item_n, price_sum
  }
}

// A path whose first join is keyed on a dimension, not the primary key: its
// items are not proven to have one order each.
#(materialize) storage="lake" inline=["items.opts"]
source: orders_alias_key is duckdb.table('orders') extend {
  primary_key: id
  dimension: oid2 is id
  join_many: items on items.order_id = oid2
${measures}
}

// A path and another join_many: two blocks of one table.
#(materialize) storage="lake" inline=["items.opts", "pays"]
source: orders is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
  join_many: pays on pays.order_id = id
  dimension: big_order is amt > 15
${measures}
    pay_n is pays.count()
    paid_sum is pays.paid.sum()
    paid_max is pays.paid.max()
    method_n is count(pays.method)
    paid_per_order is paid_sum / order_n
  view: by_color is {
    group_by: items.color
    aggregate: item_n, price_sum
  }
}
`;

describe("R4: a path of join_manys and more than one join_many, served flat from one pair table, answer equal to live", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("leaves no problem on the served copy", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   it("stores one row per row of live's three-way LEFT JOIN, with one flag per level above the leaf", async () => {
      const rows = await fixture.storedSQL(
         (t) =>
            `SELECT count(*) AS n, count(*) FILTER (WHERE __parent_row) AS parents, count(*) FILTER (WHERE __child_row) AS items FROM ${t("orders_hop")}`,
      );
      // 7 options on items 11, 13, 15, 17; items 12, 14, 16 one row each;
      // orders 4 and 6 one row each. 7 orders, and 9 order-item rows (7
      // items, 2 childless orders).
      expect(rows.map((r) => [r.n, r.parents, r.items].map(Number))).toEqual([
         [12, 7, 9],
      ]);
   });

   it("stores each block as its own path's pair table", async () => {
      const rows = await fixture.storedSQL(
         (t) =>
            `SELECT __pair_lane AS b, count(*) AS n, count(*) FILTER (WHERE __parent_row) AS parents FROM ${t("orders")} GROUP BY 1 ORDER BY 1`,
      );
      // Block 0: the items.opts path (12 rows); block 1: payments, 6 paid
      // and 2 orders with none.
      expect(rows.map((r) => [r.b, r.n, r.parents].map(Number))).toEqual([
         [0, 12, 7],
         [1, 8, 7],
      ]);
   });

   const both = (q: string) => [q, q.replace(/\borders_hop\b/g, "orders")];

   // Each against both sources: the path is one table on its own, and block
   // 0 of the two-block table.
   const served = [
      [
         "parent measures beside leaf measures: count, sum, avg, max, a distinct count",
         "run: orders_hop -> { group_by: cat; aggregate: order_n, amt_sum, amt_avg, amt_max, cat_n, opt_n, extra_sum, extra_avg, extra_min, kind_n }",
      ],
      [
         "leaf aggregates written in the query",
         "run: orders_hop -> { group_by: cat; aggregate: c is items.opts.count(), s is items.opts.extra.sum(), a is items.opts.extra.avg(), mx is items.opts.extra.max(), mn is items.opts.extra.min(), d is count(items.opts.kind) }",
      ],
      [
         "parent measures beside middle measures, the leaf unread",
         "run: orders_hop -> { group_by: cat; aggregate: order_n, amt_sum, item_n, price_sum, price_avg, price_max, color_n }",
      ],
      [
         "ratios across levels, by name",
         "run: orders_hop -> { group_by: cat; aggregate: amt_per_order, price_per_order }",
      ],
      [
         "a leaf ratio over a parent measure",
         "run: orders_hop -> { group_by: cat; aggregate: extra_per_order }",
      ],
      [
         "middle measures grouped by the middle level",
         "run: orders_hop -> { group_by: items.color; aggregate: item_n, price_sum, price_max }",
      ],
      [
         "the source's own view, grouped by the middle level",
         "run: orders_hop -> by_color",
      ],
      [
         "leaf measures grouped by the middle level",
         "run: orders_hop -> { group_by: items.color; aggregate: opt_n, extra_sum, kind_n }",
      ],
      [
         "leaf measures grouped by the leaf",
         "run: orders_hop -> { group_by: items.opts.kind; aggregate: opt_n, extra_sum, extra_avg }",
      ],
      [
         "leaf measures grouped by a parent dimension",
         "run: orders_hop -> { group_by: big_order; aggregate: opt_n, extra_sum }",
      ],
      [
         "a parent filter, parent and leaf measures",
         "run: orders_hop -> { where: amt > 9; group_by: cat; aggregate: order_n, amt_sum, opt_n, extra_sum }",
      ],
      [
         "a middle filter, middle measures",
         "run: orders_hop -> { where: items.price > 1.5; group_by: cat; aggregate: item_n, price_sum }",
      ],
      [
         "a middle filter, leaf measures",
         "run: orders_hop -> { where: items.color = 'red'; group_by: cat; aggregate: opt_n, extra_sum }",
      ],
      [
         "a leaf filter, leaf measures",
         "run: orders_hop -> { where: items.opts.kind = 'x'; group_by: cat; aggregate: opt_n, extra_sum }",
      ],
      [
         "filtered measures at the middle and the leaf",
         "run: orders_hop -> { group_by: cat; aggregate: pricey_items, price_sum }",
      ],
      [
         "a filtered leaf measure",
         "run: orders_hop -> { group_by: cat; aggregate: big_opts, opt_n }",
      ],
      [
         "leaf measures filtered by the middle level, in the query",
         "run: orders_hop -> { group_by: cat; aggregate: r is opt_n { where: items.color = 'red' } }",
      ],
      [
         "a nest of middle measures under parent ones",
         "run: orders_hop -> { group_by: cat; aggregate: order_n, amt_sum; nest: colors is { group_by: items.color; aggregate: item_n, price_sum; order_by: color } }",
      ],
      [
         "a nest of leaf measures under parent ones",
         "run: orders_hop -> { group_by: cat; aggregate: order_n, amt_sum; nest: kinds is { group_by: items.opts.kind; aggregate: opt_n, extra_sum; order_by: kind } }",
      ],
      [
         "childless orders, counted once at the middle as live counts them",
         "run: orders_hop -> { group_by: id; aggregate: item_n, price_sum }",
      ],
      [
         "childless orders and items, counted at the leaf as live counts them",
         "run: orders_hop -> { group_by: id; aggregate: opt_n, extra_sum }",
      ],
      [
         "one row per order and item, the leaf counted on each",
         "run: orders_hop -> { group_by: id, items.item_id; aggregate: opt_n }",
      ],
      [
         "one row per path",
         "run: orders_hop -> { group_by: id, items.item_id, items.opts.opt_id }",
      ],
      [
         "a having on a parent measure",
         "run: orders_hop -> { group_by: cat; aggregate: order_n, opt_n; having: order_n > 1 }",
      ],
      // A middle level whose rows have one order each (each join keyed on its
      // parent's primary key, checked at build), beside the leaf: a sum or an
      // average of a stored middle column, as live's by the item key.
      [
         "middle sums and averages beside leaf measures",
         "run: orders_hop -> { group_by: cat; aggregate: price_sum, price_avg, opt_n, extra_sum }",
      ],
      [
         "middle and parent measures beside the leaf, by order (childless orders included)",
         "run: orders_hop -> { group_by: id; aggregate: order_n, amt_sum, price_sum, price_avg, opt_n }",
      ],
      [
         "middle measures beside the leaf, grouped by the middle level",
         "run: orders_hop -> { group_by: items.color; aggregate: price_sum, price_avg, extra_sum }",
      ],
      [
         "middle measures beside the leaf, under a middle filter",
         "run: orders_hop -> { where: items.color = 'red'; group_by: cat; aggregate: price_sum, opt_n }",
      ],
      [
         "a middle measure beside a nest that reads the leaf",
         "run: orders_hop -> { group_by: cat; aggregate: price_sum; nest: k is { group_by: items.opts.kind; aggregate: opt_n; order_by: kind } }",
      ],
      [
         "a middle ratio beside the leaf",
         "run: orders_hop -> { group_by: cat; aggregate: price_per_order, extra_per_order }",
      ],
   ] as const;
   for (const [name, query] of served) {
      for (const malloy of both(query)) {
         it(`${name} (${malloy.split(" ")[1]})`, async () => {
            const run = await fixture.run({ name, malloy });
            expectPairServedVia(run, PATH_COUNTS);
            expect(run.live.length).toBeGreaterThan(0);
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });
      }
   }

   // Block 1 of the two-block table: the payments.
   const servedBlock = [
      [
         "parent measures beside the other block's measures",
         "run: orders -> { group_by: cat; aggregate: order_n, amt_sum, amt_max, cat_n, pay_n, paid_sum, paid_max, method_n }",
      ],
      [
         "the other block's aggregates written in the query, grouped by its field",
         "run: orders -> { group_by: pays.method; aggregate: pay_n, s is pays.paid.sum(), a is pays.paid.avg(), c is pays.count() }",
      ],
      [
         "a ratio over the parent",
         "run: orders -> { group_by: cat; aggregate: paid_per_order, amt_per_order }",
      ],
      [
         "childless orders in that block, counted as live counts them",
         "run: orders -> { group_by: id; aggregate: pay_n, paid_sum }",
      ],
      [
         "a filter on that block's join",
         "run: orders -> { where: pays.method = 'card'; group_by: cat; aggregate: pay_n, paid_sum }",
      ],
      [
         "a nest by that block's field",
         "run: orders -> { group_by: cat; aggregate: order_n; nest: m is { group_by: pays.method; aggregate: pay_n; order_by: method } }",
      ],
   ] as const;
   for (const [name, malloy] of servedBlock) {
      it(`the payments block: ${name}`, async () => {
         const run = await fixture.run({ name, malloy });
         expectPairServedVia(run, PATH_COUNTS);
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }

   it("answers a childless order's middle count (none, since malloydata/malloy#3208) live", async () => {
      const run = await fixture.run({
         name: "childless",
         malloy:
            "run: orders_hop -> { where: id = 4 | 1; group_by: id; aggregate: item_n }",
      });
      // The flag marks order 4's null-extended row, which would count one.
      expect(run.servedVia).toBeUndefined();
      expect(
         run.live
            .map((r) => [Number(r.id), Number(r.item_n)])
            .sort((a, b) => a[0] - b[0]),
      ).toEqual([
         [1, 2],
         [4, 0],
      ]);
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   // Each answered live, equal to live, with the reason the path report gives.
   const declined = [
      [
         "a middle measure beside a leaf measure (live aggregates the middle by its key)",
         "run: orders_hop -> { group_by: cat; aggregate: item_n, opt_n }",
         "COUNT_SEMANTICS",
      ],
      [
         "a middle measure grouped by the leaf",
         "run: orders_hop -> { group_by: items.opts.kind; aggregate: item_n, price_sum }",
         "COUNT_SEMANTICS",
      ],
      [
         "a middle measure filtered by the leaf",
         "run: orders_hop -> { where: items.opts.kind = 'x'; group_by: cat; aggregate: item_n }",
         "COUNT_SEMANTICS",
      ],
      [
         "a middle count in a query whose nest reads the leaf",
         "run: orders_hop -> { group_by: cat; aggregate: item_n; nest: k is { group_by: items.opts.kind; aggregate: opt_n } }",
         "COUNT_SEMANTICS",
      ],
      [
         "a middle average of a computed field beside the leaf (its null row is not null)",
         "run: orders_hop -> { group_by: cat; aggregate: pricey_avg, opt_n }",
         "COUNT_SEMANTICS",
      ],
      [
         "a middle sum beside the leaf, its join keyed on a dimension, not the primary key",
         "run: orders_alias_key -> { group_by: cat; aggregate: price_sum, opt_n }",
         "COUNT_SEMANTICS",
      ],
      [
         "a middle aggregate written in the query",
         "run: orders_hop -> { group_by: cat; aggregate: n is items.count(), s is items.price.sum() }",
         "COUNT_SEMANTICS",
      ],
      [
         "a parent aggregate written in the query",
         "run: orders_hop -> { group_by: cat; aggregate: c is count(), opt_n }",
         "COUNT_SEMANTICS",
      ],
      [
         "a parent measure grouped by the middle level",
         "run: orders_hop -> { group_by: items.color; aggregate: order_n, item_n }",
         "COUNT_SEMANTICS",
      ],
      [
         "a parent measure grouped by the leaf",
         "run: orders_hop -> { group_by: items.opts.kind; aggregate: order_n, opt_n }",
         "COUNT_SEMANTICS",
      ],
      [
         "a parent measure filtered by the leaf",
         "run: orders_hop -> { where: items.opts.kind = 'y'; group_by: cat; aggregate: order_n, opt_n }",
         "COUNT_SEMANTICS",
      ],
      [
         "a middle source's own measure",
         "run: orders_hop -> { group_by: cat; aggregate: items.price_total }",
         "MISSING_FIELD",
      ],
      [
         "the leaf source's own measure",
         "run: orders_hop -> { group_by: cat; aggregate: items.opts.extra_total }",
         "MISSING_FIELD",
      ],
      [
         "a select: over the path",
         "run: orders_hop -> { select: id, items.opts.kind }",
         "COUNT_SEMANTICS",
      ],
      [
         "two blocks' measures in one query",
         "run: orders -> { group_by: cat; aggregate: opt_n, pay_n }",
         "COUNT_SEMANTICS",
      ],
      [
         "two blocks, one per nest",
         "run: orders -> { group_by: cat; nest: o is { aggregate: opt_n }; nest: p is { aggregate: pay_n } }",
         "COUNT_SEMANTICS",
      ],
      [
         "one block's measures under a filter on the other's join",
         "run: orders -> { where: pays.method = 'card'; group_by: cat; aggregate: opt_n, extra_sum }",
         "COUNT_SEMANTICS",
      ],
      [
         "one block's measures grouped by the other's field",
         "run: orders -> { group_by: pays.method; aggregate: item_n }",
         "COUNT_SEMANTICS",
      ],
      [
         "the payments source's own measure",
         "run: orders -> { group_by: cat; aggregate: pays.paid_total }",
         "MISSING_FIELD",
      ],
   ] as const;
   for (const [name, malloy, reason] of declined) {
      it(`answers live: ${name}`, async () => {
         const run = await fixture.run({ name, malloy, expectLive: true });
         expect(run.servedVia).toBeUndefined();
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         expect(path?.considered?.map((c) => c.reason)).toContain(reason);
      });
   }

   // The parent-only rule holds for both layouts: a query that reads no
   // child field passes over the pair table.
   const parentOnly = [
      "run: orders_hop -> { group_by: cat; aggregate: order_n, amt_sum, amt_avg }",
      "run: orders -> { group_by: cat; aggregate: order_n, amt_sum, amt_per_order }",
      "run: orders -> { where: big_order; group_by: cat; aggregate: c is count() }",
   ];
   for (const malloy of parentOnly) {
      it(`passes over the pair table, reading no child: ${malloy}`, async () => {
         const run = await fixture.run({
            name: malloy,
            malloy,
            expectLive: true,
         });
         expect(run.servedVia).toBeUndefined();
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         fixture.setBindings(fixture.bindings);
         const { path } = await fixture.runAsIs(malloy);
         const skipped = path?.considered?.find((c) => c.outcome === "skipped");
         expect(skipped?.reason).toBe("COUNT_SEMANTICS");
         expect(skipped?.detail).toMatch(/reads no child field/);
      });
   }
});

// The query check, mutated: the same served text with a guard taken out
// answers wrong, and the check refuses the query it would have answered.
describe("R4: the pair-layout guards, mutated", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   let duckdb: DuckDBConnection;
   let dir: string;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      dir = mkdtempSync(path.join(tmpdir(), "pair-layouts-"));
      // Each stored table, copied whole (a plain runSQL caps its rows).
      for (const [table, source] of [
         ["hop", "orders_hop"],
         ["blocks", "orders"],
      ]) {
         const file = path.join(dir, `${table}.parquet`);
         await fixture.storedSQL(
            (t) => `COPY (SELECT * FROM ${t(source)}) TO '${file}'`,
         );
         await duckdb.runSQL(
            `CREATE TABLE ${table} AS SELECT * FROM '${file}'`,
         );
      }
   });
   afterAll(async () => {
      restore();
      await duckdb.close();
      rmSync(dir, { recursive: true, force: true });
      await disposeScenarios();
   });

   const typesOf = async (table: string) =>
      new Map(
         (
            (await duckdb.runSQL(`DESCRIBE ${table}`, { rowLimit: 1000 }))
               .rows as Row[]
         ).map((r) => [String(r.column_name), String(r.column_type)]),
      );

   const hopDeclaration = (
      name: string,
   ) => `${name} is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
  measure:
    order_n is count()
    item_n is items.count()
    price_sum is items.price.sum()
    opt_n is items.opts.count()
}`;

   const answer = async (
      table: string,
      declaration: string,
      pair: PairReroot,
      own: string[],
      malloy: string,
      source: string,
   ) => {
      const types = await typesOf(table);
      const columns = own.map((name) => ({
         name,
         type: types.get(name) as string,
      }));
      const record = (cs: readonly { column: string; type: string }[]) =>
         cs.map((c) => ({ name: c.column, type: c.type }));
      const text = `##! experimental { virtual_source }
${typeDeclaration({
   typeName: `${source}__served`,
   columns: [
      ...columns,
      pair.parentRowColumn,
      ...(pair.levelRows ?? []).map((l) => l.column),
      ...(pair.block ? [pair.block.laneColumn] : []),
      ...record(pair.childColumns),
      ...(pair.hops ?? []).flatMap((h) => record(h.columns)),
      ...(pair.block?.others ?? []).flatMap((o) => [
         ...record(o.childColumns),
         ...(o.hops ?? []).flatMap((h) => record(h.columns)),
      ]),
   ],
})}
source: ${rerootDeclaration(declaration, {
         sourceName: source,
         connectionAlias: "duckdb",
         virtualHandle: "pair",
         typeName: `${source}__served`,
         columns,
         storedDimensions: [],
         pair,
      })}`;
      const runtime = new SingleConnectionRuntime({ connection: duckdb });
      const virtualMap = new Map([["duckdb", new Map([["pair", table]])]]);
      const query = runtime.loadModel(text).loadQuery(malloy);
      const sql = await query.getSQL({ virtualMap } as never);
      const prepared = (await query.getPreparedQuery()) as unknown as {
         _query: { pipeline?: unknown };
         _modelDef: { contents: Record<string, unknown> };
      };
      const refusal = flatPairQueryRefusal(
         prepared._query,
         prepared._modelDef.contents[source] as never,
         {
            join: pair.join,
            parentRowField: pair.parentRowField,
            ...(pair.hops ? { hops: pair.hops.map((h) => h.join) } : {}),
            ...(pair.levelRows
               ? { levelRowFields: pair.levelRows.map((l) => l.field) }
               : {}),
            ...(pair.block
               ? { otherRecords: pair.block.others.map((o) => o.join) }
               : {}),
            ...(pair.oneParentLevels
               ? {
                    oneParentLevels: pair.oneParentLevels,
                    computedFields: pair.childStoredDimensions.map(
                       (d) => `${pair.join}.${d}`,
                    ),
                 }
               : {}),
         },
      );
      const rows = (
         (await duckdb.runSQL(sql, { rowLimit: 1000 })).rows as Row[]
      ).map((r) =>
         Object.fromEntries(
            Object.entries(r).map(([k, v]) => [
               k,
               typeof v === "string" && /^-?[\d.]+$/.test(v) ? Number(v) : v,
            ]),
         ),
      );
      return { rows, refusal };
   };

   const childColumns = async (
      table: string,
      prefix: string,
      names: string[],
   ) => {
      const types = await typesOf(table);
      const missing = names.filter((n) => !types.has(`${prefix}__${n}`));
      if (missing.length > 0) {
         throw new Error(
            `${table} stores no ${missing.join(", ")}: ${[...types.keys()].join(", ")}`,
         );
      }
      return names.map((name) => ({
         name,
         column: `${prefix}__${name}`,
         internalName: `${prefix}__${name}__pair`,
         type: types.get(`${prefix}__${name}`) as string,
      }));
   };

   const hopPair = async (
      parentMeasures: PairReroot["parentMeasures"],
   ): Promise<PairReroot> => ({
      join: "items",
      parentRowColumn: { name: "__parent_row", type: "BOOLEAN" },
      parentRowField: "__parent_row__pair",
      childColumns: await childColumns("hop", "items", [
         "item_id",
         "order_id",
         "price",
         "color",
         "pricey",
         "pricey_n",
      ]),
      hops: [
         {
            join: "opts",
            columns: await childColumns("hop", "items__opts", [
               "opt_id",
               "item_id",
               "kind",
               "extra",
               "big_extra",
            ]),
         },
      ],
      levelRows: [
         {
            column: { name: "__child_row", type: "BOOLEAN" },
            field: "__child_row__pair",
         },
      ],
      parentMeasures,
      childStoredDimensions: [],
   });

   it("a middle measure without its level's row flag answers wrong, and is refused", async () => {
      const malloy =
         "run: orders_hop -> { group_by: cat; aggregate: order_n, price_sum }";
      const live = (await fixture.run({ name: "mutation", malloy })).live;
      const flagged = await answer(
         "hop",
         hopDeclaration("orders_hop"),
         await hopPair([
            { name: "order_n", internalName: "order_n__pair" },
            { name: "price_sum", internalName: "price_sum__pair", level: 1 },
         ]),
         ["id", "cat", "amt"],
         malloy,
         "orders_hop",
      );
      expect(rowDifference(live, flagged.rows)).toBeUndefined();
      expect(flagged.refusal).toBeUndefined();
      // The middle measures left as written: each item counted once per
      // option.
      const mutated = await answer(
         "hop",
         hopDeclaration("orders_hop"),
         await hopPair([{ name: "order_n", internalName: "order_n__pair" }]),
         ["id", "cat", "amt"],
         malloy,
         "orders_hop",
      );
      expect(rowDifference(live, mutated.rows)).toBeDefined();
      expect(mutated.refusal).toMatch(/not one of the source's measures/);
   });

   it("a middle measure beside the leaf, flagged, answers wrong, and is refused", async () => {
      // Live aggregates the middle level by its key once the leaf is read: a
      // childless order's item count is 0 there, 1 under the flag.
      const malloy =
         "run: orders_hop -> { group_by: id; aggregate: item_n, opt_n }";
      const live = (await fixture.run({ name: "beside", malloy })).live;
      const flagged = await answer(
         "hop",
         hopDeclaration("orders_hop"),
         await hopPair([
            { name: "order_n", internalName: "order_n__pair" },
            { name: "item_n", internalName: "item_n__pair", level: 1 },
         ]),
         ["id", "cat", "amt"],
         malloy,
         "orders_hop",
      );
      expect(rowDifference(live, flagged.rows)).toBeDefined();
      // Since malloydata/malloy#3208 any middle count reads that row wrong.
      expect(flagged.refusal).toMatch(/a count through the inlined join/);
   });

   it("a middle sum or average of a stored column beside the leaf answers right under the flag only where each item has one order", async () => {
      const declaration = `orders_hop is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
  measure:
    order_n is count()
    price_sum is items.price.sum()
    price_avg is items.price.avg()
    pricey_avg is items.pricey_n.avg()
    item_n is items.count()
    opt_n is items.opts.count()
}`;
      const flaggedMiddle = [
         { name: "order_n", internalName: "order_n__pair" },
         { name: "price_sum", internalName: "price_sum__pair", level: 1 },
         { name: "price_avg", internalName: "price_avg__pair", level: 1 },
         { name: "pricey_avg", internalName: "pricey_avg__pair", level: 1 },
         { name: "item_n", internalName: "item_n__pair", level: 1 },
      ];
      const proven = {
         ...(await hopPair(flaggedMiddle)),
         oneParentLevels: [1],
         childStoredDimensions: ["pricey", "pricey_n"],
      };
      const sums =
         "run: orders_hop -> { group_by: id; aggregate: price_sum, price_avg, e is items.opts.extra.sum() }";
      const live = (await fixture.run({ name: "sums", malloy: sums })).live;
      const served = await answer(
         "hop",
         declaration,
         proven,
         ["id", "cat", "amt"],
         sums,
         "orders_hop",
      );
      expect(rowDifference(live, served.rows)).toBeUndefined();
      expect(served.refusal).toBeUndefined();
      // Not proven one order per item: refused, though this data has one.
      const unproven = await answer(
         "hop",
         declaration,
         await hopPair(flaggedMiddle),
         ["id", "cat", "amt"],
         sums,
         "orders_hop",
      );
      expect(unproven.refusal).toMatch(/beside fields below it/);
      // A middle count reads a childless order's null row under the flag,
      // where live's count by the item key skips it: wrong, and refused.
      const counts =
         "run: orders_hop -> { group_by: id; aggregate: item_n, opt_n }";
      const want = (await fixture.run({ name: counts, malloy: counts })).live;
      const got = await answer(
         "hop",
         declaration,
         proven,
         ["id", "cat", "amt"],
         counts,
         "orders_hop",
      );
      expect(rowDifference(want, got.rows)).toBeDefined();
      expect(got.refusal).toMatch(/a count through the inlined join/);
      // A computed column is 0, not null, on that null row; whether live's
      // symmetric average reads it depends on the dialect (DuckDB's hash of a
      // null key is not null, so it does here), so it is refused.
      const computed = await answer(
         "hop",
         declaration,
         proven,
         ["id", "cat", "amt"],
         "run: orders_hop -> { group_by: cat; aggregate: pricey_avg, e is items.opts.extra.sum() }",
         "orders_hop",
      );
      expect(computed.refusal).toMatch(
         /not null on the pair table's row|beside fields below it/,
      );
   });

   it("a query read from the wrong block answers wrong, and is refused", async () => {
      // The payments query, answered from block 0 (the items path): its
      // rows hold the payments' columns as nulls.
      const malloy = "run: orders -> { group_by: cat; aggregate: paid_sum }";
      const live = (await fixture.run({ name: "block", malloy })).live;
      const types = await typesOf("blocks");
      const declaration = `orders is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
  join_many: pays on pays.order_id = id
  measure:
    order_n is count()
    pay_n is pays.count()
    paid_sum is pays.paid.sum()
}`;
      const block = async (lane: number): Promise<PairReroot> => {
         const items = await childColumns("blocks", "items", [
            "item_id",
            "order_id",
            "price",
            "color",
            "pricey",
         ]);
         const opts = await childColumns("blocks", "items__opts", [
            "opt_id",
            "item_id",
            "kind",
            "extra",
            "big_extra",
         ]);
         const pays = await childColumns("blocks", "pays", [
            "pay_id",
            "order_id",
            "method",
            "paid",
         ]);
         const itemsRecord = {
            join: "items",
            childColumns: items,
            hops: [{ join: "opts", columns: opts }],
         };
         const paysRecord = { join: "pays", childColumns: pays };
         const [own, other] =
            lane === 0 ? [itemsRecord, paysRecord] : [paysRecord, itemsRecord];
         return {
            join: own.join,
            parentRowColumn: { name: "__parent_row", type: "BOOLEAN" },
            parentRowField: "__parent_row__pair",
            childColumns: own.childColumns,
            ...("hops" in own ? { hops: own.hops } : {}),
            ...(lane === 0
               ? {
                    levelRows: [
                       {
                          column: { name: "__child_row", type: "BOOLEAN" },
                          field: "__child_row__pair",
                       },
                    ],
                 }
               : {}),
            block: {
               lane,
               laneColumn: {
                  name: "__pair_lane",
                  type: types.get("__pair_lane") as string,
               },
               laneField: "__pair_lane__pair",
               others: [other],
            },
            parentMeasures: [
               { name: "order_n", internalName: "order_n__pair" },
            ],
            childStoredDimensions: [],
         };
      };
      const right = await answer(
         "blocks",
         declaration,
         await block(1),
         ["id", "cat", "amt"],
         malloy,
         "orders",
      );
      expect(rowDifference(live, right.rows)).toBeUndefined();
      expect(right.refusal).toBeUndefined();
      const wrong = await answer(
         "blocks",
         declaration,
         await block(0),
         ["id", "cat", "amt"],
         malloy,
         "orders",
      );
      expect(rowDifference(live, wrong.rows)).toBeDefined();
      expect(wrong.refusal).toMatch(/another inlined join_many/);
   });

   it("reads a gate through another block's record, or another join_many, as fanning out the pair rows", () => {
      // The lifted condition `pays.method = …`, on a source whose `pays` is
      // another block's record, and on one whose `pays` is a join_many.
      const condition = {
         node: "=",
         kids: {
            left: { node: "field", path: ["pays", "method"] },
            right: { node: "stringLiteral", literal: "card" },
         },
      };
      expect(
         pairConditionReads(
            condition,
            { fields: [{ name: "pays", type: "record" }] },
            { join: "items", otherRecords: ["pays"] },
         ),
      ).toEqual({ level: 0, fansOut: true });
      expect(
         pairConditionReads(
            condition,
            { fields: [{ name: "pays", join: "many", fields: [] }] },
            "items",
         ),
      ).toEqual({ level: 0, fansOut: true });
      expect(
         pairConditionReads(
            {
               node: "=",
               kids: {
                  left: { node: "field", path: ["items", "opts", "kind"] },
                  right: { node: "stringLiteral", literal: "x" },
               },
            },
            { fields: [{ name: "items", type: "record" }] },
            { join: "items", hops: ["opts"] },
         ),
      ).toEqual({ level: 2, fansOut: false });
   });
});

// Row-level gates on pair sources, proven at read (R3).
const gatedSeed = seedSql;

const gatedModel = `##! experimental.givens
given: C :: string
given: K :: string
given: M :: string

${children}

#(access_filter) cat = $C
#(materialize) storage="lake" inline=["items.opts", "pays"]
source: by_cat is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
  join_many: pays on pays.order_id = id
  measure:
    order_n is count()
    amt_sum is amt.sum()
    item_n is items.count()
    opt_n is items.opts.count()
    extra_sum is items.opts.extra.sum()
    pay_n is pays.count()
    paid_sum is pays.paid.sum()
}

#(access_filter) items.color = $K
#(materialize) storage="lake" inline=["items.opts"]
source: by_color is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
  measure:
    order_n is count()
    item_n is items.count()
    price_sum is items.price.sum()
    opt_n is items.opts.count()
    extra_sum is items.opts.extra.sum()
}

#(access_filter) pays.method = $M
#(materialize) storage="lake" inline=["items.opts", "pays"]
source: by_method is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
  join_many: pays on pays.order_id = id
  measure:
    order_n is count()
    opt_n is items.opts.count()
    extra_sum is items.opts.extra.sum()
    pay_n is pays.count()
    paid_sum is pays.paid.sum()
}

// A one-hop pair table beside a join_many it does not inline, stored in the
// same destination, so the served copy keeps that join.
#(materialize) storage="lake"
source: items_stored is duckdb.table('items') extend { primary_key: item_id }

#(access_filter) its.color = $K
#(materialize) storage="lake" inline=["pays"]
source: one_hop is duckdb.table('orders') extend {
  primary_key: id
  join_many: pays on pays.order_id = id
  join_many: its is items_stored on its.order_id = id
  measure:
    order_n is count()
    pay_n is pays.count()
    paid_sum is pays.paid.sum()
}
`;

describe("R4: row-level gates on multi-join and multi-hop pair sources", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
         },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": gatedModel },
         seedSql: gatedSeed,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("leaves no problem on the served copy", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   const cases = [
      [
         "a parent gate, the path's block",
         "run: by_cat -> { group_by: cat; aggregate: order_n, amt_sum, opt_n, extra_sum }",
         { C: "a" },
         true,
      ],
      [
         "a parent gate, the path's middle level",
         "run: by_cat -> { group_by: cat; aggregate: order_n, item_n }",
         { C: "a" },
         true,
      ],
      [
         "a parent gate, the other block",
         "run: by_cat -> { group_by: cat; aggregate: order_n, pay_n, paid_sum }",
         { C: "b" },
         true,
      ],
      [
         "a middle gate, leaf measures",
         "run: by_color -> { group_by: cat; aggregate: opt_n, extra_sum }",
         { K: "red" },
         true,
      ],
      [
         "a middle gate, middle measures",
         "run: by_color -> { group_by: cat; aggregate: item_n, price_sum }",
         { K: "red" },
         true,
      ],
      [
         "a middle gate, a parent measure (filtered by a level below it)",
         "run: by_color -> { group_by: cat; aggregate: order_n, opt_n }",
         { K: "red" },
         false,
      ],
      [
         "a gate on one block's join, that block's measures",
         "run: by_method -> { group_by: cat; aggregate: pay_n, paid_sum }",
         { M: "card" },
         true,
      ],
      [
         "a gate on one block's join, a parent measure",
         "run: by_method -> { group_by: cat; aggregate: order_n, pay_n }",
         { M: "card" },
         false,
      ],
      [
         "a gate on one block's join, the other block's measures (it fans out that block's rows)",
         "run: by_method -> { group_by: cat; aggregate: opt_n, extra_sum }",
         { M: "card" },
         false,
      ],
      [
         "a gate through a join_many the pair table does not inline (it fans out the pair rows)",
         "run: one_hop -> { group_by: cat; aggregate: pay_n, paid_sum }",
         { K: "red" },
         false,
      ],
   ] as const;
   for (const [name, malloy, givens, servedHere] of cases) {
      it(`${servedHere ? "serves" : "answers live"}: ${name}`, async () => {
         const run = await fixture.run({
            name,
            malloy,
            givens,
            ...(servedHere ? {} : { expectLive: true }),
         });
         expectPairServedVia(run, PATH_COUNTS, servedHere);
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }
});

// A level's row flag marks one row per distinct key, so a key held twice or
// null at a flagged level would count once where live counts each row: the
// build checks every flagged level's key first and is refused, so the source
// answers live, equal to live.
const keyModel = `${children}
#(materialize) storage="lake" inline=["items.opts"]
source: orders_hop is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
${measures}
}

#(materialize) storage="lake" inline=["items.opts", "pays"]
source: orders is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
  join_many: pays on pays.order_id = id
${measures}
    pay_n is pays.count()
    paid_sum is pays.paid.sum()
}
`;

const keySeeds = {
   "null source keys": [
      `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'a', 10.0), (2, 'a', 20.0), (NULL, 'b', 30.0), (NULL, 'b', 40.0), (5, 'c', 5.0)
    ) AS t(id, cat, amt)`,
   ],
   "null middle keys": [
      `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'a', 10.0), (2, 'a', 20.0), (5, 'c', 5.0)
    ) AS t(id, cat, amt)`,
      `CREATE TABLE items AS SELECT * FROM (VALUES
      (11, 1, 1.0, 'red'), (NULL, 1, 2.0, 'blue'), (NULL, 1, 3.0, 'green'), (13, 2, 4.0, 'red')
    ) AS t(item_id, order_id, price, color)`,
   ],
   "a middle key held twice": [
      `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'a', 10.0), (2, 'a', 20.0), (5, 'c', 5.0)
    ) AS t(id, cat, amt)`,
      `CREATE TABLE items AS SELECT * FROM (VALUES
      (11, 1, 1.0, 'red'), (11, 1, 2.0, 'blue'), (13, 2, 4.0, 'red')
    ) AS t(item_id, order_id, price, color)`,
   ],
};

for (const [label, overrides] of Object.entries(keySeeds)) {
   describe(`R4: a pair table over ${label} is not built, and answers live`, () => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         const replaced = new Set(overrides.map((sql) => sql.split(" ")[2]));
         fixture = await buildScenario({
            files: { "m.malloy": keyModel },
            seedSql: [
               ...seedSql.filter((sql) => !replaced.has(sql.split(" ")[2])),
               ...overrides,
            ],
            materialize: true,
            checkKeys: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      it("refuses both builds, naming the key", () => {
         expect(Object.keys(fixture.checkFailures).sort()).toEqual([
            "orders",
            "orders_hop",
         ]);
         for (const message of Object.values(fixture.checkFailures)) {
            expect(message).toMatch(/keys one row flag on '(id|item_id)'/);
         }
      });

      // Each would differ from live over the table: the flag counts the
      // shared key once.
      for (const malloy of [
         "run: orders_hop -> { group_by: cat; aggregate: order_n, amt_sum, opt_n }",
         "run: orders_hop -> { group_by: cat; aggregate: item_n, price_sum }",
         "run: orders -> { group_by: cat; aggregate: order_n, amt_sum, pay_n }",
         "run: orders -> { group_by: cat; aggregate: item_n, price_sum }",
      ]) {
         it(`answers live, equal to live: ${malloy}`, async () => {
            const run = await fixture.run({
               name: malloy,
               malloy,
               expectLive: true,
            });
            expect(run.servedVia).toBeUndefined();
            expect(rowDifference(run.live, run.served)).toBeUndefined();
         });
      }
   });
}

// Another block's columns are typed nulls on a block's rows, stacked with
// UNION ALL: a null of the warehouse's float type would widen an integer
// column to it, and a key past 2^53 would be stored rounded.
describe("R4: a block table keeps each column's own type across the union", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": keyModel },
         seedSql: [
            `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'a', 10.0), (2, 'a', 20.0), (3, 'b', 30.0)
    ) AS t(id, cat, amt)`,
            `CREATE TABLE items AS SELECT item_id::BIGINT AS item_id, order_id, price::DECIMAL(38,2) AS price, color FROM (VALUES
      (9007199254740993, 1, 12345678901234567.11, 'red'), (9007199254740992, 2, 1.0, 'blue'), (9007199254740995, 3, 0.07, 'red')
    ) AS t(item_id, order_id, price, color)`,
            `CREATE TABLE opts AS SELECT opt_id, item_id::BIGINT AS item_id, kind, extra FROM (VALUES
      (101, 9007199254740993, 'x', 0.5), (102, 9007199254740992, 'y', 4.0)
    ) AS t(opt_id, item_id, kind, extra)`,
            `CREATE TABLE pays AS SELECT pay_id::BIGINT AS pay_id, order_id, method, paid FROM (VALUES
      (9007199254740993, 1, 'card', 5.0), (9007199254740992, 1, 'cash', 7.0)
    ) AS t(pay_id, order_id, method, paid)`,
         ],
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("stores each joined column as its source types it", async () => {
      const rows = await fixture.storedSQL(
         (t) =>
            `SELECT typeof(items__item_id) AS item, typeof(items__price) AS price, typeof(pays__pay_id) AS pay FROM ${t("orders")} LIMIT 1`,
      );
      expect(rows).toEqual([
         { item: "BIGINT", price: "DECIMAL(38,2)", pay: "BIGINT" },
      ]);
   });

   for (const malloy of [
      "run: orders -> { group_by: cat; aggregate: pay_n, d is count(pays.pay_id) }",
      "run: orders -> { where: items.item_id = 9007199254740993; aggregate: opt_n }",
      "run: orders -> { where: pays.pay_id = 9007199254740992; aggregate: pay_n, paid_sum }",
      "run: orders -> { group_by: cat; aggregate: price_sum }",
   ]) {
      it(`serves, equal to live: ${malloy}`, async () => {
         const run = await fixture.run({ name: malloy, malloy });
         expectPairServedVia(run, PATH_COUNTS);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }
});

// A join_many reached through a join_one fans out the pair rows as much as
// one on the source: a gate through it, or a query that groups or filters
// through it, is answered live. Both joins are to sources stored in the same
// destination, so the served copy keeps them over the pair rows.
const fanOutModel = `##! experimental.givens
given: G :: string

source: opts is duckdb.table('opts') extend { primary_key: opt_id }
source: items is duckdb.table('items') extend {
  primary_key: item_id
  join_many: opts on opts.item_id = item_id
}

#(materialize) storage="lake"
source: gates_stored is duckdb.table('gates') extend { primary_key: gate_id }

#(materialize) storage="lake"
source: hubs_stored is duckdb.table('hubs') extend {
  primary_key: code
  join_many: gs is gates_stored on gs.code = code
}

#(access_filter) hub.gs.gate = $G
#(materialize) storage="lake" inline=["items.opts"]
source: by_gate is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
  join_one: hub is hubs_stored on hub.code = cat
  measure:
    order_n is count()
    opt_n is items.opts.count()
    extra_sum is items.opts.extra.sum()
}

#(materialize) storage="lake" inline=["items.opts"]
source: by_hub is duckdb.table('orders') extend {
  primary_key: id
  join_many: items on items.order_id = id
  join_one: hub is hubs_stored on hub.code = cat
  dimension: hub_gate is hub.gs.gate
  measure:
    order_n is count()
    item_n is items.count()
    opt_n is items.opts.count()
    extra_sum is items.opts.extra.sum()
}
`;

describe("R4: a join_many reached through a join_one, beside a pair table", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "true",
         },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": fanOutModel },
         seedSql: [
            ...seedSql.filter((sql) => !/TABLE (orders|pays) /.test(sql)),
            // Hub `a` has two gates of one name, `b` one.
            `CREATE TABLE orders AS SELECT * FROM (VALUES
      (1, 'a', 10.0), (2, 'a', 20.0), (3, 'b', 30.0), (5, 'b', 5.0)
    ) AS t(id, cat, amt)`,
            `CREATE TABLE hubs AS SELECT * FROM (VALUES ('a'), ('b')) AS t(code)`,
            `CREATE TABLE gates AS SELECT * FROM (VALUES
      (1, 'a', 'A1'), (2, 'a', 'A1'), (3, 'b', 'A1')
    ) AS t(gate_id, code, gate)`,
         ],
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   it("leaves no problem on the served copy", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   const cases = [
      [
         "a gate through it, leaf measures",
         "run: by_gate -> { group_by: cat; aggregate: opt_n, extra_sum }",
         false,
      ],
      [
         "a filter through it",
         "run: by_hub -> { where: hub.gs.gate = 'A1'; group_by: cat; aggregate: opt_n, extra_sum }",
         false,
      ],
      [
         "a group-by through it",
         "run: by_hub -> { group_by: hub.gs.gate; aggregate: opt_n, extra_sum }",
         false,
      ],
      [
         "a group-by on a dimension through it",
         "run: by_hub -> { group_by: hub_gate; aggregate: opt_n, item_n }",
         false,
      ],
      [
         "a filtered measure reading through it",
         "run: by_hub -> { group_by: cat; aggregate: r is opt_n { where: hub.gs.gate = 'A1' } }",
         false,
      ],
      [
         "the join_one's own field",
         "run: by_hub -> { group_by: hub.code; aggregate: order_n, opt_n }",
         true,
      ],
   ] as const;
   for (const [name, malloy, servedHere] of cases) {
      it(`${servedHere ? "serves" : "answers live"}: ${name}`, async () => {
         const run = await fixture.run({
            name,
            malloy,
            givens: { G: "A1" },
            ...(servedHere ? {} : { expectLive: true }),
         });
         expectPairServedVia(run, PATH_COUNTS, servedHere);
         expect(run.live.length).toBeGreaterThan(0);
         expect(rowDifference(run.live, run.served)).toBeUndefined();
      });
   }
});
