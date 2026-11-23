// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A stored source that joins other stored sources, each built from a query
 * with a `where:` of its own (a string compared, a window's `recency = 1`), is
 * served from its table, equal to live, whether or not the query reads the
 * join. Before, the row-scope walk followed an unread joined source's query
 * back to its warehouse columns, which no table the answer read holds, so
 * every query on the source, `count()` included, answered live: "scopes its
 * rows by a column a stored table does not hold exactly" (or, for a join
 * whose query compares no string, "does not compare as its source does").
 *
 * The shape: an unpersisted query stage over a warehouse table that joins an
 * unpersisted aggregate (`is_cancelled is ev.cancel_count > 0`); a stored
 * parent over it, `include { public: … }` plus `extend` with joins to
 * stored sources (one keyed through another join); a stored child adding
 * `where: not is_cancelled`, and a stored grandchild with a `where:` of its
 * own; all queried through an entry model that imports and exports them. A
 * row-level gate on an extension of the child keeps each caller's rows.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   type Answer,
   openPostgresLake,
   POSTGRES_URL,
   type PostgresLake,
} from "./postgres_lake";

const SEED = [
   `CREATE TABLE steps AS SELECT * FROM (VALUES
      (1, 1), (1, 2), (2, 1), (3, 1), (3, 3), (4, 2), (5, 1), (5, 2), (6, 3)
    ) AS t(order_id, step_rank)`,
   `CREATE TABLE events AS SELECT * FROM (VALUES
      (1, 'note'), (2, 'cancel'), (2, 'note'), (3, 'note'), (5, 'cancel'), (6, 'note')
    ) AS t(order_id, kind)`,
   `CREATE TABLE statuses AS SELECT * FROM (VALUES
      (1, 'open', 1), (1, 'Won', 2), (2, 'open', 1), (3, 'open', 1),
      (3, 'void', 2), (4, 'lost', 1), (5, 'open', 1), (6, 'open', 1), (6, 'won', 3)
    ) AS t(order_id, status, seq)`,
   `CREATE TABLE attrs AS SELECT * FROM (VALUES
      (1, 'a1', 'east'), (2, 'a2', 'west'), (3, 'a1', 'East'), (4, 'a3', 'west'),
      (5, 'a2', 'east'), (6, NULL, 'west')
    ) AS t(order_id, account_id, region)`,
   `CREATE TABLE accounts AS SELECT * FROM (VALUES
      ('a1', 'acme', 1), ('a2', 'Globex', 1), ('a3', 'initech', 0)
    ) AS t(account_id, account_name, active)`,
];

const ORDERS = `##! experimental { persistence access_modifiers givens }

given: RANKS :: number[]

source: events_live is warehouse.table('public.events') -> {
  group_by: order_id
  aggregate:
    event_count is count()
    cancel_count is count() { where: kind = 'cancel' }
}

#@ persist name="t_status_now" storage="lake"
source: status_now is warehouse.table('public.statuses') -> {
  where: status != 'void'
  group_by: order_id, status, seq
  calculate: recency is row_number() { partition_by: order_id; order_by: seq desc }
} -> {
  where: recency = 1
  select: order_id, status
}

#@ persist name="t_attrs" storage="lake"
source: attrs is warehouse.table('public.attrs') -> {
  where: region != 'north'
  select: *
}

#@ persist name="t_accounts" storage="lake"
source: accounts is warehouse.table('public.accounts') -> {
  where: active >= 0
  select: *
}

source: progress is warehouse.table('public.steps') extend {
  join_one: ev is events_live on order_id = ev.order_id
} -> {
  group_by: order_id, is_cancelled is ev.cancel_count > 0
  aggregate: step_count is count(), top_rank is step_rank.max()
}

#@ persist name="t_orders_all" storage="lake"
source: orders_all is progress include {
  public: order_id, is_cancelled, step_count, top_rank
} extend {
  join_one: status_now on order_id = status_now.order_id
  join_one: attrs on order_id = attrs.order_id
  join_one: accounts on attrs.account_id = accounts.account_id
  dimension: account_name is accounts.account_name
  measure: order_count is count()
}

#@ persist name="t_orders" storage="lake"
source: orders is orders_all extend { where: not is_cancelled }

#@ persist name="t_deep_orders" storage="lake"
source: deep_orders is orders extend { where: top_rank >= 2 }

#(access_filter) top_rank in $RANKS
source: ranked_orders is orders extend {}
`;

const INDEX = `##! experimental { persistence access_modifiers givens }
import 'orders.malloy'
export { orders_all, orders, deep_orders, ranked_orders }
`;

/** Rows in one order, for answers whose order the query leaves open. */
const sorted = (rows: Answer["rows"]) =>
   rows.map((r) => JSON.stringify(r)).sort();

interface Case {
   name: string;
   query: string;
   givens?: Record<string, unknown>;
}

const CASES: Case[] = [
   { name: "a count", query: "run: orders -> { aggregate: c is count() }" },
   {
      name: "a count on the parent",
      query: "run: orders_all -> { aggregate: order_count }",
   },
   {
      name: "a group_by on the boolean, on the parent",
      query: "run: orders_all -> { group_by: is_cancelled; aggregate: order_count; order_by: is_cancelled }",
   },
   {
      name: "a group_by on the boolean",
      query: "run: orders -> { group_by: is_cancelled; aggregate: order_count; order_by: is_cancelled }",
   },
   {
      // Each unordered by the string: ordering one is the result rule's
      // (an `en_US` source orders strings as the stored table does not).
      name: "through the window-filtered join",
      query: "run: orders -> { group_by: s is status_now.status; aggregate: order_count }",
   },
   {
      name: "through the string-filtered join",
      query: "run: orders -> { group_by: r is attrs.region; aggregate: order_count }",
   },
   {
      name: "through a join keyed through another join",
      query: "run: orders -> { group_by: account_name; aggregate: order_count }",
   },
   {
      name: "a grandchild with a where: of its own",
      query: "run: deep_orders -> { group_by: order_id, top_rank; order_by: order_id }",
   },
   {
      name: "a gate, one caller",
      query: "run: ranked_orders -> { group_by: order_id; aggregate: order_count; order_by: order_id }",
      givens: { RANKS: [2] },
   },
   {
      name: "a gate, another caller",
      query: "run: ranked_orders -> { group_by: order_id; aggregate: order_count; order_by: order_id }",
      givens: { RANKS: [3] },
   },
];

describe.skipIf(!POSTGRES_URL)(
   "a stored source joining stored sources whose builds filtered their rows (Postgres into DuckLake)",
   () => {
      let lake: PostgresLake;
      const live: Record<string, Answer> = {};
      const served: Record<string, Answer> = {};

      beforeAll(async () => {
         lake = await openPostgresLake(
            "rrj",
            { "orders.malloy": ORDERS, "index.malloy": INDEX },
            SEED,
         );
         for (const c of CASES) {
            live[c.name] = await lake.ask("index.malloy", c.query, c.givens);
         }
         await lake.build();
         for (const c of CASES) {
            served[c.name] = await lake.ask("index.malloy", c.query, c.givens);
         }
      }, 300_000);

      afterAll(async () => {
         await lake?.close();
      });

      it("answers live as the data says, before any build", () => {
         expect(live["a count"].rows).toEqual([{ c: "4" }]);
         expect(live["a count on the parent"].rows).toEqual([
            { order_count: "6" },
         ]);
         expect(live["a gate, one caller"].rows).toEqual([
            { order_id: "1", order_count: "1" },
            { order_id: "4", order_count: "1" },
         ]);
         expect(live["a gate, another caller"].rows).toEqual([
            { order_id: "3", order_count: "1" },
            { order_id: "6", order_count: "1" },
         ]);
         expect(
            live["through a join keyed through another join"].rows,
         ).toContainEqual({ account_name: null, order_count: "1" });
      });

      for (const c of CASES) {
         it(`${c.name}: answers from a stored table, equal to live`, () => {
            expect(sorted(served[c.name].rows)).toEqual(
               sorted(live[c.name].rows),
            );
            expect(
               [served[c.name].kind, served[c.name].detail],
               c.name,
            ).toEqual(["materialized", undefined]);
         });
      }
   },
);
