// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The text a pair table's source is re-rooted on for a view or query built
 * over it (pair_reroot.ts), for one join, a path of two and a table of two
 * blocks, from plans the companion made: the parent rows the flag marks, a
 * join_many per level keyed by the level's key, renamed back to the joined
 * source's names, filtered on the level flag above the leaf and on the
 * block.
 */

import { describe, expect, it } from "bun:test";
import {
   materializePlanNamed,
   planPackages,
} from "../../../test_helpers/materialization_fixture";
import {
   pairLeafContractKeys,
   pairRerootBase,
   pairRerootJoins,
} from "./pair_reroot";

const MODEL = `##! experimental { persistence }
source: opts is duckdb.sql("""select 1 as option_id, 1 as item_id, 0.5 as extra""") extend {
  primary_key: option_id
}
source: notes is duckdb.sql("""select 1 as note_id, 1 as order_id, 'n' as body""") extend {
  primary_key: note_id
}
source: items is duckdb.sql("""select 1 as item_id, 1 as order_id, 2.0 as price""") extend {
  primary_key: item_id
  join_many: options is opts on options.item_id = item_id
}
source: items1 is duckdb.sql("""select 1 as item_id, 1 as order_id, 2.0 as price""") extend {
  primary_key: item_id
}
source: raw is duckdb.sql("""select 1 as id, 'x' as cust""")
`;

const packages = planPackages();

async function planOf(declaration: string) {
   const { c } = await packages.plan({
      "m.malloy": `${MODEL}${declaration}\n`,
   });
   return materializePlanNamed(c, "orders")!;
}

describe("a pair table's source re-rooted with its join put back", () => {
   it("one join: the parent rows, and a join_many to the rows that hold a child", async () => {
      const plan = await planOf(`#(materialize) inline=["items"]
source: orders is raw extend {
  primary_key: id
  join_many: items is items1 on items.order_id = id
}`);
      expect(pairRerootBase(plan)).toBe(`orders__mat -> {
  where: __parent_row
  select: id, cust
} extend {
  primary_key: id
  join_many: items is orders__mat -> {
    where: items__item_id is not null
    select: __pair_parent_key is id, item_id is items__item_id, order_id is items__order_id, price is items__price
  } extend {
    primary_key: item_id
  } on items.__pair_parent_key = id
}`);
      expect(pairRerootJoins(plan)).toEqual(["items"]);
      expect(pairLeafContractKeys(plan)).toEqual(["leaf:items.item_id"]);
   });

   it("a path of two: one join per level, the middle one on its level flag", async () => {
      const plan = await planOf(`#(materialize) inline=["items.options"]
source: orders is raw extend {
  primary_key: id
  join_many: items is items on items.order_id = id
}`);
      expect(pairRerootBase(plan)).toBe(`orders__mat -> {
  where: __parent_row
  select: id, cust
} extend {
  primary_key: id
  join_many: items is orders__mat -> {
    where: __child_row and items__item_id is not null
    select: __pair_parent_key is id, item_id is items__item_id, order_id is items__order_id, price is items__price
  } extend {
    primary_key: item_id
    join_many: options is orders__mat -> {
      where: items__options__option_id is not null
      select: __pair_parent_key is items__item_id, option_id is items__options__option_id, item_id is items__options__item_id, extra is items__options__extra
    } extend {
      primary_key: option_id
    } on options.__pair_parent_key = item_id
  } on items.__pair_parent_key = id
}`);
      expect(pairLeafContractKeys(plan)).toEqual([
         "leaf:items.options.option_id",
      ]);
   });

   it("two blocks: each path its own join, every read on its block's rows", async () => {
      const plan = await planOf(`#(materialize) inline=["items", "notes"]
source: orders is raw extend {
  primary_key: id
  join_many: items is items1 on items.order_id = id
  join_many: notes is notes on notes.order_id = id
}`);
      expect(pairRerootBase(plan)).toBe(`orders__mat -> {
  where: __parent_row and __pair_lane = 0
  select: id, cust
} extend {
  primary_key: id
  join_many: items is orders__mat -> {
    where: items__item_id is not null and __pair_lane = 0
    select: __pair_parent_key is id, item_id is items__item_id, order_id is items__order_id, price is items__price
  } extend {
    primary_key: item_id
  } on items.__pair_parent_key = id
  join_many: notes is orders__mat -> {
    where: notes__note_id is not null and __pair_lane = 1
    select: __pair_parent_key is id, note_id is notes__note_id, order_id is notes__order_id, body is notes__body
  } extend {
    primary_key: note_id
  } on notes.__pair_parent_key = id
}`);
      expect(pairRerootJoins(plan)).toEqual(["items", "notes"]);
      expect(pairLeafContractKeys(plan)).toEqual([
         "leaf:items.item_id",
         "leaf:notes.note_id",
      ]);
   });

   it("has no text for a plain table", async () => {
      const plan = await planOf(`#(materialize)
source: orders is raw extend { primary_key: id }`);
      expect(pairRerootBase(plan)).toBeUndefined();
      expect(pairLeafContractKeys(plan)).toEqual([]);
   });
});
