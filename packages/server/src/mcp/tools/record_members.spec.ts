// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   buildFcjIndexFixture,
   CLUSTER_SETTINGS,
   type FcjIndexFixture,
} from "../../test_helpers/fcj_index_fixture";
import { find, KINDS_BY_FIND_TARGET } from "../context/find";

// A DuckDB STRUCT compiles to the same Malloy record type as a BigQuery
// RECORD, and a STRUCT[] to the same repeated record, so this stands in for
// both (issue #1526).
const SQL = `select 1 as order_id,
  {'city': 'Denver', 'region': 'West', 'address': {'zip': '80202'}} as delivery,
  [{'sku': 'a', 'qty': 2}, {'sku': 'b', 'qty': 1}] as lines,
  ['gift', 'rush'] as tags,
  7 as customer_id`;
const MODEL = `
source: customers is duckdb.sql("""select 7 as id, {'tier': 'gold'} as profile""")

#(doc) One row per order.
source: orders is duckdb.sql("""${SQL}""") extend {
  measure: order_count is count()
  join_one: customer is customers on customer_id = customer.id
}
`;

let fixture: FcjIndexFixture;
beforeAll(async () => {
   fixture = await buildFcjIndexFixture(CLUSTER_SETTINGS, {
      "shop.malloy": MODEL,
   });
});
afterAll(async () => fixture.close());

type Row = [string, string | undefined, string | undefined, string | undefined];
const membersOf = (): Row[] =>
   fixture.pkgIndex.directEntities
      .filter((e) => e.source === "orders" && e.kind === "dimension")
      .map((e) => [e.name, e.dataType, e.joinPath, e.relationship] as Row);

describe("record members (#1526)", () => {
   it("indexes a record's members, nested records included, under dotted names", () => {
      expect(membersOf()).toEqual(
         expect.arrayContaining([
            ["delivery", "record", undefined, undefined],
            ["delivery.city", "string", "delivery", "one"],
            ["delivery.region", "string", "delivery", "one"],
            ["delivery.address", "record", "delivery", "one"],
            ["delivery.address.zip", "string", "delivery.address", "one"],
         ] as Row[]),
      );
   });

   it("marks a repeated record's members many, as a fan-out join's are, and leaves an array of scalars alone", () => {
      const rows = membersOf();
      expect(rows).toEqual(
         expect.arrayContaining([
            ["lines", "array", undefined, undefined],
            ["lines.sku", "string", "lines", "many"],
            ["lines.qty", "number", "lines", "many"],
         ] as Row[]),
      );
      expect(rows.filter(([name]) => name.startsWith("tags."))).toEqual([]);
   });

   it("indexes a joined source's record members on that source, and reaches them through the join like its other fields", () => {
      const nodes = [...fixture.index.catalog.nodes.keys()];
      // On the source that holds the record.
      expect(nodes).toContain("dimension:customers@shop.malloy:profile.tier");
      // Not copied onto the joining source: like customer.id, the engine
      // reaches it from orders through the customer join.
      expect(nodes).not.toContain(
         "dimension:orders@shop.malloy:customer.profile.tier",
      );
      expect(nodes).not.toContain("dimension:orders@shop.malloy:customer.id");
      // The entity index still has the joined copy, for listings.
      expect(
         fixture.pkgIndex.retrievalEntities
            .filter((e) => e.name === "customer.profile.tier")
            .map((e) => [e.source, e.joinPath, e.relationship]),
      ).toEqual([["orders", "customer.profile", "one"]]);
   });

   it("finds a member by its words, as its own field on the source that holds it", () => {
      const target = {
         index: 0,
         type: "dimension" as const,
         text: "delivery city",
         kinds: KINDS_BY_FIND_TARGET.dimension,
      };
      const [result] = find({
         index: fixture.index,
         targets: [target],
         queries: new Map(),
         perTarget: 30,
         keyword: true,
         values: false,
         allow: () => true,
      });
      expect(result.candidates[0]?.key).toBe(
         "dimension:orders@shop.malloy:delivery.city",
      );
   });

   it("names only fields a Malloy query can use", async () => {
      const connection = new DuckDBConnection("duckdb", ":memory:");
      try {
         const model = new SingleConnectionRuntime({
            connection,
         } as never).loadModel(MODEL);
         const run = async (q: string) =>
            (await model.loadQuery(q).run()).data.toObject();
         expect(
            await run(
               "run: orders -> { group_by: delivery.city, delivery.address.zip, customer.profile.tier; aggregate: order_count }",
            ),
         ).toEqual([
            {
               city: "Denver",
               zip: "80202",
               tier: "gold",
               order_count: 1,
            },
         ]);
         // Grouping by a repeated record's member counts each element.
         expect(
            await run(
               "run: orders -> { group_by: lines.sku; aggregate: n is count() } -> { select: sku, n; order_by: sku }",
            ),
         ).toEqual([
            { sku: "a", n: 1 },
            { sku: "b", n: 1 },
         ]);
      } finally {
         await connection.close();
      }
   });
});
