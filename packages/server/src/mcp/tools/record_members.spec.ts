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
import { routeList, routeOptions, throughFanout } from "../context/routes";
import { MAX_RECORD_MEMBERS_PER_SOURCE } from "./package_entities";

// A DuckDB STRUCT compiles to the same Malloy record type as a BigQuery
// RECORD, and a STRUCT[] to the same repeated record, so this stands in for
// both (issue #1526).
const SQL = `select 1 as order_id,
  {'city': 'Denver', 'region': 'West', 'address': {'zip': '80202'}} as delivery,
  {'city': 'Boulder', 'card': '4111'} as billing,
  [{'sku': 'a', 'qty': 2}, {'sku': 'b', 'qty': 1}] as lines,
  ['gift', 'rush'] as tags,
  {'ok': 1, 'a.b': 2, 'two words': 3, 'x y': {'z': 4}} as odd,
  7 as customer_id`;
const MODEL = `##! experimental.access_modifiers
source: customers is duckdb.sql("""select 7 as id, {'tier': 'gold'} as profile, [{'sku': 'x'}] as items""")

source: renamed is duckdb.sql("""select 1 as k, {'city': 'Denver'} as delivery""") extend {
  dimension: ship_to is delivery
} include { public: k, ship_to }

#(doc) One row per order.
source: orders is duckdb.sql("""${SQL}""") extend {
  measure: order_count is count()
  join_one: customer is customers on customer_id = customer.id
  #(doc) The city the order was delivered to.
  dimension: delivery_city is delivery.city
  #(classification) "pci"
  dimension: billing_details is billing
  dimension: ship_to is delivery
}
`;

let fixture: FcjIndexFixture;
beforeAll(async () => {
   fixture = await buildFcjIndexFixture(CLUSTER_SETTINGS, {
      "shop.malloy": MODEL,
   });
});
afterAll(async () => fixture.close());

const entity = (source: string, name: string) =>
   fixture.pkgIndex.directEntities.find(
      (e) => e.source === source && e.name === name,
   );
const shape = (source: string, name: string) => {
   const e = entity(source, name);
   return e
      ? {
           dataType: e.dataType,
           joinPath: e.joinPath,
           recordPath: e.recordPath,
           relationship: e.relationship,
        }
      : undefined;
};

describe("record members (#1526)", () => {
   it("indexes a record's members, nested records included, as the source's own fields", () => {
      expect(shape("orders", "delivery.region")).toEqual({
         dataType: "string",
         joinPath: undefined,
         recordPath: "delivery",
         relationship: undefined,
      });
      expect(shape("orders", "delivery.address.zip")).toEqual({
         dataType: "string",
         joinPath: undefined,
         recordPath: "delivery.address",
         relationship: undefined,
      });
      // Not described as reached through a join: the catalog node has no
      // join path, so the search text, the judge and value search treat it
      // as a field of orders.
      expect(
         fixture.index.catalog.nodes.get(
            "dimension:orders@shop.malloy:delivery.region",
         )?.joinPath,
      ).toBeUndefined();
   });

   it("marks a repeated record's members many, and leaves an array of scalars alone", () => {
      expect(shape("orders", "lines.sku")).toEqual({
         dataType: "string",
         joinPath: undefined,
         recordPath: "lines",
         relationship: "many",
      });
      expect(
         fixture.pkgIndex.directEntities.filter((e) =>
            e.name.startsWith("tags."),
         ),
      ).toEqual([]);
   });

   it("reaches a joined source's record members through the join, as it does the source's other fields", () => {
      const tier = fixture.index.catalog.nodes.get(
         "dimension:customers@shop.malloy:profile.tier",
      )!;
      expect(tier.joinPath).toBeUndefined();
      // Not copied onto orders as a field of its own...
      expect(
         fixture.index.catalog.nodes.has(
            "dimension:orders@shop.malloy:customer.profile.tier",
         ),
      ).toBe(false);
      // ...but reached from it, like customer.id is.
      expect(routeList(fixture.index.catalog, tier).map((r) => r.id)).toEqual([
         "dimension:orders:customer.profile.tier",
      ]);
   });

   it("says many for a repeated-record member reached through a join_one, wherever the route is shown", () => {
      const sku = fixture.index.catalog.nodes.get(
         "dimension:customers@shop.malloy:items.sku",
      )!;
      expect(sku.relationship).toBe("many");
      // The route options the judge is shown.
      expect(
         routeOptions(
            fixture.index.catalog,
            sku,
            new Set(["orders@shop.malloy"]),
            3,
         ).map((o) => [o.id, o.fanout]),
      ).toEqual([["dimension:orders:customer.items.sku", "many"]]);
      expect(throughFanout("one", "many")).toBe("many");
      expect(throughFanout("one", undefined)).toBe("one");
   });

   it("lists a renamed record's members under the new name when the raw column is hidden", () => {
      expect(entity("renamed", "ship_to.city")?.recordPath).toBe("ship_to");
      expect(entity("renamed", "delivery.city")).toBeUndefined();
   });

   it("leaves out a member whose name is not a plain identifier, and everything under it", () => {
      // `odd.a.b` would read as a nested record, and none of these compile
      // as written without backticks.
      expect(
         fixture.pkgIndex.directEntities
            .filter((e) => e.source === "orders" && e.name.startsWith("odd."))
            .map((e) => e.name),
      ).toEqual(["odd.ok"]);
   });

   it("gives a member its record's classification", () => {
      expect(entity("orders", "billing_details")?.classifications).toEqual([
         "pci",
      ]);
      // Listed under the raw record's name, classified by its alias.
      expect(entity("orders", "billing.card")?.classifications).toEqual([
         "pci",
      ]);
   });

   it("folds a raw member into the documented dimension declared over it", () => {
      expect(entity("orders", "delivery.city")).toBeUndefined();
      const declared = entity("orders", "delivery_city");
      expect(declared?.aliases).toEqual(["delivery.city"]);
      expect(declared?.doc).toBe("The city the order was delivered to.");
   });

   it("lists a record's members once, under its own name, not again under an alias of the record", () => {
      expect(entity("orders", "ship_to")?.dataType).toBe("record");
      expect(
         fixture.pkgIndex.directEntities.filter(
            (e) => e.source === "orders" && e.name.startsWith("ship_to."),
         ),
      ).toEqual([]);
   });

   it("finds a member by its words", () => {
      const [result] = find({
         index: fixture.index,
         targets: [
            {
               index: 0,
               type: "dimension",
               text: "billing card",
               kinds: KINDS_BY_FIND_TARGET.dimension,
            },
         ],
         queries: new Map(),
         perTarget: 30,
         keyword: true,
         values: false,
         allow: () => true,
      });
      expect(result.candidates[0]?.key).toBe(
         "dimension:orders@shop.malloy:billing.card",
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
               "run: orders -> { group_by: delivery.city, delivery.address.zip, customer.profile.tier, billing.card; aggregate: order_count }",
            ),
         ).toEqual([
            {
               city: "Denver",
               zip: "80202",
               tier: "gold",
               card: "4111",
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

describe("record members on a wide table", () => {
   it(`indexes at most ${MAX_RECORD_MEMBERS_PER_SOURCE} members per source`, async () => {
      const members = Array.from(
         { length: MAX_RECORD_MEMBERS_PER_SOURCE + 20 },
         (_, i) => `'m${i}': ${i}`,
      ).join(", ");
      const wide = await buildFcjIndexFixture(CLUSTER_SETTINGS, {
         "wide.malloy": `source: events is duckdb.sql("""select {${members}} as params""")`,
      });
      try {
         const indexed = wide.pkgIndex.directEntities.filter(
            (e) => e.recordPath !== undefined,
         );
         expect(indexed).toHaveLength(MAX_RECORD_MEMBERS_PER_SOURCE);
         expect(indexed[0].name).toBe("params.m0");
      } finally {
         await wide.close();
      }
   });

   it("counts a source's own members before a joined source's, even for a record declared after the join", async () => {
      const members = Array.from(
         { length: MAX_RECORD_MEMBERS_PER_SOURCE + 20 },
         (_, i) => `'m${i}': ${i}`,
      ).join(", ");
      // The cap is shared, and spent in schema order. Malloy lists a source's
      // own fields before its joins, even ship_to here, declared after the
      // join, so the joined record cannot use up the source's own members.
      const joined = await buildFcjIndexFixture(CLUSTER_SETTINGS, {
         "joined.malloy": `##! experimental.access_modifiers
source: events is duckdb.sql("""select 1 as k, {${members}} as params""")
source: orders is duckdb.sql("""select 1 as k, {'city': 'Denver'} as delivery""") extend {
  join_one: event is events on k = event.k
  dimension: ship_to is delivery
} include { public: k, event, ship_to }
`,
      });
      try {
         expect(
            joined.pkgIndex.directEntities.find(
               (e) => e.source === "orders" && e.name === "ship_to.city",
            )?.recordPath,
         ).toBe("ship_to");
      } finally {
         await joined.close();
      }
   });
});
