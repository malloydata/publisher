// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { type Connection } from "@malloydata/malloy";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Model } from "../../service/model";
import type { PackageLookup } from "../../service/environment_ports";
import { fakeOf } from "../../test_helpers/store_fakes";
import { RetrievalRegistry } from "../registry";
import { valueFetcherFor } from "../search/values";
import { buildSourceSummaryInputs } from "../index/source_summaries";
import { compileFcjFixture } from "../../test_helpers/fcj_fixture";
import { DEFAULT_PACKAGE_RETRIEVAL } from "../../service/package_retrieval_settings";
import { buildCatalog, catalogInputOf } from "./catalog";
import { registerGetContextTool } from "../../mcp/tools/get_context_tool";
import { testRetrieval } from "../../test_helpers/test_retrieval";
import {
   buildFcjIndexFixture,
   CLUSTER_SETTINGS,
   type FcjIndexFixture,
} from "../../test_helpers/fcj_index_fixture";
import { find, KINDS_BY_FIND_TARGET, type FindTarget } from "../search/find";
import { alsoLikelyFields } from "../search/also_likely";
import { judgeOptions, renderJudgePrompt } from "../search/judge";
import { respond } from "../search/respond";
import { routeKey } from "../search/routes";
import { connect } from "../search/connect";
import { toUnit } from "../index/vectors";
import { embedText } from "../../test_helpers/get_context_llm_harness";
import { routeOptions, routesOf, throughFanout } from "../search/routes";
import {
   compileJoinFixture,
   storeServing,
} from "../../test_helpers/get_context_join_fixture";
import { projectEntity, toSourceResults } from "../search/response";
import {
   getPackageIndex,
   MALLOY_RESERVED_WORDS,
   MAX_RECORD_MEMBERS_PER_SOURCE,
   fieldPathOf,
} from "./package_entities";

// A DuckDB STRUCT compiles to the same Malloy record type as a BigQuery
// RECORD, and a STRUCT[] to the same repeated record, so this stands in for
// both (issue #1526).
const SQL = `select 1 as order_id,
  {'city': 'Denver', 'region': 'West', 'address': {'zip': '80202'}} as delivery,
  {'city': 'Boulder', 'card': '4111'} as billing,
  [{'sku': 'a', 'qty': 2}, {'sku': 'b', 'qty': 1}] as lines,
  ['gift', 'rush'] as tags,
  {'ok': 1, 'a.b': 2, 'two words': 3, 'x y': {'z': 4}} as odd,
  {'source': 'google', 'medium': 'cpc', 'date': {'year': 2024}} as traffic_source,
  7 as customer_id`;
const MODEL = `##! experimental.access_modifiers
source: customers is duckdb.sql("""select 7 as id, {'tier': 'gold'} as profile, [{'sku': 'x'}] as items""")

source: visits is duckdb.sql("""select {'source': 'google', 'year': 2024} as "date", {'source': 'bing', 'city': 'Lyon'} as traffic""") extend {
  dimension: src is traffic.\`source\`
}

source: tagged is duckdb.sql("""select 'A' as order_ref, ['red', 'blue'] as tags union all select 'B', ['red']""")

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
  dimension: qty_each is lines.qty
  dimension: addr is delivery.address
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

   it("marks a repeated record's members many, and an array of scalars' element too", () => {
      expect(shape("orders", "lines.sku")).toEqual({
         dataType: "string",
         joinPath: undefined,
         recordPath: "lines",
         relationship: "many",
      });
      // Malloy reads a scalar array's values as `tags.each`, its only member.
      expect(
         fixture.pkgIndex.directEntities
            .filter((e) => e.source === "orders" && e.name.startsWith("tags."))
            .map((e) => e.name),
      ).toEqual(["tags.each"]);
      expect(shape("orders", "tags.each")).toEqual({
         dataType: "string",
         joinPath: undefined,
         recordPath: "tags",
         relationship: "many",
      });
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
      expect(routesOf(fixture.index.catalog, tier)).toEqual([
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

   it("keeps many on a dimension declared over a repeated record's member", () => {
      expect(entity("orders", "lines.qty")).toBeUndefined();
      expect(entity("orders", "qty_each")).toMatchObject({
         aliases: ["lines.qty"],
         relationship: "many",
      });
   });

   it("lists a nested record's members once, not again under a dimension declared over it", () => {
      expect(entity("orders", "addr")?.dataType).toBe("record");
      expect(entity("orders", "delivery.address.zip")).toBeDefined();
      expect(
         fixture.pkgIndex.directEntities
            .filter((e) => e.source === "orders" && e.name.startsWith("addr."))
            .map((e) => e.name),
      ).toEqual([]);
   });

   it("lists a record's members once, under its own name, not again under an alias of the record", () => {
      expect(entity("orders", "ship_to")?.dataType).toBe("record");
      expect(
         fixture.pkgIndex.directEntities.filter(
            (e) => e.source === "orders" && e.name.startsWith("ship_to."),
         ),
      ).toEqual([]);
   });

   it("backticks a member named by a reserved word, so the name compiles", () => {
      expect(
         fixture.pkgIndex.directEntities
            .filter(
               (e) =>
                  e.source === "orders" && e.name.startsWith("traffic_source."),
            )
            .map((e) => [e.name, e.recordPath]),
      ).toEqual([
         ["traffic_source.`source`", "traffic_source"],
         ["traffic_source.medium", "traffic_source"],
         ["traffic_source.`date`", "traffic_source"],
         ["traffic_source.`date`.`year`", "traffic_source.`date`"],
      ]);
   });

   it("backticks a record whose own column name is a reserved word", () => {
      expect(
         fixture.pkgIndex.directEntities
            .filter((e) => e.source === "visits" && e.recordPath)
            .map((e) => [e.name, e.recordPath]),
      ).toEqual([
         ["`date`.`source`", "`date`"],
         ["`date`.`year`", "`date`"],
         ["traffic.city", "traffic"],
      ]);
      expect(fieldPathOf(entity("visits", "`date`.`year`")!)).toEqual([
         "date",
         "year",
      ]);
   });

   it("folds a member named by a reserved word into the dimension declared over it", () => {
      expect(entity("visits", "traffic.`source`")).toBeUndefined();
      expect(entity("visits", "src")?.aliases).toEqual(["traffic.`source`"]);
   });

   it("knows Malloy's reserved words, every one of them", async () => {
      // Malloy does not export its keyword list; this reads the copy it
      // generates from its grammar, so a new keyword fails here.
      const dir = path.dirname(
         require.resolve("@malloydata/malloy/package.json"),
      );
      const { KEYWORD_DISPLAY_NAMES } = (await import(
         path.join(dir, "dist/lang/lib/Malloy/keyword-display-names.js")
      )) as { KEYWORD_DISPLAY_NAMES: Record<string, string> };
      const bare = Object.values(KEYWORD_DISPLAY_NAMES)
         .filter((word) => !word.endsWith(":"))
         .sort();
      expect([...MALLOY_RESERVED_WORDS].sort()).toEqual(bare);
   });

   it("splits a member's name into the segments of its field reference", () => {
      expect(fieldPathOf(entity("orders", "traffic_source.`source`")!)).toEqual(
         ["traffic_source", "source"],
      );
      expect(fieldPathOf(entity("orders", "delivery.address.zip")!)).toEqual([
         "delivery",
         "address",
         "zip",
      ]);
      // Not a member: one name, even with a dot in it.
      expect(fieldPathOf({ name: "a.b" })).toEqual(["a.b"]);
      expect(
         fieldPathOf({
            name: "hc.profile.tier",
            joinPath: "hc",
            recordPath: "profile",
         }),
      ).toEqual(["hc", "profile", "tier"]);
      expect(fieldPathOf({ name: "hc.a.b", joinPath: "hc" })).toEqual([
         "hc",
         "a.b",
      ]);
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
         // Every member name the index holds, reserved words included.
         const members = fixture.pkgIndex.directEntities
            .filter(
               (e) =>
                  e.source === "orders" &&
                  e.recordPath !== undefined &&
                  e.dataType !== "record",
            )
            .map((e, i) => `m${i} is ${e.name}`);
         expect(members.some((m) => m.endsWith(" is delivery.region"))).toBe(
            true,
         );
         expect(
            await run(
               `run: orders -> { group_by: ${members.join(", ")} } -> { aggregate: rows is count() }`,
            ),
         ).toEqual([{ rows: 4 }]);
         expect(
            await run(
               "run: orders -> { group_by: s is traffic_source.`source`, y is traffic_source.`date`.`year` }",
            ),
         ).toEqual([{ s: "google", y: 2024 }]);
         const visits = fixture.pkgIndex.directEntities
            .filter((e) => e.source === "visits" && e.recordPath)
            .map((e, i) => `v${i} is ${e.name}`);
         expect(
            await run(`run: visits -> { group_by: ${visits.join(", ")} }`),
         ).toEqual([{ v0: "google", v1: 2024, v2: "Lyon" }]);
         // Grouping by a repeated record's member gives a group per value.
         expect(
            await run(
               "run: orders -> { group_by: lines.sku; aggregate: n is count() } -> { select: sku, n; order_by: sku }",
            ),
         ).toEqual([
            { sku: "a", n: 1 },
            { sku: "b", n: 1 },
         ]);
         // The question from #1526: two orders tagged [red, blue] and [red]
         // hold two distinct tags, counted through the indexed `tags.each`.
         expect(
            await run(
               "run: tagged -> { aggregate: distinct_tags is count(tags.each) }",
            ),
         ).toEqual([{ distinct_tags: 2 }]);
         expect(
            await run(
               "run: tagged -> { group_by: tag is tags.each; aggregate: orders is count(); order_by: tag }",
            ),
         ).toEqual([
            { tag: "blue", orders: 1 },
            { tag: "red", orders: 2 },
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

// BigQuery with `dbmeta` on annotates the fields nested in a RECORD with
// their column descriptions, as it does top-level columns (#1492).
describe("record members of joined sources, on a budget", () => {
   it("spends no member budget on a joined record its own source lists", async () => {
      const members = Array.from(
         { length: MAX_RECORD_MEMBERS_PER_SOURCE },
         (_, i) => `'m${i}': ${i}`,
      ).join(", ");
      // events lists params itself, so orders reaches params' members by
      // route; walking them on orders too would use up the budget before
      // the inline-SQL join's record, whose members exist nowhere else.
      const f = await buildFcjIndexFixture(CLUSTER_SETTINGS, {
         "m.malloy": `
source: events is duckdb.sql("""select 1 as k, {${members}} as params""")
source: orders is duckdb.sql("""select 1 as k""") extend {
  join_one: event is events on k = event.k
  join_one: geo is duckdb.sql("""select 1 as k, {'city': 'Denver'} as place""") on k = geo.k
}
`,
      });
      try {
         const own = (source: string) =>
            f.pkgIndex.directEntities
               .filter((e) => e.source === source && e.recordPath)
               .map((e) => e.name);
         expect(own("orders")).toEqual(["geo.place.city"]);
         expect(own("events")).toHaveLength(MAX_RECORD_MEMBERS_PER_SOURCE);
      } finally {
         await f.close();
      }
   });
});

describe("record members with database descriptions", () => {
   const ORDERS = `select 1 as id, 7 as customer_id, {'city': 'Denver', 'address': {'zip': '80202'}} as delivery, [{'sku': 'a'}] as lines`;
   const CUSTOMERS = `select 7 as id, {'tier': 'gold'} as profile`;
   const DESCRIBED_MODEL = `
source: customers is duckdb.sql("${CUSTOMERS}")

source: orders is duckdb.sql("${ORDERS}") extend {
  join_one: customer is customers on customer_id = customer.id
  #(doc) The city the order was delivered to.
  dimension: delivery_city is delivery.city
}

source: shipments is duckdb.sql("${ORDERS}") extend {
  dimension: ship_city is delivery.city
}
`;

   async function described() {
      const { pkg } = await compileJoinFixture("m.malloy", {
         modelText: DESCRIBED_MODEL,
         dbmeta: {
            [ORDERS]: {
               columns: {
                  "delivery.city": "City of delivery.",
                  "delivery.address.zip": "Delivery postcode.",
                  "lines.sku": "Stock keeping unit.",
               },
            },
            [CUSTOMERS]: { columns: { "profile.tier": "Loyalty tier." } },
         },
      });
      const index = await getPackageIndex(
         new WeakMap(),
         storeServing(pkg),
         "env",
         "pkg",
      );
      const get = (source: string, name: string) =>
         [...index.byId.values()].find(
            (e) => e.source === source && e.name === name,
         );
      return { index, get };
   }

   it("take a member's own description, nested and repeated records included", async () => {
      const { get } = await described();
      expect(get("orders", "delivery.address.zip")).toMatchObject({
         doc: "Delivery postcode.",
         embedDoc: "Delivery postcode.",
         docSource: "dbmeta",
      });
      expect(get("orders", "lines.sku")).toMatchObject({
         doc: "Stock keeping unit.",
         docSource: "dbmeta",
      });
   });

   it("document a member of a joined source", async () => {
      const { get } = await described();
      expect(get("customers", "profile.tier")).toMatchObject({
         doc: "Loyalty tier.",
         docSource: "dbmeta",
      });
   });

   it("give way to the #(doc) of a dimension declared over the member", async () => {
      const { get } = await described();
      const city = get("orders", "delivery_city");
      expect(city?.doc).toBe("The city the order was delivered to.");
      expect(city?.docSource).toBeUndefined();
      expect(city?.aliases).toContain("delivery.city");
   });

   it("document an undocumented dimension declared over the member", async () => {
      const { get } = await described();
      expect(get("shipments", "ship_city")).toMatchObject({
         doc: "City of delivery.",
         docSource: "dbmeta",
      });
   });

   it("mark the member's description in the response", async () => {
      const { index, get } = await described();
      const rows = [get("orders", "delivery.address.zip")!].map((e) =>
         projectEntity(e, "env", "pkg"),
      );
      const [card] = toSourceResults(rows, index.sourceContext, "env", "pkg");
      expect(card?.entities?.[0]).toMatchObject({
         name: "delivery.address.zip",
         description: "Delivery postcode.",
         description_source: "dbmeta",
      });
   });
});

// A gated source's values are read at query time with the caller's givens
// (respond.ts), through the same path as any query.
describe("reading a record member's values on a gated source", () => {
   it("quotes each segment of the field's path, reserved word and join included", async () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "record-values-"));
      const duckdb = new DuckDBConnection("duckdb", ":memory:", dir);
      try {
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            `##! experimental.givens
given: role :: string
#(authorize) 'analyst' = $role
source: visits is duckdb.sql("""select 7 as cid, {'source': 'google', 'city': 'Denver'} as traffic union all select 8, {'source': 'bing', 'city': 'Boulder'}""") extend {
  join_one: hc is duckdb.sql("""select 7 as id, {'tier': 'gold'} as profile union all select 8, {'tier': 'tin'}""") on cid = hc.id
}
`,
         );
         const model = await Model.create(
            "pkg",
            dir,
            "m.malloy",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         const pkg = {
            listModels: async () => [{ path: "m.malloy" }],
            getModel: (p: string) => (p === "m.malloy" ? model : undefined),
            getVersionId: () => undefined,
         };
         const store = fakeOf<PackageLookup>({
            getEnvironment: async () => ({
               assertCanAdmitQuery: () => undefined,
               getPackage: async () => pkg,
               getStaleCompileErrors: () => new Map(),
            }),
         });
         const index = await getPackageIndex(
            new WeakMap(),
            store,
            "env",
            "pkg",
         );
         const read = async (name: string) => {
            const member = index.retrievalEntities.find(
               (e) => e.source === "visits" && e.name === name,
            )!;
            const got = await valueFetcherFor(new RetrievalRegistry())({
               environmentStore: store,
               environmentName: "env",
               packageName: "pkg",
               modelPath: "m.malloy",
               source: "visits",
               dimension: fieldPathOf(member),
               limit: 10,
               timeoutMs: 10_000,
               givens: { role: "analyst" },
            });
            return [...got.values].sort();
         };
         expect(await read("traffic.city")).toEqual(["Boulder", "Denver"]);
         expect(await read("traffic.`source`")).toEqual(["bing", "google"]);
         // Through a join: the join's segment is quoted on its own too.
         expect(await read("hc.profile.tier")).toEqual(["gold", "tin"]);
         expect(await read("hc.id")).toEqual(["7", "8"]);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

describe("listing a source with a wide record", () => {
   it("lists the source's joins, views and fields before the record's members", async () => {
      const members = Array.from(
         { length: 200 },
         (_, i) => `'m${i}': ${i}`,
      ).join(", ");
      const { pkg } = await compileJoinFixture("m.malloy", {
         modelText: `
source: items is duckdb.sql("select 1 as k")
source: events is duckdb.sql("select 1 as k, {${members}} as params, {'x': 1} as tail") extend {
  join_one: item is items on k = item.k
  view: by_k is { group_by: k }
  dimension: late is k + 1
}
`,
      });
      const store = storeServing(pkg);
      const handlers = new Map<
         string,
         (params: Record<string, unknown>) => Promise<{
            content: Array<{ resource?: { text: string } }>;
         }>
      >();
      registerGetContextTool(
         {
            registerTool: (name: string, _config: unknown, h: never) =>
               handlers.set(name, h),
         } as never,
         store,
         testRetrieval(store),
      );
      const result = await handlers.get("get_context")!({
         scopes: [{ environment: "env", package: "pkg", source: "events" }],
      });
      const payload = JSON.parse(result.content[0].resource!.text);
      const names: string[] = payload.sources[0].entities.map(
         (e: { name: string }) => e.name,
      );
      expect(names).toContain("item");
      expect(names).toContain("by_k");
      expect(names.indexOf("by_k")).toBeLessThan(names.indexOf("params.m0"));
      // The source summary's field list is capped too: a record column after
      // the wide one still makes it in.
      const index = await getPackageIndex(new WeakMap(), store, "env", "pkg");
      const [input] = buildSourceSummaryInputs(
         index.retrievalEntities.filter((e) => e.source === "events"),
      );
      expect(input.prompt).toContain("(Showing ");
      expect(input.prompt).toContain("- tail (record)");
   });
});

describe("also likely, over record members", () => {
   it("does not take two records' same-named members for twins", async () => {
      const f = await buildFcjIndexFixture(CLUSTER_SETTINGS, {
         "m.malloy": `
source: orders is duckdb.sql("""select {'city': 'Denver'} as delivery, {'city': 'Boulder'} as billing""")
source: invoices is duckdb.sql("""select {'city': 'Golden'} as billing""")
`,
      });
      try {
         const settings = {
            ...CLUSTER_SETTINGS,
            connect: "list" as const,
            listTopK: 15,
         };
         const targets: FindTarget[] = [
            {
               index: 0,
               type: "dimension",
               text: "billing city",
               kinds: KINDS_BY_FIND_TARGET.dimension,
            },
         ];
         const queries = new Map([[0, toUnit(embedText("billing city"))]]);
         const found = find({
            index: f.index,
            targets,
            queries,
            perTarget: 30,
            keyword: true,
            values: false,
            allow: () => true,
         });
         const connected = connect({
            index: f.index,
            found,
            queries,
            settings,
            allow: () => true,
         });
         const out = alsoLikelyFields({
            index: f.index,
            found,
            connected,
            twins: 2,
            verdicts: [
               {
                  cluster: connected.clusters[0].home,
                  picks: [
                     {
                        target: 0,
                        keys: ["dimension:orders@m.malloy:billing.city"],
                     },
                  ],
                  caveats: [],
               },
            ],
         });
         // The other source's billing.city is a twin; orders' own
         // delivery.city is another field, not the same name.
         expect(out.map((a) => [a.id, a.rule])).toEqual([
            ["dimension:invoices:billing.city", "twin"],
         ]);
      } finally {
         await f.close();
      }
   });
});

describe("record members of a hidden joined source", () => {
   it("adds the members with the record when unservedCopies copies a hidden source's fields", async () => {
      const f = await compileFcjFixture(
         {
            "m.malloy": `
#(agent-hidden)
source: customers is duckdb.sql("""select 7 as id, {'tier': 'gold', 'since': {'yr': 2020}} as profile, {'yr': 1} as "date" """)
source: orders is duckdb.sql("""select 7 as customer_id""") extend {
  join_many: customer is customers on customer_id = customer.id
}
`,
         },
         {},
         DEFAULT_PACKAGE_RETRIEVAL,
      );
      const catalog = buildCatalog({
         ...(await catalogInputOf(f.pkgIndex)),
         unservedCopies: 3,
      });
      const node = (name: string) =>
         catalog.nodes.get(`dimension:orders@m.malloy:${name}`);
      expect(node("customer.profile")?.dataType).toBe("record");
      expect(node("customer.profile.tier")).toMatchObject({
         dataType: "string",
         joinPath: "customer",
         relationship: "many",
      });
      expect(node("customer.profile.since.yr")?.joinPath).toBe("customer");
      expect(node("customer.`date`.yr")?.joinPath).toBe("customer");
   });
});

describe("the judge's route line to a repeated record's member", () => {
   it("names the join's own kind, and says the member is many", async () => {
      const settings = {
         ...CLUSTER_SETTINGS,
         connect: "list" as const,
         listTopK: 5,
         collapseCopies: true,
      };
      const f = await buildFcjIndexFixture(settings, {
         "m.malloy": `
#(doc) One row per customer.
source: customers is duckdb.sql("""select 7 as id, [{'sku': 'x'}] as items""")
#(doc) One row per order.
source: orders is duckdb.sql("""select 7 as customer_id""") extend {
  join_one: customer is customers on customer_id = customer.id
  join_one: hc is duckdb.sql("""select 7 as id, [{'sku': 'y'}] as items""") on customer_id = hc.id
}
`,
      });
      try {
         const targets: FindTarget[] = [
            {
               index: 0,
               type: "dimension",
               text: "items sku",
               kinds: KINDS_BY_FIND_TARGET.dimension,
            },
         ];
         const queries = new Map([[0, toUnit(embedText("items sku"))]]);
         const found = find({
            index: f.index,
            targets,
            queries,
            perTarget: 30,
            keyword: true,
            values: false,
            allow: () => true,
         });
         const [cluster] = connect({
            index: f.index,
            found,
            queries,
            settings,
            allow: () => true,
         }).clusters;
         const options = judgeOptions(cluster, targets, {
            index: f.index,
            cap: 3,
         });
         const line = renderJudgePrompt(f.index, cluster, targets, options, 3)
            .split("\n")
            .find((l) => l.includes("orders.customer.items.sku (route"));
         expect(line).toContain(
            "(route from orders through customer, join_one; items.sku is many, one value per array element)",
         );
         // A joined copy (an inline-SQL join has no source of its own) is a
         // route already, and says the same.
         const copy = renderJudgePrompt(f.index, cluster, targets, options, 3)
            .split("\n")
            .find((l) => l.includes("a route from orders through hc"));
         expect(copy).toContain(
            "a route from orders through hc, join_one; items.sku is many, one value per array element:",
         );
      } finally {
         await f.close();
      }
   });
});

describe("a picked join reached through another join", () => {
   it("keeps the path's relationship: a join is not widened by its own", async () => {
      const settings = {
         ...CLUSTER_SETTINGS,
         connect: "list" as const,
         listTopK: 5,
         collapseCopies: true,
      };
      const f = await buildFcjIndexFixture(settings, {
         "m.malloy": `
source: addresses is duckdb.sql("""select 7 as customer_id, 'Denver' as city""")
source: customers is duckdb.sql("""select 7 as id""") extend {
  join_many: addrs is addresses on id = addrs.customer_id
}
source: orders is duckdb.sql("""select 7 as customer_id""") extend {
  join_one: customer is customers on customer_id = customer.id
}
`,
      });
      try {
         const targets: FindTarget[] = [
            {
               index: 0,
               type: "join",
               text: "addresses",
               kinds: KINDS_BY_FIND_TARGET.join,
            },
         ];
         const queries = new Map([[0, toUnit(embedText("addresses"))]]);
         const found = find({
            index: f.index,
            targets,
            queries,
            perTarget: 30,
            keyword: true,
            values: false,
            allow: () => true,
         });
         const connected = connect({
            index: f.index,
            found,
            queries,
            settings,
            allow: () => true,
         });
         const key = routeKey({
            from: "orders@m.malloy",
            path: ["customer"],
            node: "join:customers@m.malloy:addrs",
         });
         const out = await respond({
            index: f.index,
            request: {
               pureSourceListing: false,
               environmentName: "env",
               packageName: "fixture",
               kinds: new Set(),
               searches: [],
               valueSearches: [],
               listingOnly: false,
               includeCode: false,
               unsupported: [],
               limit: 20,
               offset: 0,
               entityOffset: 0,
            },
            pkgIndex: f.pkgIndex,
            connected,
            verdicts: [
               {
                  cluster: connected.clusters[0].home,
                  picks: [{ target: 0, keys: [key] }],
                  caveats: [],
               },
            ],
            targets,
            environmentStore: {} as never,
            valueFetcher: valueFetcherFor(new RetrievalRegistry()),
         });
         const row = (
            out.sources as Array<{
               entities?: Array<Record<string, unknown>>;
            }>
         )
            .flatMap((c) => c.entities ?? [])
            .find((e) => e.name === "customer.addrs");
         expect(row?.relationship).toBe("one");
      } finally {
         await f.close();
      }
   });
});
