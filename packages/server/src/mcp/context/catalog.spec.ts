// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeAll, describe, expect, it } from "bun:test";
import { createCatalogTables } from "../../storage/duckdb/schema";
import {
   compileFcjFixture,
   FCJ_PAYMENTS_FILES,
   openTempDb,
} from "../../test_helpers/fcj_fixture";
import type { Catalog } from "./catalog";
import { buildCatalog, catalogInputOf, referencedPaths } from "./catalog";
import { catalogReportLine, saveCatalog } from "./catalog_store";
import { derivedText, DERIVED_TEXT_MAX_WORDS } from "./derived_text";
import { reachOf } from "./reach";

let catalog: Catalog;
let payments: Catalog;

beforeAll(async () => {
   ({ catalog } = await compileFcjFixture());
   ({ catalog: payments } = await compileFcjFixture(FCJ_PAYMENTS_FILES, {}));
});

const plainText = (key: string) =>
   derivedText(catalog.nodes.get(key)!, {
      catalog,
      whatOf: (n) => n.doc,
      summaryOf: () => undefined,
   });

const paymentsText = (
   key: string,
   summaryOf: (k: string) => string | undefined = () => undefined,
) =>
   derivedText(payments.nodes.get(key)!, {
      catalog: payments,
      whatOf: (n) => n.doc,
      summaryOf,
   });

describe("catalog", () => {
   it("counts what the fixture declares", () => {
      expect(catalog.report).toEqual({
         sources: 8,
         nodes: { source: 8, dimension: 30, measure: 7, view: 2, join: 4 },
         edges: { contains: 43, joins: 4, extends: 1, depends: 9, uses: 4 },
         multiFileNodes: 10,
         indexedDimensions: 2,
         gatedSources: 0,
         unresolvedReferences: 3,
         copiesFolded: 0,
         joinPaths: 10,
         deepestJoinPath: 3,
         joinPathsCapped: 0,
         joinedCopiesDropped: 0,
         unservedCopiesAdded: 0,
         unservedCopiesCapped: 0,
         unservedWalkCapped: 0,
      });
   });

   it("keys a source by the file that defines it, so an imported source is one source", () => {
      const orders = catalog.sources.get("orders@shop.malloy")!;
      expect(orders.modelPaths).toEqual(["reports.malloy", "shop.malloy"]);
      expect(catalog.sources.has("orders@reports.malloy")).toBe(false);
      const sales = catalog.nodes.get("measure:orders@shop.malloy:sales")!;
      expect(sales.id).toBe("measure:orders:sales");
      expect(sales.modelPaths).toEqual(["reports.malloy", "shop.malloy"]);
      expect(sales.entities.map((e) => e.modelPath)).toEqual([
         "reports.malloy",
         "shop.malloy",
      ]);
   });

   it("records an extension and keeps the field it hides out of it", () => {
      const pub = catalog.sources.get("orders_public@shop.malloy")!;
      expect(pub.extendsKey).toBe("orders@shop.malloy");
      expect(
         catalog.nodes.has(
            "dimension:orders_public@shop.malloy:customer_email",
         ),
      ).toBe(false);
      expect(
         catalog.nodes.has("dimension:orders@shop.malloy:customer_email"),
      ).toBe(true);
   });

   it("reads what a dependent measure is computed from", () => {
      expect(
         catalog.nodes.get("measure:orders@shop.malloy:net_sales")!.dependsOn,
      ).toEqual([
         "measure:orders@shop.malloy:sales",
         "dimension:orders@shop.malloy:is_fulfilled",
      ]);
      expect(
         catalog.nodes.get("dimension:orders@shop.malloy:is_fulfilled")!
            .dependsOn,
      ).toEqual(["dimension:orders@shop.malloy:status"]);
   });

   it("reads which fields a view outputs", () => {
      expect(
         catalog.nodes.get("view:orders@shop.malloy:fulfilled_sales")!.uses,
      ).toEqual([
         "dimension:orders@shop.malloy:is_fulfilled",
         "measure:orders@shop.malloy:net_sales",
      ]);
   });

   it("marks #(index) dimensions", () => {
      const indexed = [...catalog.nodes.values()]
         .filter((n) => n.indexed)
         .map((n) => n.key);
      expect(indexed).toEqual([
         "dimension:cities@travel.malloy:city",
         "dimension:flights@travel.malloy:arrives_at",
      ]);
   });

   it("collects field paths from an expression tree, skipping locations", () => {
      expect(
         referencedPaths({
            node: "filteredExpr",
            kids: {
               e: { node: "field", path: ["sales"], at: { url: "x" } },
               filterList: [
                  { e: { node: "field", path: ["customer", "region"] } },
               ],
            },
         }),
      ).toEqual([["sales"], ["customer", "region"]]);
   });

   it("with collapseCopies, folds an extension's inherited fields into the fields they copy", async () => {
      const { pkgIndex } = await compileFcjFixture();
      const folded = buildCatalog({
         ...(await catalogInputOf(pkgIndex)),
         collapseCopies: true,
      });
      expect(folded.report.copiesFolded).toBe(8);
      const copies = [...folded.nodes.values()]
         .filter((n) => n.copyOf)
         .map((n) => [n.key, n.copyOf]);
      expect(copies).toContainEqual([
         "measure:orders_public@shop.malloy:net_sales",
         "measure:orders@shop.malloy:net_sales",
      ]);
      expect(copies).toContainEqual([
         "dimension:orders_public@shop.malloy:start_date",
         "dimension:orders@shop.malloy:start_date",
      ]);
      // Only extensions fold: races, orders and flights each define their own start_date.
      expect(copies.every(([k]) => k!.includes("orders_public@"))).toBe(true);
      expect(
         folded.nodes.get("measure:orders@shop.malloy:sales")!.copies,
      ).toEqual(["measure:orders_public@shop.malloy:sales"]);
   });
});

describe("reach", () => {
   it("follows a chain three joins deep and folds role-played paths", () => {
      const reach = [
         ...reachOf(catalog.joins, "legs@travel.malloy", 3).values(),
      ];
      expect(reach).toEqual([
         { source: "legs@travel.malloy", hops: 0, paths: [[]] },
         {
            source: "flights@travel.malloy",
            hops: 1,
            paths: [["flight"]],
            fanout: "one",
         },
         {
            source: "airports@travel.malloy",
            hops: 2,
            paths: [
               ["flight", "arrival"],
               ["flight", "origin"],
            ],
            fanout: "one",
         },
         {
            source: "regions@travel.malloy",
            hops: 3,
            paths: [
               ["flight", "arrival", "region"],
               ["flight", "origin", "region"],
            ],
            fanout: "one",
         },
      ]);
   });

   it("stops at the depth", () => {
      expect([
         ...reachOf(catalog.joins, "legs@travel.malloy", 2).keys(),
      ]).toEqual([
         "legs@travel.malloy",
         "flights@travel.malloy",
         "airports@travel.malloy",
      ]);
      expect([
         ...reachOf(catalog.joins, "legs@travel.malloy", 0).keys(),
      ]).toEqual(["legs@travel.malloy"]);
   });

   it("leaves islands alone, and does not follow an extension", () => {
      expect([
         ...reachOf(catalog.joins, "races@reports.malloy", 3).keys(),
      ]).toEqual(["races@reports.malloy"]);
      expect([
         ...reachOf(catalog.joins, "orders_public@shop.malloy", 3).keys(),
      ]).toEqual(["orders_public@shop.malloy"]);
   });
});

describe("catalog store", () => {
   it("writes one row per node and per distinct edge, and replaces them on the next save", async () => {
      const { db, close } = await openTempDb(createCatalogTables);
      try {
         const textOf = (k: string) => `text of ${k}`;
         await saveCatalog(db, "env", "fixture", catalog, textOf);
         await saveCatalog(db, "env", "fixture", catalog, textOf);
         const nodes = await db.all<{ n: number }>(
            "SELECT CAST(count(*) AS INTEGER) AS n FROM catalog_nodes",
         );
         const edges = await db.all<{ n: number }>(
            "SELECT CAST(count(*) AS INTEGER) AS n FROM catalog_edges",
         );
         expect(nodes[0].n).toBe(51);
         expect(edges[0].n).toBe(61);
         const row = await db.all<{
            model_paths: string;
            derived_text: string;
         }>(
            "SELECT model_paths, derived_text FROM catalog_nodes WHERE node_key = ?",
            ["measure:orders@shop.malloy:sales"],
         );
         expect(row).toEqual([
            {
               model_paths: '["reports.malloy","shop.malloy"]',
               derived_text: "text of measure:orders@shop.malloy:sales",
            },
         ]);
      } finally {
         await close();
      }
   });

   it("formats the report as one line", () => {
      expect(catalogReportLine(catalog.report)).toBe(
         "8 sources; nodes: 8 source, 30 dimension, 7 measure, 2 view, 4 join; edges: 43 contains, 4 joins, 1 extends, 9 depends, 4 uses; 10 nodes resolve in more than one file; 2 #(index) dimensions; 0 gated sources; 3 field references resolved to no node; 10 join paths, the longest 3 joins",
      );
   });

   it("names the join walk's guard when a path reached it", () => {
      expect(catalogReportLine({ ...catalog.report, joinPathsCapped: 2 })).toBe(
         "8 sources; nodes: 8 source, 30 dimension, 7 measure, 2 view, 4 join; edges: 43 contains, 4 joins, 1 extends, 9 depends, 4 uses; 10 nodes resolve in more than one file; 2 #(index) dimensions; 0 gated sources; 3 field references resolved to no node; 10 join paths, the longest 3 joins; 2 join paths stopped at the guard (8 joins, or 1000 paths from one source)",
      );
   });
});

describe("derived text", () => {
   it("pins a plain field", () => {
      expect(plainText("dimension:flights@travel.malloy:start_date")).toBe(
         "start date. In flights: One row per scheduled flight.",
      );
   });

   it("pins a field with a doc", () => {
      expect(plainText("measure:flights@travel.malloy:flight_count")).toBe(
         "flight count: Number of flights. In flights: One row per scheduled flight.",
      );
   });

   it("pins a dependent measure with no doc of its own", () => {
      expect(plainText("measure:orders@shop.malloy:net_sales")).toBe(
         "net sales. Computed from sales (Order amount in dollars), is fulfilled (The order has shipped). In orders: One row per order placed in the web shop.",
      );
   });

   it("pins a view", () => {
      expect(plainText("view:orders@shop.malloy:fulfilled_sales")).toBe(
         "fulfilled sales. Uses is fulfilled, net sales. In orders: One row per order placed in the web shop.",
      );
   });

   it("pins a source, preferring a stored summary", () => {
      expect(plainText("source:flights@travel.malloy:flights")).toBe(
         "flights: One row per scheduled flight.",
      );
      expect(
         derivedText(
            catalog.nodes.get("source:flights@travel.malloy:flights")!,
            {
               catalog,
               whatOf: (n) => n.doc,
               summaryOf: (k) =>
                  k === "flights@travel.malloy"
                     ? "Scheduled flights, one per departure"
                     : undefined,
            },
         ),
      ).toBe("flights: Scheduled flights, one per departure.");
   });

   it("pins a field of a SQL join: its own doc, then the path with the join's first sentence", () => {
      expect(
         paymentsText(
            "dimension:payments@payments.malloy:networks.network_name",
         ),
      ).toBe(
         "network name: Network name as printed on the card. Reached from payments through networks: Card networks, one row per network. In payments: One row per payment transaction.",
      );
      expect(
         paymentsText("dimension:payments@payments.malloy:networks.code"),
      ).toBe(
         "code. Reached from payments through networks: Card networks, one row per network. In payments: One row per payment transaction.",
      );
   });

   it("pins a field a join adds to a catalog source, with that source's line", () => {
      const node = payments.nodes.get(
         "dimension:payments@payments.malloy:labelled.loud_name",
      )!;
      expect(node.joinedSourceKey).toBe("merchants@payments.malloy");
      expect(paymentsText(node.key)).toBe(
         "loud name: Merchant name in capitals. Reached from payments through labelled: Merchants with a display name. From merchants: One row per merchant, 30 in all, carrying the account characteristics. In payments: One row per payment transaction.",
      );
      // A SQL join is made from no source.
      expect(
         payments.nodes.get("dimension:payments@payments.malloy:networks.code")!
            .joinedSourceKey,
      ).toBeUndefined();
   });

   it("gives an extension its own doc and a separate line for its parent, never a stored summary", () => {
      const summaries = (k: string) =>
         k === "payments@payments.malloy"
            ? "Payments, one row each"
            : k === "eur_payments@payments.malloy"
              ? "A summary written from the parent's notes"
              : undefined;
      expect(
         paymentsText(
            "source:eur_payments@payments.malloy:eur_payments",
            summaries,
         ),
      ).toBe(
         "eur payments: Payments settled in euros. One row per payment. Extends payments: Payments, one row each.",
      );
      expect(
         paymentsText(
            "measure:eur_payments@payments.malloy:eur_count",
            summaries,
         ),
      ).toBe(
         "eur count: Number of euro payments. In eur payments: Payments settled in euros. Extends payments: Payments, one row each.",
      );
   });

   it("does not let the parent's doc read as an undocumented extension's", () => {
      const plain = payments.sources.get("plain_payments@payments.malloy")!;
      // The compiler hands the extension its parent's notes as its own; the
      // shipped entity carries them.
      expect(plain.node.entities[0].doc).toBe(
         "One row per payment transaction.",
      );
      expect(plain.node.doc).toBe("");
      expect(plain.node.displayDoc).toBe("");
      expect(paymentsText(plain.node.key)).toBe(
         "plain payments. Extends payments: One row per payment transaction.",
      );
      expect(
         paymentsText("dimension:plain_payments@payments.malloy:is_large"),
      ).toBe(
         "is large. Computed from eur amount. In plain payments. Extends payments: One row per payment transaction.",
      );
      // An extension that writes notes keeps exactly those.
      expect(
         payments.sources.get("eur_payments@payments.malloy")!.node.doc,
      ).toBe("Payments settled in euros. One row per payment.");
   });

   it("caps the text after a joined field's own doc, so the cap cuts the context", () => {
      const long = Array.from({ length: 110 }, (_, i) => `w${i}`).join(" ");
      const key = "dimension:payments@payments.malloy:networks.network_name";
      const text = derivedText(payments.nodes.get(key)!, {
         catalog: payments,
         whatOf: (n) => (n.key === key ? long : n.doc),
         summaryOf: () => undefined,
      });
      expect(text.split(" ").length).toBe(DERIVED_TEXT_MAX_WORDS);
      expect(text).toBe(
         `network name: ${long}. Reached from payments through networks: Card networks, one`,
      );
   });

   it("uses only the what-it-is part of a doc", () => {
      expect(
         derivedText(catalog.nodes.get("measure:orders@shop.malloy:sales")!, {
            catalog,
            whatOf: (n) =>
               n.name === "sales" ? "Order amount in dollars" : n.doc,
            summaryOf: () => undefined,
         }),
      ).toBe(
         "sales: Order amount in dollars. Computed from amount. In orders: One row per order placed in the web shop.",
      );
   });

   it("caps the text", () => {
      const long = Array.from({ length: 300 }, (_, i) => `w${i}`).join(" ");
      const text = derivedText(
         catalog.nodes.get("measure:orders@shop.malloy:sales")!,
         {
            catalog,
            whatOf: () => long,
            summaryOf: () => undefined,
         },
      );
      expect(text.split(" ").length).toBe(DERIVED_TEXT_MAX_WORDS);
   });
});
