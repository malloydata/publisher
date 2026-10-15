// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeAll, describe, expect, it } from "bun:test";
import { compileFcjFixture } from "../../test_helpers/fcj_fixture";
import type { Catalog } from "./catalog";
import { buildCatalog, catalogInputOf } from "./catalog";

/**
 * Near-identical sources: extensions that inherit the same fields but hold
 * other rows, and sources marked `#(agent-hidden)`.
 */
const FILES: Record<string, string> = {
   "deliveries.malloy": `
#(doc) Every delivery row, all channels.
source: deliveries is duckdb.sql("select 1 as id, 'web' as channel, 10 as households, 'A' as audience") extend {
  #(doc) Households reached.
  measure: reach is households.sum()
  #(doc) The audience segment.
  dimension: audience_name is audience
}

#(doc) Web deliveries only.
source: web_deliveries is deliveries extend { where: channel = 'web' }

#(doc) Deliveries with a channel label.
source: labelled_deliveries is deliveries extend { dimension: channel_label is upper(channel) }

#(doc) Web deliveries with a label.
source: labelled_web is web_deliveries extend { dimension: web_label is 'web' }

source: deliveries_again is deliveries

#(agent-hidden)
#(doc) Internal base: every impression row.
source: impressions is duckdb.sql("select 1 as id, 5 as n, 'A' as audience") extend {
  #(doc) Impressions served.
  measure: impression_count is n.sum()
}

#(doc) Impressions for the total universe only.
source: impressions_total is impressions extend { where: audience = 'A' }

source: impressions_plain is impressions extend { dimension: aud is audience }
`,
   "lookups.malloy": `
##(agent-hidden)
#(doc) Code to label lookups.
source: lookups is duckdb.sql("select 'A' as code, 'Alpha' as label")
`,
   "hub.malloy": `
import "lookups.malloy"
#(doc) One row per order.
source: hub is duckdb.sql("select 'A' as code, 1 as id") extend {
  join_one: lookups on lookups.code = code
}
`,
};

let catalog: Catalog;
const node = (kind: string, source: string, name: string) =>
   [...catalog.nodes.values()].find(
      (n) => n.kind === kind && n.source === source && n.name === name,
   );

beforeAll(async () => {
   const { pkgIndex } = await compileFcjFixture(FILES, {});
   catalog = buildCatalog({
      ...(await catalogInputOf(pkgIndex)),
      collapseCopies: true,
      // The shipping setting: joined copies of fields whose source is not
      // served (here: hidden) on the served sources that join to them.
      unservedCopies: 3,
   });
});

describe("near-identical sources", () => {
   it("leaves out #(agent-hidden) sources, on the source or on its file", () => {
      const names = [...catalog.sources.values()].map((s) => s.name).sort();
      expect(names).toEqual([
         "deliveries",
         "deliveries_again",
         "hub",
         "impressions_plain",
         "impressions_total",
         "labelled_deliveries",
         "labelled_web",
         "web_deliveries",
      ]);
      expect(catalog.report.hiddenSources).toBe(2);
   });

   it("a hidden base's mark does not carry to the sources that extend it", () => {
      // Each keeps the field it inherits from the hidden base as its own.
      for (const source of ["impressions_total", "impressions_plain"]) {
         const n = node("measure", source, "impression_count");
         expect(n).toBeDefined();
         expect(n!.copyOf).toBeUndefined();
      }
   });

   it("a hidden source's fields are reached through a served source's join", () => {
      expect(node("dimension", "hub", "lookups.label")).toBeDefined();
   });

   it("keeps an extension's inherited fields on it when it adds a where:", () => {
      const web = node("measure", "web_deliveries", "reach")!;
      expect(web.copyOf).toBeUndefined();
      expect(node("dimension", "web_deliveries", "audience_name")!.copyOf).toBe(
         undefined,
      );
      expect(node("measure", "deliveries", "reach")!.copies).not.toContain(
         web.key,
      );
   });

   it("folds an extension that only adds fields, and a plain reference, into the source", () => {
      const base = node("measure", "deliveries", "reach")!;
      expect(node("measure", "labelled_deliveries", "reach")!.copyOf).toBe(
         base.key,
      );
      expect(node("measure", "deliveries_again", "reach")!.copyOf).toBe(
         base.key,
      );
   });

   it("folds along a chain only as far as the rows stay the same", () => {
      // labelled_web adds a field to web_deliveries (same rows), which adds a
      // where: to deliveries (other rows): it folds into web_deliveries.
      expect(node("measure", "labelled_web", "reach")!.copyOf).toBe(
         node("measure", "web_deliveries", "reach")!.key,
      );
      expect(catalog.report.copiesKeptApart).toBeGreaterThan(0);
   });
});

