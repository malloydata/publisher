// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DEFAULT_FCJ_SETTINGS } from "../../service/package_retrieval";
import { compileFcjFixture } from "../../test_helpers/fcj_fixture";
import {
   buildFcjIndexFixture,
   type FcjIndexFixture,
} from "../../test_helpers/fcj_index_fixture";
import { embedText } from "../../test_helpers/get_context_llm_harness";
import type { Catalog, CatalogNode } from "./catalog";
import { buildCatalog, catalogInputOf } from "./catalog";
import { connect } from "./connect";
import { find, KINDS_BY_FIND_TARGET, type FindTarget } from "./find";
import {
   dropTwins,
   judgeOptions,
   renderJudgePrompt,
   validateVerdict,
   type JudgePick,
   type JudgeVerdict,
} from "./judge";
import { toUnit } from "./vectors";

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

describe("table sources, whose extensions record no parent", () => {
   let c: Catalog;
   beforeAll(async () => {
      const { pkgIndex } = await compileFcjFixture(
         {
            "t.malloy": `
#(agent-hidden)
#(doc) Every table.
source: base is duckdb.table('information_schema.tables') extend {
  measure: n is count()
}
source: plain is base extend { dimension: d is 1 }
#(doc) Tables in the main schema only.
source: mains is base extend { where: table_schema = 'main' }
#(doc) Every table, served.
source: vbase is duckdb.table('information_schema.tables') extend { measure: m is count() }
source: vplain is vbase extend { dimension: d is 1 }
source: vwhere is vbase extend { where: table_schema = 'main' }
`,
         },
         {},
      );
      c = buildCatalog({
         ...(await catalogInputOf(pkgIndex)),
         collapseCopies: true,
      });
   });
   const at = (key: string) => c.nodes.get(key)!;

   it("reads a hidden mark only from the notes above the source itself", () => {
      // `plain` has no notes of its own; the compiler hands it base's.
      expect([...c.sources.values()].map((s) => s.name)).toEqual([
         "plain",
         "mains",
         "vbase",
         "vplain",
         "vwhere",
      ]);
   });

   it("folds a same-rows extension into the source that defines the field, and marks the rest as twins", () => {
      expect(at("measure:vplain@t.malloy:m").copyOf).toBe(
         "measure:vbase@t.malloy:m",
      );
      expect(at("measure:vwhere@t.malloy:m").copyOf).toBeUndefined();
      expect(at("measure:vwhere@t.malloy:m").twinKey).toBe(
         at("measure:vbase@t.malloy:m").twinKey,
      );
      expect(at("measure:vbase@t.malloy:m").twinKey).toBeDefined();
      expect(at("measure:vplain@t.malloy:m").twinKey).toBeUndefined();
   });
});

describe("the judge reads each twin source's doc", () => {
   let p: FcjIndexFixture;
   beforeAll(async () => {
      p = await buildFcjIndexFixture(
         { ...DEFAULT_FCJ_SETTINGS, collapseCopies: true },
         FILES,
      );
   });
   afterAll(async () => p.close());

   it("puts a source's doc beside its fields when a field name is listed under several sources", () => {
      const targets: FindTarget[] = [
         {
            index: 0,
            type: "measure",
            text: "households reached",
            kinds: KINDS_BY_FIND_TARGET.measure,
         },
      ];
      const queries = new Map([[0, toUnit(embedText("households reached"))]]);
      const found = find({
         index: p.index,
         targets,
         queries,
         perTarget: 30,
         keyword: true,
         values: true,
         allow: () => true,
      });
      const cluster = connect({
         index: p.index,
         found,
         queries,
         settings: { ...DEFAULT_FCJ_SETTINGS, connect: "list", listTopK: 5 },
         allow: () => true,
      }).clusters[0];
      // A summary that leaves out which rows the source holds.
      const web = [...p.index.catalog.sources.values()].find(
         (s) => s.name === "web_deliveries",
      )!;
      p.index.summaries.set(web.key, "Delivery metrics.");
      const prompt = renderJudgePrompt(
         p.index,
         cluster,
         targets,
         judgeOptions(cluster, targets),
      );
      expect(prompt).toContain(
         '- web_deliveries - Delivery metrics.\n  Its doc begins: "Web deliveries only."',
      );
      expect(prompt).toContain(
         '  From web_deliveries (its doc: "Web deliveries only."):',
      );
      expect(prompt).toContain(
         '  From deliveries (its doc: "Every delivery row, all channels."):',
      );
      // Folded copies are not listed: one reach per set of rows.
      expect(prompt).not.toContain("From labelled_deliveries");
   });
});

describe("the judge's best pick and twinRule", () => {
   const node = (key: string, twinKey?: string, joinPath?: string) =>
      ({ key, twinKey, joinPath }) as unknown as CatalogNode;
   const nodes = new Map(
      [
         node("m:a:reach", "T"),
         node("m:b:reach", "T"),
         node("m:c:reach", "T"),
         node("m:a:audience"),
         node("m:hub:a.reach", "T", "a"),
      ].map((n) => [n.key, n]),
   );
   const verdict = (pick: Partial<JudgePick>): JudgeVerdict[] => [
      { cluster: "x", caveats: [], picks: [{ target: 0, keys: [], ...pick }] },
   ];

   it("reads best and lists it first", () => {
      const targets: FindTarget[] = [
         {
            index: 0,
            type: "measure",
            text: "reach",
            kinds: KINDS_BY_FIND_TARGET.measure,
         },
      ];
      const options = ["m:a:reach", "m:b:reach"].map((key, i) => ({
         number: i + 1,
         key,
         targets: [0],
      }));
      const out = validateVerdict(
         targets,
         options,
      )({
         picks: [{ target: 1, candidates: [1, 2], best: 2, reason: "r" }],
         caveats: [],
      });
      expect(out.picks[0].keys).toEqual(["m:b:reach", "m:a:reach"]);
      expect(out.picks[0].best).toBe("m:b:reach");
   });

   it("keeps the best twin and drops the others; off, keeps all", () => {
      const v = verdict({
         keys: ["m:a:reach", "m:b:reach", "m:c:reach", "m:a:audience"],
         best: "m:b:reach",
      });
      const out = dropTwins(v, true, (k) => nodes.get(k))[0].picks[0];
      expect(out.keys).toEqual(["m:b:reach", "m:a:audience"]);
      expect(out.droppedTwins).toEqual(["m:a:reach", "m:c:reach"]);
      expect(
         dropTwins(v, false, (k) => nodes.get(k))[0].picks[0].keys,
      ).toHaveLength(4);
   });

   it("leaves a readings pick, joined copies and fields without twins alone", () => {
      const readings = verdict({
         keys: ["m:a:reach", "m:b:reach"],
         readings: true,
      });
      expect(
         dropTwins(readings, true, (k) => nodes.get(k))[0].picks[0].keys,
      ).toEqual(["m:a:reach", "m:b:reach"]);
      const joined = verdict({ keys: ["m:a:reach", "m:hub:a.reach"] });
      expect(
         dropTwins(joined, true, (k) => nodes.get(k))[0].picks[0].keys,
      ).toEqual(["m:a:reach", "m:hub:a.reach"]);
   });
});
