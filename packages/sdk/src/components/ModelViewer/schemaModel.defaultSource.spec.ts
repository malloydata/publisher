// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { pickDefaultSource } from "./schemaModel";
import type { ExplorerSource } from "./schemaModel";

/**
 * A source with `views` many views. View NAMES are namespaced per source, so
 * two unrelated sources never look like one extends the other — which is what
 * a shared name would mean (see pickDefaultSource). `extendsOf` opts a fixture
 * into that relationship explicitly by inheriting the base's names.
 */
function src(
   name: string,
   views: number,
   extendsOf?: ExplorerSource,
): ExplorerSource {
   const inherited = extendsOf?.views.map((v) => v.name) ?? [];
   const own = Array.from(
      { length: views - inherited.length },
      (_, i) => `${name}_v${i}`,
   );
   return {
      name,
      accent: "steel",
      calculations: [],
      dimensions: [],
      measures: [],
      views: [...inherited, ...own].map((n) => ({
         name: n,
         kind: "view" as const,
      })),
      joins: [],
   };
}

describe("pickDefaultSource", () => {
   // The two shapes that motivated the change, taken from the shipped samples:
   // both declare a lookup table first and their subject last.
   it("opens flights.malloy on flights, not the carriers lookup it declares first", () => {
      const sources = [
         src("carriers", 0),
         src("airports", 1),
         src("aircraft", 0),
         src("flights", 6),
      ];
      expect(pickDefaultSource(sources, "faa/flights.malloy")?.name).toBe(
         "flights",
      );
   });

   it("opens imdb.malloy on movies — the file has no namesake source at all", () => {
      const sources = [
         src("people", 1),
         src("principals", 0),
         src("movies", 5),
      ];
      expect(pickDefaultSource(sources, "imdb/imdb.malloy")?.name).toBe(
         "movies",
      );
   });

   it("breaks a views tie toward the file's namesake", () => {
      const sources = [src("carriers", 2), src("flights", 2)];
      expect(pickDefaultSource(sources, "flights.malloy")?.name).toBe(
         "flights",
      );
   });

   it("falls back to declaration order when nothing distinguishes the sources", () => {
      const sources = [src("alpha", 0), src("beta", 0)];
      expect(pickDefaultSource(sources, "gamma.malloy")?.name).toBe("alpha");
   });

   it("keeps the most-viewed source even when another matches the filename", () => {
      // Namesake is the TIEBREAK, not the rule: a namesake with fewer views
      // does not beat the source the model is actually written around.
      const sources = [src("orders", 9), src("sales", 1)];
      expect(pickDefaultSource(sources, "sales.malloy")?.name).toBe("orders");
   });

   it("prefers the base over a source extended from it", () => {
      // ga4.malloy: `purchase_events is events extend { … }` carries events'
      // views PLUS its own, so on a raw count the narrow slice outranks the
      // subject of the file.
      const events = src("events", 1);
      const purchase = src("purchase_events", 3, events);
      expect(
         pickDefaultSource([events, purchase], "ga4/ga4.malloy")?.name,
      ).toBe("events");
   });

   it("picks the earliest base in a chain, not the widest leaf", () => {
      // a <- b <- c, all three listed: only `a` extends nothing, so it wins
      // despite having the fewest views. The file is about its base.
      const a = src("a", 1);
      const b = src("b", 2, a);
      const c = src("c", 3, b);
      expect(pickDefaultSource([a, b, c], "x.malloy")?.name).toBe("a");
   });

   it("falls back to the count when every listed source extends another", () => {
      // Two siblings off the same hidden base (imported, or not exported):
      // each contains the other's inherited view, so neither is a base and the
      // pool is the whole list.
      const hidden = src("hidden", 1);
      const b = src("b", 2, hidden);
      const c = src("c", 2, hidden);
      // c gains a view b lacks, so it is the wider of two equals.
      c.views.push({ name: "c_extra", kind: "view" });
      expect(pickDefaultSource([b, c], "x.malloy")?.name).toBe("c");
   });

   it("treats - and _ as the same character in a namesake tie", () => {
      const sources = [src("other", 2), src("purchase_events", 2)];
      expect(pickDefaultSource(sources, "purchase-events.malloy")?.name).toBe(
         "purchase_events",
      );
   });

   it("survives an unknown path and an empty list", () => {
      expect(pickDefaultSource([src("only", 0)], undefined)?.name).toBe("only");
      expect(pickDefaultSource([], "x.malloy")).toBeNull();
   });
});
