// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   getTagLines,
   hasPersistTag,
   hasPreaggregateTag,
   getDocParts,
   ownSourceDoc,
   tagNames,
} from "./docAnnotations";

describe("tagNames", () => {
   it("names a tag without its parameters", () => {
      expect(
         tagNames(["line_chart { y=season_share series=common_name }"]),
      ).toEqual(["line_chart"]);
      expect(tagNames(["bar_chart { y=species_per_checklist }"])).toEqual([
         "bar_chart",
      ]);
      expect(tagNames(["bar_chart"])).toEqual(["bar_chart"]);
   });

   it("leaves out valued settings and removed tags, and names each tag once", () => {
      expect(tagNames(["bar_chart size=lg", "size=sm"])).toEqual(["bar_chart"]);
      expect(tagNames(["-bar_chart line_chart", "line_chart"])).toEqual([
         "line_chart",
      ]);
      expect(tagNames(['label="Revenue" currency=euro'])).toEqual([]);
      expect(
         tagNames(["dashboard { table.max_height=none }", "percent"]),
      ).toEqual(["dashboard", "percent"]);
   });

   it("names the chart of the `viz=` spelling, and nothing else valued", () => {
      expect(
         tagNames(["viz=bar { title='Flights' subtitle='Top 10' }"]),
      ).toEqual(["bar"]);
      expect(tagNames(["viz=line size=lg"])).toEqual(["line"]);
   });

   it("names a line Malloy can't parse by its first word", () => {
      expect(tagNames(["line_chart { y="])).toEqual(["line_chart"]);
      expect(tagNames(["size= {"])).toEqual([]);
   });
});

describe("getTagLines", () => {
   it("returns renderer/behavior tags with the # prefix stripped", () => {
      expect(
         getTagLines([{ value: "# bar_chart" }, { value: "# size=lg" }]),
      ).toEqual(["bar_chart", "size=lg"]);
   });

   it("excludes doc lines — those belong to getDocString", () => {
      expect(
         getTagLines([
            { value: '#" Top carriers by flight count.' },
            { value: '#(doc) "Carrier docs"' },
            { value: "# line_chart" },
         ]),
      ).toEqual(["line_chart"]);
   });

   it("excludes namespaced system tags — (malloy)/(index) metadata is not a renderer tag", () => {
      expect(getTagLines([{ value: "#(index)" }])).toEqual([]);
      expect(
         getTagLines([{ value: '#(malloy) drill_filters = [{ code = "x" }]' }]),
      ).toEqual([]);
      expect(getTagLines([{ value: "#@ persist refresh=7d" }])).toEqual([
         "persist refresh=7d",
      ]);
   });

   it("returns [] for missing or empty annotations", () => {
      expect(getTagLines(undefined)).toEqual([]);
      expect(getTagLines([{ value: "  " }])).toEqual([]);
   });
});

describe("hasPersistTag", () => {
   it("detects the #@ persist source tagline (with or without params)", () => {
      expect(hasPersistTag([{ value: "#@ persist" }])).toBe(true);
      expect(
         hasPersistTag([{ value: "#@ persist refresh=7d name=orders_t" }]),
      ).toBe(true);
   });

   it("does not fire on docs, renderer tags, or persist-prefixed words", () => {
      expect(hasPersistTag([{ value: '#" persisted data notes' }])).toBe(false);
      expect(hasPersistTag([{ value: "# bar_chart" }])).toBe(false);
      expect(hasPersistTag([{ value: "#@ persistence_mode" }])).toBe(false);
      expect(hasPersistTag(undefined)).toBe(false);
   });
});

describe("hasPreaggregateTag", () => {
   it("detects the #@ preaggregate measure tagline, which always carries a grain", () => {
      expect(
         hasPreaggregateTag([
            { value: '#@ preaggregate grain="category_grain"' },
         ]),
      ).toBe(true);
      expect(
         hasPreaggregateTag([
            { value: '#@ preaggregate grain="a, b" refresh=1d' },
         ]),
      ).toBe(true);
   });

   it("does not fire on docs, renderer tags, or the sibling persistence verb", () => {
      expect(
         hasPreaggregateTag([{ value: '#" preaggregate this later' }]),
      ).toBe(false);
      expect(hasPreaggregateTag([{ value: "# bar_chart" }])).toBe(false);
      expect(
         hasPreaggregateTag([{ value: "#@ preaggregated_elsewhere" }]),
      ).toBe(false);
      // The two verbs are different declarations on different grains; neither
      // may be read as the other.
      expect(hasPreaggregateTag([{ value: "#@ persist" }])).toBe(false);
      expect(hasPersistTag([{ value: '#@ preaggregate grain="c"' }])).toBe(
         false,
      );
      expect(hasPreaggregateTag(undefined)).toBe(false);
   });
});

describe("ownSourceDoc", () => {
   // A source that extends another carries the base's doc ahead of its own, as
   // `order_items` over an unjoined base does in the ecommerce sample.
   const base = {
      name: "order_items_base",
      docParts: ["Order line items, unjoined: one row per item sold."],
   };
   const joined = {
      name: "order_items",
      docParts: [
         ...base.docParts,
         "Order line items joined to inventory, product and customer.",
      ],
   };

   it("drops the doc a source inherited from the source it extends", () => {
      expect(ownSourceDoc(joined, base)).toBe(
         "Order line items joined to inventory, product and customer.",
      );
   });

   it("keeps a doc that merely starts like an unrelated source's: only its base's is inherited", () => {
      // Shared boilerplate between two sources that don't extend each other.
      expect(ownSourceDoc(joined, undefined)).toBe(joined.docParts.join("\n"));
   });

   it("keeps an inherited doc when the source has none of its own", () => {
      const bare = {
         name: "order_items_w_synergy",
         docParts: [...base.docParts],
      };
      expect(ownSourceDoc(bare, base)).toBe(
         "Order line items, unjoined: one row per item sold.",
      );
   });

   it("keeps everything when its base's doc isn't what it starts with", () => {
      expect(ownSourceDoc(joined, { docParts: ["Something else."] })).toBe(
         joined.docParts.join("\n"),
      );
   });

   it("reads each doc annotation as a part", () => {
      expect(
         getDocParts([
            { value: '#(doc) "One."\n' },
            { value: "# bar_chart\n" },
            { value: '#" Two.' },
         ]),
      ).toEqual(["One.", "Two."]);
   });
});
