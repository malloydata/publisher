import { parseAnnotation } from "@malloydata/malloy-tag";
import { describe, expect, it } from "bun:test";
import {
   drillDestinations,
   drillValueToFilter,
   humanizeSlug,
   resolveDrill,
   type DrillClickPayload,
} from "./resolveDrill";

/**
 * A click payload whose field carries the given annotation lines, parsed the
 * way the renderer parses them. Going through the real tag parser rather than a
 * hand-built stub is the point: the grammar is the contract with Malloyyo, and a
 * stub would agree with itself no matter what MOTLY actually accepts.
 */
function click(
   annotations: string[],
   value: unknown,
   options: {
      name?: string;
      isHeader?: boolean;
      wasDimension?: boolean;
   } = {},
): DrillClickPayload {
   return {
      field: {
         name: options.name ?? "brand_name",
         tag: parseAnnotation(annotations).tag,
         wasDimension:
            options.wasDimension === undefined
               ? undefined
               : () => options.wasDimension as boolean,
      },
      value,
      isHeader: options.isHeader,
   };
}

const DRILL_TO_TWO = '# drill { to=["overview", "regions"] given=BRAND }\n';

describe("resolveDrill", () => {
   it("reads destinations, the given override, and the clicked value", () => {
      expect(resolveDrill(click([DRILL_TO_TWO], "Nike"))).toEqual({
         to: ["overview", "regions"],
         given: "BRAND",
         value: "Nike",
         label: "Nike",
      });
   });

   it("upper-cases the dimension name when the tag names no given", () => {
      const intent = resolveDrill(
         click(["# drill { to=overview }\n"], "US", { name: "region_name" }),
      );
      expect(intent?.given).toBe("REGION_NAME");
      // A lone destination may be written unbracketed, as Malloyyo allows.
      expect(intent?.to).toEqual(["overview"]);
   });

   it("passes `self` through as a destination for the host to interpret", () => {
      expect(resolveDrill(click(["# drill { to=self }\n"], "EU"))?.to).toEqual([
         "self",
      ]);
   });

   it("ignores a click on a field with no drill tag", () => {
      expect(resolveDrill(click([], "Nike"))).toBeUndefined();
      expect(
         resolveDrill(click(["# currency\n"], 10, { name: "amount" })),
      ).toBeUndefined();
   });

   it("ignores a header click, which names a field rather than a value", () => {
      expect(
         resolveDrill(click([DRILL_TO_TWO], "Nike", { isHeader: true })),
      ).toBeUndefined();
   });

   it("ignores a drill tag that names no destination", () => {
      // The server lints this as an authoring error; clicking it must not
      // navigate to nowhere in the meantime.
      expect(resolveDrill(click(["# drill\n"], "Nike"))).toBeUndefined();
      expect(
         resolveDrill(click(["# drill { to=[] }\n"], "Nike")),
      ).toBeUndefined();
   });

   it("ignores clicks with nothing to resolve", () => {
      expect(resolveDrill(undefined)).toBeUndefined();
      expect(resolveDrill({})).toBeUndefined();
   });

   it("ignores an aggregate that inherited the tag", () => {
      // `# drill` is declared on a dimension, but a measure defined over it can
      // carry the annotation through. Its cell holds a total, not the value the
      // filter would name, so clicking it must not navigate.
      expect(
         resolveDrill(click([DRILL_TO_TWO], "Nike", { wasDimension: false })),
      ).toBeUndefined();
      expect(
         resolveDrill(click([DRILL_TO_TWO], "Nike", { wasDimension: true })),
      ).toBeDefined();
   });

   it("quotes a clicked value that would otherwise re-parse as two", () => {
      // The whole reason the value goes through the filter encoder: a comma in
      // the data would silently become "either of these" in the filter.
      expect(resolveDrill(click([DRILL_TO_TWO], "Ben & Jerry, Inc"))).toEqual({
         to: ["overview", "regions"],
         given: "BRAND",
         value: '"Ben & Jerry, Inc"',
         // The label stays human — it is only ever shown, never parsed.
         label: "Ben & Jerry, Inc",
      });
   });

   it("declines a value it cannot express, rather than filtering by the wrong thing", () => {
      expect(resolveDrill(click([DRILL_TO_TWO], null))).toBeUndefined();
      expect(resolveDrill(click([DRILL_TO_TWO], undefined))).toBeUndefined();
      expect(
         resolveDrill(click([DRILL_TO_TWO], { nested: 1 })),
      ).toBeUndefined();
   });
});

describe("drillDestinations", () => {
   // The affordance reads destinations off field metadata, before any click, so
   // it has to answer the same as the click path — otherwise a cell reads as
   // clickable and then does nothing, or the reverse.
   const field = (annotations: string[], wasDimension?: boolean) => ({
      name: "brand_name",
      tag: parseAnnotation(annotations).tag,
      wasDimension: wasDimension === undefined ? undefined : () => wasDimension,
   });

   it("agrees with resolveDrill about what drills", () => {
      expect(drillDestinations(field([DRILL_TO_TWO]))).toEqual([
         "overview",
         "regions",
      ]);
      expect(drillDestinations(field(["# drill { to=self }\n"]))).toEqual([
         "self",
      ]);
      expect(drillDestinations(field(["# drill { to=overview }\n"]))).toEqual([
         "overview",
      ]);
      expect(drillDestinations(field([]))).toEqual([]);
      expect(drillDestinations(field(["# drill\n"]))).toEqual([]);
      expect(drillDestinations(field([DRILL_TO_TWO], false))).toEqual([]);
      expect(drillDestinations(undefined)).toEqual([]);
   });
});

describe("humanizeSlug", () => {
   it("reads a slug as a sentence, the way Malloyyo labels the same menu", () => {
      expect(humanizeSlug("category_detail")).toBe("Category detail");
      expect(humanizeSlug("brand-explorer")).toBe("Brand explorer");
      expect(humanizeSlug("overview")).toBe("Overview");
      expect(humanizeSlug("")).toBe("");
   });
});

describe("drillValueToFilter", () => {
   it("encodes a string as a filter list entry", () => {
      expect(drillValueToFilter("Nike")).toBe("Nike");
      expect(drillValueToFilter("a, b")).toBe('"a, b"');
      // An apostrophe is ordinary inside a filter word, so it stays bare.
      expect(drillValueToFilter("Levi's")).toBe("Levi's");
   });

   it("encodes a number as equality and a boolean as itself", () => {
      expect(drillValueToFilter(100)).toBe("100");
      expect(drillValueToFilter(-2.5)).toBe("-2.5");
      expect(drillValueToFilter(0)).toBe("0");
      expect(drillValueToFilter(true)).toBe("true");
      expect(drillValueToFilter(false)).toBe("false");
   });

   it("truncates a date to the day the cell showed", () => {
      expect(drillValueToFilter(new Date("2024-03-05T13:45:00Z"))).toBe(
         "2024-03-05",
      );
   });

   it("declines what has no filter spelling", () => {
      expect(drillValueToFilter(null)).toBeUndefined();
      expect(drillValueToFilter(undefined)).toBeUndefined();
      expect(drillValueToFilter(NaN)).toBeUndefined();
      expect(drillValueToFilter(Infinity)).toBeUndefined();
      expect(drillValueToFilter([1, 2])).toBeUndefined();
   });
});
