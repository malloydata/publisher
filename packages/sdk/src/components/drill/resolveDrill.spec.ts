import {
   NumberFilterExpression,
   StringFilterExpression,
} from "@malloydata/malloy-filter";
import { parseAnnotation } from "@malloydata/malloy-tag";
import { describe, expect, it } from "bun:test";
import {
   drillDestinations,
   drillGivenName,
   encodeDrillValue,
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
         rawValue: "Nike",
         label: "Nike",
      });
   });

   it("uses the dimension name verbatim when the tag names no given", () => {
      const intent = resolveDrill(
         click(["# drill { to=overview }\n"], "US", { name: "region_name" }),
      );
      // NOT upper-cased. Given names are matched exactly, so upper-casing here
      // made `to=self` on a lowercase-named given resolve to nothing at all.
      expect(intent?.given).toBe("region_name");
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

   it("escapes a clicked value that would otherwise re-parse as two", () => {
      // The whole reason the value goes through the filter encoder: a comma in
      // the data would silently become "either of these" in the filter.
      const intent = resolveDrill(click([DRILL_TO_TWO], "Ben & Jerry, Inc"));
      expect(intent).toEqual({
         to: ["overview", "regions"],
         given: "BRAND",
         value: "Ben\\ &\\ Jerry\\,\\ Inc",
         rawValue: "Ben & Jerry, Inc",
         // The label stays human: it is only ever shown, never parsed.
         label: "Ben & Jerry, Inc",
      });
      // And the encoding is checked against the parser that will read it,
      // rather than against our own idea of what it should look like.
      expect(StringFilterExpression.parse(intent!.value).parsed).toEqual({
         operator: "=",
         values: ["Ben & Jerry, Inc"],
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
   // it has to answer the same as the click path: otherwise a cell reads as
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
      expect(drillValueToFilter("a, b")).toBe("a\\,\\ b");
      // An apostrophe is ordinary inside a filter word, so it stays bare.
      expect(drillValueToFilter("Levi's")).toBe("Levi's");
      // The values a hazardous cell will actually match, per the real parser.
      for (const value of ["a, b", "-Nike", "100%", "null"]) {
         expect(
            StringFilterExpression.parse(drillValueToFilter(value)!).parsed,
         ).toEqual({ operator: "=", values: [value] });
      }
   });

   it("leaves a number bare, which the number grammar reads as the value", () => {
      // Not a negation: checked against the real parser, because `-2.5` looks
      // exactly like the `-` that negates in the *string* grammar.
      for (const value of [100, -2.5, 0]) {
         expect(
            NumberFilterExpression.parse(drillValueToFilter(value)!).parsed,
         ).toEqual({ operator: "=", values: [String(value)] });
      }
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

describe("encodeDrillValue", () => {
   it("escapes for a filter given, which is what a filter parses", () => {
      expect(encodeDrillValue("Ben & Jerry, Inc", "filter<string>")).toBe(
         "Ben\\ &\\ Jerry\\,\\ Inc",
      );
   });

   it("leaves the value alone for a plain string given", () => {
      // The escaping is meaningful only to the filter parser. A plain `string`
      // given is bound as a value, so a seeded `Ben\ &\ Jerry\,\ Inc` would
      // be compared literally, backslashes and all, and match nothing.
      expect(encodeDrillValue("Ben & Jerry, Inc", "string")).toBe(
         "Ben & Jerry, Inc",
      );
      expect(encodeDrillValue("-Outerwear", "string")).toBe("-Outerwear");
      expect(encodeDrillValue("100%", "string")).toBe("100%");
   });

   it("passes numbers and booleans through in their own type", () => {
      // The STRING forms ("100", "true") would be wrong here, not merely a
      // different spelling: `givensToRequest` converts only `Date`, so a string
      // sent for a `number` given travels as a string and Malloy refuses it.
      expect(encodeDrillValue(100, "number")).toBe(100);
      expect(encodeDrillValue(-2.5, "number")).toBe(-2.5);
      expect(encodeDrillValue(true, "boolean")).toBe(true);
   });

   it("falls back to the filter encoding when the type is unknown", () => {
      // A cross-dashboard drill: the destination declares its own givens and
      // this side cannot see them, so the filter spelling is the useful guess.
      expect(encodeDrillValue("a, b", undefined)).toBe("a\\,\\ b");
   });

   it("declines a value with no spelling", () => {
      expect(encodeDrillValue(null, "string")).toBeUndefined();
      expect(encodeDrillValue(undefined, "filter<string>")).toBeUndefined();
      expect(encodeDrillValue(NaN, "number")).toBeUndefined();
   });
});

describe("a drill on an empty cell", () => {
   it("is refused rather than encoding the match-everything filter", () => {
      // `encodeFilterList` drops an empty value, so encoding one yields the
      // EMPTY filter, which matches every row. A drill that widened instead of
      // narrowing is worse than one that does nothing, and `null` is already
      // refused on exactly that reasoning.
      expect(drillValueToFilter("")).toBeUndefined();
      expect(encodeDrillValue("", "filter<string>")).toBeUndefined();
      expect(encodeDrillValue("", "string")).toBeUndefined();
      expect(encodeDrillValue("", undefined)).toBeUndefined();
   });

   it("still resolves a cell that only looks empty", () => {
      expect(drillValueToFilter(" ")).toBeDefined();
      expect(drillValueToFilter(0)).toBe("0");
      expect(drillValueToFilter(false)).toBe("false");
   });
});

describe("encodeDrillValue returns the given's own type", () => {
   it("gives a number given a number, not a string", () => {
      // `givensToRequest` converts only `Date`, so a string sent for a `number`
      // given goes over the wire as a string and Malloy refuses the query.
      expect(encodeDrillValue(100, "number")).toBe(100);
      expect(encodeDrillValue("100", "number")).toBe(100);
      expect(encodeDrillValue(-2.5, "number")).toBe(-2.5);
      expect(encodeDrillValue("not a number", "number")).toBeUndefined();
   });

   it("gives a boolean given a boolean", () => {
      expect(encodeDrillValue(true, "boolean")).toBe(true);
      expect(encodeDrillValue(false, "boolean")).toBe(false);
      expect(encodeDrillValue("true", "boolean")).toBe(true);
      expect(encodeDrillValue("false", "boolean")).toBe(false);
      expect(encodeDrillValue("yes", "boolean")).toBeUndefined();
   });

   it("still gives a string given a string and a filter given filter syntax", () => {
      expect(encodeDrillValue("Outerwear", "string")).toBe("Outerwear");
      expect(encodeDrillValue("a, b", "filter<string>")).toBe("a\\,\\ b");
   });
});

describe("drillGivenName", () => {
   it("prefers the tag's given= and otherwise uses the field name as spelled", () => {
      const tagged = parseAnnotation(["# drill { to=self given=BRAND }\n"]);
      expect(drillGivenName({ name: "brand_name", tag: tagged.tag })).toBe(
         "BRAND",
      );
      const untagged = parseAnnotation(["# drill { to=self }\n"]);
      expect(drillGivenName({ name: "brand_name", tag: untagged.tag })).toBe(
         "brand_name",
      );
   });

   it("matches a lowercase given declaration, which upper-casing did not", () => {
      // The whole point of the change. `declaredTypes` is keyed by the name the
      // model spells and probed with `.has()`, so an upper-cased lookup missed
      // and the click was dropped with no affordance and no error.
      const declared = new Map([["region", "string"]]);
      const untagged = parseAnnotation(["# drill { to=self }\n"]);
      expect(
         declared.has(drillGivenName({ name: "region", tag: untagged.tag })),
      ).toBe(true);
   });
});
