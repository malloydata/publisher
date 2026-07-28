import { describe, expect, it } from "bun:test";
import { readGivenControlSpec } from "./given";
import { readAutorun, readStartingGivens } from "./motly";
import { motlyTag } from "./motly";

/**
 * The control contract is read off the `given:` declaration rather than off the
 * surface presenting it, which is what lets a notebook and a dashboard render
 * the same control. These pin that reading, independently of either surface.
 */
describe("readGivenControlSpec", () => {
   it("reads the presentation a declaration asks for", () => {
      expect(
         readGivenControlSpec([
            `# label="Brand" control=select suggest { source=orders dimension=brand }`,
         ]),
      ).toEqual({
         label: "Brand",
         control: "select",
         suggest: { query: undefined, source: "orders", dimension: "brand" },
      });
   });

   it("reads a bounded numeric range", () => {
      expect(
         readGivenControlSpec([`# label="Minimum" range_min=0 range_max=500`]),
      ).toEqual({ label: "Minimum", rangeMin: 0, rangeMax: 500 });
   });

   it("returns nothing for an untagged declaration, so the type decides", () => {
      expect(readGivenControlSpec([])).toEqual({});
      expect(readGivenControlSpec([`#(doc) A caller-facing note\n`])).toEqual(
         {},
      );
   });

   it("ignores a control kind it does not recognize", () => {
      expect(readGivenControlSpec([`# control=radio`])).toEqual({});
   });

   it("omits suggest entirely when the tag names no target", () => {
      expect(readGivenControlSpec([`# control=select suggest { }`])).toEqual({
         control: "select",
      });
   });
});

/**
 * `autorun` means the same thing on a notebook's `## autorun=false` and a
 * dashboard's `# artifact { autorun=false }`, so it is read in one place.
 */
describe("readAutorun", () => {
   it("defaults to true, including with no tag at all", () => {
      expect(readAutorun(undefined)).toBe(true);
      expect(readAutorun(motlyTag([`# label="x"`]))).toBe(true);
   });

   it("is false only for an explicit false", () => {
      expect(readAutorun(motlyTag([`## autorun=false`]))).toBe(false);
      expect(readAutorun(motlyTag([`## autorun=true`]))).toBe(true);
   });
});

/**
 * Starting values, read from the same block whether it sits at a notebook's file
 * level or inside a dashboard's artifact tag.
 */
describe("readStartingGivens", () => {
   it("reads a notebook's file-level block", () => {
      expect(
         readStartingGivens(
            motlyTag([`## givens { SINCE="2024-03-01" REGION=f'US' }`]),
         ),
      ).toEqual({ SINCE: "2024-03-01", REGION: "US" });
   });

   it("unwraps a filter literal to the body the query endpoint takes", () => {
      expect(
         readStartingGivens(
            motlyTag([`## givens { REGION=f'us-east, us-west' }`]),
         )?.REGION,
      ).toBe("us-east, us-west");
   });

   it("reads the same block inside an artifact tag", () => {
      const artifact = motlyTag([
         `# artifact { givens { REGION=f'US' } }`,
      ])?.tag("artifact");
      expect(readStartingGivens(artifact)).toEqual({ REGION: "US" });
   });

   it("is undefined with no block, and with an empty one", () => {
      expect(readStartingGivens(undefined)).toBeUndefined();
      expect(
         readStartingGivens(motlyTag([`## autorun=false`])),
      ).toBeUndefined();
      expect(readStartingGivens(motlyTag([`## givens { }`]))).toBeUndefined();
   });
});
