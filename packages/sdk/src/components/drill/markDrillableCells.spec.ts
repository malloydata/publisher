import { parseAnnotation } from "@malloydata/malloy-tag";
import { describe, expect, it } from "bun:test";
import { drillableFieldNames } from "./markDrillableCells";
import { drillDestinations, type DrillField } from "./resolveDrill";

/**
 * Which columns the affordance marks. The marking itself walks the renderer's
 * own table DOM, and stubbing that markup would only test our idea of it, so
 * this file covers the field-name decision and not the DOM walk.
 *
 * The DOM walk is currently verified BY HAND against the real renderer, not by a
 * committed spec: on a notebook whose model carries two `# drill` dimensions,
 * the reachable one's column marks all 10 of its cells and the unreachable one's
 * marks none. Recorded here rather than left implied, and deliberately NOT as a
 * pointer to `packages/app/tests/playwright/package-dashboards.spec.ts`, which
 * does not exist in this repository: it arrives with the dashboard slice. That
 * spec is
 * the natural home for pinning it, since it will have a drill-carrying fixture
 * to assert against; until then, treat the DOM half as unpinned.
 */
describe("drillableFieldNames", () => {
   const field = (name: string, annotations: string[]): DrillField => ({
      name,
      tag: parseAnnotation(annotations).tag,
   });
   const viz = (fields: DrillField[]) => ({
      getMetadata: () => ({ getAllFields: () => fields }),
   });
   /** A surface that can navigate but cannot filter itself. */
   const navigateOnly = (f: DrillField) =>
      drillDestinations(f).some((destination) => destination !== "self");

   it("collects a drillable field's name and its label", () => {
      // Both, because the header this is matched against renders `getLabel()`,
      // the label when the author set one, the name otherwise, and the marking
      // cannot tell which from the DOM.
      const names = drillableFieldNames(
         viz([
            field("category", ['# drill { to=overview } label="Category"\n']),
            field("total_sales", ["# currency\n"]),
         ]),
         navigateOnly,
      );
      expect([...names].sort()).toEqual(["Category", "category"]);
   });

   it("leaves out a drill this surface cannot honor", () => {
      // Malloyyo's rule, and the reason the predicate belongs to the host: a
      // dead link is worse than none, so a `to=self` on a surface with no
      // controls to write to stays plain text rather than promising a click it
      // would then swallow.
      const names = drillableFieldNames(
         viz([field("region", ["# drill { to=self }\n"])]),
         navigateOnly,
      );
      expect(names.size).toBe(0);
   });

   it("survives a viz with no metadata", () => {
      // Clicks still resolve in this case; only the affordance is missing, so a
      // renderer that has not published metadata yet must not throw here.
      expect(
         drillableFieldNames({ getMetadata: () => null }, () => true).size,
      ).toBe(0);
      expect(
         drillableFieldNames(
            {
               getMetadata: () => {
                  throw new Error("not ready");
               },
            },
            () => true,
         ).size,
      ).toBe(0);
   });
});
