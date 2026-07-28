// Contract test for the Malloy property `# drill` navigation rests on: an
// annotation written on a source *dimension* reaches the *query result's* output
// field, verbatim.
//
// That is what lets drill be resolved in the browser off the clicked field,
// with no drill-specific REST surface and no second copy of the tag grammar on
// the wire — and it is why the same click path works in a dashboard tile and in
// a notebook cell (docs/malloyyo-dashboards-design.md §Guiding design
// principle, "one drill implementation"). If a Malloy upgrade stopped
// propagating dimension annotations to result fields, drill would silently stop
// firing everywhere at once, so pin it here against the real compiler rather
// than a re-implementation. See dashboard.spec.ts for the tag parsing itself.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { parseAnnotation } from "@malloydata/malloy-tag";
import { beforeAll, describe, expect, it } from "bun:test";

const ROOT = "file:///probe/";
let connections: FixedConnectionMap;

beforeAll(() => {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
});

const MODEL = `
source: orders is duckdb.sql("""
  select 'Nike' as brand, 'US' as region, 10 as amount
""") extend {
  # drill { to=["overview", "regions"] given=BRAND }
  dimension: brand_name is brand

  # drill { to=self }
  dimension: region_name is region

  measure: total_amount is sum(amount)

  view: by_brand is {
    group_by: brand_name, region_name
    aggregate: total_amount
  }
}
`;

/**
 * Annotation lines per output field of `orders -> by_brand`, as the browser
 * receives them. Compile-only: the schema is settled at prepare time, so this
 * needs no query execution.
 */
async function outputFieldAnnotations(): Promise<Map<string, string[]>> {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, MODEL]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   const prepared = await runtime
      .loadModel(new URL(`${ROOT}m.malloy`), { importBaseURL: new URL(ROOT) })
      .loadQuery("run: orders -> by_brand")
      .getPreparedResult();

   const byName = new Map<string, string[]>();
   for (const field of prepared.toStableResult().schema?.fields ?? []) {
      byName.set(
         field.name,
         (field.annotations ?? []).map((a) => a.value),
      );
   }
   return byName;
}

describe("Malloy dimension-annotation propagation (drill depends on this)", () => {
   it("carries a dimension's # drill tag onto the result field", async () => {
      const fields = await outputFieldAnnotations();
      expect([...fields.keys()]).toEqual([
         "brand_name",
         "region_name",
         "total_amount",
      ]);
      expect(fields.get("brand_name")).toContain(
         '# drill { to=["overview", "regions"] given=BRAND }\n',
      );
      expect(fields.get("region_name")).toContain("# drill { to=self }\n");
   });

   it("keeps the tag parseable, so the browser reads it with no server help", async () => {
      const fields = await outputFieldAnnotations();
      // The renderer parses these same lines into the `tag` it hands a click
      // handler; parse them the same way here to pin the shape drill reads.
      const tag = parseAnnotation(fields.get("brand_name") ?? []).tag?.tag(
         "drill",
      );
      expect(tag?.textArray("to")).toEqual(["overview", "regions"]);
      expect(tag?.text("given")).toBe("BRAND");
   });

   it("leaves an untagged field alone, so only tagged cells are clickable", async () => {
      const fields = await outputFieldAnnotations();
      const drillTags = (fields.get("total_amount") ?? []).filter((line) =>
         line.startsWith("# drill"),
      );
      expect(drillTags).toEqual([]);
   });
});
