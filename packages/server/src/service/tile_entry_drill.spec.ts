// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A tile's click actions written on its own `tiles=[…]` entry, read off a real
// compiled report: the manifest serves them, and the lints check them the way
// they check a model's `# drill` tag.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import type { ModelDef } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";

import {
   buildReportManifest,
   lintDrillTargets,
   lintReport,
   lintSelfDrills,
   readReportModelFacts,
} from "./report_manifest";

const ROOT = "file:///probe/";
let connections: FixedConnectionMap;

beforeAll(() => {
   connections = new FixedConnectionMap(
      new Map([["duckdb", new DuckDBConnection("duckdb", ":memory:")]]),
      "duckdb",
   );
});

const report = (tiles: string) => `
##! experimental.givens
## artifact { layout=grid tiles=[${tiles}] } dashboard { columns=12 }

given: REGION :: filter<string> is f''

source: orders is duckdb.sql("""
  select 'EU' as region, 'Nike' as brand, 10 as amount
""") extend {
  # drill { to=self given=REGION }
  dimension: region_name is region
  measure: total is sum(amount)
  view: by_region is { group_by: region_name, brand; aggregate: total }
  view: by_brand is { group_by: brand; aggregate: total }
}
`;

async function factsOf(tiles: string) {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}r.malloy`, report(tiles)]]),
   );
   const model = await new Runtime({ urlReader, connections })
      .loadModel(new URL(`${ROOT}r.malloy`), { importBaseURL: new URL(ROOT) })
      .getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const def = (model as any)._modelDef as ModelDef;
   return readReportModelFacts("r.malloy", def, ["REGION"]);
}

describe("a tile entry's drill", () => {
   it("is served on the tile, by dimension", async () => {
      const facts = await factsOf(
         `"orders -> by_region" { colspan=6 drill { brand { to=self given=REGION } } }, "orders -> by_brand"`,
      );
      const tiles = buildReportManifest(facts)?.tiles;
      expect(tiles?.[0]).toMatchObject({
         query: "orders -> by_region",
         colspan: 6,
         drill: { brand: { to: ["self"], given: "REGION" } },
      });
      expect(tiles?.[1]).not.toHaveProperty("drill");
   });

   it("takes one destination unbracketed, and several as a list", async () => {
      const facts = await factsOf(
         `"orders -> by_region" { drill { brand { to="r" } region_name { to=[self, "r"] } } }`,
      );
      expect(buildReportManifest(facts)?.tiles?.[0]).toMatchObject({
         drill: {
            brand: { to: ["r"] },
            region_name: { to: ["self", "r"] },
         },
      });
   });

   it("sits beside the model's tag, which the lints still read", async () => {
      const facts = await factsOf(
         `"orders -> by_region" { drill { brand { to=self given=REGION } } }`,
      );
      expect(facts.drills).toEqual([
         {
            to: ["self"],
            given: "REGION",
            source: "orders",
            dimension: "region_name",
         },
         {
            to: ["self"],
            given: "REGION",
            source: "orders -> by_region",
            dimension: "brand",
            tile: "orders -> by_region",
         },
      ]);
   });

   it("is checked for a report that does not exist", async () => {
      const facts = await factsOf(
         `"orders -> by_region" { drill { brand { to="nowhere" } } }`,
      );
      expect(
         lintDrillTargets([facts], new Set(["r"])).map((f) => f.message),
      ).toEqual([
         'The `drill` for `brand` on tile `orders -> by_region` targets "nowhere", which is not a report in this package.',
      ]);
   });

   it("is checked for a given no model declares", async () => {
      const facts = await factsOf(
         `"orders -> by_region" { drill { brand { to=self given=BRAND } } }`,
      );
      expect(lintSelfDrills([facts]).map((f) => f.message)).toEqual([
         'The `drill` for `brand` on tile `orders -> by_region` has to=self, but no model in this package declares a given "BRAND" for the clicked value to filter on, so the drill cannot fire anywhere. Declare it, or point the tag at a given that exists with given=.',
      ]);
   });

   it("is a lint finding, never a load failure, when it names a field the view does not output", async () => {
      const facts = await factsOf(
         `"orders -> by_brand" { drill { region_name { to=self given=REGION } } }`,
      );
      const manifest = buildReportManifest(facts);
      expect(manifest?.tiles?.[0]).toMatchObject({ query: "orders -> by_brand" });
      expect(
         lintReport(facts, manifest!).map((f) => f.message),
      ).toContain(
         "`drill` on `orders -> by_brand` names `region_name`, which that view does not output, so no click offers it. Fix: name one of its fields (brand, total).",
      );
   });

   it("is read on a query tile only: a text tile's entry carrying one is reported", async () => {
      const facts = await factsOf(`note { kind=text drill { brand { to=self } } }`);
      const manifest = buildReportManifest(facts);
      expect(lintReport(facts, manifest!).map((f) => f.message)).toContain(
         "`note` in `tiles=[…]` carries `drill`, which Publisher does not read: a text tile's entry reads only `row`, `col`, `colspan`, `rowspan` and `break`.",
      );
   });
});
