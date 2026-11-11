// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { buildReportManifest, readReportModelFacts } from "./report_manifest";

/**
 * A tile is titled by its view's `# label` however the report names that view:
 * as a served view, refined in its entry (`"src -> v + { where: … }"`), or
 * through a query of the report's own that refines it. Malloy keeps the view's
 * tags under `annotations.inherits` once that query has tags of its own (the
 * builder's `# viz=bar`), so they must be read there. Compiled for real: the
 * shape of `inherits` is Malloy's, not this file's.
 */
const MODEL_PATH = "report.malloy";
const URL_OF = `file:///titles/${MODEL_PATH}`;

let duckdb: DuckDBConnection;
let connections: FixedConnectionMap;

beforeAll(() => {
   duckdb = new DuckDBConnection("duckdb", ":memory:");
   connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
});

afterAll(async () => {
   await duckdb.close();
});

async function tilesOf(source: string) {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[URL_OF, source]])),
      connections,
   });
   const model = await runtime.loadModel(new URL(URL_OF)).getModel();
   const modelDef = (model as unknown as { _modelDef: ModelDef })._modelDef;
   return buildReportManifest(readReportModelFacts(MODEL_PATH, modelDef, []))
      ?.tiles;
}

const MODEL = `
source: flights is duckdb.sql("select 'AA' as carrier, 1 as n") extend {
  # label="Flights by airline"
  view: by_carrier is { group_by: carrier; aggregate: c is count() }
}
`;

describe("a tile's title", () => {
   it("is the view's # label for a query of the report's own with tags of its own", async () => {
      const tiles = await tilesOf(`## artifact { tiles=["by_carrier_tile"] }
${MODEL}
# viz=bar
query: by_carrier_tile is flights -> by_carrier + { where: carrier = 'AA' }
`);
      expect(tiles?.[0]).toMatchObject({
         query: "by_carrier_tile",
         label: "Flights by airline",
      });
   });

   it("is the query's own # label when it has one", async () => {
      const tiles = await tilesOf(`## artifact { tiles=["by_carrier_tile"] }
${MODEL}
# label="Airlines"
query: by_carrier_tile is flights -> by_carrier
`);
      expect(tiles?.[0]).toMatchObject({ label: "Airlines" });
   });

   it("is the view's # label for a refined entry", async () => {
      const tiles =
         await tilesOf(`## artifact { tiles=["flights -> by_carrier + { where: carrier = 'AA' }"] }
${MODEL}
`);
      expect(tiles?.[0]).toMatchObject({ label: "Flights by airline" });
   });

   it("is the view's # label for an extension view that adds a tag of its own", async () => {
      const tiles =
         await tilesOf(`## artifact { tiles=["flights_tiles -> by_carrier_tile"] }
${MODEL}
source: flights_tiles is flights extend {
  # subtitle="Busiest first"
  view: by_carrier_tile is by_carrier
}
`);
      expect(tiles?.[0]).toMatchObject({
         query: "flights_tiles -> by_carrier_tile",
         label: "Flights by airline",
         subtitle: "Busiest first",
      });
   });

   it("is the view's # label for an extension view with no tag of its own", async () => {
      const tiles =
         await tilesOf(`## artifact { tiles=["flights_tiles -> by_carrier_tile"] }
${MODEL}
source: flights_tiles is flights extend {
  view: by_carrier_tile is by_carrier
}
`);
      expect(tiles?.[0]).toMatchObject({ label: "Flights by airline" });
   });

   it("is an extension view's own # label over the view it renames", async () => {
      const tiles =
         await tilesOf(`## artifact { tiles=["flights_tiles -> by_carrier_tile"] }
${MODEL}
source: flights_tiles is flights extend {
  # label="Airlines"
  view: by_carrier_tile is by_carrier
}
`);
      expect(tiles?.[0]).toMatchObject({ label: "Airlines" });
   });
});

/**
 * With no `# label` anywhere, a viewer titles a tile from what it names. A query
 * of the report's own carries `view`, the view it reads, so a viewer can title
 * the builder's `<view>_tile` as the view rather than "… tile".
 */
describe("the view a query tile reads", () => {
   const UNLABELLED = `
source: brands is duckdb.sql("select 'x' as brand, 'y' as region, 1 as n") extend {
  view: top_brands is { group_by: brand; aggregate: c is count() }
  view: key_figures is { aggregate: c is count(), total is sum(n) }
}
`;

   it("is the view a filtered query refines", async () => {
      const tiles =
         await tilesOf(`## artifact { tiles=["top_brands_tile", "top_brands_tile_2"] }
${UNLABELLED}
query: top_brands_tile is brands -> top_brands + { where: region = 'y' }
query: top_brands_tile_2 is brands -> top_brands + { where: region = 'z' }
`);
      expect(tiles?.[0]).toMatchObject({
         query: "top_brands_tile",
         view: "brands -> top_brands",
      });
      expect(tiles?.[1]).toMatchObject({ view: "brands -> top_brands" });
      expect(tiles?.[0]).not.toHaveProperty("label");
   });

   it("is absent when the query's outputs are no view's", async () => {
      const tiles = await tilesOf(`## artifact { tiles=["kpis_tile"] }
${UNLABELLED}
query: kpis_tile is brands -> key_figures + { aggregate: most is max(n) }
`);
      expect(tiles?.[0]).toMatchObject({ query: "kpis_tile" });
      expect(tiles?.[0]).not.toHaveProperty("view");
   });

   it("is absent on a served-view entry", async () => {
      const tiles =
         await tilesOf(`## artifact { tiles=["brands -> top_brands"] }
${UNLABELLED}
`);
      expect(tiles?.[0]).not.toHaveProperty("view");
   });
});
