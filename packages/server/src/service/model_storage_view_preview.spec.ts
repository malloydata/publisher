// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `Model.previewStorageViews` (ms2data/service#7090): each named view of a
 * stored source, compiled against a throwaway serve shape, with no table
 * built. A view the shape reproduces serves; one it cannot names the compile
 * error that sends it live. The model's own shape is left alone.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   MalloyConfig,
   modelDefToModelInfo,
   Runtime,
} from "@malloydata/malloy";
import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import type { ServeBinding } from "./materialization/serve/materialization_serve_transform";
import { Model } from "./model";

// On disk: the serve shape lifts a source's views verbatim from its file.
const DIR = fs.mkdtempSync(path.join(os.tmpdir(), "storage-view-preview-"));
const ROOT = `${pathToFileURL(DIR).href}/`;
const MODEL = `source: X is duckdb.sql("SELECT 1 AS id, 'east' AS region, 10 AS amount") extend {
   measure: total is amount.sum()
   view: totals is { aggregate: total }
   view: by_region is { group_by: region; aggregate: total }
}
`;

/** Declares every column but `region`, as a table built without it would. */
const BINDING: ServeBinding = {
   sourceName: "X",
   destinationName: "duckdb",
   virtualHandle: "h",
   tablePath: "never_read",
   schema: [
      { name: "id", type: "INTEGER" },
      { name: "amount", type: "INTEGER" },
   ],
};

const duckdb = new DuckDBConnection("duckdb", ":memory:");
fs.writeFileSync(path.join(DIR, "m.malloy"), MODEL);
afterAll(async () => {
   await duckdb.close();
   fs.rmSync(DIR, { recursive: true, force: true });
});

async function buildModel(withDestinations: boolean): Promise<Model> {
   const connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map([[pathToFileURL(path.join(DIR, "m.malloy")).href, MODEL]]),
      ),
      connections,
   });
   const materializer = runtime.loadModel(
      pathToFileURL(path.join(DIR, "m.malloy")),
      {
         importBaseURL: new URL(ROOT),
      },
   );
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const modelDef = ((await materializer.getModel()) as any)._modelDef;
   const model = new Model({
      packageName: "pkg",
      modelPath: "m.malloy",
      dataStyles: {},
      modelType: "model",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      modelMaterializer: materializer as any,
      modelDef,
      modelInfo: modelDefToModelInfo(modelDef),
   });
   if (withDestinations) {
      const serveConfig = new MalloyConfig({ connections: {} });
      serveConfig.wrapConnections(() => connections);
      model.setServeDestinationConfig(() => serveConfig);
   }
   return model;
}

describe("previewing a stored source's views", () => {
   it("serves the views of a shape that reproduces them", async () => {
      const model = await buildModel(true);
      const full: ServeBinding = {
         ...BINDING,
         schema: [...BINDING.schema, { name: "region", type: "VARCHAR" }],
      };
      const verdicts = await model.previewStorageViews([full]);
      expect(verdicts.get("X")).toEqual([
         { view: "totals", verdict: "serves" },
         { view: "by_region", verdict: "serves" },
      ]);
      // A preview binds nothing on the model itself.
      expect(model.servedStorageSourceNames()).toEqual([]);
   });

   // What serving does today: one view the stored columns cannot answer drops
   // every view of the source from the shape, so the preview says so for each.
   it("says a source's views are live when one of them cannot compile against the stored columns", async () => {
      const model = await buildModel(true);
      const views = (await model.previewStorageViews([BINDING])).get("X");
      expect(views?.map((v) => [v.view, v.verdict])).toEqual([
         ["totals", "live"],
         ["by_region", "live"],
      ]);
      expect(views?.[0].detail).toContain("drops all of them");
   });

   it("says nothing without a storage destination, since nothing serves from storage then", async () => {
      const model = await buildModel(false);
      expect((await model.previewStorageViews([BINDING])).size).toBe(0);
   });
});
