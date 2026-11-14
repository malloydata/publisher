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
import { afterAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import type { ServeBinding } from "./materialization/serve/materialization_serve_transform";
import { logger } from "../logger";
import { resetMaterializationTelemetryForTesting } from "../materialization_metrics";
import { startMetricsHarness } from "../test_helpers/metrics_harness";
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

async function buildModel(
   withDestinations: boolean,
   text: string = MODEL,
   file = "m.malloy",
   givens?: { name: string; type: string }[],
): Promise<Model> {
   fs.writeFileSync(path.join(DIR, file), text);
   const connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map([[pathToFileURL(path.join(DIR, file)).href, text]]),
      ),
      connections,
   });
   const materializer = runtime.loadModel(pathToFileURL(path.join(DIR, file)), {
      importBaseURL: new URL(ROOT),
   });
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const modelDef = ((await materializer.getModel()) as any)._modelDef;
   const model = new Model({
      packageName: "pkg",
      modelPath: file,
      dataStyles: {},
      modelType: "model",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      modelMaterializer: materializer as any,
      modelDef,
      modelInfo: modelDefToModelInfo(modelDef),
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ...(givens ? { givens: givens as any } : {}),
   });
   if (withDestinations) {
      const serveConfig = new MalloyConfig({ connections: {} });
      serveConfig.wrapConnections(() => connections);
      model.setServeDestinationConfig(() => serveConfig);
   }
   // The preview compiles a served copy of the author's own files.
   model.setServedCopyContext(DIR, new Map([["duckdb", duckdb]]));
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

   // A table that lacks a column the source declares (`region`) leaves the
   // source as written in the served copy, so every view of it answers live,
   // each with the reason. (The retired serve shape dropped only the view
   // that read the column; the preview reports what the served copy will
   // actually do.)
   it("reports every view live, with the column the table lacks, when the source cannot be re-rooted", async () => {
      const model = await buildModel(true);
      const views = (await model.previewStorageViews([BINDING])).get("X");
      expect(views?.map((v) => [v.view, v.verdict])).toEqual([
         ["totals", "live"],
         ["by_region", "live"],
      ]);
      expect(views?.[1].detail).toContain("region");
   });

   // A DuckLake destination is looked up by attaching its catalog, which
   // fails before the first build has created it. Malloy learns the dialect
   // from that lookup, so a failed one left the dialect unknown and every view
   // previewed live with "Cannot determine dialect".
   it("serves the views of a destination whose catalog cannot be attached yet", async () => {
      const model = await buildModel(true);
      const unattachable = new MalloyConfig({ connections: {} });
      unattachable.wrapConnections(() => ({
         lookupConnection: async () => {
            throw new Error(
               'Existing DuckLake at metadata catalog "postgres:…" does not exist - and creating a new DuckLake is explicitly disabled',
            );
         },
      }));
      model.setServeDestinationConfig(() => unattachable);
      const full: ServeBinding = {
         ...BINDING,
         schema: [...BINDING.schema, { name: "region", type: "VARCHAR" }],
      };
      expect((await model.previewStorageViews([full])).get("X")).toEqual([
         { view: "totals", verdict: "serves" },
         { view: "by_region", verdict: "serves" },
      ]);
   });

   it("says nothing without a storage destination, since nothing serves from storage then", async () => {
      const model = await buildModel(false);
      expect((await model.previewStorageViews([BINDING])).size).toBe(0);
   });

   // A given with no default (every #(secure) attribute) is required to run a
   // query, not to say whether a view compiles against the stored columns: a
   // view that reads one serves once the caller supplies it.
   it("serves a view that reads a given with no default", async () => {
      const model = await buildModel(
         true,
         `##! experimental.givens\n#(secure)\ngiven:\n  TENANTS :: string[]\ngiven:\n  REGION :: string\ngiven:\n  REGIONS :: filter<string>\n\n${MODEL.replace(
            "view: totals",
            "view: in_region is { where: region = $REGION; aggregate: total }\n   view: in_regions is { where: region ~ $REGIONS; aggregate: total }\n   view: totals",
         )}`,
         "given.malloy",
         [
            { name: "TENANTS", type: "string[]" },
            { name: "REGION", type: "string" },
            { name: "REGIONS", type: "filter<string>" },
         ],
      );
      const full: ServeBinding = {
         ...BINDING,
         schema: [...BINDING.schema, { name: "region", type: "VARCHAR" }],
      };
      expect((await model.previewStorageViews([full])).get("X")).toEqual([
         { view: "in_region", verdict: "serves" },
         { view: "in_regions", verdict: "serves" },
         { view: "totals", verdict: "serves" },
         { view: "by_region", verdict: "serves" },
      ]);
   });

   // Nothing is served, so a preview must not look like serve traffic.
   it("records no serve metric and logs no serve warning", async () => {
      const harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
      const warn = spyOn(logger, "warn");
      const info = spyOn(logger, "info");
      try {
         const model = await buildModel(true);
         // BINDING lacks `region`, so the served copy cannot re-root X.
         await model.previewStorageViews([BINDING]);
         expect(warn).not.toHaveBeenCalled();
         expect(info).not.toHaveBeenCalled();
         expect(
            await harness.collectCounter(
               "publisher_storage_bindings_withheld_total",
               {},
            ),
         ).toBe(0);
      } finally {
         warn.mockRestore();
         info.mockRestore();
         resetMaterializationTelemetryForTesting();
         await harness.shutdown();
      }
   });
});
