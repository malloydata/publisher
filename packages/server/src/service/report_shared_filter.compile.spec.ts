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
import {
   buildReportManifest,
   lintReport,
   readReportModelFacts,
} from "./report_manifest";

/**
 * The missing-filter warning compares tiles over the same source, so it has to
 * know each query's source. A report imports its model, and Malloy names an
 * imported source differently from one defined in the file (the source itself,
 * with its name in `as`, rather than the name), so this is compiled for real.
 */
const REPORT_PATH = "report.malloy";
const BASE = "file:///shared/";

const MODEL = `
source: orders is duckdb.sql("select 'West' as region, 'web' as channel, 1 as n") extend {
  view: kpis is { aggregate: c is count() }
  view: by_channel is { group_by: channel; aggregate: c is count() }
  view: by_region is { group_by: region; aggregate: c is count() }
}
`;

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

async function findings(report: string) {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(
         new Map([
            [`${BASE}model.malloy`, MODEL],
            [`${BASE}${REPORT_PATH}`, report],
         ]),
      ),
      connections,
   });
   const model = await runtime
      .loadModel(new URL(`${BASE}${REPORT_PATH}`))
      .getModel();
   const modelDef = (model as unknown as { _modelDef: ModelDef })._modelDef;
   // The surfaced givens, as Model passes them: every given the file declares.
   const facts = readReportModelFacts(
      REPORT_PATH,
      modelDef,
      Object.values(modelDef.givens ?? {}).map((given) => given.name),
   );
   const manifest = buildReportManifest(facts);
   if (!manifest) throw new Error("expected a report");
   return lintReport(facts, manifest).map((finding) => finding.message);
}

const report = (third: string) => `##! experimental.givens
## artifact { tiles=["a", "b", "c"] }
import "model.malloy"

given: REGION :: filter<string> is f''

query: a is orders -> kpis + { where: region ~ $REGION }
query: b is orders -> by_channel + { where: region ~ $REGION }
query: c is ${third}
`;

describe("the missing shared filter, on a report that imports its model", () => {
   it("names the one tile over the source that leaves the filter out", async () => {
      const messages = await findings(report("orders -> by_region"));
      expect(
         messages.filter((m) => m.includes("does not filter on $REGION")),
      ).toEqual([
         expect.stringContaining(
            `"c" does not filter on $REGION, which 2 of this report's 3 tiles over "orders" do`,
         ),
      ]);
   });

   it("says nothing when every tile reads the filter", async () => {
      const messages = await findings(
         report("orders -> by_region + { where: region ~ $REGION }"),
      );
      expect(messages.filter((m) => m.includes("does not filter on"))).toEqual(
         [],
      );
   });
});
