// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `required` on a published given: set exactly when the declaration has no
 * default, on both paths a given reaches a client (the model surface, and a
 * report's manifest), so a reader can ask for the value instead of running
 * tiles into "has no value and no default".
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { malloyGivenToApi, type MalloyGiven } from "./given";
import { readReportModelFacts } from "./report_manifest";

const ROOT = "file:///given-required/";
const MODEL = `##! experimental.givens
given:
  RECALL_YEAR :: number
  MAKER :: filter<string> is f''
  SINCE :: date is @2020-01-01

source: s is duckdb.sql("select 2020 as y, 'Ford' as maker, date '2021-01-01' as d") extend {
  where: y = $RECALL_YEAR, maker ~ $MAKER, d >= $SINCE
}
`;

let connections: FixedConnectionMap;
beforeAll(() => {
   connections = new FixedConnectionMap(
      new Map([["duckdb", new DuckDBConnection("duckdb", ":memory:")]]),
      "duckdb",
   );
});

async function compile() {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[`${ROOT}m.malloy`, MODEL]])),
      connections,
   });
   return runtime
      .loadModel(new URL(`${ROOT}m.malloy`), { importBaseURL: new URL(ROOT) })
      .getModel();
}

describe("a published given's required flag", () => {
   it("is set on the model surface only for a given with no default", async () => {
      const model = await compile();
      const api = Object.fromEntries(
         (Array.from(model.givens.values()) as unknown as MalloyGiven[])
            .map(malloyGivenToApi)
            .map((given) => [given.name, given]),
      );
      expect(api.RECALL_YEAR?.required).toBe(true);
      // An empty filter default is a default: it filters nothing.
      expect(api.MAKER?.required).toBeUndefined();
      expect(api.SINCE?.required).toBeUndefined();
   });

   it("is carried into a report's given declarations", async () => {
      const model = await compile();
      const { givens } = readReportModelFacts("m.malloy", model._modelDef, [
         "RECALL_YEAR",
         "MAKER",
         "SINCE",
      ]);
      expect(givens.get("RECALL_YEAR")?.required).toBe(true);
      expect(givens.get("MAKER")?.required).toBeUndefined();
      expect(givens.get("SINCE")?.required).toBeUndefined();
   });
});
