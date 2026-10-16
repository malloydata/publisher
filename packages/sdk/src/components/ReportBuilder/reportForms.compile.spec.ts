// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, setDefaultTimeout } from "bun:test";
import { applyMapping, mappingOf } from "./controls";
import { edited } from "./testing/fixtures";

/**
 * The report the builder now writes, run by Malloy on DuckDB against the same
 * report written the way it used to be, a `<source>_tiles` extension whose
 * `where:` filters every tile. Each tile must return the same rows, for a
 * one-stage view and a two-stage one whose second stage drops the filtered
 * field, with the filter bound to givens the report declares.
 */

// DuckDB from cold.
setDefaultTimeout(30_000);

const MODEL = `source: flights is duckdb.sql("""
  select * from (values
    (1, 'AA', 'SFO'), (2, 'AA', 'LAX'), (3, 'UA', 'SFO'), (4, 'UA', 'ORD'),
    (5, 'UA', 'SFO'), (6, 'WN', 'DEN'), (7, 'AA', 'SFO')
  ) as t(id, carrier_code, origin_code)
""") extend {
  measure: flight_count is count()
  view: by_carrier is {
    group_by: carrier_code
    aggregate: flight_count
    order_by: carrier_code
  }
  view: by_route is {
    group_by: carrier_code, origin_code
    aggregate: flight_count
  } -> {
    group_by: carrier_code
    aggregate: routes is count(), flights is flight_count.sum()
    order_by: carrier_code
  }
}
`;

const GIVENS = `given: CARRIER :: filter<string> is f''
given: ORIGIN :: filter<string> is f''
`;

const OLD = `##! experimental.givens
## artifact { title="T" tiles=["flights_tiles -> by_carrier_tile", "flights_tiles -> by_route_tile"] }
import { flights } from "m.malloy"

${GIVENS}
source: flights_tiles is flights extend {
  where: carrier_code ~ $CARRIER, origin_code ~ $ORIGIN

  view: by_carrier_tile is by_carrier

  view: by_route_tile is by_route
}
`;

const NEW_START = `##! experimental.givens
## artifact { title="T" tiles=["flights -> by_carrier", "flights -> by_route"] }
import { flights } from "m.malloy"

${GIVENS}`;

async function runner() {
   const { Runtime } = await import("@malloydata/malloy");
   const { DuckDBConnection } = await import("@malloydata/db-duckdb");
   const connection = new DuckDBConnection("duckdb", ":memory:");
   const files = new Map<string, string>([["file:///r/m.malloy", MODEL]]);
   return {
      /** Each tile of `report`'s rows under `givens`. */
      rows: async (
         report: string,
         tiles: string[],
         givens: Record<string, string>,
      ) => {
         files.set("file:///r/report.malloy", report);
         const runtime = new Runtime({
            urlReader: { readURL: async (at: URL) => files.get(at.href) ?? "" },
            connections: { lookupConnection: async () => connection },
            givens,
         } as never);
         const model = runtime.loadModel(new URL("file:///r/report.malloy"));
         const out: unknown[] = [];
         for (const tile of tiles)
            out.push(
               (await model.loadQuery(`run: ${tile}`).run()).data.toObject(),
            );
         return out;
      },
      close: () => connection.close(),
   };
}

describe("a report in the new form", () => {
   it("returns the rows the extension form does, bound to the report's givens", async () => {
      // Built the way the builder builds it: both controls on both tiles.
      const now = await edited(NEW_START, (d) => {
         for (const [given, field] of [
            ["CARRIER", "carrier_code"],
            ["ORIGIN", "origin_code"],
         ]) {
            const rows = mappingOf(d, {
               name: given,
               field,
               type: "filter<string>",
            }).map((row) => ({ ...row, include: true }));
            applyMapping(d, given, rows);
         }
      });
      expect(now).toContain(
         "query: by_route_tile is flights -> by_route + { where: carrier_code ~ $CARRIER, where: origin_code ~ $ORIGIN }",
      );
      expect(now).not.toContain("source:");
      const run = await runner();
      try {
         for (const givens of [
            { CARRIER: "UA" },
            { CARRIER: "AA", ORIGIN: "SFO" },
            { ORIGIN: "SFO" },
            {},
         ]) {
            const old = await run.rows(
               OLD,
               [
                  "flights_tiles -> by_carrier_tile",
                  "flights_tiles -> by_route_tile",
               ],
               givens,
            );
            const fresh = await run.rows(
               now,
               ["by_carrier_tile", "by_route_tile"],
               givens,
            );
            expect(fresh).toEqual(old);
         }
         // The filter applies to the rows read, not to the view's output: UA
         // flew from SFO twice and from ORD once, and the second stage, which
         // drops origin_code, counts only the SFO route.
         expect(
            await run.rows(now, ["by_route_tile"], {
               CARRIER: "UA",
               ORIGIN: "SFO",
            }),
         ).toEqual([[{ carrier_code: "UA", routes: 1, flights: 2 }]]);
      } finally {
         await run.close();
      }
   });
});
