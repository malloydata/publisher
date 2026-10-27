// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection, GivenValue } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Model } from "./model";

/**
 * Access control on a report's tiles, through Publisher's own query path
 * (`Model.getQueryResults`, which applies `#(access_filter)` and
 * `#(authorize)`), for every form a report tile takes:
 *
 * - a tile naming a served view, `"flights_af -> by_carrier"`;
 * - a query of the report's own over it, with tags;
 * - one with the report's own `where:`;
 * - one whose `where:` is bound to a given the report declares;
 *
 * and each in the form reports were written before, a `<source>_tiles`
 * extension. A tile must never return a row the served source would not
 * return to the same caller, and a report's `where:` can only narrow.
 */

const TEST_DIR = path.join(os.tmpdir(), "report-tile-access-tests");
const DB_DIR = path.join(TEST_DIR, "db");
const PKG_DIR = path.join(TEST_DIR, "pkg");

let duckdb: DuckDBConnection;
const connections = (): Map<string, Connection> =>
   new Map<string, Connection>([["duckdb", duckdb]]);

const SEED = `
CREATE TABLE flights (id INTEGER, carrier_code VARCHAR, origin_code VARCHAR);
INSERT INTO flights VALUES
  (1, 'AA', 'SFO'), (2, 'AA', 'LAX'), (3, 'WN', 'SFO'), (4, 'WN', 'DEN'),
  (5, 'WN', 'SFO'), (6, 'UA', 'ORD');
`;

const VIEWS = `extend {
  measure: flight_count is count()
  view: by_carrier is {
    group_by: carrier_code
    aggregate: flight_count
    order_by: carrier_code
  }
}`;

const MODEL = `##! experimental.givens

given:
  CARRIER_SCOPE :: string
  ROLE :: string

#(access_filter) carrier_code = $CARRIER_SCOPE
source: flights_af is duckdb.table('flights') ${VIEWS}

#(authorize) 'analyst' = $ROLE
source: flights_gate is duckdb.table('flights') ${VIEWS}
`;

/**
 * A report over `source` with one tile of each form, as the builder writes
 * them now. It imports the model whole: a gate's given has to be one the
 * report's own file can bind, and importing only the source by name leaves
 * it out (the query then fails to compile), in either form.
 */
const NEW_REPORT = (source: string) => `##! experimental.givens
## artifact { title="T" tiles=["${source} -> by_carrier", "tagged_tile", "sfo_tile", "carrier_tile"] }
import "model.malloy"

given: CARRIER :: filter<string> is f''

# viz=bar
# colspan=2
query: tagged_tile is ${source} -> by_carrier

query: sfo_tile is ${source} -> by_carrier + { where: origin_code = 'SFO' }

query: carrier_tile is ${source} -> by_carrier + { where: carrier_code ~ $CARRIER }
`;

/** The same tiles as a report-local extension, the way reports were written before. */
const OLD_REPORT = (source: string) => `##! experimental.givens
## artifact { title="T" tiles=["${source}_tiles -> plain", "${source}_tiles -> tagged_tile", "${source}_sfo -> sfo_tile", "${source}_tiles -> carrier_tile"] }
import "model.malloy"

given: CARRIER :: filter<string> is f''

source: ${source}_tiles is ${source} extend {
  view: plain is by_carrier

  # bar_chart
  # colspan=2
  view: tagged_tile is by_carrier

  view: carrier_tile is by_carrier + { where: carrier_code ~ $CARRIER }
}

source: ${source}_sfo is ${source} extend {
  where: origin_code = 'SFO'
  view: sfo_tile is by_carrier
}
`;

/** Each tile's run statement, in order: direct, tagged, the report's where, the report's given. */
const NEW_TILES = (source: string) => [
   `run: ${source} -> by_carrier`,
   "run: tagged_tile",
   "run: sfo_tile",
   "run: carrier_tile",
];
const OLD_TILES = (source: string) => [
   `run: ${source}_tiles -> plain`,
   `run: ${source}_tiles -> tagged_tile`,
   `run: ${source}_sfo -> sfo_tile`,
   `run: ${source}_tiles -> carrier_tile`,
];

type Outcome = { rows: unknown } | { refused: string };

/** What a caller with `givens` gets for `query` run against `file`, as Publisher answers it. */
async function run(
   file: string,
   query: string,
   givens?: Record<string, GivenValue>,
): Promise<Outcome> {
   const model = await Model.create("pkg", PKG_DIR, file, connections());
   try {
      const { compactResult } = await model.getQueryResults(
         undefined,
         undefined,
         query,
         undefined,
         false,
         givens,
      );
      return { rows: compactResult };
   } catch (error) {
      return { refused: (error as Error).constructor.name };
   }
}

async function tiles(
   file: string,
   queries: string[],
   givens?: Record<string, GivenValue>,
) {
   const out: Outcome[] = [];
   for (const query of queries) out.push(await run(file, query, givens));
   return out;
}

beforeAll(async () => {
   await fs.mkdir(DB_DIR, { recursive: true });
   await fs.mkdir(PKG_DIR, { recursive: true });
   duckdb = new DuckDBConnection(
      "duckdb",
      path.join(DB_DIR, "test.duckdb"),
      DB_DIR,
   );
   for (const statement of SEED.trim()
      .split(";")
      .filter((s) => s.trim()))
      await duckdb.runSQL(`${statement.trim()};`);
   await fs.writeFile(path.join(PKG_DIR, "model.malloy"), MODEL);
   for (const source of ["flights_af", "flights_gate"]) {
      await fs.writeFile(
         path.join(PKG_DIR, `${source}_new.malloy`),
         NEW_REPORT(source),
      );
      await fs.writeFile(
         path.join(PKG_DIR, `${source}_old.malloy`),
         OLD_REPORT(source),
      );
   }
});

afterAll(async () => {
   try {
      await duckdb.close();
      await fs.rm(TEST_DIR, { recursive: true, force: true });
   } catch {
      // ignore cleanup errors
   }
});

const WN = [{ carrier_code: "WN", flight_count: 3 }];
const WN_SFO = [{ carrier_code: "WN", flight_count: 2 }];

describe("a report's tiles over a source with an access filter", () => {
   it("returns only the caller's rows, with the report's own where on top", async () => {
      const got = await tiles(
         "flights_af_new.malloy",
         NEW_TILES("flights_af"),
         { CARRIER_SCOPE: "WN", CARRIER: "WN" },
      );
      expect(got).toEqual([
         { rows: WN },
         { rows: WN },
         { rows: WN_SFO },
         { rows: WN },
      ]);
   });

   it("never widens the access filter: a report filter on another carrier returns nothing", async () => {
      const got = await tiles("flights_af_new.malloy", ["run: carrier_tile"], {
         CARRIER_SCOPE: "WN",
         CARRIER: "AA",
      });
      expect(got).toEqual([{ rows: [] }]);
   });

   it("answers a caller with no givens as the source does when queried directly", async () => {
      const direct = await run("model.malloy", "run: flights_af -> by_carrier");
      // A 403: an access filter whose given is not set admits nobody.
      expect(direct).toEqual({ refused: "AccessDeniedError" });
      expect(
         await tiles("flights_af_new.malloy", NEW_TILES("flights_af")),
      ).toEqual(NEW_TILES("flights_af").map(() => direct));
   });

   it("gives the same answers as the extension form, for every caller", async () => {
      for (const givens of <Array<Record<string, GivenValue> | undefined>>[
         { CARRIER_SCOPE: "WN", CARRIER: "WN" },
         { CARRIER_SCOPE: "WN", CARRIER: "AA" },
         { CARRIER_SCOPE: "AA" },
         undefined,
      ])
         expect(
            await tiles(
               "flights_af_new.malloy",
               NEW_TILES("flights_af"),
               givens,
            ),
         ).toEqual(
            await tiles(
               "flights_af_old.malloy",
               OLD_TILES("flights_af"),
               givens,
            ),
         );
   });
});

describe("a report's tiles over a source with an authorize gate", () => {
   it("returns rows to a caller the gate admits, with the report's own where on top", async () => {
      const got = await tiles(
         "flights_gate_new.malloy",
         NEW_TILES("flights_gate"),
         { ROLE: "analyst", CARRIER: "WN" },
      );
      expect(got).toEqual([
         {
            rows: [
               { carrier_code: "AA", flight_count: 2 },
               { carrier_code: "UA", flight_count: 1 },
               { carrier_code: "WN", flight_count: 3 },
            ],
         },
         {
            rows: [
               { carrier_code: "AA", flight_count: 2 },
               { carrier_code: "UA", flight_count: 1 },
               { carrier_code: "WN", flight_count: 3 },
            ],
         },
         {
            rows: [
               { carrier_code: "AA", flight_count: 1 },
               { carrier_code: "WN", flight_count: 2 },
            ],
         },
         { rows: WN },
      ]);
   });

   it("refuses a caller with no givens, or one the gate does not admit, on every tile", async () => {
      const direct = await run(
         "model.malloy",
         "run: flights_gate -> by_carrier",
      );
      expect(direct).toEqual({ refused: "AccessDeniedError" });
      for (const givens of [undefined, { ROLE: "intern", CARRIER: "WN" }])
         expect(
            await tiles(
               "flights_gate_new.malloy",
               NEW_TILES("flights_gate"),
               givens,
            ),
         ).toEqual(
            NEW_TILES("flights_gate").map(() => ({
               refused: "AccessDeniedError",
            })),
         );
   });

   it("gives the same answers as the extension form, for every caller", async () => {
      for (const givens of <Array<Record<string, GivenValue> | undefined>>[
         { ROLE: "analyst", CARRIER: "WN" },
         { ROLE: "intern" },
         undefined,
      ])
         expect(
            await tiles(
               "flights_gate_new.malloy",
               NEW_TILES("flights_gate"),
               givens,
            ),
         ).toEqual(
            await tiles(
               "flights_gate_old.malloy",
               OLD_TILES("flights_gate"),
               givens,
            ),
         );
   });
});

/**
 * The other place a report could put a filter, on the tile's source:
 * `flights extend { where: … } -> by_carrier`. The builder writes
 * `+ { where: … }` on the named view instead, but an author may write this
 * form, and Publisher applies the source's `#(access_filter)` to it the same
 * way: the caller's rows only, with the report's filter on top.
 */
describe("a filter on the tile's source, the form the builder does not write", () => {
   const SCOPED = (source: string) => `##! experimental.givens
## artifact { title="T" tiles=["scoped_tile"] }
import "model.malloy"

given: CARRIER :: filter<string> is f''

query: scoped_tile is ${source} extend { where: carrier_code ~ $CARRIER } -> by_carrier
`;

   it("returns only the caller's rows over an access filter, and admits through a gate", async () => {
      await fs.writeFile(
         path.join(PKG_DIR, "af_scoped.malloy"),
         SCOPED("flights_af"),
      );
      await fs.writeFile(
         path.join(PKG_DIR, "gate_scoped.malloy"),
         SCOPED("flights_gate"),
      );
      expect(
         await run("af_scoped.malloy", "run: scoped_tile", {
            CARRIER_SCOPE: "WN",
            CARRIER: "WN",
         }),
      ).toEqual({ rows: WN });
      // No report filter: still only the access filter's rows.
      expect(
         await run("af_scoped.malloy", "run: scoped_tile", {
            CARRIER_SCOPE: "WN",
         }),
      ).toEqual({ rows: WN });
      // A report filter on another carrier never widens it.
      expect(
         await run("af_scoped.malloy", "run: scoped_tile", {
            CARRIER_SCOPE: "WN",
            CARRIER: "AA",
         }),
      ).toEqual({ rows: [] });
      // No givens: a 403, as when the source is queried directly.
      expect(await run("af_scoped.malloy", "run: scoped_tile")).toEqual({
         refused: "AccessDeniedError",
      });
      expect(
         await run("gate_scoped.malloy", "run: scoped_tile", {
            ROLE: "analyst",
            CARRIER: "WN",
         }),
      ).toEqual({ rows: WN });
   });
});
