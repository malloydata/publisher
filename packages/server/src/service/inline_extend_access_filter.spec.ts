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
 * A query whose source is an inline extension of a source with an
 * `#(access_filter)`: `flights_af extend { where: … } -> by_carrier`.
 *
 * Declared as a named query (`query: q is flights_af extend { … } -> v`, the
 * way a report writes a tile), Malloy stores the extension as a new struct
 * with no `sourceID`. The access filter's condition was grafted only onto
 * `flights_af` and onto named queries sharing its `sourceID`, so it never
 * reached that struct, and the proof that it landed refused every caller.
 *
 * Each case runs both as a named query and as ad-hoc text, for a caller the
 * filter admits to WN, and for one with no givens. A case either returns only
 * WN rows or is refused; none returns another carrier's rows.
 */

const TEST_DIR = path.join(os.tmpdir(), "inline-extend-access-filter-tests");
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

const HEADER = `##! experimental.givens
import "model.malloy"

given: CARRIER :: filter<string> is f''
`;

type Outcome = { rows: unknown } | { refused: string };

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

/** Runs `source -> view` as a named query in its own file, and as ad-hoc text. */
async function both(
   name: string,
   query: string,
   givens?: Record<string, GivenValue>,
): Promise<{ named: Outcome; adhoc: Outcome }> {
   const file = `q_${name}.malloy`;
   await fs.writeFile(
      path.join(PKG_DIR, file),
      `${HEADER}\nquery: q is ${query}\n`,
   );
   return {
      named: await run(file, "run: q", givens),
      adhoc: await run(file, `run: ${query}`, givens),
   };
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
const SCOPED =
   "flights_af extend { where: carrier_code ~ $CARRIER } -> by_carrier";
const DENIED = { refused: "AccessDeniedError" };
const same = (outcome: Outcome) => ({ named: outcome, adhoc: outcome });

describe("an inline extend with a where: over an access-filtered source", () => {
   it("returns the caller's rows, as a named query and as text", async () => {
      expect(await both("scoped", SCOPED, { CARRIER_SCOPE: "WN" })).toEqual(
         same({ rows: WN }),
      );
      expect(
         await both("scoped", SCOPED, { CARRIER_SCOPE: "WN", CARRIER: "WN" }),
      ).toEqual(same({ rows: WN }));
   });

   it("only narrows: a where: on another carrier returns nothing", async () => {
      expect(
         await both("scoped", SCOPED, { CARRIER_SCOPE: "WN", CARRIER: "AA" }),
      ).toEqual(same({ rows: [] }));
   });

   it("gives the rows a named extension and a refinement give, for every caller", async () => {
      await fs.writeFile(
         path.join(PKG_DIR, "forms.malloy"),
         `${HEADER}
source: r is flights_af extend { where: carrier_code ~ $CARRIER }
query: inline is ${SCOPED}
query: named is r -> by_carrier
query: refined is flights_af -> by_carrier + { where: carrier_code ~ $CARRIER }
`,
      );
      const callers: Array<Record<string, GivenValue> | undefined> = [
         { CARRIER_SCOPE: "WN" },
         { CARRIER_SCOPE: "WN", CARRIER: "AA" },
         { CARRIER_SCOPE: "AA" },
         undefined,
      ];
      for (const givens of callers) {
         const inline = await run("forms.malloy", "run: inline", givens);
         expect(inline).toEqual(
            await run("forms.malloy", "run: named", givens),
         );
         expect(inline).toEqual(
            await run("forms.malloy", "run: refined", givens),
         );
      }
   });

   it("refuses a caller with no givens, as querying the source directly does", async () => {
      expect(
         await run("model.malloy", "run: flights_af -> by_carrier"),
      ).toEqual(DENIED);
      expect(await both("scoped", SCOPED)).toEqual(same(DENIED));
   });

   it("is unchanged over an authorize gate", async () => {
      expect(
         await both(
            "gate",
            "flights_gate extend { where: carrier_code ~ $CARRIER } -> by_carrier",
            { ROLE: "analyst", CARRIER: "WN" },
         ),
      ).toEqual(same({ rows: WN }));
   });
});

describe("an inline extend cannot widen the access filter", () => {
   // Each returns only WN rows or is refused, as a named query and as text.
   const CASES: Array<[string, string, { named: Outcome; adhoc: Outcome }]> = [
      [
         "where_true",
         "flights_af extend { where: true } -> by_carrier",
         same({ rows: WN }),
      ],
      [
         "or_true",
         "flights_af extend { where: carrier_code ~ $CARRIER or true } -> by_carrier",
         same({ rows: WN }),
      ],
      [
         "chained",
         "flights_af extend { where: carrier_code ~ $CARRIER } extend { where: true } -> by_carrier",
         same({ rows: WN }),
      ],
      [
         "multi_stage",
         "flights_af extend { where: true } -> by_carrier -> { select: carrier_code, flight_count; order_by: carrier_code }",
         same({ rows: WN }),
      ],
      [
         "alias_field",
         "flights_af extend { dimension: cc2 is carrier_code } -> { group_by: carrier_code is cc2; aggregate: flight_count; order_by: carrier_code }",
         same({ rows: WN }),
      ],
      [
         "join_gate_source",
         "flights_af extend { join_one: g is flights_gate on id = g.id } -> { group_by: carrier_code is g.carrier_code; aggregate: flight_count; order_by: carrier_code }",
         same({ rows: WN }),
      ],
      // The access filter's column is renamed away and its name redefined to
      // a constant: the condition would read the constant, so it is refused.
      [
         "rename_shadow",
         "flights_af extend { rename: real_cc is carrier_code; dimension: carrier_code is 'WN' } -> by_carrier",
         same(DENIED),
      ],
      [
         "except_shadow",
         "flights_af extend { except: carrier_code; dimension: carrier_code is 'WN' } -> { group_by: carrier_code; aggregate: flight_count }",
         same(DENIED),
      ],
   ];

   for (const [name, query, expected] of CASES)
      it(name, async () => {
         expect(
            await both(name, query, { CARRIER_SCOPE: "WN", ROLE: "analyst" }),
         ).toEqual(expected);
      });

   it("refuses redefining the filtered dimension outright", async () => {
      const got = await both(
         "redefine",
         "flights_af extend { dimension: carrier_code is 'WN' } -> by_carrier",
         { CARRIER_SCOPE: "WN" },
      );
      expect(got.named).not.toHaveProperty("rows");
      expect(got.adhoc).not.toHaveProperty("rows");
   });
});
