// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Second review of #1703, G8 with gated sources on: a #(materialize) source
 * over a same-file access-filtered base. With the base declaring a measure
 * (G8 keeps its block), against the same base without one (no kept block).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'acme', 10.0), (2, 'acme', 20.0), (3, 'globex', 5.0),
      (4, 'globex', 7.0), (5, 'initech', 3.0)
    ) AS t(id, org, amt)`,
];

const model = `##! experimental.givens
given: GROUPS :: string[]

#(access_filter) org in $GROUPS
source: b_gate is duckdb.table('ev') extend {
  measure: n is count()
}
#(materialize) storage="lake"
source: m_gate is b_gate extend { measure: total is amt.sum() }

#(access_filter) org in $GROUPS
source: b_plain is duckdb.table('ev') extend {
  dimension: big is amt > 6
}
#(materialize) storage="lake"
source: m_plain is b_plain extend { measure: total is amt.sum() }
`;

describe("review 1703b G8: an access-filtered same-file base, gated sources on", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
            ...(process.env.R_GATED
               ? { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" }
               : {}),
         },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model },
         seedSql,
         materialize: true,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   for (const [source, measures] of [
      ["m_plain", "total"],
      ["m_gate", "total, n"],
   ] as const) {
      for (const GROUPS of [["acme"], ["globex", "initech"]]) {
         it(`${source} as ${JSON.stringify(GROUPS)}: live's rows, from the table`, async () => {
            const malloy = `run: ${source} -> { group_by: org; aggregate: ${measures} }`;
            const run = await fixture.run({
               name: malloy,
               malloy,
               givens: { GROUPS },
            });
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            // The gate was declared on the base the re-root replaces: the served
            // copy declines, and live answers under the gate (second review).
            expect(run.servedVia).toBeUndefined();
         });
      }
   }
});
