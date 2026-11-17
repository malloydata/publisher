// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A persisted query's trailing `extend` that renames a column to a name
 * another column had (a swap, or a reuse of the old name by another rename or
 * a dimension). Its table holds the query's output names, and the serve side
 * must read each field from its own stored column, with every digit: served
 * equals live, on the `storage=` tier and in the source's own database.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { useMaterializationOn } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

/** A BIGINT past 2^53; with 1 added, a sum no JavaScript number holds. */
const BIG = 4530000000000225186n;
const SUM = `${BIG + 1n}`;

const SEED = [
   `CREATE TABLE raw AS SELECT * FROM (VALUES
      ('a', CAST('${BIG}' AS BIGINT)), ('a', CAST('1' AS BIGINT)),
      ('b', CAST('${BIG}' AS BIGINT)), ('b', CAST('1' AS BIGINT)),
      ('b', CAST('5' AS BIGINT))
    ) AS t(sub, v)`,
];

const QUERY = "-> { group_by: sub; aggregate: total is v.sum(), n is count() }";

/** Each source, the query that reads it, and what live must give. */
const CASES: {
   name: string;
   source: string;
   extend: string;
   malloy: string;
   /** Per row, by `sub`: each checked column's exact value. */
   expect: Record<string, Record<string, string>>;
}[] = [
   {
      name: "X1: a renamed BIGINT sum whose old name another rename takes",
      source: "px",
      extend: "rename: total2 is total\n  rename: total is n",
      malloy: "run: px -> { select: sub, total2, total; order_by: sub }",
      expect: {
         a: { total2: SUM, total: "2" },
         b: { total2: `${BIG + 6n}`, total: "3" },
      },
   },
   {
      name: "X7: a renamed BIGINT sum whose old name a dimension takes",
      source: "pd",
      extend: "rename: amount is total\n  dimension: total is concat(sub, '!')",
      malloy: "run: pd -> { select: sub, amount, total; order_by: sub }",
      expect: {
         a: { amount: SUM, total: "a!" },
         b: { amount: `${BIG + 6n}`, total: "b!" },
      },
   },
   {
      name: "a rename that swaps two names",
      source: "sw",
      extend: "rename: tmp is total\n  rename: total is n\n  rename: n is tmp",
      malloy: "run: sw -> { select: sub, n, total; order_by: sub }",
      expect: {
         a: { n: SUM, total: "2" },
         b: { n: `${BIG + 6n}`, total: "3" },
      },
   },
];

function model(placement: (name: string) => string): string {
   return `##! experimental { persistence }
source: raw is duckdb.table('raw')
${CASES.map(
   (c) => `
#@ persist ${placement(c.source)}
source: ${c.source} is raw ${QUERY} extend {
  ${c.extend}
}`,
).join("\n")}
`;
}

for (const tier of [
   {
      label: "storage=",
      files: model((name) => `name="${name}_t" storage="${DESTINATION}"`),
      colocated: undefined,
   },
   {
      label: "in the source's own database",
      files: model((name) => `name="${name}_c"`),
      colocated: CASES.map((c) => c.source),
   },
]) {
   describe(`a rename that reuses or swaps names reads the right stored column [${tier.label}]`, () => {
      let fixture: R4Fixture;
      useMaterializationOn();
      beforeAll(async () => {
         fixture = await buildScenario({
            files: { "m.malloy": tier.files },
            seedSql: SEED,
            ...(tier.colocated ? { colocated: tier.colocated } : {}),
         });
      });
      afterAll(async () => {
         await disposeScenarios();
      });

      for (const c of CASES) {
         it(c.name, async () => {
            const run = await fixture.run({ name: c.name, malloy: c.malloy });
            for (const [which, rows] of [
               ["live", run.live],
               ["served", run.served],
            ] as const) {
               expect({
                  which,
                  rows: Object.fromEntries(
                     rows.map((r) => [
                        String(r.sub),
                        Object.fromEntries(
                           Object.keys(c.expect.a).map((k) => [
                              k,
                              String(r[k]),
                           ]),
                        ),
                     ]),
                  ),
               }).toEqual({ which, rows: c.expect });
            }
            expect(rowDifference(run.live, run.served)).toBeUndefined();
            expect(run.servedVia).toBe("served_copy");
         });
      }
   });
}
