// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A persisted source whose trailing `extend` renames a column to a name
 * another column had: a swap, a chain, or a reuse of the old name by another
 * rename or a dimension. A v1 `#@ persist` table stores the query's output
 * names, and a `#(materialize)` table the source after its renames, so the
 * serve side must match each field to its stored column by the name that
 * table stores it under, and keep every digit of a big integer. Every case
 * must equal live, on the `storage=` tier and in the source's own database.
 *
 * For a gated `#@ persist` source a mismatch served rows its row-level gate
 * hides (live 0 rows, served 2); the gated cases pin that served rows equal
 * live's for each caller.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

/** A BIGINT past 2^53; with 1 added, a sum no JavaScript number holds. */
const BIG = 4530000000000225186n;

const SEED = [
   `CREATE TABLE raw AS SELECT * FROM (VALUES
      ('a', CAST('${BIG}' AS BIGINT), 'x', 1, DATE '2026-01-01'),
      ('a', CAST('1' AS BIGINT), 'y', 2, DATE '2026-01-02'),
      ('b', CAST('${BIG}' AS BIGINT), 'x', 3, DATE '2026-01-03'),
      ('b', CAST('1' AS BIGINT), 'y', 4, DATE '2026-01-04'),
      ('b', CAST('5' AS BIGINT), 'z', 5, DATE '2026-01-05')
    ) AS t(sub, v, g, id, d)`,
   `CREATE TABLE dim AS SELECT * FROM (VALUES ('a','Alpha'),('b','Beta')) AS t(sub, label)`,
];

const Q = "-> { group_by: sub; aggregate: total is v.sum(), n is count() }";
const SWAP = "rename: tmp is total\n  rename: total is n\n  rename: n is tmp";

const HEAD =
   "##! experimental { persistence givens access_modifiers }\nsource: raw is duckdb.table('raw')\n";

interface Case {
   name: string;
   model: (place: (src: string) => string, kind: "persist" | "mat") => string;
   kinds: ("persist" | "mat")[];
   srcs: string[];
   queries: { malloy: string; givens?: Record<string, unknown> }[];
   env?: Record<string, string>;
}

const CASES: Case[] = [
   {
      name: "EXT persisted extension of swapped base",
      kinds: ["persist", "mat"],
      srcs: ["xb", "xe"],
      model: (p, k) => `${HEAD}
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("xb")}
source: xb is raw ${Q} extend {
  ${SWAP}
}
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("xe")}
source: xe is xb extend {
  dimension: big is n
  rename: m is total
}
source: xd is xe extend { measure: sn is n.sum() }`,
      queries: [
         { malloy: "run: xe -> { select: sub, n, m, big; order_by: sub }" },
         {
            malloy:
               "run: xd -> { group_by: sub; aggregate: sn; order_by: sub }",
         },
      ],
   },
   {
      name: "EXT query over swapped persisted base",
      kinds: ["persist", "mat"],
      srcs: ["qb", "qe"],
      model: (p, k) => `${HEAD}
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("qb")}
source: qb is raw -> { group_by: sub, g; aggregate: total is v.sum(), n is count() } extend {
  ${SWAP}
}
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("qe")}
source: qe is qb -> { group_by: sub; aggregate: total is n.sum(), n is total.sum() } extend {
  ${SWAP}
}`,
      queries: [
         { malloy: "run: qe -> { select: sub, n, total; order_by: sub }" },
      ],
   },
   {
      name: "swap",
      kinds: ["persist", "mat"],
      srcs: ["sw"],
      model: (p, k) => `${HEAD}
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("sw")}
source: sw is raw ${Q} extend {
  ${SWAP}
}`,
      queries: [
         { malloy: "run: sw -> { select: sub, n, total; order_by: sub }" },
      ],
   },
   {
      name: "rename onto another's old name (n2 is n; n is total)",
      kinds: ["persist", "mat"],
      srcs: ["ro"],
      model: (p, k) => `${HEAD}
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("ro")}
source: ro is raw ${Q} extend {
  rename: n2 is n
  rename: n is total
}`,
      queries: [{ malloy: "run: ro -> { select: sub, n, n2; order_by: sub }" }],
   },
   {
      name: "4-cycle chain",
      kinds: ["persist", "mat"],
      srcs: ["cy"],
      model: (p, k) => `${HEAD}
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("cy")}
source: cy is raw -> { group_by: sub, g; aggregate: total is v.sum(), n is count() } extend {
  rename: t0 is total
  rename: total is n
  rename: n is g
  rename: g is sub
  rename: sub is t0
}`,
      queries: [
         { malloy: "run: cy -> { select: sub, total, n, g; order_by: g, n }" },
      ],
   },
   {
      name: "swap with a join",
      kinds: ["persist", "mat"],
      srcs: ["sj"],
      model: (p, k) => `${HEAD}
source: dim is duckdb.table('dim') extend { primary_key: sub }
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("sj")}
source: sj is raw ${Q} extend {
  ${SWAP}
  join_one: d is dim on sub = d.sub
}`,
      queries: [
         {
            malloy:
               "run: sj -> { select: sub, n, total, d.label; order_by: sub }",
         },
         {
            malloy:
               "run: sj -> { group_by: d.label; aggregate: s is n.sum(), c is total.sum(); order_by: label }",
         },
      ],
   },
   {
      name: "refinement-rooted swap (table rooted)",
      kinds: ["persist", "mat"],
      srcs: ["rr"],
      model: (p, k) => `${HEAD}
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("rr")}
source: rr is raw extend {
  rename: tmp is v
  rename: v is id
  rename: id is tmp
}`,
      queries: [
         {
            malloy:
               "run: rr -> { group_by: sub; aggregate: sv is v.sum(), si is id.sum(); order_by: sub }",
         },
         { malloy: "run: rr -> { select: sub, v, id; order_by: v }" },
      ],
   },
   {
      name: "string swap under a row gate",
      kinds: ["persist", "mat"],
      srcs: ["gs"],
      env: { PUBLISHER_MATERIALIZE_GATED_SOURCES: "true" },
      model: (p, k) => `${HEAD}
given: ORGS :: string[]
#(access_filter) sub in $ORGS
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("gs")}
source: gs is raw -> { group_by: sub, g; aggregate: total is v.sum() } extend {
  rename: tmp is sub
  rename: sub is g
  rename: g is tmp
  where: sub in $ORGS
}`,
      queries: [
         {
            malloy: "run: gs -> { select: sub, g, total; order_by: sub, g }",
            givens: { ORGS: ["x"] },
         },
         {
            malloy: "run: gs -> { select: sub, g, total; order_by: sub, g }",
            givens: { ORGS: ["a"] },
         },
      ],
   },
   {
      name: "string swap, gated downstream extension",
      kinds: ["persist", "mat"],
      srcs: ["gb"],
      model: (p, k) => `${HEAD}
given: ORGS :: string[]
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("gb")}
source: gb is raw -> { group_by: sub, g; aggregate: total is v.sum() } extend {
  rename: tmp is sub
  rename: sub is g
  rename: g is tmp
}
#(access_filter) sub in $ORGS
source: gdown is gb extend { where: sub in $ORGS }`,
      queries: [
         {
            malloy: "run: gdown -> { select: sub, g, total; order_by: sub, g }",
            givens: { ORGS: ["x"] },
         },
         {
            malloy: "run: gdown -> { select: sub, g, total; order_by: sub, g }",
            givens: { ORGS: ["a"] },
         },
      ],
   },
   {
      name: "incremental swap",
      kinds: ["persist", "mat"],
      srcs: ["inc"],
      model: (p, k) => `${HEAD}
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("inc")} refresh="incremental" watermark="d"
source: inc is raw -> { group_by: sub, d; aggregate: total is v.sum(), n is count() } extend {
  ${SWAP}
}`,
      queries: [
         {
            malloy:
               "run: inc -> { select: sub, d, n, total; order_by: sub, d }",
         },
      ],
   },
   {
      name: "X1: a renamed BIGINT sum whose old name another rename takes",
      kinds: ["persist", "mat"],
      srcs: ["px"],
      model: (p, k) => `${HEAD}
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("px")}
source: px is raw ${Q} extend {
  rename: total2 is total
  rename: total is n
}`,
      queries: [
         { malloy: "run: px -> { select: sub, total2, total; order_by: sub }" },
      ],
   },
   {
      name: "X7: a renamed BIGINT sum whose old name a dimension takes",
      kinds: ["persist", "mat"],
      srcs: ["pd"],
      model: (p, k) => `${HEAD}
${k === "persist" ? "#@ persist" : "#(materialize)"} ${p("pd")}
source: pd is raw ${Q} extend {
  rename: amount is total
  dimension: total is concat(sub, '!')
}`,
      queries: [
         { malloy: "run: pd -> { select: sub, amount, total; order_by: sub }" },
      ],
   },
];

/** A row's values, a bigint as its exact digits. */
const exact = (rows: Record<string, unknown>[]) =>
   JSON.stringify(rows, (_k, v) => (typeof v === "bigint" ? `${v}n` : v));

for (const c of CASES) {
   for (const kind of c.kinds) {
      for (const tier of ["storage=", "database"] as const) {
         const place = (src: string) =>
            tier === "storage="
               ? `name="${src}_t" storage="${DESTINATION}"`
               : `name="${src}_c"`;
         describe(`a rename that reuses or swaps names: ${c.name} [${kind === "mat" ? "#(materialize)" : "#@ persist"}, ${tier}]`, () => {
            let fixture: R4Fixture;
            let restore: () => void = () => {};
            beforeAll(async () => {
               restore = installSettings({
                  env: {
                     PERSIST_STORAGE_MODE: "on",
                     PUBLISHER_MATERIALIZE: "true",
                     ...(c.env ?? {}),
                  },
               }).restore;
               fixture = await buildScenario({
                  files: { "m.malloy": c.model(place, kind) },
                  seedSql: SEED,
                  ...(kind === "mat" ? { materialize: true } : {}),
                  ...(tier === "database" ? { database: c.srcs } : {}),
               });
            });
            afterAll(async () => {
               await disposeScenarios();
               restore();
            });
            for (const q of c.queries) {
               it(`${q.malloy}${q.givens ? ` ${JSON.stringify(q.givens)}` : ""}: equals live, every digit`, async () => {
                  const run = await fixture.run({
                     name: q.malloy,
                     malloy: q.malloy,
                     givens: q.givens as never,
                  });
                  expect(exact(run.served)).toBe(exact(run.live));
                  expect(rowDifference(run.live, run.served)).toBeUndefined();
               });
            }
         });
      }
   }
}
