// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The drill expression a nested field
// carries must name the author's field, not the served copy's internal column.

import { afterAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { buildScenario, disposeScenarios } from "./harness";

const SEED = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 10.0, TIMESTAMP '2026-01-15 00:00:00', 1),
      (2, 20.0, TIMESTAMP '2026-02-01 00:00:00', 0)
   ) AS t(id, amt, ts, flag_int)`,
];
const MODEL = `##! experimental { persistence sql_functions }
source: ev is duckdb.table('ev')

#@ persist name="t" storage="lake"
source: t is ev -> {
  select: id, amt, d is sql_date("\${ts}"), flag is sql_boolean("\${flag_int}"), m is ts.month
}
`;
const drills = (fields: { name: string; type: unknown }[]) =>
   JSON.stringify(fields).match(/drill_expression \{[^}]*\}/g) ?? [];

for (const [mode, env, uncast] of [
   ["serve_shape uncast", {}, true],
   ["served_copy (materialize) cast", { PUBLISHER_MATERIALIZE: "true" }, false],
   ["cutover uncast", { PUBLISHER_MATERIALIZE: "true" }, true],
   ["cutover cast", { PUBLISHER_MATERIALIZE: "true" }, false],
] as const) {
   describe(mode, () => {
      afterAll(disposeScenarios);
      for (const q of [
         "run: t -> { group_by: flag; nest: x is { group_by: d, m; aggregate: s is amt.sum() } }",
      ]) {
         it(q, async () => {
            const restore = installSettings({
               env: { PERSIST_STORAGE_MODE: "on", ...env },
            }).restore;
            try {
               const f = await buildScenario({
                  files: { "m.malloy": MODEL },
                  seedSql: SEED,
                  uncast,
               });
               const run = await f.run({ name: q, malloy: q });
               // A drill code must name the author's field, never the internal column.
               expect(
                  drills(run.servedFields).filter((d) =>
                     d.includes("__stored"),
                  ),
               ).toEqual([]);
            } finally {
               restore();
            }
         });
      }
   });
}
