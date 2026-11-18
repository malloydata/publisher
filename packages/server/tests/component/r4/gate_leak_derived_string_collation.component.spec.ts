// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A gate or a `where:` over a dimension computed from a string compares that
 * string, though the dimension itself is a boolean or a number: `late = $FLAG`
 * over `dimension: late is region > 'M'` orders `region`, and `'a' > 'M'`
 * holds byte-exact but not case-insensitively. Declared on a source that
 * extends a stored one, the dimension is computed from the stored string when
 * the table is read, so a case-insensitive column on either side (the
 * source's `COLLATE NOCASE`, or the stored table's, altered after the build)
 * admits other rows than live does. Every caller of such a gate or scope gets
 * live's answer; a numeric dimension no collation changes (`length(region)`)
 * and a gate over the string itself are the controls.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   LAKE,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import { rowDifference } from "./harness";

const DIMENSIONS = `dimension:
    late is region > 'M'
    is_n is region = 'n'
    late_n is pick 1 when region > 'M' else 0
    late_p is pick true when region > 'M' else false
    len is length(region)`;

const model = (
   collate: string,
) => `##! experimental { givens access_modifiers persistence }
given: FLAG :: boolean
given: N :: number
given: R :: string
source: rows is duckdb.sql("""SELECT CAST(id AS BIGINT) AS id, CAST(amount AS DOUBLE) AS amount, region${collate} AS region
  FROM (VALUES (1, 1.0, 'a'), (2, 2.0, 'B'), (3, 4.0, 'm'), (4, 8.0, 'N'), (5, 16.0, 'z')) AS t(id, amount, region)""") extend {
  primary_key: id
}
#@ persist name="p_base" storage="${LAKE}"
source: p_base is rows extend {}
#(materialize) name="m_base" storage="${LAKE}"
source: m_base is rows extend {}
#(access_filter) late = $FLAG
source: p_late is p_base extend { ${DIMENSIONS} }
#(access_filter) is_n = $FLAG
source: p_is_n is p_base extend { ${DIMENSIONS} }
source: p_where_is_n is p_base extend { ${DIMENSIONS}
  where: is_n }
#(access_filter) late_n = $N
source: p_late_n is p_base extend { ${DIMENSIONS} }
#(access_filter) late_p = $FLAG
source: p_late_p is p_base extend { ${DIMENSIONS} }
#(access_filter) len = $N
source: p_len is p_base extend { ${DIMENSIONS} }
#(access_filter) late = $FLAG
source: m_late is m_base extend { ${DIMENSIONS} }
// The control: a gate over the string itself.
#(access_filter) region = $R
source: p_region is p_base extend {}
`;

const sel = (s: string) =>
   `run: ${s} -> { group_by: region; order_by: region }`;
const CASES: [string, string][] = [
   "p_late",
   "p_is_n",
   "p_where_is_n",
   "p_late_n",
   "p_late_p",
   "p_len",
   "m_late",
   "p_region",
].map((s) => [s, sel(s)]);
const CALLERS: Record<string, boolean | number | string>[] = [
   { FLAG: true, N: 1, R: "n" },
   { FLAG: false, N: 0, R: "A" },
];

function show(o: unknown): string {
   return JSON.stringify(o, (_k, v) =>
      typeof v === "bigint" ? `${v}n` : (v as unknown),
   );
}

function difference(live: Outcome, served: Outcome): string | undefined {
   if ("error" in live || "error" in served) {
      return "error" in live && "error" in served && live.error === served.error
         ? undefined
         : `live ${show(live)} against served ${show(served)}`;
   }
   const diff = rowDifference(live.answer.rows, served.answer.rows);
   return diff === undefined
      ? undefined
      : `${diff}; live ${show(live.answer.rows)}, served (${served.answer.servedBy}) ${show(served.answer.rows)}`;
}

for (const side of ["source", "stored"] as const) {
   for (const gatedSources of [false, true]) {
      describe(`a gate over a dimension computed from a string compares as live does, its ${side} column NOCASE (gated sources ${gatedSources ? "on" : "off"})`, () => {
         let restore: () => void;
         let loader: Awaited<ReturnType<typeof loadFixture>>;
         let fixture: LoadedFixture;
         const live = new Map<string, Outcome>();

         beforeAll(async () => {
            restore = installSettings({
               env: {
                  PERSIST_STORAGE_MODE: "on",
                  PUBLISHER_MATERIALIZE: "true",
                  PUBLISHER_MATERIALIZE_GATED_SOURCES: String(gatedSources),
               },
            }).restore;
            await freshLoadPool();
            loader = await loadFixture({
               "m.malloy": model(side === "source" ? " COLLATE NOCASE" : ""),
            });
            for (const [name, malloy] of CASES) {
               for (const [i, g] of CALLERS.entries()) {
                  live.set(
                     `${name}${i}`,
                     await loader.run("m.malloy", malloy, g),
                  );
               }
            }
            fixture = await loader.bind({ includeRollups: true });
            if (side === "stored") {
               for (const entry of Object.values(fixture.stored)) {
                  const table = `"${String(entry.physicalTableName).replace(/"/g, '""')}"`;
                  await loader.lake.runSQL(
                     `ALTER TABLE ${table} ALTER COLUMN region SET DATA TYPE VARCHAR COLLATE NOCASE`,
                  );
               }
               await fixture.rebind();
            }
         }, 900_000);

         afterAll(async () => {
            await fixture?.dispose();
            await loader?.dispose().catch(() => {});
            restore?.();
            await freshLoadPool();
         });

         it("builds every stored table", () => {
            expect(fixture.buildErrors).toEqual({});
            expect(Object.keys(fixture.stored).length).toBe(2);
         });

         for (const [name, malloy] of CASES) {
            it(`${name}: every caller gets live's answer`, async () => {
               for (const [i, g] of CALLERS.entries()) {
                  const got = await fixture.run("m.malloy", malloy, g);
                  const diff = difference(live.get(`${name}${i}`)!, got);
                  expect(diff, `${JSON.stringify(g)}: ${diff}`).toBeUndefined();
               }
            });
         }
      });
   }
}
