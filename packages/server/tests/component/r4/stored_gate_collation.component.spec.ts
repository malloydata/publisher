// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` table compares a string column byte-exact; the source compares
 * it under its own collation. A gate or a re-applied `where:` over a column
 * that compares case-insensitively at the source (here DuckDB's `NOCASE`)
 * must not be read from the stored copy: live hides `East` from
 * `region != 'east'` and the copy showed it, and live admits `East` and
 * `EAST` to `region = 'east'` and the copy dropped them. Through the real
 * package path, on the served copy (`#(materialize)` and `#@ persist`,
 * gated and not) and the rollup leg, with the gated-sources switch off and
 * on. Every caller equals live; a byte-exact column is still served.
 *
 * A gate column stored with a zone (`TIMESTAMP WITH TIME ZONE`) is compared
 * in the destination session's zone, so it is not read from the copy either.
 *
 * A composite source's own row filter is refused live for every caller; a
 * rollup of it is not built, so its queries are refused as live refuses them.
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

const rowsOf = (collate: string) => `duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, CAST(amount AS DOUBLE) AS amount, category,
         region${collate} AS region
  FROM (VALUES (1, 10.0, 'A', 'east'), (2, 20.0, 'A', 'East'), (3, 30.0, 'B', 'EAST'),
               (4, 5.0, 'B', 'west'), (5, 1.0, 'C', 'West')) AS t(id, amount, category, region)
""")`;
const CI = rowsOf(" COLLATE NOCASE");
const BIN = rowsOf("");

const MODEL = `##! experimental { givens persistence access_modifiers composite_sources }
given: REGION :: string
given: EXCL :: string
given: CAT :: string
given: AT :: timestamp

#(access_filter) region = $REGION
#@ persist name="ci_p_eq" storage="${LAKE}"
source: ci_p_eq is ${CI} extend { primary_key: id }

#(access_filter) region = $REGION
#(materialize) name="ci_m_eq" storage="${LAKE}"
source: ci_m_eq is ${CI} extend { primary_key: id }

#(access_filter) category = $CAT
#(materialize) name="ci_m_ne" storage="${LAKE}"
source: ci_m_ne is ${CI} extend { primary_key: id; where: region != $EXCL }

#(access_filter) category = $CAT
#@ persist name="ci_p_ne" storage="${LAKE}"
source: ci_p_ne is ${CI} extend { primary_key: id; where: region != $EXCL }

#(materialize) name="ci_open" storage="${LAKE}"
source: ci_open is ${CI} extend { primary_key: id; where: region != $EXCL }

source: ci_roll is ${CI} extend {
  where: region != $EXCL
  #(rollup) grain=["category", "region"] storage="${LAKE}"
  measure: total is amount.sum()
}

// Controls: the same over a byte-exact column, still served.
#(access_filter) category = $CAT
#(materialize) name="bin_m_ne" storage="${LAKE}"
source: bin_m_ne is ${BIN} extend { primary_key: id; where: region != $EXCL }

#(access_filter) region = $REGION
#(materialize) name="bin_m_eq" storage="${LAKE}"
source: bin_m_eq is ${BIN} extend { primary_key: id }

source: bin_roll is ${BIN} extend {
  where: region != $EXCL
  #(rollup) grain=["category", "region"] storage="${LAKE}"
  measure: total is amount.sum()
}

// A gate column stored with a zone: compared in the destination's zone.
#(access_filter) tsz = $AT
#(materialize) name="tz_m_eq" storage="${LAKE}"
source: tz_m_eq is duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, CAST(ts AS TIMESTAMPTZ) AS tsz
  FROM (VALUES (1, '2026-01-04 04:00:00'), (2, '2026-01-05 06:00:00')) AS t(id, ts)
""") extend { primary_key: id }

// A composite with its own row filter: refused live for every caller.
source: plain is ${BIN} extend { primary_key: id }
#(access_filter) region = $REGION
source: comp_own is compose(plain, plain extend { dimension: x is 1 }) extend {
  #(rollup) grain=["category", "region"]
  measure: total is amount.sum()
}

// A row filter declared on a composite, reached through an extension or a
// query-shaped source over it: refused live for every caller too.
#(access_filter) region = $REGION
source: gated_comp is compose(plain, plain extend { dimension: x is 1 })
source: ext_of_gated_comp is gated_comp extend {
  #(rollup) grain=["category", "region"]
  measure: total is amount.sum()
}
source: q_of_gated_comp is gated_comp -> { select: id, amount, category, region } extend {
  #(rollup) grain=["category", "region"]
  measure: total is amount.sum()
}
`;

const byRegion = (s: string) =>
   `run: ${s} -> { group_by: region; aggregate: n is count(); order_by: region }`;

interface Case {
   name: string;
   malloy: string;
   callers: Record<string, string>[];
   /** Where the first caller is answered from: the stored copy, or live. */
   served?: "served_copy" | "rollup" | "live";
}

const CASES: Case[] = [
   {
      name: "a NOCASE gate, persist",
      malloy: byRegion("ci_p_eq"),
      callers: [{ REGION: "east" }, { REGION: "EAST" }, { REGION: "West" }],
      served: "live",
   },
   {
      name: "a NOCASE gate, materialize",
      malloy: byRegion("ci_m_eq"),
      callers: [{ REGION: "east" }, { REGION: "West" }],
      served: "live",
   },
   {
      name: "a NOCASE where: beside a gate, materialize",
      malloy: byRegion("ci_m_ne"),
      callers: [
         { CAT: "A", EXCL: "east" },
         { CAT: "B", EXCL: "east" },
      ],
      served: "live",
   },
   {
      name: "a NOCASE where: beside a gate, persist",
      malloy: byRegion("ci_p_ne"),
      callers: [
         { CAT: "A", EXCL: "east" },
         { CAT: "B", EXCL: "EAST" },
      ],
      served: "live",
   },
   {
      name: "a NOCASE where: on an ungated source",
      malloy: byRegion("ci_open"),
      callers: [{ EXCL: "east" }, { EXCL: "west" }],
      served: "live",
   },
   {
      name: "a NOCASE where: on an ungated source's storage= rollup",
      malloy:
         "run: ci_roll -> { group_by: region; aggregate: total; order_by: region }",
      callers: [{ EXCL: "east" }, { EXCL: "WEST" }],
      served: "live",
   },
   {
      name: "a byte-exact where: beside a gate (control)",
      malloy: byRegion("bin_m_ne"),
      callers: [
         { CAT: "A", EXCL: "east" },
         { CAT: "B", EXCL: "east" },
      ],
      served: "served_copy",
   },
   {
      name: "a byte-exact gate (control)",
      malloy: byRegion("bin_m_eq"),
      callers: [{ REGION: "east" }, { REGION: "West" }],
      served: "served_copy",
   },
   {
      name: "a byte-exact where: on an ungated source's storage= rollup (control)",
      malloy:
         "run: bin_roll -> { group_by: region; aggregate: total; order_by: region }",
      callers: [{ EXCL: "east" }, { EXCL: "West" }],
      served: "rollup",
   },
   {
      name: "a TIMESTAMPTZ gate column",
      malloy: "run: tz_m_eq -> { select: id; order_by: id }",
      callers: [{ AT: "2026-01-04 04:00:00" }, { AT: "2026-01-05 06:00:00" }],
      served: "live",
   },
   {
      name: "an extension of a composite with a row filter",
      malloy:
         "run: ext_of_gated_comp -> { group_by: category; aggregate: total; order_by: category }",
      callers: [{ REGION: "east" }, { REGION: "west" }],
   },
   {
      name: "a query-shaped source over a composite with a row filter",
      malloy:
         "run: q_of_gated_comp -> { group_by: category; aggregate: total; order_by: category }",
      callers: [{ REGION: "east" }, { REGION: "west" }],
   },
   {
      name: "a composite with its own row filter",
      malloy:
         "run: comp_own -> { group_by: category; aggregate: total; order_by: category }",
      callers: [{ REGION: "east" }, { REGION: "west" }],
   },
];

function show(o: Outcome): string {
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
   return rowDifference(live.answer.rows, served.answer.rows);
}

for (const gatedSources of [false, true]) {
   describe(`a stored copy compares a string column as its source does (gated sources ${gatedSources ? "on" : "off"})`, () => {
      let restore: () => void;
      let loader: Awaited<ReturnType<typeof loadFixture>>;
      let fixture: LoadedFixture;
      const live = new Map<string, Outcome>();
      const served = new Map<string, Outcome[]>();
      const key = (c: Case, i: number) => `${c.name}\u0000${i}`;

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_MATERIALIZE_GATED_SOURCES: String(gatedSources),
            },
         }).restore;
         await freshLoadPool();
         loader = await loadFixture({ "m.malloy": MODEL });
         for (const c of CASES) {
            for (const [i, givens] of c.callers.entries()) {
               live.set(
                  key(c, i),
                  await loader.run("m.malloy", c.malloy, givens),
               );
            }
         }
         fixture = await loader.bind({ includeRollups: true });
         for (const round of [0, 1]) {
            for (const c of CASES) {
               const order = [...c.callers.keys()];
               for (const i of round === 0 ? order : order.reverse()) {
                  const outcomes = served.get(key(c, i)) ?? [];
                  outcomes.push(
                     await fixture.run("m.malloy", c.malloy, c.callers[i]),
                  );
                  served.set(key(c, i), outcomes);
               }
            }
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
      });

      for (const c of CASES) {
         it(`${c.name}: every caller gets live's answer`, () => {
            for (const i of c.callers.keys()) {
               for (const got of served.get(key(c, i)) ?? []) {
                  const diff = difference(live.get(key(c, i))!, got);
                  expect(
                     diff,
                     `${JSON.stringify(c.callers[i])}: ${diff}`,
                  ).toBeUndefined();
               }
            }
         });
         if (c.served !== undefined) {
            it(`${c.name}: is answered from ${c.served}`, () => {
               const got = served.get(key(c, 0))![0];
               expect("answer" in got, show(got)).toBe(true);
               if (!("answer" in got)) return;
               if (c.served === "served_copy") {
                  expect(got.answer.via).toBe("served_copy");
               } else if (c.served === "rollup") {
                  expect(got.answer.servedBy).toBe("rollup");
               } else {
                  expect(got.answer.servedBy).toBe("live");
               }
            });
         }
      }
   });
}

/**
 * The stored side: a `storage=` table's gate column made case-insensitive
 * after the build (`ALTER … SET DATA TYPE VARCHAR COLLATE NOCASE` in the
 * destination), or a destination instance with a `default_collation`,
 * compares other rows than the byte-exact source. Read past its own gate
 * (G3), such a table answers live; untouched, it is served.
 */
const STORED_MODEL = `##! experimental { givens persistence access_modifiers }
given: REGION :: string

#(access_filter) region = $REGION
#(materialize) name="dest_altered" storage="${LAKE}"
source: dest_altered is ${BIN} extend { primary_key: id }

#(access_filter) region = $REGION
#(materialize) name="dest_kept" storage="${LAKE}"
source: dest_kept is ${BIN} extend { primary_key: id }
`;

for (const gatedSources of [false, true]) {
   describe(`a stored table whose gate column the destination compares case-insensitively answers live (gated sources ${gatedSources ? "on" : "off"})`, () => {
      let restore: () => void;
      let loader: Awaited<ReturnType<typeof loadFixture>>;
      let fixture: LoadedFixture;
      const query = (s: string) =>
         `run: ${s} -> { group_by: region; aggregate: n is count(); order_by: region }`;
      const callers = [
         { REGION: "east" },
         { REGION: "EAST" },
         { REGION: "West" },
      ];
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
         loader = await loadFixture({ "m.malloy": STORED_MODEL });
         for (const s of ["dest_altered", "dest_kept"]) {
            for (const [i, g] of callers.entries()) {
               live.set(`${s}${i}`, await loader.run("m.malloy", query(s), g));
            }
         }
         fixture = await loader.bind();
         for (const entry of Object.values(fixture.stored)) {
            if (entry.sourceName !== "dest_altered") continue;
            const table = `"${String(entry.physicalTableName).replace(/"/g, '""')}"`;
            await loader.lake.runSQL(
               `ALTER TABLE ${table} ALTER COLUMN region SET DATA TYPE VARCHAR COLLATE NOCASE`,
            );
         }
         await fixture.rebind();
      }, 900_000);

      afterAll(async () => {
         await fixture?.dispose();
         await loader?.dispose().catch(() => {});
         restore?.();
         await freshLoadPool();
      });

      it("answers every caller of the altered table as live does, from live", async () => {
         for (const [i, g] of callers.entries()) {
            const got = await fixture.run("m.malloy", query("dest_altered"), g);
            expect(
               difference(live.get(`dest_altered${i}`)!, got),
            ).toBeUndefined();
            expect("answer" in got && got.answer.servedBy).toBe("live");
         }
      });

      it("still serves the untouched table from the copy", async () => {
         for (const [i, g] of callers.entries()) {
            const got = await fixture.run("m.malloy", query("dest_kept"), g);
            expect(difference(live.get(`dest_kept${i}`)!, got)).toBeUndefined();
            expect("answer" in got && got.answer.via).toBe("served_copy");
         }
      });

      it("answers live under a destination default_collation", async () => {
         await loader.lake.runSQL("SET default_collation = 'nocase'");
         try {
            for (const [i, g] of callers.entries()) {
               const got = await fixture.run("m.malloy", query("dest_kept"), g);
               expect(
                  difference(live.get(`dest_kept${i}`)!, got),
               ).toBeUndefined();
               expect("answer" in got && got.answer.servedBy).toBe("live");
            }
         } finally {
            await loader.lake.runSQL("SET default_collation = ''");
         }
      });
   });
}
