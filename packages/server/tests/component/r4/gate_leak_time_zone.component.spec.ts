// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * L1: a row-level gate over a stored time truncation, read under a caller's
 * own `timezone:`, through the real package path (plan, bind, router,
 * `govern()`), on every tier that keeps a gate under G3: v1 `#@ persist` and
 * `#(materialize)`, in the database and in a `storage=` destination, and a
 * database rollup whose grain holds the gated day.
 *
 * A day computed in a query stage (`-> { select: dday is ts.day }`) is stored
 * as a column. Live reads that column as it is; the served copy restates it
 * (`dday is dday__stored.day`), which truncates it again in the reading
 * query's zone. The gate is grafted after the leg answers, so a zone check
 * that read only the caller's fields let a zoned query through and the gate
 * compared the re-truncated value: a caller west of UTC was admitted to the
 * next day's rows. The rollup leg had the same blind spot and answered no
 * rows where live had some.
 *
 * Every caller, interleaved, gets exactly live's outcome, for each zone. A
 * query with no zone of its own is still served from the table, so the
 * comparison exercises the stored path.
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

const EVENTS = `
source: ev is duckdb.sql("""
  SELECT CAST(id AS BIGINT) AS id, tenant, CAST(ts AS TIMESTAMP) AS ts,
         CAST(amount AS DOUBLE) AS amount
  FROM (VALUES
    (1, 'a', '2026-01-04 04:00:00', 1),
    (2, 'a', '2026-01-04 20:00:00', 2),
    (3, 'b', '2026-01-05 06:00:00', 1000),
    (4, 'b', '2026-01-05 07:00:00', 2000),
    (5, 'c', '2026-01-06 12:00:00', 50)
  ) AS t(id, tenant, ts, amount)
""") extend { primary_key: id }
`;

const MODEL = `##! experimental { givens persistence access_modifiers }
given: ON_DAY :: timestamp
${EVENTS}

#(access_filter) dday = $ON_DAY
#@ persist name="tz_v1_sto" storage="${LAKE}"
source: v1_sto is ev -> { where: id > -1; select: id, tenant, amount, ts, dday is ts.day }

// An ungated stored day, scoped per caller by a where: that reads it through
// a dimension, and inside a query-shaped source over it.
#@ persist name="tz_open_sto" storage="${LAKE}"
source: open_sto is ev -> { where: id > -9; select: id, tenant, amount, ts, dday is ts.day }
source: scope_dim is open_sto extend { dimension: mine is dday = $ON_DAY; where: mine }
source: scope_qs is open_sto extend { where: dday = $ON_DAY } -> { select: id, tenant, amount }

#(access_filter) dday = $ON_DAY
#@ persist name="tz_v1_db"
source: v1_db is ev -> { where: id > -2; select: id, tenant, amount, ts, dday is ts.day }

#(access_filter) dday = $ON_DAY
#(materialize) name="tz_mz_sto" storage="${LAKE}"
source: mz_sto is ev -> { where: id > -3; select: id, tenant, amount, ts, dday is ts.day }

#(access_filter) dday = $ON_DAY
#(materialize) name="tz_mz_db"
source: mz_db is ev -> { where: id > -4; select: id, tenant, amount, ts, dday is ts.day }

// The gate reads a dimension computed from the stored day, not the day.
#(access_filter) dday_alias = $ON_DAY
#@ persist name="tz_derived_sto" storage="${LAKE}"
source: derived_sto is ev -> { where: id > -5; select: id, tenant, amount, ts, dday is ts.day } extend {
  dimension: dday_alias is dday
}

// The query's stage computes the day in its base's own zone, which the
// stored column keeps; read in the default zone, the copy cannot restate it,
// so a query its gate reads it for answers live.
#(access_filter) dday = $ON_DAY
#@ persist name="tz_srctz_sto" storage="${LAKE}"
source: srctz_sto is ev extend { timezone: 'America/Los_Angeles' } -> { where: id > -7; select: id, tenant, amount, ts, dday is ts.day }

#(access_filter) dday = $ON_DAY
#(materialize) name="tz_srctz_mz"
source: srctz_mz is ev extend { timezone: 'America/Los_Angeles' } -> { where: id > -8; select: id, tenant, amount, ts, dday is ts.day }

// A database rollup whose grain holds the gated day.
#(access_filter) dday = $ON_DAY
source: rolled is ev extend {
  dimension: dday is ts.day
  #(preaggregate) grain=["tenant", "dday"]
  measure: total is amount.sum()
}
`;

const ZONES: [string, string][] = [
   ["no zone", ""],
   ["America/Los_Angeles", "timezone: 'America/Los_Angeles'; "],
   ["Etc/GMT+12", "timezone: 'Etc/GMT+12'; "],
   ["Asia/Tokyo", "timezone: 'Asia/Tokyo'; "],
   ["Pacific/Kiritimati", "timezone: 'Pacific/Kiritimati'; "],
];

/** Each stored source, and whether a query with no zone is served from it. */
const STORED: [string, boolean][] = [
   ["v1_sto", true],
   ["v1_db", true],
   ["mz_sto", true],
   ["mz_db", true],
   ["derived_sto", true],
   ["srctz_sto", false],
   ["srctz_mz", false],
   ["scope_dim", true],
   ["scope_qs", true],
];

interface Case {
   source: string;
   zone: string;
   malloy: string;
   /** Where the query with no zone of its own is answered from. */
   served?: "served_copy" | "rollup" | "live";
}

const CASES: Case[] = [
   ...STORED.flatMap(([source, servedWithNoZone]) =>
      ZONES.map(([zone, tz]) => ({
         source,
         zone,
         malloy: `run: ${source} -> { ${tz}select: id, tenant, amount; order_by: id }`,
         ...(tz === ""
            ? {
                 served: servedWithNoZone
                    ? ("served_copy" as const)
                    : ("live" as const),
              }
            : {}),
      })),
   ),
   ...ZONES.map(([zone, tz]) => ({
      source: "rolled",
      zone,
      malloy: `run: rolled -> { ${tz}group_by: tenant; aggregate: total; order_by: tenant }`,
      ...(tz === "" ? { served: "rollup" as const } : {}),
   })),
];

const CALLERS: Record<string, Record<string, string>> = {
   "the 2026-01-04 caller": { ON_DAY: "2026-01-04 00:00:00" },
   "the 2026-01-05 caller": { ON_DAY: "2026-01-05 00:00:00" },
   "the 2026-01-06 caller": { ON_DAY: "2026-01-06 00:00:00" },
   "a caller with no day's rows": { ON_DAY: "2025-06-01 00:00:00" },
};
// Interleaved and repeated, so state shared across callers would show.
const ORDER = [...Object.keys(CALLERS), ...Object.keys(CALLERS).reverse()];

/** An outcome as text, its big integers included. */
function show(o: Outcome): string {
   return JSON.stringify(o, (_k, v) =>
      typeof v === "bigint" ? `${v}n` : (v as unknown),
   );
}

function difference(live: Outcome, served: Outcome): string | undefined {
   if ("error" in live || "error" in served) {
      return "error" in live && "error" in served
         ? undefined
         : `live ${show(live)} against served ${show(served)}`;
   }
   return rowDifference(live.answer.rows, served.answer.rows);
}

describe("L1: a caller's time zone never widens a gate over a stored truncation", () => {
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: LoadedFixture;
   const live = new Map<string, Outcome>();
   const served = new Map<string, Outcome[]>();
   const key = (c: Case, caller: string) => `${c.malloy}\u0000${caller}`;

   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
            PUBLISHER_MATERIALIZE_GATED_SOURCES: "false",
         },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({ "m.malloy": MODEL });
      for (const c of CASES) {
         for (const caller of Object.keys(CALLERS)) {
            live.set(
               key(c, caller),
               await loader.run("m.malloy", c.malloy, CALLERS[caller]),
            );
         }
      }
      fixture = await loader.bind({ includeRollups: true });
      for (const c of CASES) {
         for (const caller of ORDER) {
            const outcomes = served.get(key(c, caller)) ?? [];
            outcomes.push(
               await fixture.run("m.malloy", c.malloy, CALLERS[caller]),
            );
            served.set(key(c, caller), outcomes);
         }
      }
   }, 600_000);

   afterAll(async () => {
      await fixture?.dispose();
      await loader?.dispose().catch(() => {});
      restore?.();
      await freshLoadPool();
   });

   it("builds every stored table and the rollup", () => {
      expect(fixture.buildErrors).toEqual({});
      const built = fixture.built.map((b) => `${b.name}:${b.tier}`);
      // The scoped sources are entry points over `open_sto`'s table.
      for (const name of [
         ...STORED.map(([n]) => n).filter((n) => !n.startsWith("scope_")),
         "open_sto",
      ]) {
         expect(built.some((b) => b.startsWith(`${name}:`))).toBe(true);
      }
   });

   for (const c of CASES) {
      it(`${c.source}, ${c.zone}: every caller gets live's answer`, () => {
         for (const caller of Object.keys(CALLERS)) {
            const want = live.get(key(c, caller))!;
            for (const got of served.get(key(c, caller)) ?? []) {
               const diff = difference(want, got);
               expect(
                  diff,
                  `${caller}: ${diff ?? ""}\n${show(got)}`,
               ).toBeUndefined();
            }
         }
      });
      if (c.served !== undefined) {
         it(`${c.source}, ${c.zone}: is answered from ${c.served === "live" ? "live" : "the stored table"}`, () => {
            const got = served.get(key(c, "the 2026-01-04 caller"))![0];
            expect("answer" in got).toBe(true);
            if (!("answer" in got)) return;
            if (c.served === "rollup") {
               expect(got.answer.servedBy).toBe("rollup");
            } else if (c.served === "live") {
               expect(got.answer.servedBy).toBe("live");
            } else {
               expect(got.answer.via).toBe("served_copy");
            }
         });
      }
   }
});
