// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A persisted view's stored rows read back with live's values, not values
 * near them, and with live's result metadata (only `drill_expression` set
 * aside), through the real package path:
 *
 * - a timestamp truncation under a time zone (the view's own `timezone:`, or
 *   its source's) keeps the zone's midnight: a `storage=` read truncated the
 *   stored value again in UTC, so New York's 04:00Z came back as 00:00Z;
 * - a BIGINT above 2^53 keeps every digit, as a dimension and as a measure:
 *   a `storage=` read declared it a `number`, which rounds;
 * - a TIMESTAMP_NS, which live answers as a warehouse-native value with every
 *   digit, is answered live: no stored read keeps both its digits and its type;
 * - a date `.week` key is a date;
 * - an `index:` view, whose result live writes with no `#(malloy)` annotation,
 *   is answered live: a stored read annotates every column.
 *
 * Each runs against a `storage=` table built as a build casts it, one stored
 * uncast (as a build stores it when it declines or fails a cast), and a
 * database table as the control.
 *
 * Query-shaped `#@ persist` sources answered by the served copy are compared
 * alongside. Its big integers were already exact, and its TIMESTAMP_NS already
 * answered live. Its timestamp truncations were not, in either tier: the served
 * copy re-truncates a stored truncation in the reading query's zone, which
 * differs from the build's when the source sets a zone in its query
 * (`zoned_src`), on itself (`zoned_own_src`), when the caller's query sets
 * one (`utc_src`), or when an extension does (`utc_ny`). Each read of such a
 * truncation is now answered live, with the `TIME_ZONE` reason; a query that
 * reads no truncation, or reads one in UTC, stays served. A second zone
 * (Kolkata, Sydney) and instants either side of each daylight-saving change
 * (`dst`) are compared alongside New York.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { DATA_FILES } from "./conformance/data";
import {
   typeWithoutDrillExpression,
   withoutDrillExpression,
} from "./conformance/drill_expression";
import {
   type Answer,
   freshLoadPool,
   type LoadedFixture,
   loadFixture,
   type Outcome,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";
import { renderFiles, type Shape } from "./conformance/shapes";
import { rowDifference } from "./harness";

const MODEL = `##! experimental { persistence }
import "raw.malloy"

source: e is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, CAST('2025-01-01 00:00:00.123456001' AS TIMESTAMP_NS), CAST(9007199254740993 AS BIGINT), 10.0),
    (2, CAST('2025-01-01 00:00:00.123456002' AS TIMESTAMP_NS), CAST(9007199254740992 AS BIGINT), 20.0),
    (3, CAST('2025-01-01 00:00:00.123456002' AS TIMESTAMP_NS), CAST(9007199254740993 AS BIGINT), 30.0)
  ) t(id, tsn, big, amt)
""") extend {
  primary_key: id
  measure: total is amt.sum()
  measure: n is count()

  @PVIEW(e_by_big)
  view: by_big is { group_by: big; aggregate: total, n; order_by: big }
  @PVIEW(e_by_big_only)
  view: by_big_only is { group_by: big; order_by: big }
  @PVIEW(e_big_measure)
  view: big_measure is { group_by: id; aggregate: big_max is big.max(); order_by: id }
  @PVIEW(e_by_tsn)
  view: by_tsn is { group_by: tsn; aggregate: total, n }
}

source: ov is orders_raw extend {
  measure: total is amount.sum()
  measure: order_count is count()

  @PVIEW(ov_zoned_by_day)
  view: zoned_by_day is {
    timezone: 'America/New_York'
    group_by: created_day is created_at.day
    aggregate: total
    order_by: created_day
  }

  @PVIEW(ov_zoned_grains)
  view: zoned_grains is {
    timezone: 'America/New_York'
    group_by:
      t_minute is created_at.minute
      t_hour is created_at.hour
      t_day is created_at.day
      t_week is created_at.week
      t_month is created_at.month
      t_quarter is created_at.quarter
      t_year is created_at.year
      d_week is order_date.week
    aggregate: order_count
    order_by: t_minute
  }

  @PVIEW(ov_wk)
  view: wk is { group_by: d is order_date.week; aggregate: total; order_by: d }
  @PVIEW(ov_wk_only)
  view: wk_only is { group_by: d is order_date.week; order_by: d }

  @PVIEW(ov_idx)
  view: idx is { index: region, status }
}

source: ovz is orders_raw extend {
  timezone: 'America/New_York'
  measure: total is amount.sum()

  @PVIEW(ovz_by_day)
  view: by_day is {
    group_by: created_day is created_at.day
    aggregate: total
    order_by: created_day
  }
}

@PERSIST(big_src)
source: big_src is e -> { group_by: id, big; aggregate: big_max is big.max() }

@PERSIST(tsn_src)
source: tsn_src is e -> { group_by: id, tsn }

@PERSIST(zoned_src)
source: zoned_src is orders_raw -> {
  timezone: 'America/New_York'
  group_by: d is created_at.day, h is created_at.hour
  aggregate: c is count()
}

@PERSIST(utc_src)
source: utc_src is orders_raw -> {
  group_by: d is created_at.day, h is created_at.hour
  aggregate: c is count()
}

@PERSIST(zoned_own_src)
source: zoned_own_src is orders_raw -> {
  group_by: d is created_at.day, h is created_at.hour
  aggregate: c is count()
} extend {
  timezone: 'America/New_York'
}

source: utc_ny is utc_src extend { timezone: 'America/New_York' }
source: utc_utc is utc_src extend { timezone: 'UTC' }

// Instants either side of each daylight-saving change: New York's (2024-03-10
// 07:00Z, 2024-11-03 06:00Z) and Sydney's (2024-04-06 16:00Z, 2024-10-05
// 16:00Z); Kolkata's half-hour offset has none.
source: dst is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, TIMESTAMP '2024-03-10 06:30:00'), (2, TIMESTAMP '2024-03-10 07:30:00'),
    (3, TIMESTAMP '2024-11-03 04:59:59'), (4, TIMESTAMP '2024-11-03 05:30:00'),
    (5, TIMESTAMP '2024-11-03 06:30:00'), (6, TIMESTAMP '2024-04-06 15:30:00'),
    (7, TIMESTAMP '2024-04-06 16:30:00'), (8, TIMESTAMP '2024-10-05 15:30:00'),
    (9, TIMESTAMP '2024-10-05 16:30:00'), (10, TIMESTAMP '2024-12-31 18:45:00')
  ) t(id, ts)
""") extend {
  primary_key: id
  measure: n is count()

  @PVIEW(dst_ny)
  view: ny is {
    timezone: 'America/New_York'
    group_by: d is ts.day, h is ts.hour
    aggregate: n
    order_by: h
  }

  @PVIEW(dst_syd)
  view: syd is {
    timezone: 'Australia/Sydney'
    group_by: d is ts.day, w is ts.week, m is ts.month, h is ts.hour
    aggregate: n
    order_by: h
  }
}

@PERSIST(dst_kol)
source: dst_kol is dst -> {
  timezone: 'Asia/Kolkata'
  group_by: d is ts.day, h is ts.hour, y is ts.year
  aggregate: c is count()
}

@PERSIST(dst_utc)
source: dst_utc is dst -> {
  group_by: d is ts.day, h is ts.hour
  aggregate: c is count()
}
`;

const SHAPE: Shape = {
   name: "persisted view storage fidelity",
   about: "a persisted view's stored rows read back with live's values",
   files: { "m.malloy": MODEL },
   queries: [],
};

type Tier = "storage" | "database";

interface Case {
   name: string;
   malloy: string;
   /** Tiers whose answer is live by design. */
   liveIn?: Tier[];
   /** What answers it when not live (default the view leg). */
   servedBy?: string;
   /** Text the view leg's decline detail must carry, by tier. */
   declinedBecause?: Partial<Record<Tier, string>>;
   /**
    * The served copy's skip reason, when it declines a `storage=` table (a
    * database one's path report does not list the served copy's skip).
    */
   servedSkipReason?: string;
}

const BOTH: Tier[] = ["storage", "database"];

const CASES: Case[] = [
   // Bug A: a timestamp truncation under a time zone.
   {
      name: "every truncation under a view's time zone",
      malloy: "run: ov -> zoned_grains",
   },
   {
      name: "a later stage over a zoned truncation",
      malloy:
         "run: ov -> zoned_by_day -> { group_by: m is created_day.month; aggregate: t is total.sum(); order_by: m }",
   },
   {
      name: "a timestamp day under its source's time zone",
      malloy: "run: ovz -> by_day",
   },
   {
      name: "a later stage under its source's time zone",
      malloy:
         "run: ovz -> by_day -> { group_by: m is created_day.month; aggregate: t is total.sum(); order_by: m }",
   },
   // Bug B: values and types a stored read loses.
   { name: "a BIGINT key above 2^53", malloy: "run: e -> by_big" },
   { name: "a BIGINT key alone", malloy: "run: e -> by_big_only" },
   { name: "a BIGINT measure above 2^53", malloy: "run: e -> big_measure" },
   {
      name: "a later stage over a BIGINT key",
      malloy: "run: e -> by_big -> { select: big, n; order_by: big }",
   },
   {
      name: "a TIMESTAMP_NS key",
      malloy: "run: e -> by_tsn",
      liveIn: ["storage"],
      declinedBecause: { storage: "as a warehouse-native value" },
   },
   { name: "a date week key", malloy: "run: ov -> wk" },
   { name: "a date week key alone", malloy: "run: ov -> wk_only" },
   {
      name: "an index: view",
      malloy: "run: ov -> idx",
      liveIn: ["storage", "database"],
      declinedBecause: {
         storage: "it is an `index:`",
         database: "it is an `index:`",
      },
   },
   // The served copy, for comparison.
   {
      name: "a query-shaped source's BIGINT columns, through the served copy",
      malloy: "run: big_src -> { select: *; order_by: id }",
      servedBy: "materialized",
   },
   {
      name: "a query-shaped source's TIMESTAMP_NS column, through the served copy",
      malloy: "run: tsn_src -> { select: *; order_by: id }",
      liveIn: ["storage", "database"],
   },
   {
      name: "a query-shaped source's zoned truncations, through the served copy",
      malloy: "run: zoned_src -> { select: *; order_by: h }",
      liveIn: ["storage", "database"],
      servedSkipReason: "TIME_ZONE",
   },
   {
      name: "a query that sets a time zone over a query-shaped source's truncations, through the served copy",
      malloy:
         "run: utc_src -> { timezone: 'America/New_York'; select: *; order_by: h }",
      liveIn: ["storage", "database"],
      servedSkipReason: "TIME_ZONE",
   },
   {
      name: "a query-shaped source with its own time zone, through the served copy",
      malloy: "run: zoned_own_src -> { select: *; order_by: h }",
      liveIn: ["storage", "database"],
      servedSkipReason: "TIME_ZONE",
   },
   {
      name: "a later stage over a query-shaped source with its own time zone, through the served copy",
      malloy:
         "run: zoned_own_src -> { group_by: m is d.month; aggregate: n is c.sum(); order_by: m }",
      liveIn: ["storage", "database"],
      servedSkipReason: "TIME_ZONE",
   },
   {
      name: "a later stage over a query-shaped source's zoned truncation, through the served copy",
      malloy:
         "run: zoned_src -> { group_by: m is d.month; aggregate: n is c.sum(); order_by: m }",
      liveIn: ["storage", "database"],
      servedSkipReason: "TIME_ZONE",
   },
   // A zone the read is not built in declines only the queries that read a
   // truncation; every other query stays on the table.
   {
      name: "S1: a zoned query-shaped source serves a query that reads no truncation",
      malloy: "run: zoned_src -> { aggregate: n is c.sum() }",
      servedBy: "materialized",
   },
   {
      name: "S4: a caller's zone over a table, reading no truncation, stays served",
      malloy:
         "run: utc_src -> { timezone: 'America/New_York'; aggregate: n is c.sum() }",
      servedBy: "materialized",
   },
   {
      name: "a caller's explicit UTC over a table's truncations stays served",
      malloy: "run: utc_src -> { timezone: 'UTC'; select: *; order_by: h }",
      servedBy: "materialized",
   },
   {
      name: "S5: an extension that sets a zone over a table's truncations answers live",
      malloy:
         "run: utc_ny -> { group_by: d; aggregate: n is c.sum(); order_by: d }",
      liveIn: BOTH,
      servedSkipReason: "TIME_ZONE",
   },
   {
      name: "S5: an extension that sets a zone, reading no truncation, stays served",
      malloy: "run: utc_ny -> { aggregate: n is c.sum() }",
      servedBy: "materialized",
   },
   {
      name: "an extension that sets UTC over a table's truncations stays served",
      malloy:
         "run: utc_utc -> { group_by: d; aggregate: n is c.sum(); order_by: d }",
      servedBy: "materialized",
   },
   // Across daylight-saving changes, and in zones other than New York.
   {
      name: "a view's New York day and hour across DST",
      malloy: "run: dst -> ny",
   },
   {
      name: "a view's Sydney truncations across DST",
      malloy: "run: dst -> syd",
   },
   {
      name: "a later stage over a view's Sydney truncations",
      malloy:
         "run: dst -> syd -> { group_by: m; aggregate: t is n.sum(); order_by: m }",
   },
   {
      name: "a Kolkata query-shaped source's truncations answer live",
      malloy: "run: dst_kol -> { select: *; order_by: h }",
      liveIn: BOTH,
      servedSkipReason: "TIME_ZONE",
   },
   {
      name: "a Kolkata query-shaped source serves a query that reads no truncation",
      malloy: "run: dst_kol -> { aggregate: t is c.sum() }",
      servedBy: "materialized",
   },
   {
      name: "a caller's Kolkata zone over a table's truncations across DST answers live",
      malloy:
         "run: dst_utc -> { timezone: 'Asia/Kolkata'; select: *; order_by: h }",
      liveIn: BOTH,
      servedSkipReason: "TIME_ZONE",
   },
   {
      name: "a caller's New York filter on a truncation across DST answers live",
      malloy:
         "run: dst_utc -> { timezone: 'America/New_York'; where: d = @2024-11-03; aggregate: t is c.sum() }",
      liveIn: BOTH,
      servedSkipReason: "TIME_ZONE",
   },
   {
      name: "a table's truncations across DST, read in UTC, stay served",
      malloy: "run: dst_utc -> { select: *; order_by: h }",
      servedBy: "materialized",
   },
];

/** Every field's kind, type and annotations, with only drill expressions aside. */
function metadata(answer: Answer): unknown {
   return answer.fields.map((f) => ({
      name: f.name,
      kind: f.kind,
      type: typeWithoutDrillExpression(stripSubtype(f.type)),
      annotations: f.annotations.map(withoutDrillExpression),
   }));
}

/** A number's integer/decimal subtype, which a `storage=` read loses (a separate tolerance). */
function stripSubtype(t: unknown): unknown {
   if (Array.isArray(t)) return t.map(stripSubtype);
   if (t && typeof t === "object") {
      return Object.fromEntries(
         Object.entries(t as Record<string, unknown>)
            .filter(([k]) => k !== "subtype")
            .map(([k, v]) => [k, stripSubtype(v)]),
      );
   }
   return t;
}

beforeAll(freshLoadPool);
afterAll(freshLoadPool);

const VARIANTS: { name: string; tier: Tier; storeUncast?: true }[] = [
   { name: "storage", tier: "storage" },
   { name: "storage, stored uncast", tier: "storage", storeUncast: true },
   { name: "database", tier: "database" },
];

for (const { name: variantName, tier, storeUncast } of VARIANTS) {
   describe(`a persisted view's stored values [${variantName}]`, () => {
      let restore: () => void = () => {};
      let fixture: LoadedFixture | undefined;
      const live = new Map<string, Outcome>();
      const served = new Map<string, Outcome>();

      beforeAll(async () => {
         restore = withSwitch("cutover");
         await freshLoadPool();
         const loader = await loadFixture({
            ...DATA_FILES,
            ...renderFiles(SHAPE, tier),
         });
         for (const c of CASES) {
            live.set(c.name, await loader.run("m.malloy", c.malloy));
         }
         fixture = await loader.bind({
            includeRollups: true,
            ...(storeUncast ? { storeUncast } : {}),
         });
         for (const c of CASES) {
            served.set(c.name, await fixture.run("m.malloy", c.malloy));
         }
      });

      afterAll(async () => {
         restore();
         await fixture?.dispose();
      });

      for (const c of CASES) {
         const want = c.liveIn?.includes(tier)
            ? "live"
            : (c.servedBy ?? "view");
         it(`${c.name}: answered by ${want}, rows and metadata equal to live`, () => {
            const l = live.get(c.name)!;
            const s = served.get(c.name)!;
            if ("error" in l) throw new Error(`live failed: ${l.error}`);
            if ("error" in s) throw new Error(`served failed: ${s.error}`);
            expect(s.answer.servedBy).toBe(want);
            expect(rowDifference(l.answer.rows, s.answer.rows)).toBeUndefined();
            expect(metadata(s.answer)).toEqual(metadata(l.answer));
            if (c.servedSkipReason && tier === "storage") {
               expect(s.answer.servedSkip?.reason).toBe(c.servedSkipReason);
            }
            const because = c.declinedBecause?.[tier];
            if (because) expect(s.answer.viewSkip?.detail).toContain(because);
         });
      }
   });
}
