// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A persisted view's answer carries live's result metadata, not only live's
 * rows: each column's kind (a measure stays a measure) and its annotations,
 * Malloy's own `#(malloy)` one included, with only its `drill_expression` set
 * aside (a stored column's drill code legitimately differs). Measures (a
 * filtered one and boolean ones among them, NULL included), dimensions,
 * timeframes, a calculation, a `having:`, a multi-stage view and a query that
 * goes on past the view are each compared, through the real package path, for
 * a `storage=` table and a colocated one. Before the view leg regrouped the
 * stored rows, every measure came back a dimension.
 *
 * A view whose stored rows cannot be regrouped into exactly themselves (no
 * dimension to key them, a nest) is declined by the view leg and answers
 * live; those cases assert the decline, and rows and metadata equal to live.
 *
 * A column's type is compared too: every date and timestamp truncation, for
 * a `storage=` table built as a build casts it (a date truncation stored as a
 * DATE) and one stored uncast, as a build stores it when it declines or fails
 * a cast (a date truncation stored as a TIMESTAMP, which the view leg reads
 * through the served copy's `::"DATE"` cast rather than typing a timestamp).
 * The same truncations through a query-shaped `storage=` source answered by
 * the served copy are compared alongside.
 */

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../../../src/logger";
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
} from "./conformance/package_harness";
import { renderFiles, type Shape } from "./conformance/shapes";
import { rowDifference } from "./harness";

const MODEL = `##! experimental { persistence }
import "raw.malloy"

source: ov is orders_raw extend {
  # currency
  #(doc) "Revenue"
  measure: total is amount.sum()
  measure: order_count is count()

  @PVIEW(ov_by_region)
  view: by_region is { group_by: region; aggregate: total, order_count }

  @PVIEW(ov_totals)
  view: totals is { aggregate: total, order_count }

  @PVIEW(ov_by_month)
  view: by_month is {
    group_by: created_month is created_at.month
    aggregate: total
    order_by: created_month
  }

  @PVIEW(ov_by_order_month)
  view: by_order_month is {
    group_by: order_month is order_date.month
    aggregate: total
    order_by: order_month
  }

  @PVIEW(ov_ranked)
  view: ranked is {
    group_by: region
    aggregate: total
    calculate: total_rank is rank()
    order_by: total desc
  }

  @PVIEW(ov_two_stage)
  view: two_stage is { group_by: region, status; aggregate: total }
    -> { group_by: region; aggregate: status_count is count(), top is max(total) }

  @PVIEW(ov_filtered)
  view: filtered is {
    group_by: region
    aggregate: placed is order_count { where: status = 'placed' }
  }

  @PVIEW(ov_flagged)
  view: flagged is { group_by: region; aggregate: busy is order_count > 3 }

  view: by_status is { group_by: status; aggregate: order_count; order_by: status }

  @PVIEW(ov_deep)
  view: deep is {
    group_by: region
    nest: by_status
    nest: by_status_month is {
      group_by: status
      aggregate: total
      nest: per_month is {
        group_by: created_month is created_at.month
        aggregate: order_count
        order_by: created_month
      }
      order_by: status
    }
  }

  @PVIEW(ov_filtered_nest)
  view: filtered_nest is {
    group_by: region
    nest: returned is {
      where: status = 'returned'
      group_by: status
      aggregate: total
    }
  }

  @PVIEW(ov_select_nest)
  view: select_nest is {
    group_by: region
    aggregate: total
    nest: lines is { select: status }
  }

  @PVIEW(ov_empty_nest)
  view: empty_nest is {
    group_by: region
    aggregate: total
    nest: north_placed is {
      where: status = 'placed' and region = 'north'
      group_by: status
      aggregate: total
    }
  }

  @PVIEW(ov_having_only)
  view: having_only is { aggregate: order_count; having: order_count > 1000000 }

  @PVIEW(ov_having_grouped)
  view: having_grouped is {
    group_by: region
    aggregate: order_count
    having: order_count > 2
  }

  @PVIEW(ov_mixed_flag)
  view: mixed_flag is {
    group_by: region
    aggregate: big is amount.avg() { where: region != 'west' } > 100
    order_by: region
  }

  @PVIEW(ov_null_flag)
  view: null_flag is {
    group_by: region
    aggregate: any_null is max(amount) { where: amount is null } > 100000
  }

  @PVIEW(ov_select_calc)
  view: select_calc is {
    select: region
    calculate: prev_region is lag(region)
    order_by: region
  }

  @PVIEW(ov_listing)
  view: listing is { select: id, region, amount; order_by: id }

  @PVIEW(ov_date_grains)
  view: date_grains is {
    group_by:
      d_day is order_date.day
      d_week is order_date.week
      d_month is order_date.month
      d_quarter is order_date.quarter
      d_year is order_date.year
    aggregate: order_count
    order_by: d_day
  }

  @PVIEW(ov_timestamp_grains)
  view: timestamp_grains is {
    group_by:
      t_minute is created_at.minute
      t_hour is created_at.hour
      t_day is created_at.day
      t_week is created_at.week
      t_month is created_at.month
      t_quarter is created_at.quarter
      t_year is created_at.year
    aggregate: order_count
    order_by: t_minute
  }

  @PVIEW(ov_zoned_by_month)
  view: zoned_by_month is {
    timezone: 'America/New_York'
    group_by: order_month is order_date.month
    aggregate: total
    order_by: order_month
  }

  @PVIEW(ov_zoned_by_day)
  view: zoned_by_day is {
    timezone: 'America/New_York'
    group_by: created_day is created_at.day
    aggregate: total
    order_by: created_day
  }

  @PVIEW(ov_date_measure)
  view: date_measure is {
    group_by: region
    aggregate: last_order is max(order_date.month)
  }

  @PVIEW(ov_nested)
  view: nested is {
    group_by: region
    aggregate: total
    nest: by_status is {
      group_by: status
      aggregate: order_count
      order_by: status
      limit: 2
    }
  }
}

@PERSIST(grains_src)
source: grains is orders_raw -> {
  group_by:
    d_day is order_date.day
    d_week is order_date.week
    d_month is order_date.month
    d_quarter is order_date.quarter
    d_year is order_date.year
    t_hour is created_at.hour
    t_day is created_at.day
    t_month is created_at.month
    t_year is created_at.year
  aggregate: n is count()
}
`;

const SHAPE: Shape = {
   name: "persisted view metadata",
   about: "a persisted view's columns keep live's kind and annotations",
   files: { "m.malloy": MODEL },
   queries: [],
};

type Tier = "storage" | "colocated";

interface Case {
   name: string;
   malloy: string;
   /** Tiers whose answer is live by design (a nest is stored only colocated). */
   liveIn?: Tier[];
   /** What answers it when not live (default the view leg). */
   servedBy?: string;
   /** A known difference this spec does not fix, by tier, with its reason. */
   knownGap?: Partial<Record<Tier, string>>;
   /** Values a column must hold among the answer's rows, so the case covers each. */
   covers?: { column: string; values: unknown[] };
   /** Text the view leg's decline detail must carry, by tier. */
   declinedBecause?: Partial<Record<Tier, string>>;
}

/** The warning a variant that fails to compile logs; a refusal never does. */
const DID_NOT_COMPILE = "A persisted-view variant did not compile";

const CASES: Case[] = [
   { name: "measures and a dimension", malloy: "run: ov -> by_region" },
   { name: "a timestamp timeframe", malloy: "run: ov -> by_month" },
   { name: "a date timeframe", malloy: "run: ov -> by_order_month" },
   {
      name: "every date truncation",
      malloy: "run: ov -> date_grains",
   },
   {
      name: "every timestamp truncation",
      malloy: "run: ov -> timestamp_grains",
   },
   {
      name: "a date truncation under a view's time zone",
      malloy: "run: ov -> zoned_by_month",
   },
   {
      name: "a timestamp truncation under a view's time zone",
      malloy: "run: ov -> zoned_by_day",
      knownGap: {
         storage:
            "the stored day reads back at UTC midnight where live answers New York midnight (rows differ, not only a type)",
      },
   },
   {
      name: "a date-typed measure",
      malloy: "run: ov -> date_measure",
   },
   {
      name: "a later stage over date truncations",
      malloy:
         "run: ov -> date_grains -> { group_by: d_month, d_year; aggregate: c is order_count.sum(); order_by: d_month }",
   },
   {
      name: "a query-shaped source's truncations, through the served copy",
      malloy: "run: grains -> { select: *; order_by: d_day, t_hour }",
      servedBy: "materialized",
   },
   { name: "a calculation", malloy: "run: ov -> ranked" },
   { name: "a multi-stage view", malloy: "run: ov -> two_stage" },
   { name: "a select view", malloy: "run: ov -> listing" },
   { name: "a filtered measure", malloy: "run: ov -> filtered" },
   { name: "a boolean measure", malloy: "run: ov -> flagged" },
   {
      name: "a later stage that aggregates",
      malloy: "run: ov -> by_region -> { aggregate: grand is total.sum() }",
   },
   {
      name: "a later stage that selects",
      malloy: "run: ov -> by_region -> { select: region, total }",
   },
   {
      name: "a boolean measure true, false and NULL by group",
      malloy: "run: ov -> mixed_flag",
      covers: { column: "big", values: [true, false, null] },
   },
   {
      name: "a NULL boolean measure stays NULL",
      malloy: "run: ov -> null_flag",
   },
   {
      name: "a having: on a grouped view",
      malloy: "run: ov -> having_grouped",
   },
   // Shapes whose stored rows cannot be regrouped into exactly themselves:
   // the view leg declines them, so they answer live.
   {
      name: "measures alone (no dimension to key the stored rows)",
      malloy: "run: ov -> totals",
      liveIn: ["storage", "colocated"],
   },
   {
      name: "a having: that leaves no row, with no dimension",
      malloy: "run: ov -> having_only",
      liveIn: ["storage", "colocated"],
   },
   {
      name: "a select: with a calculate:, whose duplicate rows a regroup would merge",
      malloy: "run: ov -> select_calc",
      liveIn: ["storage", "colocated"],
   },
   {
      name: "a select: nest, whose duplicate rows a regroup would merge",
      malloy: "run: ov -> select_nest",
      liveIn: ["storage", "colocated"],
      declinedBecause: { colocated: "it nests `lines`" },
   },
   {
      name: "a later stage counting a select: nest's rows",
      malloy:
         "run: ov -> select_nest -> { aggregate: g is total.sum(), n is lines.count() }",
      liveIn: ["storage", "colocated"],
   },
   {
      name: "a nest left empty by its where:",
      malloy: "run: ov -> empty_nest",
      liveIn: ["storage", "colocated"],
      declinedBecause: { colocated: "it nests `north_placed`" },
   },
   {
      name: "a view nested by name and a nest in a nest",
      malloy: "run: ov -> deep",
      liveIn: ["storage", "colocated"],
      declinedBecause: { colocated: "it nests `by_status`" },
   },
   {
      name: "a nest with its own where:",
      malloy: "run: ov -> filtered_nest",
      liveIn: ["storage", "colocated"],
      declinedBecause: { colocated: "it nests `returned`" },
   },
   {
      name: "a nest's fields",
      malloy: "run: ov -> nested",
      liveIn: ["storage", "colocated"],
      declinedBecause: { colocated: "it nests `by_status`" },
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
   { name: "colocated", tier: "colocated" },
];

for (const { name: variantName, tier: variant, storeUncast } of VARIANTS) {
   describe(`a persisted view's result metadata [${variantName}]`, () => {
      let restore: () => void = () => {};
      let fixture: LoadedFixture | undefined;
      const live = new Map<string, Outcome>();
      const served = new Map<string, Outcome>();
      /** The warnings logged while each case was served. */
      const warnings = new Map<string, string[]>();

      beforeAll(async () => {
         restore = withSwitch("cutover");
         await freshLoadPool();
         const loader = await loadFixture({
            ...DATA_FILES,
            ...renderFiles(SHAPE, variant),
         });
         for (const c of CASES) {
            live.set(c.name, await loader.run("m.malloy", c.malloy));
         }
         fixture = await loader.bind({
            includeRollups: true,
            ...(storeUncast ? { storeUncast } : {}),
         });
         for (const c of CASES) {
            const logged: string[] = [];
            const warn = spyOn(logger, "warn").mockImplementation(((
               message: unknown,
            ) => {
               logged.push(String(message));
               return logger;
            }) as never);
            try {
               served.set(c.name, await fixture.run("m.malloy", c.malloy));
            } finally {
               warn.mockRestore();
            }
            warnings.set(c.name, logged);
         }
      });

      afterAll(async () => {
         restore();
         await fixture?.dispose();
      });

      for (const c of CASES) {
         const want = c.liveIn?.includes(variant)
            ? "live"
            : (c.servedBy ?? "view");
         const gap = c.knownGap?.[variant];
         (gap ? it.skip : it)(
            `${c.name}${gap ? ` (known gap: ${gap})` : ""}: answered by ${want}, kinds and annotations equal to live`,
            () => {
               const l = live.get(c.name)!;
               const s = served.get(c.name)!;
               if ("error" in l) throw new Error(`live failed: ${l.error}`);
               if ("error" in s) throw new Error(`served failed: ${s.error}`);
               expect(s.answer.servedBy).toBe(want);
               expect(
                  rowDifference(l.answer.rows, s.answer.rows),
               ).toBeUndefined();
               expect(metadata(s.answer)).toEqual(metadata(l.answer));
               // A declined shape is a refusal with its reason, never a
               // variant that fails to compile.
               expect(warnings.get(c.name)).not.toContain(DID_NOT_COMPILE);
               const because = c.declinedBecause?.[variant];
               if (because) {
                  expect(s.answer.viewSkip?.detail).toContain(because);
               }
               if (c.covers) {
                  const held = s.answer.rows.map((r) => r[c.covers!.column]);
                  for (const v of c.covers.values) expect(held).toContain(v);
               }
            },
         );
      }
   });
}
