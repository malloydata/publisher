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
`;

const SHAPE: Shape = {
   name: "persisted view metadata",
   about: "a persisted view's columns keep live's kind and annotations",
   files: { "m.malloy": MODEL },
   queries: [],
};

interface Case {
   name: string;
   malloy: string;
   /** Variants whose answer is live by design (a nest is stored only colocated). */
   liveIn?: ("storage" | "colocated")[];
   /** A known difference this spec does not fix, by variant, with its reason. */
   knownGap?: Partial<Record<"storage" | "colocated", string>>;
}

const CASES: Case[] = [
   { name: "measures and a dimension", malloy: "run: ov -> by_region" },
   { name: "a timestamp timeframe", malloy: "run: ov -> by_month" },
   {
      name: "a date timeframe",
      malloy: "run: ov -> by_order_month",
      knownGap: {
         storage:
            "a date truncation is stored as a TIMESTAMP and served as a timestamp, where live types it a date (a column type, not a kind)",
      },
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
   },
   {
      name: "a view nested by name and a nest in a nest",
      malloy: "run: ov -> deep",
      liveIn: ["storage", "colocated"],
   },
   {
      name: "a nest with its own where:",
      malloy: "run: ov -> filtered_nest",
      liveIn: ["storage", "colocated"],
   },
   {
      name: "a nest's fields",
      malloy: "run: ov -> nested",
      liveIn: ["storage", "colocated"],
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

for (const variant of ["storage", "colocated"] as const) {
   describe(`a persisted view's result metadata [${variant}]`, () => {
      let restore: () => void = () => {};
      let fixture: LoadedFixture | undefined;
      const live = new Map<string, Outcome>();
      const served = new Map<string, Outcome>();

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
         fixture = await loader.bind({ includeRollups: true });
         for (const c of CASES) {
            served.set(c.name, await fixture.run("m.malloy", c.malloy));
         }
      });

      afterAll(async () => {
         restore();
         await fixture?.dispose();
      });

      for (const c of CASES) {
         const want = c.liveIn?.includes(variant) ? "live" : "view";
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
            },
         );
      }
   });
}
