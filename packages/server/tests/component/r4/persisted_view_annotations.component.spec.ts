// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a persisted view's annotations: a `#@ persist` view's answer returns
 * each output column with the annotations live's does (`#(classification)`,
 * `#(doc)`, render tags such as `# currency` and `# label`, a multi-line
 * `#|`…`|#` note), colocated and
 * `storage=`, for a column, a computed dimension, a timeframe, a measure and
 * a nest; and the access record reads the same fields as live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { unrecordedAudit } from "../../../src/service/access_record";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
} from "./harness";

const seedSql = [
   `CREATE TABLE orders_raw AS SELECT * FROM (VALUES
      (1, 10.0, 'east', 'ann', TIMESTAMP '2026-01-01 08:00:00'),
      (2, 20.0, 'west', 'bob', TIMESTAMP '2026-01-20 09:00:00'),
      (3, 30.0, 'east', 'cy',  TIMESTAMP '2026-02-01 10:00:00'),
      (4,  5.0, 'east', 'ann', TIMESTAMP '2026-03-03 11:00:00')
    ) AS t(id, amount, region, buyer, order_time)`,
];

/** Persisted views whose output columns carry a class, a doc and render tags. */
const model = (
   persist: (view: string) => string,
) => `##! experimental.access_modifiers
source: orders is duckdb.table('orders_raw') include {
  #(classification) "none"
  public: id, amount, region, order_time
  #(classification) "pii"
  # label="Buyer"
  public: buyer
} extend {
  primary_key: id
  #(classification) "pii"
  # label="Region tag"
  dimension: region_tag is concat(region, '/', buyer)
  #(classification) "none"
  #|
    # label="Month"
  |#
  dimension: order_month is order_time.month
  #(classification) "none"
  #(doc) "Order revenue"
  # currency
  #|
    # description="Net of refunds, \\"as booked\\" |# not a closer"
  |#
  measure: revenue is amount.sum()

  # bar_chart
  ${persist("by_month")}
  view: by_month is { group_by: order_month, region_tag; aggregate: revenue }
  ${persist("by_buyer")}
  view: by_buyer is {
    group_by: buyer
    # label="Day"
    group_by: order_day is order_time.day
    aggregate: revenue
  }
  ${persist("nested")}
  view: nested is { group_by: region; nest: by_buyer is { group_by: buyer; aggregate: revenue } }
}
`;

interface Answer {
   rows: Row[];
   /** Each result column's author annotations (Malloy's own `#(malloy)` notes left out). */
   notes: Record<string, string[]>;
   /** A nest's columns' annotations, by `nest.column`. */
   nestedNotes: Record<string, string[]>;
   servedBy?: string;
   fieldsRead?: unknown;
   error?: string;
}

type Field = {
   name: string;
   annotations?: { value: string }[];
   type?: { element_type?: { fields?: Field[] } };
};

const authorNotes = (f: Field) =>
   (f.annotations ?? [])
      .map((a) => a.value.trim())
      .filter((v) => !v.startsWith("#(malloy)"));

describe.each([
   ["colocated", (v: string) => `#@ persist name="orders_${v}"`, true],
   [
      "in a storage destination",
      (_v: string) => `#@ persist storage="${DESTINATION}"`,
      false,
   ],
] as const)(
   "R4: a persisted view %s answers with live's annotations",
   (_label, persist, colocated) => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model(persist) },
            seedSql,
            persistedViews: true,
            ...(colocated
               ? {
                    colocated: [
                       "orders__by_month__view",
                       "orders__by_buyer__view",
                       "orders__nested__view",
                    ],
                 }
               : {}),
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      const answer = async (malloy: string, served: boolean) => {
         fixture.setBindings(served ? fixture.bindings : []);
         const audit = unrecordedAudit();
         try {
            const r = await fixture.model.runQuery(
               audit,
               { query: malloy, filterParams: {} },
               QueryPrivileges.of({ bypassFilters: true }),
            );
            const notes: Record<string, string[]> = {};
            const nestedNotes: Record<string, string[]> = {};
            for (const raw of r.result?.schema?.fields ?? []) {
               const f = raw as Field;
               notes[f.name] = authorNotes(f);
               for (const inner of f.type?.element_type?.fields ?? []) {
                  nestedNotes[`${f.name}.${inner.name}`] = authorNotes(inner);
               }
            }
            return {
               rows: r.compactResult as unknown as Row[],
               notes,
               nestedNotes,
               servedBy: r.path?.servedBy.kind,
               fieldsRead: audit.summary?.fieldsRead,
            } satisfies Answer;
         } catch (error) {
            return {
               rows: [],
               notes: {},
               nestedNotes: {},
               error: error instanceof Error ? error.message : String(error),
               fieldsRead: audit.summary?.fieldsRead,
            } satisfies Answer;
         }
      };

      const both = async (malloy: string) => ({
         live: await answer(malloy, false),
         served: await answer(malloy, true),
      });

      for (const [name, malloy, column, expected] of [
         [
            "a measure",
            "run: orders -> by_month",
            "revenue",
            [
               '#(classification) "none"',
               '#(doc) "Order revenue"',
               "# currency",
               // A multi-line note, held without its closer: written back
               // as held, it would leave the variant's annotation open.
               '#|\n# description="Net of refunds, \\"as booked\\" |# not a closer"',
            ],
         ],
         [
            "a timeframe dimension",
            "run: orders -> by_month",
            "order_month",
            ['#(classification) "none"', '#|\n# label="Month"'],
         ],
         [
            "a computed dimension",
            "run: orders -> by_month",
            "region_tag",
            ['#(classification) "pii"', '# label="Region tag"'],
         ],
         [
            // An include block's notes are not on a view's output column,
            // live or served; the column is compared all the same.
            "a table column",
            "run: orders -> by_buyer",
            "buyer",
            [],
         ],
         [
            "a timeframe the view labels",
            "run: orders -> by_buyer",
            "order_day",
            ['# label="Day"'],
         ],
         [
            "a column after the view, in a later stage",
            "run: orders -> by_month -> { select: order_month, revenue }",
            "revenue",
            ["# currency"],
         ],
      ] as const) {
         it(`returns ${name} with its annotations, and the same rows and access record`, async () => {
            const { live, served } = await both(malloy);
            expect(live.error).toBeUndefined();
            expect(served.error).toBeUndefined();
            expect(live.servedBy).toBe("live");
            expect(served.servedBy).toBe("view");
            expect(rowDifference(live.rows, served.rows)).toBeUndefined();
            for (const e of expected) expect(live.notes[column]).toContain(e);
            expect(served.notes).toEqual(live.notes);
            expect(served.fieldsRead).toEqual(live.fieldsRead);
         });
      }

      // A nested view is stored colocated only (a destination holds no nest).
      it.if(colocated)(
         "returns a nest's columns with their annotations",
         async () => {
            const { live, served } = await both("run: orders -> nested");
            expect(live.error).toBeUndefined();
            expect(served.error).toBeUndefined();
            expect(served.servedBy).toBe("view");
            expect(rowDifference(live.rows, served.rows)).toBeUndefined();
            expect(live.nestedNotes["by_buyer.revenue"]).toContain(
               "# currency",
            );
            expect(served.notes).toEqual(live.notes);
            expect(served.nestedNotes).toEqual(live.nestedNotes);
            expect(served.fieldsRead).toEqual(live.fieldsRead);
         },
      );
   },
);
