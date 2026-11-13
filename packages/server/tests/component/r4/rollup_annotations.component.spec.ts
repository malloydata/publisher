// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a `#(preaggregate)` rollup's annotations: a rollup answer returns
 * each column with the annotations live's does (`#(classification)`, `#(doc)`,
 * render tags such as `# currency` and `# label`), on both legs (a source's
 * own rollup, colocated and `storage=`; a joined source's rollup read through
 * the join, A7), and the governance that reads annotations reads the same
 * compile as live: the access record's field classes, and the `#(carries)`
 * ceiling, which refuses a query a rollup could otherwise answer.
 *
 * The variant re-emits each stored measure (`total is total__partial.sum()`)
 * and, for a `storage=` rollup, each grain column (a `type:` field), so the
 * author's notes are written on them again; a colocated grain column is a
 * `group_by:` output and keeps its dimension's notes from Malloy itself.
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
   `CREATE TABLE lines_raw AS SELECT * FROM (VALUES
      (10, 1, 'p', 2.0), (11, 1, 'q', 3.0), (12, 2, 'p', 4.0), (13, 4, 'q', 1.5)
    ) AS t(line_id, order_id, kind, qty)`,
];

/**
 * A rollup over a source whose grain dimensions (a column, a computed
 * dimension, a timeframe) and stored measure carry a class and a render tag;
 * and (A7) a joined source whose grain dimension and measure carry them.
 */
const model = (placement: string) => `##! experimental.access_modifiers
source: lines is duckdb.table('lines_raw') include {
  #(classification) "none"
  public: line_id, order_id, qty
  #(classification) "none"
  # label="Line kind"
  public: kind
} extend {
  primary_key: line_id
  #(classification) "none"
  #(doc) "Units shipped"
  # number="#,##0.0"
  #(preaggregate) grain=["order_id", "kind"]
  measure: shipped is qty.sum()
}

#(carries) ["none", "pii"]
source: orders is duckdb.table('orders_raw') include {
  #(classification) "none"
  public: id, amount, region, order_time
  #(classification) "pii"
  # label="Buyer"
  public: buyer
} extend {
  primary_key: id
  join_many: lines on lines.order_id = id
  #(classification) "pii"
  # label="Region tag"
  dimension: region_tag is concat(region, '/', buyer)
  #(classification) "none"
  # label="Month"
  dimension: order_month is order_time.month
  #(classification) "none"
  #(doc) "Order revenue"
  # currency
  #|
    # description="Net of refunds"
  |#
  #(preaggregate) grain=["region"]${placement}
  #(preaggregate) grain=["region", "buyer", "region_tag", "order_month"]${placement}
  measure: revenue is amount.sum()
}

#(carries) ["none"]
source: open_orders is duckdb.table('orders_raw') include {
  #(classification) "none"
  public: id, amount, region
  #(classification) "pii"
  internal: buyer
} extend {
  primary_key: id
  #(classification) "none"
  # currency
  #(preaggregate) grain=["region"]${placement}
  measure: open_revenue is amount.sum()
}
`;

interface Answer {
   rows: Row[];
   /** Each result column's author annotations (Malloy's own `#(malloy)` notes left out). */
   notes: Record<string, string[]>;
   servedBy?: string;
   fieldsRead?: unknown;
   error?: string;
}

describe.each([
   ["colocated", ""],
   ["in a storage destination", ` storage="${DESTINATION}"`],
] as const)(
   "R4: a #(preaggregate) rollup %s answers with live's annotations",
   (_label, placement) => {
      let restore: () => void;
      let fixture: R4Fixture;
      beforeAll(async () => {
         restore = installSettings({
            env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
         }).restore;
         fixture = await buildScenario({
            files: { "m.malloy": model(placement) },
            seedSql,
            preaggregate: true,
         });
      });
      afterAll(async () => {
         restore();
         await disposeScenarios();
      });

      const answer = async (
         malloy: string,
         served: boolean,
         bypassAuthorize = false,
      ): Promise<Answer> => {
         fixture.setBindings(served ? fixture.bindings : []);
         const audit = unrecordedAudit();
         try {
            const r = await fixture.model.runQuery(
               audit,
               { query: malloy, filterParams: {} },
               QueryPrivileges.of({ bypassFilters: true, bypassAuthorize }),
            );
            const notes: Record<string, string[]> = {};
            for (const f of r.result?.schema?.fields ?? []) {
               notes[f.name] = (
                  (f as { annotations?: { value: string }[] }).annotations ?? []
               )
                  .map((a) => a.value.trim())
                  .filter((v) => !v.startsWith("#(malloy)"));
            }
            return {
               rows: r.compactResult as unknown as Row[],
               notes,
               servedBy: r.path?.servedBy.kind,
               fieldsRead: audit.summary?.fieldsRead,
            } satisfies Answer;
         } catch (error) {
            return {
               rows: [],
               notes: {},
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
            "a stored measure, by a grain column",
            "run: orders -> { group_by: region; aggregate: revenue }",
            "revenue",
            [
               '#(classification) "none"',
               '#(doc) "Order revenue"',
               "# currency",
               // A multi-line note, held without its closer.
               '#|\n# description="Net of refunds"',
            ],
         ],
         [
            "a grain column that is a table column",
            "run: orders -> { group_by: buyer; aggregate: revenue }",
            "buyer",
            ['#(classification) "pii"', '# label="Buyer"'],
         ],
         [
            "a grain column that is a computed dimension",
            "run: orders -> { group_by: region_tag; aggregate: revenue }",
            "region_tag",
            ['#(classification) "pii"', '# label="Region tag"'],
         ],
         [
            "a grain column with a timeframe",
            "run: orders -> { group_by: order_month; aggregate: revenue }",
            "order_month",
            ['#(classification) "none"', '# label="Month"'],
         ],
         [
            "a stored measure in a nest",
            "run: orders -> { group_by: region; nest: n is { group_by: buyer; aggregate: revenue } }",
            "region",
            ['#(classification) "none"'],
         ],
      ] as const) {
         it(`returns ${name} with its annotations, and the same rows and access record`, async () => {
            const { live, served } = await both(malloy);
            expect(live.error).toBeUndefined();
            expect(live.servedBy).toBe("live");
            expect(served.servedBy).toBe("rollup");
            expect(rowDifference(live.rows, served.rows)).toBeUndefined();
            for (const e of expected) expect(live.notes[column]).toContain(e);
            expect(served.notes).toEqual(live.notes);
            expect(served.fieldsRead).toEqual(live.fieldsRead);
         });
      }

      it("refuses a query past the #(carries) ceiling, though a rollup could answer it", async () => {
         // The caller join's `on` reads an internal pii column of a source that
         // carries ["none"]; the query's columns are the rollup's grain and
         // measure.
         const malloy =
            "run: open_orders -> { extend: { join_one: o is orders on o.buyer = region } group_by: region; aggregate: open_revenue }";
         const { live, served } = await both(malloy);
         expect(live.error).toContain("carries [none]");
         expect(served.error).toBe(live.error);
         // Under the data-management bypass, which skips the ceiling, a
         // colocated rollup answers it (a `storage=` one cannot read the
         // caller's join, which is on the source's own connection): what
         // refused it was the ceiling, read on live.
         const bypassed = await answer(malloy, true, true);
         expect(bypassed.error).toBeUndefined();
         if (placement === "") expect(bypassed.servedBy).toBe("rollup");
         // The same query within the ceiling is the rollup's.
         const within = await both(
            "run: open_orders -> { group_by: region; aggregate: open_revenue }",
         );
         expect(within.served.servedBy).toBe("rollup");
         expect(within.served.notes).toEqual(within.live.notes);
      });

      it("reports the rollup as the table that answered", async () => {
         const { served } = await both(
            "run: orders -> { group_by: region; aggregate: revenue }",
         );
         expect(served.servedBy).toBe("rollup");
      });

      if (placement === "") {
         // A7: a joined source's colocated rollup, read through the join.
         for (const [name, malloy, column] of [
            [
               "a joined stored measure",
               "run: orders -> { group_by: region; aggregate: lines.shipped }",
               "shipped",
            ],
            [
               "a joined grain column",
               "run: orders -> { group_by: lines.kind; aggregate: lines.shipped }",
               "kind",
            ],
         ] as const) {
            it(`(A7) returns ${name} with its annotations`, async () => {
               const { live, served } = await both(malloy);
               expect(served.servedBy).toBe("rollup");
               expect(rowDifference(live.rows, served.rows)).toBeUndefined();
               expect(live.notes[column]?.length).toBeGreaterThan(0);
               expect(served.notes).toEqual(live.notes);
               expect(served.fieldsRead).toEqual(live.fieldsRead);
            });
         }
      }
   },
);
