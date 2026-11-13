// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R4 for a stored timeframe truncation in the served copy (materialization v2,
 * decision 3): a `type:` field cannot declare a timeframe, so the served copy
 * renames the stored column internally and redefines the field as that column
 * truncated again. Every answer equals live, and so does the result's field
 * metadata: `order_date.month` reads back as a `date` with timeframe `month`,
 * not a bare `date`, so charts and filters behave as they do live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   buildScenario,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
} from "./harness";

const seedSql = [
   `CREATE TABLE ev AS SELECT * FROM (VALUES
      (1, 'east', 10.5, DATE '2024-03-15', TIMESTAMP '2024-03-15 13:45:10'),
      (2, 'east', 20.0, DATE '2024-03-02', TIMESTAMP '2024-03-02 08:05:00'),
      (3, 'west', 5.5, DATE '2024-07-20', TIMESTAMP '2024-07-20 22:10:00'),
      (4, 'west', 1.0, DATE '2023-12-31', TIMESTAMP '2023-12-31 23:59:00')
    ) AS t(id, region, amt, d, ts)`,
];

/** Every timeframe the decision names, on a date and on a timestamp. */
const TRUNCATIONS = [
   "order_year",
   "order_quarter",
   "order_month",
   "order_week",
   "ts_day",
   "ts_hour",
   "ts_month",
];

// In a file of its own: a timestamp truncation declared on a base in another
// file is stored as the build computed it, in the default zone (the served
// copy keeps only the blocks of bases declared in the source's own file), so
// a timezone: query over the table declines.
const baseModel = `source: orders is duckdb.table('ev') extend {
  dimension: inherited_month is d.month
  dimension: inherited_day is ts.day
}
`;

const model = `import "base.malloy"

#(materialize) storage="lake"
source: stored is orders extend {
  dimension:
    order_year is d.year
    order_quarter is d.quarter
    #(doc) "The month of the order"
    order_month is d.month
    order_week is d.week
  dimension: ts_day is ts.day, ts_hour is ts.hour, ts_month is ts.month
  dimension: plain is amt > 10
  measure: total is amt.sum()
}

source: stored_ext is stored extend {
  dimension: is_march is order_month = @2024-03
}
`;

const persistModel = `##! experimental { persistence }
source: ev is duckdb.table('ev')

#@ persist name="monthly" storage="lake"
source: monthly is ev -> {
  group_by: m is d.month, h is ts.hour
  aggregate: total is amt.sum()
}
`;

/** The timeframe a wire field type carries, if any. */
function timeframes(fields: { name: string; type: unknown }[]) {
   return Object.fromEntries(
      fields.map((f) => [
         f.name,
         `${(f.type as { kind?: string }).kind}/${(f.type as { timeframe?: string }).timeframe ?? ""}`,
      ]),
   );
}

describe("R4: a stored timeframe truncation keeps its timeframe in the served copy", () => {
   let restore: () => void;
   let fixture: R4Fixture;
   let persisted: R4Fixture;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      fixture = await buildScenario({
         files: { "m.malloy": model, "base.malloy": baseModel },
         seedSql,
         materialize: true,
      });
      persisted = await buildScenario({
         files: { "m.malloy": persistModel },
         seedSql,
      });
   });
   afterAll(async () => {
      restore();
      await disposeScenarios();
   });

   const queries = [
      [
         "every truncation, on a date and a timestamp",
         `run: stored -> { group_by: ${TRUNCATIONS.join(", ")}; order_by: ts_day }`,
      ],
      [
         "a timeframe the source inherits",
         "run: stored -> { group_by: inherited_month; aggregate: total }",
      ],
      [
         "a re-truncation to a coarser timeframe",
         "run: stored -> { group_by: y is order_month.year; aggregate: total }",
      ],
      [
         "a filter by a month literal",
         "run: stored -> { where: order_month = @2024-03; aggregate: total }",
      ],
      [
         "a filter by a day literal on a month field",
         "run: stored -> { where: order_month = @2024-03-15; aggregate: total }",
      ],
      [
         "a filter range on an hour field",
         "run: stored -> { where: ts_hour ? @2024-03-15 13 to @2024-07-21; aggregate: total }",
      ],
      [
         "a filter expression on a month field",
         "run: stored -> { where: order_month ~ f'2024-03'; aggregate: total }",
      ],
      [
         "an extension that filters on a timeframe field",
         "run: stored_ext -> { group_by: is_march; aggregate: total }",
      ],
   ] as const;
   for (const [name, malloy] of queries) {
      it(name, async () => {
         const run = await fixture.run({ name, malloy });
         expect(run.servedVia).toBe("served_copy");
         expect(run.servedFrom).toBe("storage");
         expect(rowDifference(run.live, run.served)).toBeUndefined();
         expect(timeframes(run.servedFields)).toEqual(
            timeframes(run.liveFields),
         );
      });
   }

   it("reads back each truncation's timeframe, as live does", async () => {
      const run = await fixture.run({
         name: "metadata",
         malloy: `run: stored -> { group_by: ${TRUNCATIONS.join(", ")}, plain }`,
      });
      expect(run.servedVia).toBe("served_copy");
      expect(timeframes(run.servedFields)).toEqual({
         order_year: "date_type/year",
         order_quarter: "date_type/quarter",
         order_month: "date_type/month",
         order_week: "date_type/week",
         ts_day: "timestamp_type/day",
         ts_hour: "timestamp_type/hour",
         ts_month: "timestamp_type/month",
         plain: "boolean_type/",
      });
   });

   it("answers a query that sets a timezone live, since the build truncated in the default one", async () => {
      const run = await fixture.run({
         name: "timezone",
         malloy:
            "run: stored -> { timezone: 'America/New_York'; group_by: ts_day; aggregate: total }",
      });
      expect(run.servedVia).toBeUndefined();
      expect(rowDifference(run.live, run.served)).toBeUndefined();
   });

   it("places the table on the queried source's chain in the path report, whether or not it answered", async () => {
      fixture.setBindings(fixture.bindings);
      const declined = await fixture.runAsIs(
         "run: stored -> { timezone: 'America/New_York'; group_by: ts_day }",
      );
      const answered = await fixture.runAsIs(
         "run: stored -> { group_by: ts_day }",
      );
      const tables = (path: typeof declined.path) =>
         (path?.considered ?? []).map((c) =>
            c.reason ? [c.kind, c.outcome, c.reason] : [c.kind, c.outcome],
         );
      // Keyed by the author's sourceID, the table is found on the chain of
      // `stored` even when the served copy declined.
      // Declined for its timezone, and the report says so.
      expect(tables(declined.path)).toEqual([
         ["materialized", "skipped", "TIME_ZONE"],
      ]);
      expect(declined.path?.servedBy.kind).toBe("live");
      expect(tables(answered.path)).toEqual([["materialized", "used"]]);
      const ext = await fixture.runAsIs(
         "run: stored_ext -> { timezone: 'UTC'; group_by: is_march }",
      );
      // The extension declares no #(materialize), so it has no table of its
      // own: `stored`'s table answers it. Malloy records no `extends` on an
      // extension of a table-backed source; the source graph reads its base
      // from the compiled structure, so the declined table is still placed.
      expect(tables(ext.path)).toEqual([
         ["materialized", "skipped", "TIME_ZONE"],
      ]);
      expect(ext.path?.servedBy.kind).toBe("live");
      const extAnswered = await fixture.runAsIs(
         "run: stored_ext -> { group_by: is_march }",
      );
      expect(tables(extAnswered.path)).toEqual([["materialized", "used"]]);
      expect(
         JSON.stringify([
            declined.path,
            answered.path,
            ext.path,
            extAnswered.path,
         ]),
      ).not.toContain("__mat");
   });

   it("keeps the author's annotation on a field redefined in place", async () => {
      expect(await fixture.servedFieldNotes("stored", "order_month")).toContain(
         '#(doc) "The month of the order"',
      );
   });

   it("hides the internal stored column from a query", async () => {
      await expect(
         fixture.runAsIs("run: stored -> { group_by: order_month__stored }"),
      ).rejects.toThrow();
   });

   it("leaves no served-copy problem", async () => {
      expect(await fixture.servedCopyProblems()).toEqual([]);
   });

   it("restores the timeframe of a #@ persist query's output column", async () => {
      const run = await persisted.run({
         name: "persist",
         malloy:
            "run: monthly -> { group_by: m, h; aggregate: t is total.sum(); order_by: h }",
      });
      expect(run.servedVia).toBe("served_copy");
      expect(rowDifference(run.live, run.served)).toBeUndefined();
      expect(timeframes(run.servedFields)).toEqual(timeframes(run.liveFields));
      expect(timeframes(run.servedFields).m).toBe("date_type/month");
   });
});
