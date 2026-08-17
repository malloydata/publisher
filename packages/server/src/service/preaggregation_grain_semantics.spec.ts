// What a GRAIN may contain, established by experiment rather than by reading.
//
// This file exists because the obvious way to build a time-grain rollup returns
// silently wrong numbers, and the safe-looking alternative breaks queries as soon
// as the rollup goes stale. Both are pinned below as canaries. The conclusion —
// **a grain may name only dimensions that already exist on the base source** — is
// enforced by preaggregation_validation.ts, and this is the evidence for it.
//
// The distinction is easy to get wrong from reading alone: it is tempting to
// suppose a query writing the raw expression `order_time.month` merely falls back
// to the base while only *named* dimensions route, and therefore that
// `grain="order_time.day, category"` is a harmless spelling. It is not, and the
// tests below are the reason. See docs/preaggregation.md, "Grains name
// dimensions".
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { BuildManifest, FixedConnectionMap } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { computeSourceEntityId } from "./build_plan";
import {
   duckdbTestConnections,
   loadTestModel,
} from "./incremental_test_harness";

// Two rows on the SAME DAY at different hours. A day-truncated store collapses
// them, so an untruncated query served from the rollup is visibly, checkably
// wrong rather than merely suspect.
const ROWS = `SELECT * FROM (VALUES
    (10, TIMESTAMP '2024-01-01 01:00:00', 'A'),
    (20, TIMESTAMP '2024-01-01 05:00:00', 'A'),
    (30, TIMESTAMP '2024-01-02 01:00:00', 'B')
  ) AS t(amount, order_time, category)`;

/**
 * Three ways a rollup could store a day grain:
 *
 *  A — under a synthesized name (`order_time__day`), which the base does not have.
 *  B — under the base's OWN name (`order_time`), holding truncated values.
 *  C — as a named dimension the base itself declares (`order_day`).
 */
const MODEL = `##! experimental { persistence composite_sources }
source: base is duckdb.sql("""${ROWS}""") extend {
  measure: total is amount.sum()
  dimension: order_day is order_time.day
}

#@ persist
source: rollup_a is base -> {
  group_by: order_time__day is order_time.day, category
  aggregate: total__partial is total
} extend { measure: total is total__partial.sum() }
source: orders_a is compose(rollup_a, base)

#@ persist
source: rollup_b is base -> {
  group_by: order_time is order_time.day, category
  aggregate: total__partial is total
} extend { measure: total is total__partial.sum() }
source: orders_b is compose(rollup_b, base)

#@ persist
source: rollup_c is base -> {
  group_by: order_day, category
  aggregate: total__partial is total
} extend { measure: total is total__partial.sum() }
source: orders_c is compose(rollup_c, base)
`;

let connections: FixedConnectionMap;
let duckdb: DuckDBConnection;
let manifest: BuildManifest;
/** rollup name -> the physical table it was built into. */
const tables: Record<string, string> = {};

beforeAll(async () => {
   ({ duckdb, connections } = duckdbTestConnections());
   const digest = await duckdb.getDigest();
   const compiled = await loadTestModel(connections, MODEL).getModel();
   const entries: BuildManifest["entries"] = {};
   for (const source of Object.values(compiled.getBuildPlan().sources)) {
      const table = `mz_${source.name}`;
      await duckdb.runSQL(
         `CREATE OR REPLACE TABLE ${table} AS ${source.getSQL()}`,
      );
      entries[computeSourceEntityId(source, { duckdb: digest })] = {
         tableName: table,
      };
      tables[source.name] = table;
   }
   manifest = { entries, strict: false };
});

/** Did `query` read `rollup`'s physical table? */
async function routedTo(rollup: string, query: string): Promise<boolean> {
   const sql = await loadTestModel(connections, MODEL)
      .loadQuery(query)
      .getSQL({ buildManifest: manifest });
   return sql.includes(tables[rollup]);
}

/** Run with the manifest live, returning plain rows. */
async function rows(
   query: string,
   withManifest = true,
): Promise<Record<string, unknown>[]> {
   const result = await loadTestModel(connections, MODEL)
      .loadQuery(query)
      .run(withManifest ? { buildManifest: manifest } : undefined);
   return result.data.toObject() as unknown as Record<string, unknown>[];
}

describe("a time grain stored under the base's own name is UNSAFE", () => {
   it("CANARY: it routes an untruncated query and returns a WRONG answer", async () => {
      // The whole reason grains are restricted. `rollup_b` stores day-truncated
      // values in a column still called `order_time`, so the composite resolver
      // sees a member that satisfies `group_by: order_time` and binds it — and
      // the rows come back collapsed to midnight with 10 and 20 merged into 30.
      //
      // It compiles. It runs. It looks like a result set. It is the "plausible
      // wrong number" the whole feature is supposed to be incapable of.
      const query =
         "run: orders_b -> { group_by: order_time; aggregate: total }";
      expect(await routedTo("rollup_b", query)).toBe(true);

      const served = await rows(query);
      const live = await rows(
         "run: base -> { group_by: order_time; aggregate: total }",
         false,
      );
      // Three distinct timestamps live; two midnight-truncated ones served.
      expect(live).toHaveLength(3);
      expect(served).toHaveLength(2);
      expect(served).not.toEqual(live);
      // Spelled out so the failure mode is unmissable in the diff: the 10 and the
      // 20 are gone, replaced by a 30 that no row in the source has.
      expect(live.map((r) => r.total).sort()).toEqual([10, 20, 30]);
      expect(served.map((r) => r.total).sort()).toEqual([30, 30]);
   });

   it("it is right for the truncated query, which is what makes it dangerous", async () => {
      // If it were wrong for everything it would be caught immediately. It is
      // correct exactly when the author tests the query they had in mind.
      const query =
         "run: orders_b -> { group_by: order_time.day; aggregate: total }";
      expect(await routedTo("rollup_b", query)).toBe(true);
      expect((await rows(query)).map((r) => r.total).sort()).toEqual([30, 30]);
   });
});

describe("a time grain stored under a synthesized name is safe but useless", () => {
   it("an author's truncation expression does NOT route", async () => {
      // The query references `order_time`,
      // the member has no such field, the member is rejected, and the base serves
      // it. Correct, and completely unaccelerated.
      expect(
         await routedTo(
            "rollup_a",
            "run: orders_a -> { group_by: order_time.day; aggregate: total }",
         ),
      ).toBe(false);
   });

   it("only the synthesized name routes, and nothing else defines it", async () => {
      // So the sole query that DOES route is one no author would write — and
      // because `order_time__day` exists only on the member, that query stops
      // compiling the moment the rollup goes stale and drops out of the compose().
      // A rollup whose queries break when it expires is not a cache.
      expect(
         await routedTo(
            "rollup_a",
            "run: orders_a -> { group_by: order_time__day; aggregate: total }",
         ),
      ).toBe(true);
      await expect(
         rows(
            "run: base -> { group_by: order_time__day; aggregate: total }",
            false,
         ),
      ).rejects.toThrow();
   });
});

describe("a grain naming a dimension the base declares is safe AND useful", () => {
   it("the named dimension routes and agrees with live", async () => {
      const query =
         "run: orders_c -> { group_by: order_day; aggregate: total }";
      expect(await routedTo("rollup_c", query)).toBe(true);
      expect(await rows(query)).toEqual(
         await rows(
            "run: base -> { group_by: order_day; aggregate: total }",
            false,
         ),
      );
   });

   it("a COARSER truncation of the stored dimension also routes, correctly", async () => {
      // A month is functionally determined by a day, so a day-grain rollup can
      // answer it. It works, and it is the payoff for making the author name the dimension:
      // one day-grain rollup serves day, month, quarter and year.
      const query =
         "run: orders_c -> { group_by: order_day.month; aggregate: total }";
      expect(await routedTo("rollup_c", query)).toBe(true);
      const served = await rows(query);
      expect(served).toHaveLength(1);
      expect(served[0].total).toBe(60);
      expect(served).toEqual(
         await rows(
            "run: base -> { group_by: order_day.month; aggregate: total }",
            false,
         ),
      );
   });

   it("and it still falls back cleanly, because the base defines it too", async () => {
      // The property variant A lacks: with no manifest at all the same query
      // compiles and answers from the base.
      const served = await rows(
         "run: orders_c -> { group_by: order_day; aggregate: total }",
         false,
      );
      expect(served).toHaveLength(2);
   });
});
