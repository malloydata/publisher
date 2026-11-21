// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A later stage's filter on a number an earlier stage of the same query
 * computes by a window function (`rn is row_number()`, then `where: rn = 1`)
 * is read through what orders and partitions the window: it compares
 * `category` as an ordering does. Over a source that compares bytes, as the
 * copy does, the dimensional indexer's shard-boundary query is then served
 * from a `storage=` table and from a database table, equal to live.
 *
 * Before the fix the window function's output was read as a column with no
 * path to a stored one, the filter check read the query's filters as unread,
 * and every such query answered live: "the query filters on a column a
 * stored table does not compare as its source does: gate reads unread".
 * (The database table here was served before too: DuckDB judges it by its
 * definition. The Postgres half is database_query_filter_collation_alter.)
 *
 * Still judged: a later stage's string comparison on a value an earlier
 * stage computes from a stored string, which over a source whose column
 * compares case-insensitively answers live; and a gate on the source, which
 * still scopes the served rows per caller.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   type Answer,
   freshLoadPool,
   loadFixture,
   type LoadedFixture,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";

const MODEL = `##! experimental { persistence givens }
given: ROLES :: string[]

source: raw is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'books'), (2, 'Toys'), (3, 'apple'), (4, 'toys'),
                        (5, 'books'), (6, 'zebra'))
    AS t(id, category)
""") extend {
  primary_key: id
}

source: raw_nocase is duckdb.sql("""
  SELECT id, category COLLATE NOCASE AS category FROM (VALUES (1, 'books'),
    (2, 'Toys'), (3, 'apple'), (4, 'toys')) AS t(id, category)
""") extend {
  primary_key: id
}

#@ persist name="lsc_stored" storage="lake"
source: stored_s is raw -> { select: * }

#@ persist name="lsc_db"
source: db_s is raw -> { select: * }

#(access_filter) category in $ROLES
#@ persist name="lsc_gated" storage="lake"
source: gated_s is raw -> { select: * }

#@ persist name="lsc_nocase" storage="lake"
source: nocase_s is raw_nocase -> { select: * }
`;

/** The indexer's shard-boundary query, verbatim but for its source. */
const indexer = (source: string) =>
   `run: ${source} -> { group_by: value is category order_by: value calculate: rn is row_number() } -> { where: rn = 1 or rn = 2 select: rn, value order_by: rn }`;

interface Case {
   name: string;
   query: string;
   givens?: Record<string, string[]>;
   expected: "served" | "live";
   /** With `live`: what the served copy's decline says. */
   declined?: RegExp;
}

const CASES: Case[] = [
   {
      name: "the indexer query over a storage= table",
      query: indexer("stored_s"),
      expected: "served",
   },
   {
      name: "the indexer query over a database table",
      query: indexer("db_s"),
      expected: "served",
   },
   {
      name: "a later-stage rank over a partition, over a storage= table",
      query: "run: stored_s -> { group_by: value is category, id calculate: r is rank() { partition_by: value order_by: id } } -> { where: r = 1 select: value, id order_by: value }",
      expected: "served",
   },
   {
      name: "the indexer query through a gate",
      query: indexer("gated_s"),
      givens: { ROLES: ["toys", "zebra"] },
      expected: "served",
   },
   {
      name: "a later-stage string comparison on a value computed from a case-insensitive column",
      query: "run: nocase_s -> { group_by: k is category } -> { where: k = 'TOYS' select: k }",
      expected: "live",
      declined:
         /the query filters on a column a stored table does not compare as its source does/,
   },
];

/** Every value as a string, so a served row equals a live one only as text. */
const asText = (a: Answer) =>
   a.rows.map((r) =>
      Object.fromEntries(Object.entries(r).map(([k, v]) => [k, String(v)])),
   );

describe("a later stage's filter on a window function an earlier stage computes", () => {
   let restore: (() => void) | undefined;
   let fixture: LoadedFixture | undefined;
   const live = new Map<string, Answer>();
   const served = new Map<string, Answer>();

   beforeAll(async () => {
      restore = withSwitch("on");
      await freshLoadPool();
      const loaded = await loadFixture({ "m.malloy": MODEL });
      const answer = async (c: Case, run: LoadedFixture["run"]) => {
         const outcome = await run("m.malloy", c.query, c.givens);
         if ("error" in outcome) throw new Error(`${c.name}: ${outcome.error}`);
         return outcome.answer;
      };
      for (const c of CASES) live.set(c.name, await answer(c, loaded.run));
      fixture = await loaded.bind();
      expect(fixture.buildErrors).toEqual({});
      for (const c of CASES) served.set(c.name, await answer(c, fixture.run));
   }, 120_000);

   afterAll(async () => {
      await fixture?.dispose();
      restore?.();
      await freshLoadPool();
   });

   for (const c of CASES) {
      it(`${c.name}: ${c.expected === "served" ? "served" : "answers live"}, equal to live`, () => {
         const before = live.get(c.name)!;
         const after = served.get(c.name)!;
         expect(before.servedBy).toBe("live");
         expect(before.rows.length).toBeGreaterThan(0);
         expect(asText(after)).toEqual(asText(before));
         if (c.expected === "served") {
            expect(after.servedSkip).toBeUndefined();
            expect(after.servedBy).toBe("materialized");
         } else {
            expect(after.servedBy).toBe("live");
            expect(after.servedSkip?.detail ?? "").toMatch(c.declined!);
         }
      });
   }

   it("the gate still scopes the served rows to the caller", () => {
      const rows = asText(served.get("the indexer query through a gate")!);
      expect(rows).toEqual([
         { rn: "1", value: "toys" },
         { rn: "2", value: "zebra" },
      ]);
   });

   it("the case-insensitive column's stored copy would have answered otherwise", () => {
      // Live groups 'Toys' and 'toys' as one and matches it to 'TOYS' under
      // NOCASE: one row, where the stored bytes would match none.
      const rows = live.get(
         "a later-stage string comparison on a value computed from a case-insensitive column",
      )!.rows;
      expect(rows.map((r) => String(r.k).toLowerCase())).toEqual(["toys"]);
   });
});
