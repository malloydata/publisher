// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * R-10: a row scope over a column a `storage=` table stores as a DuckDB
 * DOUBLE is read from the table when the column is a plain copy of a
 * warehouse double compared as DuckDB compares it (its recorded upstream
 * type, `ManifestEntry.upstreamSchemas`), and still answered live when the
 * build computed the column or the upstream type is not known. On dev the
 * column was Snowflake's FLOAT; here it is a DuckDB DOUBLE read from a CSV,
 * through the real package path (plan, bind, router).
 *
 * Every served answer is compared with live's as text: each value is cast
 * to a string in the query, so no comparison goes through a double.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   LAKE,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../src/test_helpers/conformance/package_runner";

const CSV = `id,region,score
1,east,0.1
2,east,0.30000000000000004
3,west,-2.5
4,west,1e-300
5,north,0
6,north,7.25
`;

const MODEL = `##! experimental { persistence }
// A plain copy of the warehouse double.
#(materialize) name="scores_mz" storage="${LAKE}"
source: scores_mz is duckdb.table('scores.csv')
source: s_float is scores_mz extend { where: score > 0.1 }

// The build computes the column: its stored DOUBLE copies no upstream column.
#(materialize) name="scores_calc" storage="${LAKE}"
source: scores_calc is duckdb.table('scores.csv') -> { select: id, region, score2 is score * 2 }
source: s_calc is scores_calc extend { where: score2 > 0.2 }

// Its tables cannot be known (author SQL), so no upstream type is recorded.
#(materialize) name="scores_sql" storage="${LAKE}"
source: scores_sql is duckdb.sql("SELECT * FROM 'scores.csv'")
source: s_sql is scores_sql extend { where: score > 0.1 }
`;

/** Each query, its values cast to text in the warehouse. */
const QUERIES = {
   s_float:
      "run: s_float -> { select: id, region, score_text is score::string; order_by: id }",
   s_calc:
      "run: s_calc -> { select: id, region, score_text is score2::string; order_by: id }",
   s_sql: "run: s_sql -> { select: id, region, score_text is score::string; order_by: id }",
} as const;

function rowsOf(o: Outcome): unknown {
   return "error" in o ? `ERR ${o.error}` : o.answer.rows;
}

function servedByOf(o: Outcome): string {
   return "error" in o ? "error" : o.answer.servedBy;
}

describe("R-10: a row scope over a stored DOUBLE that copies a warehouse double", () => {
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>>;
   let fixture: LoadedFixture;
   const live: Record<string, Outcome> = {};
   const served: Record<string, Outcome> = {};

   beforeAll(async () => {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
         },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({ "m.malloy": MODEL, "scores.csv": CSV });
      // Before any bind: every answer is live's.
      for (const [name, query] of Object.entries(QUERIES)) {
         live[name] = await loader.run("m.malloy", query);
      }
      fixture = await loader.bind();
      for (const [name, query] of Object.entries(QUERIES)) {
         served[name] = await fixture.run("m.malloy", query);
      }
   }, 600_000);

   afterAll(async () => {
      await fixture?.dispose();
      await loader?.dispose().catch(() => {});
      restore?.();
      await freshLoadPool();
   });

   it("built every table, each holding its scope column as a DOUBLE", async () => {
      expect(fixture.buildErrors).toEqual({});
      for (const entry of Object.values(fixture.stored)) {
         const schema = entry.schema as { name: string; type: string }[];
         expect(
            schema.find((c) => c.name === "score" || c.name === "score2")?.type,
         ).toBe("DOUBLE");
      }
   });

   it("recorded the upstream type of the copied column, and none for author SQL", () => {
      const byName = Object.fromEntries(
         Object.values(fixture.stored).map((e) => [e.sourceName, e]),
      );
      expect(byName.scores_mz.upstreamSchemas).toEqual([
         expect.objectContaining({
            connectionName: "duckdb",
            dialect: "duckdb",
            columns: expect.arrayContaining([
               { name: "score", nativeType: "DOUBLE" },
            ]),
         }),
      ]);
      expect(byName.scores_sql.upstreamSchemas).toBeUndefined();
   });

   it("records the copied double gate-exact in its contract, and neither the computed one nor author SQL's", () => {
      const gateExactOf = (sourceName: string, column: string) =>
         (
            Object.values(fixture.stored).find(
               (e) => e.sourceName === sourceName,
            )?.contract as
               | { columns: { name: string; gateExact: boolean }[] }
               | undefined
         )?.columns.find((c) => c.name === column)?.gateExact;
      expect(gateExactOf("scores_mz", "score")).toBe(true);
      expect(gateExactOf("scores_calc", "score2")).toBe(false);
      expect(gateExactOf("scores_sql", "score")).toBe(false);
      // A column of an exact type is gate-exact wherever it came from.
      expect(gateExactOf("scores_sql", "id")).toBe(true);
   });

   it("answers each query live before the bind", () => {
      for (const name of Object.keys(QUERIES)) {
         expect({ name, by: servedByOf(live[name]) }).toEqual({
            name,
            by: "live",
         });
      }
      expect(rowsOf(live.s_float)).toEqual([
         { id: 2n, region: "east", score_text: "0.30000000000000004" },
         { id: 6n, region: "north", score_text: "7.25" },
      ]);
   });

   it("serves the scope over the copied double from the table, equal to live as text", () => {
      expect(servedByOf(served.s_float)).toBe("materialized");
      expect(rowsOf(served.s_float)).toEqual(rowsOf(live.s_float));
   });

   it("answers live a scope over a DOUBLE the build computed, equal to live", () => {
      expect(servedByOf(served.s_calc)).toBe("live");
      expect(rowsOf(served.s_calc)).toEqual(rowsOf(live.s_calc));
   });

   it("answers live a scope over a DOUBLE whose upstream type is not known, equal to live", () => {
      expect(servedByOf(served.s_sql)).toBe("live");
      expect(rowsOf(served.s_sql)).toEqual(rowsOf(live.s_sql));
   });
});
