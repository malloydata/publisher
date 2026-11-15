// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A v1 re-root leaves a stored column out of its `type:` when no type reads
 * it back (an undeclarable column such as an INTERVAL or a BLOB, a nest a
 * destination did not store, a big integer no native cast reads exactly), so
 * a query naming it fails to compile on the served copy and answers live.
 * A wildcard names it without naming it: `select: *` expands over the served
 * copy's fields, which lack it, so the served answer would silently drop a
 * column live returns. Such a query is answered from the table only when its
 * result has the author's shape; the rest of the source keeps its table.
 */
import { afterAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { DATA_FILES } from "./conformance/data";
import { freshLoadPool, loadFixture } from "./conformance/package_harness";
import { rowDifference } from "./harness";

const MODEL = `##! experimental.persistence
#@ persist name="odd_columns" storage="lake"
source: odd is duckdb.sql("""
  SELECT 1 AS id, INTERVAL 3 DAY AS iv, 'x'::BLOB AS bl, 'a' AS tag
  UNION ALL SELECT 2, INTERVAL 1 DAY, 'y'::BLOB, 'b'
""") extend {
  dimension: id2 is id * 2
  view: everything is { select: * }
}
`;

const WILDCARD = [
   "run: odd -> { select: * }",
   "run: odd -> everything",
   "run: odd -> { group_by: tag; nest: rows is { select: * } }",
];
const NAMED = ["run: odd -> { group_by: id, id2, tag }"];

describe("a wildcard over a v1 table with a column left out", () => {
   afterAll(async () => {
      await freshLoadPool();
   });

   it("returns every column live returns, and the rest still reads the table", async () => {
      const restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on" },
      }).restore;
      await freshLoadPool();
      const loader = await loadFixture({ ...DATA_FILES, "m.malloy": MODEL });
      try {
         const live = new Map<string, Awaited<ReturnType<typeof loader.run>>>();
         for (const q of [...WILDCARD, ...NAMED]) {
            live.set(q, await loader.run("m.malloy", q));
         }
         const fixture = await loader.bind();
         try {
            for (const q of [...WILDCARD, ...NAMED]) {
               const l = live.get(q)!;
               const served = await fixture.run("m.malloy", q);
               if ("error" in l) throw new Error(l.error);
               if ("error" in served) throw new Error(served.error);
               expect({
                  q,
                  diff: rowDifference(l.answer.rows, served.answer.rows),
               }).toEqual({ q, diff: undefined });
               if (NAMED.includes(q)) {
                  expect(served.answer.via).toBe("served_copy");
               }
            }
         } finally {
            await fixture.dispose();
         }
      } finally {
         restore();
      }
   }, 180_000);
});
