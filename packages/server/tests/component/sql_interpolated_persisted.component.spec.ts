// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `.sql()` source that reads stored sources through `%{ x -> { … } }`, the
 * field shape: two persisted rollups unioned in SQL, published through
 * `index.malloy`, beside an unrelated persisted source in the same file.
 *
 * - In the sources' own database (a plain DuckDB connection) the union is
 *   served from both tables: the served copy compiles with its `virtualMap`,
 *   which the `%{}`'s schema fetch reads, the reach walk follows the `%{}` so
 *   neither table reads `unreachable`, and the path report names both.
 * - In a storage destination the `%{}`'s SQL runs on its own connection, which
 *   cannot reach the destination: the two sources are left as written, live,
 *   with `sql_embed_cross_connection` and a fix, and the file's other stored
 *   source still serves. (A managed catalog read through its destination is
 *   the same rule: `storage_same_catalog_join.component.spec.ts`.)
 * - A `%{}` of a gated source refuses the load, before and after a bind
 *   alike (`sql_embed_gates.ts`): the gate is kept exactly as live keeps it.
 *
 * Through a real package (plan, build, bind, the served copy's compile, the
 * router), on DuckDB with a DuckDB `lake` destination.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { unrecordedAudit } from "../../src/service/access_record";
import { QueryPrivileges } from "../../src/service/model_query_request";
import { installSettings } from "../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   LAKE,
   type LoadedFixture,
   loadFixture,
   type Outcome,
} from "../../src/test_helpers/conformance/package_runner";
import type { components } from "../../src/api";

type ServeVerdict = components["schemas"]["ServeVerdict"];
type QueryPath = components["schemas"]["QueryPath"];

const DATA = {
   "email.csv": "d,sent\n2024-01-01,3\n2024-01-01,4\n2024-01-02,5\n",
   "linkedin.csv": "d,sent\n2024-01-01,1\n2024-01-03,2\n",
   "carriers.csv": "code,region\nAA,east\nUA,west\nDL,east\n",
};

function outreach(storage?: string): string {
   const at = storage ? ` storage="${storage}"` : "";
   return `##! experimental { persistence }
#@ persist name="email_daily"${at}
source: email_daily_rows is duckdb.table('email.csv') -> {
  group_by: d
  aggregate: sent is sent.sum()
}

#@ persist name="li_daily"${at}
source: linkedin_daily_rows is duckdb.table('linkedin.csv') -> {
  group_by: d
  aggregate: sent is sent.sum()
}

#@ persist name="carriers_copy"${at}
source: carriers_copy is duckdb.table('carriers.csv') -> { select: * } extend {
  measure: carrier_count is count()
}

source: outreach_daily is duckdb.sql("""select * from %{ email_daily_rows -> { select: * } } union all by name select * from %{ linkedin_daily_rows -> { select: * } }""")
`;
}

const INDEX = `import "outreach.malloy"
export { outreach_daily, carriers_copy }
`;

const QUERIES = {
   outreach:
      "run: outreach_daily -> { aggregate: n is count(), total is sent.sum() }",
   carriers:
      "run: carriers_copy -> { group_by: region; aggregate: carrier_count; order_by: region }",
};

/** A package loaded, built and bound, with what was answered before the bind. */
function scenario(storage?: string) {
   const state: {
      fixture?: LoadedFixture;
      live: Record<string, Outcome>;
      served: Record<string, Outcome>;
      path?: QueryPath;
   } = { live: {}, served: {} };
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({
         "outreach.malloy": outreach(storage),
         "index.malloy": INDEX,
         ...DATA,
      });
      for (const [name, q] of Object.entries(QUERIES)) {
         state.live[name] = await loader.run("index.malloy", q);
      }
      state.fixture = await loader.bind({});
      for (const [name, q] of Object.entries(QUERIES)) {
         state.served[name] = await state.fixture.run("index.malloy", q);
      }
      const result = await state.fixture.pkg
         .getModel("index.malloy")!
         .runQuery(
            unrecordedAudit(),
            { query: QUERIES.outreach } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
         );
      state.path = result.path as QueryPath | undefined;
   }, 600_000);
   afterAll(async () => {
      await state.fixture?.dispose();
      await loader?.dispose().catch(() => {});
      restore?.();
      await freshLoadPool();
   });
   return state;
}

function verdict(fixture: LoadedFixture | undefined, name: string) {
   const found = (fixture?.pkg.materialization().serveVerdicts() ?? []).filter(
      (v: ServeVerdict) => v.sourceName === name,
   );
   expect(found).toHaveLength(1);
   return found[0];
}

/** Where an answer came from, and its rows as text. */
function answer(o: Outcome | undefined): { servedBy: string; rows: string } {
   if (!o) return { servedBy: "missing", rows: "" };
   if ("error" in o) return { servedBy: "error", rows: o.error };
   return {
      servedBy: o.answer.servedBy,
      rows: JSON.stringify(o.answer.rows, (_k, v: unknown) =>
         typeof v === "bigint" ? v.toString() : v,
      ),
   };
}

describe("a .sql() source's %{} of stored sources in their own database", () => {
   const s = scenario();

   it("built every table", () => {
      expect(s.fixture?.buildErrors).toEqual({});
   });

   it("serves the union from both tables, equal to live", () => {
      expect(answer(s.live.outreach).rows).toBe('[{"n":4,"total":"15"}]');
      expect(answer(s.served.outreach)).toEqual({
         servedBy: "materialized",
         rows: answer(s.live.outreach).rows,
      });
      expect(s.path?.servedBy).toMatchObject({
         kind: "materialized",
         tier: "database",
         via: "served_copy",
      });
      // Both tables, each named used.
      const used = (s.path?.considered ?? []).filter(
         (c) => c.outcome === "used",
      );
      expect(new Set(used.map((c) => c.tableId)).size).toBe(2);
      expect(used).toHaveLength(2);
   });

   it("reads both tables `serves`, never `unreachable`", () => {
      for (const name of ["email_daily_rows", "linkedin_daily_rows"]) {
         expect(verdict(s.fixture, name)).toMatchObject({
            verdict: "serves",
            tier: "database",
         });
      }
      expect(
         (s.fixture?.warnings ?? []).filter(
            (w) =>
               w.code === "materialization-unreachable" ||
               w.code === "served-copy-not-served",
         ),
      ).toEqual([]);
   });

   it("still serves the file's unrelated stored source", () => {
      expect(verdict(s.fixture, "carriers_copy").verdict).toBe("serves");
      expect(answer(s.served.carriers)).toEqual({
         servedBy: "materialized",
         rows: answer(s.live.carriers).rows,
      });
   });
});

describe("a .sql() source's %{} of stored sources in a storage destination", () => {
   const s = scenario(LAKE);

   it("built every table", () => {
      expect(s.fixture?.buildErrors).toEqual({});
   });

   it("answers the union live, equal to live", () => {
      expect(answer(s.served.outreach)).toEqual({
         servedBy: "live",
         rows: answer(s.live.outreach).rows,
      });
   });

   it("says why each source the %{} reads is live, with the fix", () => {
      for (const name of ["email_daily_rows", "linkedin_daily_rows"]) {
         const v = verdict(s.fixture, name);
         expect(v).toMatchObject({
            verdict: "live",
            reason: "served_copy",
            tier: "storage",
            notServedIn: [
               {
                  model: "index.malloy",
                  reasons: ["sql_embed_cross_connection"],
               },
               {
                  model: "outreach.malloy",
                  reasons: ["sql_embed_cross_connection"],
               },
            ],
         });
         expect(v.message).toContain("read through a `.sql()` source's `%{}`");
         expect(v.fixes?.join(" ")).toContain("persist the `.sql()` source");
         expect(v.fixes?.join(" ")).toContain("`.table()`");
      }
      const warned = (s.fixture?.warnings ?? []).filter(
         (w) => w.code === "served-copy-not-served",
      );
      expect([...new Set(warned.map((w) => w.subject))].sort()).toEqual([
         "email_daily_rows",
         "linkedin_daily_rows",
      ]);
      for (const w of warned) {
         expect(w.message).toContain("`outreach_daily` on `duckdb`");
      }
   });

   it("still serves the file's unrelated stored source", () => {
      expect(verdict(s.fixture, "carriers_copy").verdict).toBe("serves");
      expect(answer(s.served.carriers)).toEqual({
         servedBy: "materialized",
         rows: answer(s.live.carriers).rows,
      });
   });
});

describe("a .sql() source's %{} of a gated stored source", () => {
   it("refuses the load, as live does", async () => {
      const restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      try {
         await freshLoadPool();
         const gated = outreach().replace(
            '#@ persist name="email_daily"',
            '#(authorize) false\n#@ persist name="email_daily"',
         );
         await expect(
            loadFixture({
               "outreach.malloy": gated,
               "index.malloy": INDEX,
               ...DATA,
            }),
         ).rejects.toThrow("A sql() embeds a gated source");
      } finally {
         restore();
         await freshLoadPool();
      }
   }, 600_000);
});
