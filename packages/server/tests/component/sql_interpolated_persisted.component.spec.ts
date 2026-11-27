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

type Query = { model: string; query: string };

const QUERIES: Record<string, Query> = {
   outreach: {
      model: "index.malloy",
      query: "run: outreach_daily -> { aggregate: n is count(), total is sent.sum() }",
   },
   carriers: {
      model: "index.malloy",
      query: "run: carriers_copy -> { group_by: region; aggregate: carrier_count; order_by: region }",
   },
};

/**
 * A package loaded, built and bound, with each query answered before the
 * bind and after it, and each served answer's path report.
 */
function scenario(
   files: Record<string, string>,
   queries: Record<string, Query> = QUERIES,
) {
   const state: {
      fixture?: LoadedFixture;
      live: Record<string, Outcome>;
      served: Record<string, Outcome>;
      paths: Record<string, QueryPath | undefined>;
   } = { live: {}, served: {}, paths: {} };
   let restore: () => void;
   let loader: Awaited<ReturnType<typeof loadFixture>> | undefined;
   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      loader = await loadFixture({ ...files, ...DATA });
      for (const [name, q] of Object.entries(queries)) {
         state.live[name] = await loader.run(q.model, q.query);
      }
      state.fixture = await loader.bind({});
      for (const [name, q] of Object.entries(queries)) {
         state.served[name] = await state.fixture.run(q.model, q.query);
         const result = await state.fixture.pkg
            .getModel(q.model)!
            .runQuery(
               unrecordedAudit(),
               { query: q.query } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         state.paths[name] = result.path as QueryPath | undefined;
      }
   }, 600_000);
   afterAll(async () => {
      await state.fixture?.dispose();
      await loader?.dispose().catch(() => {});
      restore?.();
      await freshLoadPool();
   });
   return state;
}

/** The package's warnings of a code. */
function warningsOf(fixture: LoadedFixture | undefined, code: string) {
   return (fixture?.warnings ?? []).filter((w) => w.code === code);
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
   const s = scenario({
      "outreach.malloy": outreach(),
      "index.malloy": INDEX,
   });

   it("built every table", () => {
      expect(s.fixture?.buildErrors).toEqual({});
   });

   it("serves the union from both tables, equal to live", () => {
      expect(answer(s.live.outreach).rows).toBe('[{"n":4,"total":"15"}]');
      expect(answer(s.served.outreach)).toEqual({
         servedBy: "materialized",
         rows: answer(s.live.outreach).rows,
      });
      expect(s.paths.outreach?.servedBy).toMatchObject({
         kind: "materialized",
         tier: "database",
         via: "served_copy",
      });
      // Both tables, each named used.
      const used = (s.paths.outreach?.considered ?? []).filter(
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
   const s = scenario({
      "outreach.malloy": outreach(LAKE),
      "index.malloy": INDEX,
   });

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
         expect(v.message).toContain(
            "a `.sql()` source's `%{}` reads it with SQL",
         );
         expect(v.fixes?.join(" ")).toContain(
            "persist the `.sql()` source itself with the same `storage=`",
         );
         expect(v.fixes?.join(" ")).toContain("drop `storage=`");
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

/** `outreach(storage)` with the union's SQL replaced. */
function withUnion(storage: string | undefined, union: string): string {
   return outreach(storage).replace(
      /source: outreach_daily is duckdb\.sql\(""".*"""\)/,
      () => `source: outreach_daily is duckdb.sql("""${union}""")`,
   );
}

const BARE_UNION =
   "select * from %{ email_daily_rows } union all by name select * from %{ linkedin_daily_rows -> { select: * } }";

describe("a file another file imports selectively, its %{} union in a storage destination", () => {
   // `b.malloy` names only `email_daily_rows` and `carriers_copy`, but its
   // copy compiles `outreach.malloy` whole, `outreach_daily` included.
   const B = `import { email_daily_rows, carriers_copy } from "outreach.malloy"
`;
   const queries: Record<string, Query> = {
      carriers: {
         model: "b.malloy",
         query: "run: carriers_copy -> { group_by: region; aggregate: carrier_count; order_by: region }",
      },
      email: {
         model: "b.malloy",
         query: "run: email_daily_rows -> { aggregate: total is sent.sum() }",
      },
   };
   const s = scenario(
      { "outreach.malloy": outreach(LAKE), "b.malloy": B },
      queries,
   );

   it("serves the importer's other stored source, its copy compiling", () => {
      expect(answer(s.served.carriers)).toEqual({
         servedBy: "materialized",
         rows: answer(s.live.carriers).rows,
      });
      expect(
         warningsOf(s.fixture, "served-copy-not-served").filter(
            (w) => !w.subject,
         ),
      ).toEqual([]);
   });

   it("leaves the %{}-read source live in the importer too, for the same reason", () => {
      expect(answer(s.served.email)).toEqual({
         servedBy: "live",
         rows: answer(s.live.email).rows,
      });
      expect(verdict(s.fixture, "email_daily_rows").notServedIn).toEqual([
         { model: "b.malloy", reasons: ["sql_embed_cross_connection"] },
         { model: "outreach.malloy", reasons: ["sql_embed_cross_connection"] },
      ]);
   });
});

for (const [tier, storage] of [
   ["their own database", undefined],
   ["a storage destination", LAKE],
] as const) {
   describe(`a bare %{ x } of a stored source, in ${tier}`, () => {
      const s = scenario({
         "outreach.malloy": withUnion(storage, BARE_UNION),
         "index.malloy": INDEX,
      });

      it("built every table", () => {
         expect(s.fixture?.buildErrors).toEqual({});
      });

      it("answers the union equal to live", () => {
         expect(answer(s.served.outreach).rows).toBe(
            answer(s.live.outreach).rows,
         );
      });

      it("leaves only the source embedded bare as written, with its reason and fix", () => {
         const v = verdict(s.fixture, "email_daily_rows");
         expect(v).toMatchObject({ verdict: "live", reason: "served_copy" });
         expect(
            new Set(v.notServedIn?.flatMap((n) => n.reasons) ?? []),
         ).toEqual(new Set(["sql_embed_whole_source"]));
         expect(v.message).toContain("whole (`%{ <source> }`)");
         expect(v.fixes?.join(" ")).toContain(
            "`%{ <source> -> { select: * } }`",
         );
         const warned = warningsOf(s.fixture, "served-copy-not-served").find(
            (w) => w.subject === "email_daily_rows",
         );
         expect(warned?.message).toContain("embedded whole");
      });

      it("still serves the file's other stored sources", () => {
         expect(verdict(s.fixture, "carriers_copy").verdict).toBe("serves");
         expect(answer(s.served.carriers)).toEqual({
            servedBy: "materialized",
            rows: answer(s.live.carriers).rows,
         });
         // The query embed: served in its own database; elsewhere, the
         // cross-connection reason.
         const li = verdict(s.fixture, "linkedin_daily_rows");
         if (storage) {
            expect(li.notServedIn?.[0]?.reasons).toEqual([
               "sql_embed_cross_connection",
            ]);
         } else {
            expect(li.verdict).toBe("serves");
         }
      });
   });
}

describe("the fix: the .sql() source persisted with the same storage= as what it reads", () => {
   const persisted = outreach(LAKE).replace(
      "source: outreach_daily is",
      `#@ persist name="outreach_rows" storage="${LAKE}"\nsource: outreach_daily is`,
   );
   // Its rows by day. (Its `sent`, a sum the destination stores as DOUBLE,
   // is a column-type matter of its own, which a `.sql()` source declares no
   // subtype for: not read here.)
   const s = scenario(
      { "outreach.malloy": persisted, "index.malloy": INDEX },
      {
         ...QUERIES,
         outreach: {
            model: "index.malloy",
            query: "run: outreach_daily -> { group_by: d; aggregate: n is count(); order_by: d }",
         },
      },
   );

   it("built every table", () => {
      expect(s.fixture?.buildErrors).toEqual({});
   });

   it("serves the union from its own table, equal to live, and every source it reads", () => {
      expect(answer(s.served.outreach)).toEqual({
         servedBy: "materialized",
         rows: answer(s.live.outreach).rows,
      });
      for (const name of [
         "outreach_daily",
         "email_daily_rows",
         "linkedin_daily_rows",
         "carriers_copy",
      ]) {
         expect([name, verdict(s.fixture, name).verdict]).toEqual([
            name,
            "serves",
         ]);
      }
      expect(warningsOf(s.fixture, "served-copy-not-served")).toEqual([]);
   });
});

describe("a .sql() union with a join to another stored source, in their own database", () => {
   const joined = `${outreach()}
source: outreach_joined is outreach_daily extend {
  join_one: c is carriers_copy on c.code = 'AA'
}
`;
   const queries: Record<string, Query> = {
      joined: {
         model: "outreach.malloy",
         query: "run: outreach_joined -> { aggregate: n is count(), regions is c.carrier_count }",
      },
      carriers: {
         model: "outreach.malloy",
         query: "run: carriers_copy -> { aggregate: carrier_count }",
      },
   };
   const s = scenario({ "outreach.malloy": joined }, queries);

   it("names only the tables its %{} reads used, never the join's", () => {
      expect(answer(s.served.joined)).toEqual({
         servedBy: "materialized",
         rows: answer(s.live.joined).rows,
      });
      const carriersTable = s.paths.carriers?.servedBy.tableId;
      expect(carriersTable).toBeDefined();
      const used = (s.paths.joined?.considered ?? []).filter(
         (c) => c.outcome === "used",
      );
      expect(used).toHaveLength(2);
      expect(used.map((c) => c.tableId)).not.toContain(carriersTable);
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
