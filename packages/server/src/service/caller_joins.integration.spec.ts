// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A join written in CALLER text is checked as if it were an extra run target:
// the joined source's `#(authorize)` lock is decided, its `#(access_filter)` is
// grafted into the caller join's ON, and a source off the query boundary is
// unreachable. Author (model-declared) joins keep the entry-point-only rule.

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Connection, GivenValue } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { AccessDeniedError, NotQueryableError } from "../errors";
import { Model } from "./model";

const TEST_DIR = path.join(os.tmpdir(), "caller-joins-integration");
const PKG_DIR = path.join(TEST_DIR, "pkg");
let duck: DuckDBConnection;

const MODEL = `##! experimental { givens composite_sources }
given:
  GROUPS :: string[]

#(authorize) 'g_allowed' in $GROUPS
source: gated is duckdb.table('t') extend { dimension: secret is name }

#(access_filter) region in $GROUPS
source: rowgated is duckdb.table('t') extend {
  primary_key: id
  measure: rc is count()
}

source: helper is duckdb.table('t')

#(authorize) 'g_allowed' in $GROUPS
source: hidden_gated is duckdb.table('t') extend { dimension: hsecret is name }

source: plain is duckdb.table('t') extend { measure: c is count() }

source: open_src is duckdb.table('t') extend {
  measure: c is count()
  join_one: author_rg is rowgated on id = author_rg.id
  join_one: author_g is gated on id = author_g.id
}

source: gated_ext is gated extend { dimension: s2 is secret }

#(authorize) 'g_allowed' in $GROUPS
source: gated_sql is duckdb.sql("select 1 as id, 'x' as name") extend {}

query: gated_q is gated -> { group_by: id, secret }
query: hidden_q is helper -> { group_by: id, name }
query: hq_exp is helper -> { group_by: id }
query: hgq is hidden_gated -> { group_by: id, hsecret }

query: rg_q is rowgated -> { group_by: id, region }
query: rg_agg is rowgated -> { group_by: id; aggregate: rc }
source: nq_author is duckdb.table('t') extend {
  join_one: q is rg_q on id = q.id
}

source: comp is compose(gated, plain)
source: comp_rg is compose(rowgated, plain)
source: comp_plain is compose(plain, open_src)

export {
  gated, rowgated, plain, open_src, gated_ext, gated_sql, gated_q,
  rg_q, hq_exp, nq_author, comp, comp_rg, comp_plain
}
`;

const ADMIT: Record<string, GivenValue> = { GROUPS: ["g_allowed", "US"] };
const DENY: Record<string, GivenValue> = { GROUPS: ["g_other"] };

let plainModel: Model;
let boundaryModel: Model;

async function loadModel(boundary: boolean): Promise<Model> {
   const model = await Model.create(
      "test-pkg",
      PKG_DIR,
      "m.malloy",
      new Map<string, Connection>([["duckdb", duck as Connection]]),
   );
   if (boundary) {
      // What `Package` does for a package whose manifest lists `explores`.
      model.setDiscoveryCuration(true);
      model.setQueryBoundary({
         mode: "declared",
         exploresDeclared: true,
         isQueryEntryPoint: true,
      });
   }
   return model;
}

async function run(
   model: Model,
   query: string,
   givens: Record<string, GivenValue>,
   bypassAuthorize = false,
): Promise<{ rows: Record<string, unknown>[]; sql: string }> {
   const { compactResult, result } = await model.getQueryResults(
      undefined,
      undefined,
      query,
      undefined,
      undefined,
      givens,
      undefined,
      undefined,
      "full",
      bypassAuthorize,
   );
   return {
      rows: compactResult as unknown as Record<string, unknown>[],
      sql: result.sql ?? "",
   };
}

function byId(rows: Record<string, unknown>[]): Record<string, unknown>[] {
   return [...rows].sort((a, b) => Number(a.id) - Number(b.id));
}

async function expectDenied(
   model: Model,
   query: string,
   givens: Record<string, GivenValue>,
   alias: string,
): Promise<void> {
   await expect(run(model, query, givens)).rejects.toThrow(
      new AccessDeniedError(`Access denied for source "${alias}".`),
   );
}

async function expectNotQueryable(
   model: Model,
   query: string,
   givens: Record<string, GivenValue>,
   bypassAuthorize = false,
): Promise<void> {
   let caught: unknown;
   try {
      await run(model, query, givens, bypassAuthorize);
   } catch (err) {
      caught = err;
   }
   expect(caught).toBeInstanceOf(NotQueryableError);
   expect((caught as Error).message).toBe("Query target is not queryable.");
}

beforeAll(async () => {
   await fs.mkdir(PKG_DIR, { recursive: true });
   duck = new DuckDBConnection(
      "duckdb",
      path.join(TEST_DIR, "t.duckdb"),
      TEST_DIR,
   );
   await duck.runSQL(
      "CREATE OR REPLACE TABLE t AS SELECT * FROM (VALUES (1,'a','US'),(2,'b','EU')) v(id,name,region)",
   );
   await fs.writeFile(path.join(PKG_DIR, "m.malloy"), MODEL);
   plainModel = await loadModel(false);
   boundaryModel = await loadModel(true);
});

afterAll(async () => {
   await duck.close();
   await fs.rm(TEST_DIR, { recursive: true, force: true }).catch(() => {});
});

// Every spelling of a caller join into the `gated` lock, keyed by the alias the
// 403 must name. Each projects the locked `secret` column.
const LOCK_FORMS: [string, string, string][] = [
   [
      "extend on the run target",
      "run: plain extend { join_cross: g is gated } -> { group_by: g.secret }",
      "g",
   ],
   [
      "query-local join_one",
      "run: plain -> { join_one: g is gated on id = g.id; group_by: g.secret }",
      "g",
   ],
   [
      "later-stage extend:",
      "run: plain -> { group_by: id } -> { extend: { join_one: g is gated on id = g.id } group_by: g.secret }",
      "g",
   ],
   [
      "a join inside a nest:",
      "run: plain -> { group_by: id; nest: n is { extend: { join_one: g is gated on id = g.id } group_by: g.secret } }",
      "g",
   ],
   [
      "an annotated join (the join struct loses the gate note)",
      "run: plain extend {\n # some_tag\n join_one: g is gated on id = g.id } -> { group_by: g.secret }",
      "g",
   ],
   [
      "shorthand join",
      "run: plain extend { join_one: gated on id = gated.id } -> { group_by: gated.secret }",
      "gated",
   ],
   [
      "join_many",
      "run: plain extend { join_many: g is gated on id = g.id } -> { group_by: g.secret }",
      "g",
   ],
   [
      "a query-expression join",
      "run: plain extend { join_one: q is gated -> { group_by: id, secret } on id = q.id } -> { group_by: q.secret }",
      "q",
   ],
   [
      "a named-query join",
      "run: plain extend { join_one: q is gated_q on id = q.id } -> { group_by: q.secret }",
      "q",
   ],
   [
      "a caller source: alias chain, joined",
      "source: mine is gated extend {}\nsource: mine2 is mine\nrun: plain extend { join_one: m is mine2 on id = m.id } -> { group_by: m.secret }",
      "m",
   ],
   [
      "a caller source: holding the join, then run",
      "source: mine is plain extend { join_one: g is gated on id = g.id }\nrun: mine -> { group_by: g.secret }",
      "g",
   ],
   [
      "a caller query: holding the join, then run",
      "query: qq is plain -> { extend: { join_one: g is gated on id = g.id } group_by: g.secret }\nrun: qq",
      "g",
   ],
   [
      "a caller query-source run target holding the join",
      "source: qs is plain extend { join_one: g is gated on id = g.id } -> { group_by: id, g.secret }\nrun: qs -> { group_by: secret }",
      "g",
   ],
   [
      "a join inside a caller view",
      "run: plain extend { view: v is { join_one: g is gated on id = g.id; group_by: g.secret } } -> v",
      "g",
   ],
   [
      "an inline extend over the locked source",
      "run: plain extend { join_one: e is gated extend { dimension: z is 1 } on id = e.id } -> { group_by: e.secret }",
      "e",
   ],
   [
      "a join to a model extension of the locked source",
      "run: plain extend { join_one: e is gated_ext on id = e.id } -> { group_by: e.s2 }",
      "e",
   ],
   [
      "a join to a locked duckdb.sql source",
      "run: plain extend { join_one: s is gated_sql on id = s.id } -> { group_by: s.name }",
      "s",
   ],
   [
      "a composite join with a locked member",
      "run: plain extend { join_one: c2 is comp on id = c2.id } -> { group_by: c2.secret }",
      "c2",
   ],
   [
      "JOIN_ONE: in upper case",
      "RUN: plain EXTEND { JOIN_ONE: g IS gated ON id = g.id } -> { GROUP_BY: g.secret }",
      "g",
   ],
   [
      "Join_Cross: in mixed case",
      "run: plain extend { Join_Cross: g is gated } -> { group_by: g.secret }",
      "g",
   ],
   [
      "a second join item with no comma",
      "run: plain extend { join_one: a is helper on id = a.id e is gated extend {} on id = e.id } -> { group_by: e.secret }",
      "e",
   ],
   [
      "annotations between the alias and its base",
      "run: plain extend { join_one: e # x\n is # y\n gated extend {} on id = e.id } -> { group_by: e.secret }",
      "e",
   ],
];

describe("a caller join into an #(authorize) lock", () => {
   for (const [label, query, alias] of LOCK_FORMS) {
      it(`403 for a non-admitted caller: ${label}`, async () => {
         await expectDenied(plainModel, query, DENY, alias);
      });
      // Malloy does not compile a join inside a nest (ungated too), so only
      // its denial is observable.
      if (label === "a join inside a nest:") continue;
      it(`200 for an admitted caller: ${label}`, async () => {
         const { rows } = await run(plainModel, query, ADMIT);
         expect(rows.length).toBeGreaterThan(0);
      });
   }

   it("names the caller's alias, never the joined base", async () => {
      await expect(
         run(
            plainModel,
            "run: plain extend { join_one: whatever is gated on id = whatever.id } -> { group_by: whatever.secret }",
            DENY,
         ),
      ).rejects.toThrow(/"whatever"/);
   });

   it("a nonexistent column on a denied join is a 403, not a compile error naming it", async () => {
      let caught: unknown;
      try {
         await run(
            plainModel,
            "run: plain extend { join_one: g is gated on id = g.id } -> { group_by: g.no_such_field }",
            DENY,
         );
      } catch (err) {
         caught = err;
      }
      expect(caught).toBeInstanceOf(AccessDeniedError);
      expect((caught as Error).message).not.toContain("no_such_field");
   });

   it("an ungated exported source and an extend over a table source are admitted", async () => {
      const a = await run(
         plainModel,
         "run: plain extend { join_one: p is plain on id = p.id } -> { group_by: id; aggregate: n is p.c }",
         DENY,
      );
      expect(byId(a.rows)).toEqual([
         { id: 1, n: 1 },
         { id: 2, n: 1 },
      ]);
      const b = await run(
         plainModel,
         "run: plain extend { join_one: p is plain extend { dimension: z is 1 } on id = p.id } -> { group_by: p.z }",
         DENY,
      );
      expect(b.rows).toEqual([{ z: 1 }]);
   });
});

describe("a caller join into an #(access_filter) row gate", () => {
   it("grafts the filter into the caller join's ON; the author join of the same source stays unfiltered", async () => {
      const { rows, sql } = await run(
         plainModel,
         "run: open_src extend { join_cross: r is rowgated } -> { group_by: r_region is r.region, a_region is author_rg.region }",
         { GROUPS: ["US"] },
      );
      expect(
         [...rows].sort((a, b) =>
            String(a.a_region).localeCompare(String(b.a_region)),
         ),
      ).toEqual([
         { r_region: "US", a_region: "EU" },
         { r_region: "US", a_region: "US" },
      ]);
      // The caller join carries the gate; the author join's ON is its own key only.
      expect(sql).toMatch(
         /JOIN t AS r_0\s+ON 1=1 AND \(+r_0\."region" IN \('US'\)/,
      );
      expect(sql).toMatch(
         /JOIN t AS author_rg_0\s+ON base\."id"=author_rg_0\."id"\s*\n/,
      );
   });

   it("a query-local join NULLs out the rows the caller may not see", async () => {
      const { rows } = await run(
         plainModel,
         "run: plain -> { join_one: r is rowgated on id = r.id; group_by: id, r.region }",
         { GROUPS: ["US"] },
      );
      expect(rows).toEqual([
         { id: 1, region: "US" },
         { id: 2, region: null },
      ]);
   });

   it("a caller alias of the row-gated source, joined, is filtered", async () => {
      const { rows } = await run(
         plainModel,
         "source: mine is rowgated extend {}\nrun: plain extend { join_one: m is mine on id = m.id } -> { group_by: id, m.region }",
         { GROUPS: ["EU"] },
      );
      expect(rows).toEqual([
         { id: 1, region: null },
         { id: 2, region: "EU" },
      ]);
   });

   it("an inline extend over the row-gated source is filtered", async () => {
      const { rows } = await run(
         plainModel,
         "run: plain extend { join_one: e is rowgated extend { dimension: z is 1 } on id = e.id } -> { group_by: id, e.region }",
         { GROUPS: ["US"] },
      );
      expect(rows).toEqual([
         { id: 1, region: "US" },
         { id: 2, region: null },
      ]);
   });

   it("a query-expression join into the row-gated source is filtered even when the filtered field isn't projected", async () => {
      const { rows, sql } = await run(
         plainModel,
         "run: plain extend { join_one: q is rowgated -> { group_by: id; aggregate: rc } on id = q.id } -> { group_by: id, q.rc }",
         { GROUPS: ["US"] },
      );
      expect(byId(rows)).toEqual([
         { id: 1, rc: 1 },
         { id: 2, rc: null },
      ]);
      expect(sql).toMatch(/"region" IN \('US'\)/);
   });

   it("a named-query join into the row-gated source is filtered even when the filtered field isn't projected", async () => {
      const { rows, sql } = await run(
         plainModel,
         "run: plain extend { join_one: q is rg_agg on id = q.id } -> { group_by: id, q.rc }",
         { GROUPS: ["US"] },
      );
      expect(byId(rows)).toEqual([
         { id: 1, rc: 1 },
         { id: 2, rc: null },
      ]);
      expect(sql).toMatch(/"region" IN \('US'\)/);
   });

   it("a composite join with a row-gated member is refused", async () => {
      await expectDenied(
         plainModel,
         "run: plain extend { join_one: c2 is comp_rg on id = c2.id } -> { group_by: c2.region }",
         ADMIT,
         "c2",
      );
   });

   it("an author join inside a caller-joined model source is not walked", async () => {
      const { rows } = await run(
         plainModel,
         "run: plain extend { join_one: o is open_src on id = o.id } -> { group_by: id, o.author_rg.region }",
         DENY,
      );
      expect(rows).toEqual([
         { id: 1, region: "US" },
         { id: 2, region: "EU" },
      ]);
   });

   it("the grafted materializer a caller join built does not filter a later request's author join", async () => {
      const fresh = await loadModel(false);
      const first = await run(
         fresh,
         "run: plain extend { join_one: r is rowgated on id = r.id } -> { group_by: id, r.region }",
         DENY,
      );
      expect(first.rows).toEqual([
         { id: 1, region: null },
         { id: 2, region: null },
      ]);
      const second = await run(
         fresh,
         "run: open_src -> { group_by: id, author_rg.region }",
         DENY,
      );
      expect(second.rows).toEqual([
         { id: 1, region: "US" },
         { id: 2, region: "EU" },
      ]);
      expect(second.sql).not.toMatch(/author_rg_0\."region"\s*(=|IN|in)/);
   });
});

describe("the query boundary reaches caller joins", () => {
   it("control: the hidden source is not queryable as a run target", async () => {
      await expectNotQueryable(
         boundaryModel,
         "run: helper -> { group_by: name }",
         ADMIT,
      );
   });

   it("a join into a hidden source is 404", async () => {
      await expectNotQueryable(
         boundaryModel,
         "run: plain extend { join_one: h is helper on id = h.id } -> { group_by: h.name }",
         ADMIT,
      );
   });

   it("a join into a hidden AND gated source is 404 even for a caller the lock admits", async () => {
      await expectNotQueryable(
         boundaryModel,
         "run: plain extend { join_one: h is hidden_gated on id = h.id } -> { group_by: h.hsecret }",
         ADMIT,
      );
      await expectNotQueryable(
         boundaryModel,
         "run: plain extend { join_one: h is hidden_gated on id = h.id } -> { group_by: h.hsecret }",
         DENY,
      );
   });

   it("a join into a non-exported named query over a hidden AND gated source is 404 whether or not the lock admits", async () => {
      await expectNotQueryable(
         boundaryModel,
         "run: plain extend { join_one: hq is hgq on id = hq.id } -> { group_by: hq.hsecret }",
         DENY,
      );
      await expectNotQueryable(
         boundaryModel,
         "run: plain extend { join_one: hq is hgq on id = hq.id } -> { group_by: hq.hsecret }",
         ADMIT,
      );
      // control: an exported named query over the same kind of locked source
      // is admitted for an admitted caller.
      const { rows } = await run(
         boundaryModel,
         "run: plain extend { join_one: q is gated_q on id = q.id } -> { group_by: q.secret }",
         ADMIT,
      );
      expect(rows.length).toBe(2);
   });

   it("an inline extend over a hidden table source is 404", async () => {
      await expectNotQueryable(
         boundaryModel,
         "run: plain extend { join_one: e is helper extend { dimension: z is 1 } on id = e.id } -> { group_by: e.name }",
         ADMIT,
      );
   });

   it("a caller alias of a hidden source, joined, is 404", async () => {
      await expectNotQueryable(
         boundaryModel,
         "source: mine is helper extend {}\nrun: plain extend { join_one: m is mine on id = m.id } -> { group_by: m.name }",
         ADMIT,
      );
   });

   it("a query-expression join over a hidden source is 404", async () => {
      await expectNotQueryable(
         boundaryModel,
         "run: plain extend { join_one: q is helper -> { group_by: id, name } on id = q.id } -> { group_by: q.name }",
         ADMIT,
      );
   });

   it("a named query the boundary does not curate is 404; a curated one over a curated source is admitted", async () => {
      await expectNotQueryable(
         boundaryModel,
         "run: plain extend { join_one: q is hidden_q on id = q.id } -> { group_by: q.name }",
         ADMIT,
      );
      const { rows } = await run(
         boundaryModel,
         "run: plain extend { join_one: q is gated_q on id = q.id } -> { group_by: q.secret }",
         ADMIT,
      );
      expect(rows.length).toBe(2);
   });

   it("a nonexistent column on a hidden join is 404 before compiling", async () => {
      await expectNotQueryable(
         boundaryModel,
         "run: plain extend { join_one: h is helper on id = h.id } -> { group_by: h.no_such_field }",
         ADMIT,
      );
   });

   it("a curated join is admitted and an author join to a hidden source is not walked", async () => {
      const { rows } = await run(
         boundaryModel,
         "run: plain extend { join_one: g is gated on id = g.id } -> { group_by: g.secret }",
         ADMIT,
      );
      expect(rows.length).toBe(2);
   });

   it("bypassAuthorize still 404s a hidden join, and admits a locked one", async () => {
      await expectNotQueryable(
         boundaryModel,
         "run: plain extend { join_one: h is helper on id = h.id } -> { group_by: h.name }",
         DENY,
         true,
      );
      const { rows } = await run(
         boundaryModel,
         "run: plain extend { join_one: g is gated on id = g.id } -> { group_by: g.secret }",
         DENY,
         true,
      );
      expect(rows.length).toBe(2);
   });
});

describe("a caller join whose chain cannot be proven", () => {
   const UNPROVABLE =
      "source: mine is compose(gated, helper)\nrun: plain extend { join_one: m is mine on id = m.id } -> { group_by: m.name }";

   it("is 404 when the boundary is active", async () => {
      await expectNotQueryable(boundaryModel, UNPROVABLE, ADMIT);
   });

   it("is 403 when the boundary is inert and the model declares a gate", async () => {
      await expectDenied(plainModel, UNPROVABLE, ADMIT, "m");
   });

   it("walks caller joins up to the depth bound and denies past it", async () => {
      const chain = (depth: number): string => {
         const lines = ["source: s0 is plain extend {}"];
         for (let i = 1; i < depth; i++) {
            lines.push(
               `source: s${i} is plain extend { join_one: j${i} is s${i - 1} on id = j${i}.id }`,
            );
         }
         lines.push(
            `run: plain extend { join_one: top is s${depth - 1} on id = top.id } -> { aggregate: n is count() }`,
         );
         return lines.join("\n");
      };
      const { rows } = await run(plainModel, chain(8), DENY);
      expect(rows).toEqual([{ n: 2 }]);
      await expect(run(plainModel, chain(24), DENY)).rejects.toBeInstanceOf(
         AccessDeniedError,
      );
   });

   it("is admitted in a model that declares no gate and no boundary", async () => {
      await fs.writeFile(
         path.join(PKG_DIR, "ungated.malloy"),
         `##! experimental { composite_sources }
source: plain is duckdb.table('t')
source: helper is duckdb.table('t')
`,
      );
      const ungated = await Model.create(
         "test-pkg",
         PKG_DIR,
         "ungated.malloy",
         new Map<string, Connection>([["duckdb", duck as Connection]]),
      );
      const { rows } = await run(
         ungated,
         "source: mine is compose(plain, helper)\nrun: plain extend { join_one: m is mine on id = m.id } -> { group_by: m.name }",
         {},
      );
      expect(rows.length).toBe(2);
   });
});

describe("a caller join cannot hide a laundered run target", () => {
   // Pins the ordering on the over-denial the laundering check gives an
   // unprovable request-declared entry point.
   it("an unprovable run target stays 403 with a row-gated caller join beside it", async () => {
      await expectDenied(
         plainModel,
         "source: mine is compose(helper, open_src)\nrun: mine -> { group_by: name }",
         { GROUPS: ["US"] },
         "mine",
      );
      await expectDenied(
         plainModel,
         "source: mine is compose(helper, open_src)\nrun: mine extend { join_cross: r is rowgated } -> { group_by: name, r.region }",
         { GROUPS: ["US"] },
         "mine",
      );
   });
});

describe("a composite run target with a caller extend join", () => {
   it("decides a lock on the join", async () => {
      const query =
         "run: comp_plain extend { join_one: g is gated on id = g.id } -> { group_by: g.secret }";
      await expectDenied(plainModel, query, DENY, "g");
      const { rows } = await run(plainModel, query, ADMIT);
      expect(rows.length).toBe(2);
   });

   it("grafts a row gate into the join the resolved composite runs", async () => {
      const { rows, sql } = await run(
         plainModel,
         "run: comp_plain extend { join_one: r is rowgated on id = r.id } -> { group_by: id, r.region }",
         { GROUPS: ["US"] },
      );
      expect(byId(rows)).toEqual([
         { id: 1, region: "US" },
         { id: 2, region: null },
      ]);
      expect(sql).toMatch(/r_0\."region" IN \('US'\)/);
   });

   it("is 404 for a hidden join", async () => {
      await expectNotQueryable(
         boundaryModel,
         "run: comp_plain extend { join_one: h is helper on id = h.id } -> { group_by: h.name }",
         ADMIT,
      );
   });
});

// Malloy cannot compile a join inside these views, so the text pass is what
// answers 403 ahead of a compile error naming the gated source's columns.
describe("a join inside a view Malloy cannot compile", () => {
   it("is denied in a view of a joined caller source", async () => {
      const query =
         "source: mine is plain extend { view: v is { extend: { join_one: g is gated on id = g.id } group_by: g.secret } }\nrun: plain extend { join_one: m is mine on id = m.id } -> { nest: m.v }";
      await expectDenied(plainModel, query, DENY, "g");
   });

   it("is denied in a view declared in a query-local extend", async () => {
      const query =
         "run: plain -> { extend: { view: v is { extend: { join_one: g is gated on id = g.id } group_by: g.secret } } nest: v }";
      await expectDenied(plainModel, query, DENY, "g");
   });
});

describe("a row gate on the other join kinds", () => {
   it("join_many carries the gate in its ON", async () => {
      const { sql } = await run(
         plainModel,
         "run: plain extend { join_many: r is rowgated on id = r.id } -> { group_by: id; aggregate: n is r.rc }",
         { GROUPS: ["US"] },
      );
      expect(sql).toMatch(/JOIN t AS r_0\s+ON [^\n]*r_0\."region" IN \('US'\)/);
   });

   it("a foreign-key with join carries the gate in its ON", async () => {
      const { rows, sql } = await run(
         plainModel,
         "run: plain extend { join_one: r is rowgated with id } -> { group_by: id, r.region }",
         { GROUPS: ["US"] },
      );
      expect(sql).toMatch(/JOIN t AS r_0\s+ON [^\n]*r_0\."region" IN \('US'\)/);
      expect(byId(rows)).toEqual([
         { id: 1, region: "US" },
         { id: 2, region: null },
      ]);
   });
});

describe("author joins", () => {
   it("every author join in the IR carries its file's URL", async () => {
      const materializer = (
         plainModel as unknown as {
            modelMaterializer: {
               loadRestrictedQuery(q: string): {
                  getPreparedQuery(): Promise<unknown>;
               };
            };
         }
      ).modelMaterializer;
      const prepared = (await materializer
         .loadRestrictedQuery("run: open_src -> { group_by: author_rg.region }")
         .getPreparedQuery()) as {
         _query: { structRef: unknown };
         _modelDef: { contents: Record<string, unknown> };
      };
      const ref = prepared._query.structRef;
      const struct = (
         typeof ref === "string" ? prepared._modelDef.contents[ref] : ref
      ) as { fields: { join?: string; location?: { url?: string } }[] };
      const joins = struct.fields.filter((f) => f.join);
      expect(joins.length).toBe(2);
      for (const join of joins) {
         expect(join.location?.url).toStartWith("file://");
      }
   });

   it("a caller join grafting a source filters an author join through a named query over it when the source is imported", async () => {
      // An imported source reaches the named query as an object snapshot, which
      // `graftIntoNamedQuerySnapshots` grafts too: over-filtering, never under.
      await fs.writeFile(
         path.join(PKG_DIR, "base.malloy"),
         `##! experimental { givens }
given:
  GROUPS :: string[]

#(access_filter) region in $GROUPS
source: rowgated2 is duckdb.table('t') extend { primary_key: id }
`,
      );
      await fs.writeFile(
         path.join(PKG_DIR, "imported.malloy"),
         `##! experimental { givens }
import { rowgated2, GROUPS } from "base.malloy"
query: rg2_q is rowgated2 -> { group_by: id, region }
source: nq2_author is duckdb.table('t') extend {
  join_one: q is rg2_q on id = q.id
}
`,
      );
      const imported = await Model.create(
         "test-pkg",
         PKG_DIR,
         "imported.malloy",
         new Map<string, Connection>([["duckdb", duck as Connection]]),
      );
      const alone = await run(
         imported,
         "run: nq2_author -> { group_by: id, qr is q.region }",
         { GROUPS: ["US"] },
      );
      expect(byId(alone.rows)).toEqual([
         { id: 1, qr: "US" },
         { id: 2, qr: "EU" },
      ]);
      const withCaller = await run(
         imported,
         "run: nq2_author extend { join_one: r is rowgated2 on id = r.id } -> { group_by: id, qr is q.region, rr is r.region }",
         { GROUPS: ["US"] },
      );
      expect(byId(withCaller.rows)).toEqual([
         { id: 1, qr: "US", rr: "US" },
         { id: 2, qr: null, rr: null },
      ]);
   });

   it("a caller join grafting a source leaves an author join through a same-file named query over it unfiltered", async () => {
      const withCaller = await run(
         plainModel,
         "run: nq_author extend { join_one: r is rowgated on id = r.id } -> { group_by: id, qr is q.region, rr is r.region }",
         { GROUPS: ["US"] },
      );
      const alone = await run(
         plainModel,
         "run: nq_author -> { group_by: id, qr is q.region }",
         { GROUPS: ["US"] },
      );
      expect(byId(alone.rows)).toEqual([
         { id: 1, qr: "US" },
         { id: 2, qr: "EU" },
      ]);
      expect(byId(withCaller.rows)).toEqual([
         { id: 1, qr: "US", rr: "US" },
         { id: 2, qr: "EU", rr: null },
      ]);
   });
});

describe("a caller join under an author named-query run target", () => {
   for (const [label, query] of [
      [
         "a refinement's extend",
         "run: hidden_q + { extend: { join_one: g is gated_q on id = g.id } group_by: g.secret }",
      ],
      [
         "a pipe stage's extend",
         "run: hidden_q -> { extend: { join_one: g is gated_q on id = g.id } group_by: g.secret }",
      ],
      [
         "a refinement's query-local join",
         "run: hidden_q + { join_one: g is gated_q on id = g.id; group_by: g.secret }",
      ],
   ]) {
      it(`decides the lock: ${label}`, async () => {
         await expectDenied(plainModel, query, DENY, "g");
      });
   }

   it("grafts the row filter", async () => {
      const { rows } = await run(
         plainModel,
         "run: hidden_q + { extend: { join_one: g is rowgated on id = g.id } group_by: g.region }",
         { GROUPS: ["US"] },
      );
      expect(byId(rows)).toEqual([
         { id: 1, name: "a", region: "US" },
         { id: 2, name: "b", region: null },
      ]);
   });

   it("holds the join to the boundary", async () => {
      await expectNotQueryable(
         boundaryModel,
         "run: rg_q + { extend: { join_one: h is hidden_q on id = h.id } group_by: h.name }",
         ADMIT,
      );
   });
});

// Each declares `mine` over a gated or hidden base the text reader could once
// miss, with a decoy `source: mine is plain` the compiler never reads.
const DECOY = "dimension: `source: mine is plain` is 1";

describe("a caller-declared source the text could misread", () => {
   const runJoin =
      "run: plain extend { join_one: m is mine on id = m.id } -> { group_by: m.secret }";
   for (const [label, declaration] of [
      ["a parenthesised base", `source: mine is ((gated)) extend { ${DECOY} }`],
      [
         "the second item of a source: list",
         `source: a is plain extend {} mine is gated extend { ${DECOY} }`,
      ],
      [
         "an annotation after is",
         "# source: mine is plain\nsource: mine is\n# note\ngated extend {}",
      ],
   ]) {
      it(`decides the lock through ${label}`, async () => {
         await expectDenied(
            plainModel,
            `${declaration}\n${runJoin}`,
            DENY,
            "m",
         );
      });
   }

   it("grafts the row filter through a parenthesised base", async () => {
      const { rows } = await run(
         plainModel,
         `source: mine is ((rowgated)) extend { ${DECOY} }\nrun: plain extend { join_one: m is mine on id = m.id } -> { group_by: id, m.region }`,
         { GROUPS: ["US"] },
      );
      expect(byId(rows)).toEqual([
         { id: 1, region: "US" },
         { id: 2, region: null },
      ]);
   });

   it("holds the join to the boundary", async () => {
      await expectNotQueryable(
         boundaryModel,
         `source: mine is ((helper)) extend { ${DECOY} }\nrun: plain extend { join_one: m is mine on id = m.id } -> { group_by: m.name }`,
         ADMIT,
      );
   });

   it("decides the lock for an inline extend over it", async () => {
      await expectDenied(
         plainModel,
         `source: mine is ((gated)) extend { ${DECOY} }\nrun: plain extend { join_one: e is mine extend {} on id = e.id } -> { group_by: e.secret }`,
         DENY,
         "e",
      );
   });

   it("holds the same declaration run directly to the boundary", async () => {
      await expectNotQueryable(
         boundaryModel,
         `source: mine is ((helper)) extend { ${DECOY} }\nrun: mine -> { group_by: name }`,
         ADMIT,
      );
   });
});

describe("a caller join refining a curated author query", () => {
   it("is admitted unmodified", async () => {
      const { rows } = await run(
         boundaryModel,
         "run: plain extend { join_one: q is hq_exp on id = q.id } -> { group_by: q.id }",
         ADMIT,
      );
      expect(rows.length).toBe(2);
   });

   for (const refinement of [
      "hq_exp + { group_by: name }",
      "hq_exp + { where: name = 'a' }",
      "hq_exp -> { group_by: id }",
   ]) {
      it(`is 404 over a hidden base: ${refinement}`, async () => {
         await expectNotQueryable(
            boundaryModel,
            `run: plain extend { join_one: q is ${refinement} on id = q.id } -> { group_by: q.id }`,
            ADMIT,
         );
      });
   }
});

describe("record and array literals are not joins", () => {
   it("admits a record literal", async () => {
      const { rows } = await run(
         plainModel,
         "run: plain extend { dimension: r is {a is 1} } -> { group_by: r.a }",
         DENY,
      );
      expect(rows).toEqual([{ a: 1 }]);
   });

   it("admits an array literal", async () => {
      const { rows } = await run(
         plainModel,
         "run: plain extend { dimension: xs is [1, 2] } -> { group_by: xs.each }",
         DENY,
      );
      expect(rows.length).toBe(2);
   });
});

describe("caller joins are resolved once per request", () => {
   it("walks the compiled query once for the boundary and authorize passes", async () => {
      const model = await loadModel(true);
      const internals = model as unknown as {
         computeCallerJoins: (...args: unknown[]) => Promise<unknown>;
      };
      const original = internals.computeCallerJoins.bind(model);
      let calls = 0;
      internals.computeCallerJoins = (...args: unknown[]) => {
         calls++;
         return original(...args);
      };
      await run(
         model,
         "run: plain extend { join_one: g is gated on id = g.id } -> { group_by: g.secret }",
         ADMIT,
      );
      expect(calls).toBe(1);
   });
});
