// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#@ persist` on a top-level `query:`: what the package's build plan holds,
 * and warns about, with PUBLISHER_MATERIALIZE off (a persisted query is not
 * built, warned `persist-query-not-built`, and answered live) and on.
 */

import {
   modelPathsIn,
   sourcesIn,
} from "../../../test_helpers/materialization_test_fixtures";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { compilePackageBuildPlan } from "./build_plan_compile";
import { buildKindOf } from "../run/run_telemetry";
import { PERSIST_QUERY_NOT_BUILT } from "./persisted_query";

const TABLE = `duckdb.sql("""
  select * from (values
    (1, 'aa', 't1', 10.5),
    (2, 'bb', 't2', 20.0)
  ) as t(id, carrier, tenant, d)
""")`;

const BASE = `source: base is ${TABLE} extend { measure: n is count() }\n`;

const packages = tempBuildPlanPackages();
let restore: (() => void) | undefined;

function enable(on: boolean, gated = false) {
   restore?.();
   restore = installSettings({
      env: {
         PUBLISHER_MATERIALIZE: on ? "true" : "false",
         PUBLISHER_MATERIALIZE_GATED_SOURCES: gated ? "true" : "false",
      },
   }).restore;
}

beforeEach(() => enable(false));
afterEach(async () => {
   restore?.();
   restore = undefined;
   await packages.cleanup();
});

async function compiledFiles(files: Record<string, string>) {
   const pkg = await packages.create(files);
   return compilePackageBuildPlan(pkg);
}

async function compiled(text: string) {
   return compiledFiles({ "m.malloy": text });
}

function codes(c: Awaited<ReturnType<typeof compiled>>): string[] {
   return (c.annotationFindings ?? []).map((f) => f.code);
}

describe("#@ persist on a named query, PUBLISHER_MATERIALIZE off", () => {
   it("builds nothing, and warns persist-query-not-built naming the workaround", async () => {
      const c = await compiled(
         `${BASE}#@ persist name="nq_tbl"\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
      );
      expect(Object.keys(sourcesIn(c))).toEqual([]);
      const finding = (c.annotationFindings ?? []).find(
         (f) => f.code === PERSIST_QUERY_NOT_BUILT,
      );
      expect(finding?.subject).toBe("q");
      expect(finding?.model).toBe("m.malloy");
      expect(finding?.tier).toBe("unservable");
      expect(finding?.message).toContain("answered live");
      expect(finding?.message).toContain("source: q_result is q");
      // Never the source warning, which told the author to "persist a query
      // source" about something that already is a query.
      expect(c.droppedPersistSources ?? []).toEqual([]);
   });

   it("reports a misspelled key on the query's line", async () => {
      const c = await compiled(
         `${BASE}#@ persist nmae="nq_tbl"\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
      );
      const typo = (c.annotationFindings ?? []).find(
         (f) => f.code === "persist-unknown-key",
      );
      expect(typo?.subject).toBe("q");
      expect(typo?.message).toContain('#@ persist query "q"');
      expect(typo?.message).toContain("did you mean `name=`");
   });

   it("warns once, in the file that declares the query, not in one that imports it", async () => {
      const c = await compiledFiles({
         "a.malloy": `${BASE}#@ persist\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
         "b.malloy": `import "a.malloy"\nrun: q\n`,
      });
      const found = (c.annotationFindings ?? []).filter(
         (f) => f.code === PERSIST_QUERY_NOT_BUILT,
      );
      expect(found.map((f) => f.model)).toEqual(["a.malloy"]);
   });

   it("says nothing about a query without the annotation, or one that opts out", async () => {
      const c = await compiled(
         `${BASE}query: q is base -> { group_by: carrier }\n#@ -persist\nquery: q2 is base -> { group_by: carrier }\n`,
      );
      expect(codes(c)).not.toContain(PERSIST_QUERY_NOT_BUILT);
   });

   it("still plans a persisted source over the query, the workaround", async () => {
      const c = await compiled(
         `##! experimental.persistence\n${BASE}query: q is base -> { group_by: carrier; aggregate: n }\n#@ persist name="q_tbl"\nsource: q_result is q\n`,
      );
      expect(Object.values(sourcesIn(c)).map((s) => s.name)).toEqual([
         "q_result",
      ]);
      expect(codes(c)).not.toContain(PERSIST_QUERY_NOT_BUILT);
   });
});

const GIVENS = `##! experimental { givens }\ngiven: TENANT :: string is ""\n`;
const GATED = `${GIVENS}#(access_filter) tenant = $TENANT\nsource: ops is ${TABLE} extend { measure: n is count() }\n`;
/** {@link GATED} without its gate: the twin a gate refusal is built as. */
const UNGATED = `${GIVENS}source: ops is ${TABLE} extend { measure: n is count() }\n`;

/** The build sources a package plans, by name. */
function built(c: Awaited<ReturnType<typeof compiled>>): string[] {
   return Object.values(sourcesIn(c))
      .map((s) => s.name)
      .sort();
}

/** The `persist-query-not-built` messages, one per refused query. */
function notBuilt(c: Awaited<ReturnType<typeof compiled>>): string[] {
   return (c.annotationFindings ?? [])
      .filter((f) => f.code === PERSIST_QUERY_NOT_BUILT)
      .map((f) => f.message);
}

describe("#@ persist on a named query, PUBLISHER_MATERIALIZE on", () => {
   beforeEach(() => enable(true));

   it("plans one generated build source per query, named <query>__query, in the companion", async () => {
      const c = await compiled(
         `${BASE}#@ persist name="nq_tbl"\nquery: q is base -> { group_by: carrier; aggregate: n; order_by: n desc; limit: 1 }\n`,
      );
      const sources = Object.entries(sourcesIn(c));
      expect(sources.map(([, s]) => s.name)).toEqual(["q__query"]);
      const [id, source] = sources[0];
      expect(id).toMatch(/^q__query@.*m\.malloy\.persist-view\.malloy$/);
      // The stored rows are exactly `run: q`'s: its ordering and limit kept.
      expect(source.getSQL()).toContain("LIMIT 1");
      // Planned and built as a persisted view's table, counted as a query.
      expect(c.tables[id].kind).toBe("view");
      expect(buildKindOf(c, id)).toBe("query");
      expect(notBuilt(c)).toEqual([]);
      expect(c.droppedPersistSources ?? []).toEqual([]);
   });

   it("plans an inline run target with no gate", async () => {
      const c = await compiled(
         `#@ persist\nquery: q is ${TABLE} -> { group_by: carrier }\n`,
      );
      expect(built(c)).toEqual(["q__query"]);
   });

   it("plans a query beside a persisted view, in one companion", async () => {
      const c = await compiled(
         `source: base is ${TABLE} extend {\n  measure: n is count()\n  #@ persist\n  view: v is { group_by: carrier; aggregate: n }\n}\n#@ persist\nquery: q is base -> { group_by: tenant; aggregate: n }\n`,
      );
      expect(built(c)).toEqual(["base__v__view", "q__query"]);
   });

   it("lists a query once, in its declaring model, however many models import it", async () => {
      const c = await compiledFiles({
         "a.malloy": `${BASE}#@ persist\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
         "b.malloy": `import "a.malloy"\nquery: q2 is q -> { select: * }\n`,
      });
      expect(built(c)).toEqual(["q__query"]);
      expect(Object.values(modelPathsIn(c) ?? {})).toEqual(["a.malloy"]);
   });

   // Each refusal beside the twin it is built as, so the check named is the
   // one that refuses (mutation-checked: the twin differs by that one thing).
   const refusals: {
      name: string;
      refused: string | Record<string, string>;
      twin: string | Record<string, string>;
      reason: string;
      gated?: boolean;
   }[] = [
      {
         name: "a query that reads a given",
         refused: `${GIVENS}${BASE}#@ persist\nquery: q is base -> { where: tenant = $TENANT; group_by: carrier; aggregate: n }\n`,
         twin: `${GIVENS}${BASE}#@ persist\nquery: q is base -> { where: tenant = 't1'; group_by: carrier; aggregate: n }\n`,
         reason: "reads a given",
      },
      {
         name: "a query whose source has its own given-reading where:",
         refused: `${GIVENS}source: base is ${TABLE} extend { where: tenant = $TENANT; measure: n is count() }\n#@ persist\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
         twin: `${GIVENS}source: base is ${TABLE} extend { where: tenant = 't1'; measure: n is count() }\n#@ persist\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
         reason: "reads a given",
      },
      {
         name: "a query over a source with its own access filter",
         refused: `${GATED}#@ persist\nquery: q is ops -> { group_by: tenant; aggregate: n }\n`,
         twin: `${UNGATED}#@ persist\nquery: q is ops -> { group_by: tenant; aggregate: n }\n`,
         reason: "row-level gate",
      },
      {
         name: "a gated query, with the gated-sources switch on",
         refused: `${GATED}#@ persist\nquery: q is ops -> { group_by: tenant; aggregate: n }\n`,
         twin: `${UNGATED}#@ persist\nquery: q is ops -> { group_by: tenant; aggregate: n }\n`,
         reason: "row-level gate",
         gated: true,
      },
      {
         name: "a query whose source inherits an access filter",
         refused: `${GATED}source: ops2 is ops extend {}\n#@ persist\nquery: q is ops2 -> { group_by: tenant; aggregate: n }\n`,
         twin: `${UNGATED}source: ops2 is ops extend {}\n#@ persist\nquery: q is ops2 -> { group_by: tenant; aggregate: n }\n`,
         reason: "row-level gate",
      },
      {
         name: "a query whose source derives from a gated source",
         refused: `${GATED}source: ops2 is ops -> { group_by: tenant, carrier; aggregate: n }\n#@ persist\nquery: q is ops2 -> { group_by: tenant; aggregate: total is n.sum() }\n`,
         twin: `${UNGATED}source: ops2 is ops -> { group_by: tenant, carrier; aggregate: n }\n#@ persist\nquery: q is ops2 -> { group_by: tenant; aggregate: total is n.sum() }\n`,
         reason: "row-level gate",
      },
      {
         name: "a query that reaches a gate through a join",
         refused: `${GIVENS}#(access_filter) tenant = $TENANT\nsource: tenants is duckdb.sql("select 't1' as tenant") extend { primary_key: tenant }\nsource: base is ${TABLE} extend { join_one: t is tenants on tenant = t.tenant; measure: n is count() }\n#@ persist\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
         twin: `${GIVENS}source: tenants is duckdb.sql("select 't1' as tenant") extend { primary_key: tenant }\nsource: base is ${TABLE} extend { join_one: t is tenants on tenant = t.tenant; measure: n is count() }\n#@ persist\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
         reason: "row-level gate",
      },
      {
         name: "an inline run target that reaches a gate",
         refused: `${GATED}#@ persist\nquery: q is ops extend { dimension: t2 is tenant } -> { group_by: t2; aggregate: n }\n`,
         twin: `${UNGATED}#@ persist\nquery: q is ops extend { dimension: t2 is tenant } -> { group_by: t2; aggregate: n }\n`,
         reason: "row-level gate",
      },
      {
         name: "an inline run target that joins a gated source",
         refused: `${GIVENS}#(access_filter) tenant = $TENANT\nsource: tenants is duckdb.sql("select 't1' as tenant") extend { primary_key: tenant }\nsource: base is ${TABLE} extend { measure: n is count() }\n#@ persist\nquery: q is base extend { join_one: t is tenants on tenant = t.tenant } -> { group_by: carrier; aggregate: n }\n`,
         twin: `${GIVENS}source: tenants is duckdb.sql("select 't1' as tenant") extend { primary_key: tenant }\nsource: base is ${TABLE} extend { measure: n is count() }\n#@ persist\nquery: q is base extend { join_one: t is tenants on tenant = t.tenant } -> { group_by: carrier; aggregate: n }\n`,
         reason: "row-level gate",
      },
      {
         name: "a query over a gated source declared in another file",
         refused: {
            "a.malloy": GATED,
            "b.malloy": `import "a.malloy"\n#@ persist\nquery: q is ops -> { group_by: tenant; aggregate: n }\n`,
         },
         twin: {
            "a.malloy": UNGATED,
            "b.malloy": `import "a.malloy"\n#@ persist\nquery: q is ops -> { group_by: tenant; aggregate: n }\n`,
         },
         reason: "row-level gate",
      },
      {
         name: "a query whose result nests",
         refused: `${BASE}#@ persist\nquery: q is base -> { group_by: carrier; nest: t is { group_by: tenant } }\n`,
         twin: `${BASE}#@ persist\nquery: q is base -> { group_by: carrier, tenant }\n`,
         reason: "nests",
      },
      {
         name: "a query whose result has a select: nest",
         refused: `${BASE}#@ persist\nquery: q is base -> { group_by: carrier; nest: t is { select: tenant } }\n`,
         twin: `${BASE}#@ persist\nquery: q is base -> { select: carrier, tenant }\n`,
         reason: "nests",
      },
      {
         name: "a generated name another source already has",
         refused: `${BASE}source: q__query is base\n#@ persist\nquery: q is base -> { group_by: carrier }\n`,
         twin: `${BASE}source: q__other is base\n#@ persist\nquery: q is base -> { group_by: carrier }\n`,
         reason: "collides",
      },
   ];
   const asOfBuild: [string, string][] = [
      [
         "a query that reads the current time",
         `source: base is duckdb.sql("select 1 as n, timestamp '2026-01-01' as ts") extend { measure: c is count() }\n#@ persist\nquery: q is base -> { where: ts > now - 30 days; aggregate: c }\n`,
      ],
      [
         "a query whose select: * reads a current-time dimension",
         `source: base is duckdb.sql("select 1 as n, timestamp '2026-01-01' as ts") extend { dimension: past is ts < now }\n#@ persist\nquery: q is base -> { select: * }\n`,
      ],
      [
         "a query over a query-shaped source whose query reads the current time",
         `source: qb is duckdb.sql("select 1 as n, timestamp '2026-01-01' as ts") -> { where: ts > now - 30 days; select: * } extend { measure: c is count() }\n#@ persist\nquery: q is qb -> { group_by: n; aggregate: c }\n`,
      ],
   ];
   for (const [name, text] of asOfBuild) {
      it(`plans ${name} as of its build, with no refusal`, async () => {
         enable(true, false);
         const c = await compiled(text);
         expect(built(c)).toContain("q__query");
         expect(notBuilt(c)).toEqual([]);
         expect(c.asOfBuild).toEqual([expect.stringContaining("q__query")]);
      });
   }

   for (const r of refusals) {
      it(`refuses ${r.name}, with persist-query-not-built, and builds its twin`, async () => {
         enable(true, r.gated ?? false);
         const plan = (files: string | Record<string, string>) =>
            typeof files === "string" ? compiled(files) : compiledFiles(files);
         const refused = await plan(r.refused);
         expect(built(refused)).not.toContain("q__query");
         const messages = notBuilt(refused);
         expect(messages.length).toBe(1);
         expect(messages[0]).toContain(r.reason);
         expect(messages[0]).toContain("source: q_result is q");
         const twin = await plan(r.twin);
         expect(built(twin)).toContain("q__query");
         expect(notBuilt(twin)).toEqual([]);
      });
   }

   it.each([
      [
         "an @env. reference on the line",
         "#@ persist name=@env.TBL",
         "@env.",
         "persist-query-invalid",
      ],
      [
         "an incremental key",
         "#@ persist watermark=id",
         "incremental key",
         "persist-query-invalid",
      ],
      [
         'a lone refresh="incremental"',
         '#@ persist refresh="incremental"',
         "incremental key",
         "persist-query-invalid",
      ],
      [
         "an empty storage=",
         '#@ persist storage=""',
         "names no destination",
         "persist-query-invalid",
      ],
      [
         "a line that does not parse",
         '#@ persist name="tbl',
         "does not parse",
         "persist-query-unparseable",
      ],
   ])(
      "finds %s invalid, which fails the load, and plans nothing",
      async (_case, line, reason, code) => {
         enable(true);
         const c = await compiled(
            `${BASE}${line}\nquery: q is base -> { group_by: carrier }\n`,
         );
         expect(built(c)).not.toContain("q__query");
         expect(notBuilt(c)).toEqual([]);
         const invalid = (c.annotationFindings ?? []).filter(
            (f) => f.tier === "malformed",
         );
         expect(invalid.map((f) => f.code)).toEqual([code]);
         expect(invalid[0].message).toContain(reason);
      },
   );

   it("builds a query over an ungated source declared in another file", async () => {
      const c = await compiledFiles({
         "a.malloy": BASE,
         "b.malloy": `import "a.malloy"\n#@ persist\nquery: q is base -> { group_by: carrier; aggregate: n }\n`,
      });
      expect(built(c)).toEqual(["q__query"]);
      expect(notBuilt(c)).toEqual([]);
   });
});
