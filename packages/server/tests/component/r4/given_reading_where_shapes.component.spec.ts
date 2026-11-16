// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a materialized source's given-reading `where:` is written, and what
 * that decides, through the real package path (load, plan, build, bind,
 * serve). Each shape is one source on a root of its own (so no two share a
 * content address), queried by two callers with different givens,
 * interleaved and repeated, served and live.
 *
 * - simple: the term is on the materialized source itself. It is left out of
 *   the build (`WHERE true`) and re-applied at read, so the table holds every
 *   caller's rows and each caller reads only its own.
 * - refused: not built (a package warning, or a `refusedSources` entry), so
 *   the source answers live, under its terms. A term inherited from any base
 *   (in this file or another, through `extend`, a rename or a materialized
 *   parent) is refused at plan, as is a given-reading dimension a base
 *   declares: the served copy keeps only the source's own declaration, so it
 *   could not re-apply either at read.
 * - live: built, but the served copy does not re-root it, so every query is
 *   answered live. (A v1 `#@ persist` that inherits its term from a base used
 *   to be this; it is now refused, `inherited_where_with_given`.)
 *
 * A query-shaped source over a scoped base is refused by the eligibility gate
 * (`given_in_persisted_query`). A zone-dependent dimension a base declares is
 * stored as built, and a `timezone:` query over the table is answered live.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   loadFixture,
   type LoadedFixture,
   type Outcome,
} from "../../../src/test_helpers/conformance/package_runner";
import { rowDifference } from "./harness";

const HEAD = `##! experimental { persistence givens }`;
/** A root of its own per shape, so no two shapes' tables share a content address. */
const root = (x: string) =>
   `source: ev_${x} is duckdb.sql("SELECT id, org, amt, '${x}' AS tag FROM read_csv_auto('data/ev.csv')")`;
/** The text every refusal of an inherited given-reading `where:` carries. */
const MOVE_WHERE =
   "move the given-reading `where:` onto the materialized source itself";
const base = (x: string) =>
   `${root(x)}\nsource: base_${x} is ev_${x} extend { where: org = $ORG }`;

const files: Record<string, string> = {
   "data/ev.csv":
      "id,org,amt\n1,acme,10\n2,acme,20\n3,globex,5\n4,globex,7\n5,globex,9\n6,initech,3\n",
   "base.malloy": `${HEAD}
given: ORG :: string
${base("mi_one")}
${base("vi_one")}
`,
   "mat_storage.malloy": `${HEAD}
given: ORG :: string
${root("ms_own")}
${base("ms_one")}
${base("ms_two")}
source: mid_ms_two is base_ms_two extend { dimension: big is amt > 6 }
${base("ms_query")}
${root("ms_own_query")}
${root("ms_join")}
${base("ms_joined")}
${root("ms_dim")}
source: dim_base is ev_ms_dim extend { dimension: mine is org = $ORG }

#(materialize) storage="lake"
source: ms_own is ev_ms_own extend {
  where: org = $ORG
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: ms_one is base_ms_one extend { measure: total is amt.sum() }

#(materialize) storage="lake"
source: ms_two is mid_ms_two extend { measure: total is amt.sum() }

#(materialize) storage="lake"
source: ms_query is base_ms_query -> { group_by: id, org, amt }

#(materialize) storage="lake"
source: ms_own_query is ev_ms_own_query -> { group_by: id, org, amt } extend {
  where: org = $ORG
}

#(materialize) storage="lake"
source: ms_join is ev_ms_join extend {
  join_one: b is base_ms_joined on b.id = id
  measure: total is amt.sum()
}

#(materialize) storage="lake"
source: ms_dim is dim_base extend { measure: total is amt.sum() }

source: ev_ms_zone is duckdb.sql("""
  SELECT id, org, amt, 'ms_zone' AS tag,
    TIMESTAMP '2026-01-01 20:30:00' + INTERVAL (id) HOUR AS ts
  FROM read_csv_auto('data/ev.csv')
""")
source: zone_base is ev_ms_zone extend { dimension: ts_day is ts.day }

#(materialize) storage="lake"
source: ms_zone is zone_base extend { measure: total is amt.sum() }

${root("ms_parent")}
#(materialize) storage="lake"
source: ms_parent is ev_ms_parent extend { where: org = $ORG }

#(materialize) storage="lake"
source: ms_child is ms_parent extend {
  dimension: amt2 is amt * 2
  measure: total is amt.sum()
}

${base("ms_alias")}
source: alias_ms_alias is base_ms_alias

#(materialize) storage="lake"
source: ms_alias is alias_ms_alias extend { measure: total is amt.sum() }
`,
   "mat_colocated.malloy": `${HEAD}
given: ORG :: string
${root("mc_own")}
${base("mc_one")}

#(materialize) name="mc_own"
source: mc_own is ev_mc_own extend {
  where: org = $ORG
  measure: total is amt.sum()
}

#(materialize) name="mc_one"
source: mc_one is base_mc_one extend { measure: total is amt.sum() }
`,
   "mat_import.malloy": `${HEAD}
import "base.malloy"

#(materialize) storage="lake"
source: mi_one is base_mi_one extend { measure: total is amt.sum() }
`,
   "v1_storage.malloy": `${HEAD}
given: ORG :: string
${root("vs_own")}
${base("vs_one")}
${base("vs_query")}
${root("vs_own_query")}
${root("vs_join")}
${base("vs_joined")}

#@ persist name="vs_own" storage="lake"
source: vs_own is ev_vs_own extend {
  where: org = $ORG
  measure: total is amt.sum()
}

#@ persist name="vs_one" storage="lake"
source: vs_one is base_vs_one extend { measure: total is amt.sum() }

#@ persist name="vs_query" storage="lake"
source: vs_query is base_vs_query -> { group_by: id, org, amt }

#@ persist name="vs_own_query" storage="lake"
source: vs_own_query is ev_vs_own_query -> { group_by: id, org, amt } extend {
  where: org = $ORG
}

${root("vs_parent")}
#@ persist name="vs_parent" storage="lake"
source: vs_parent is ev_vs_parent extend {
  where: org = $ORG
  measure: total is amt.sum()
}

// Declares no #@ persist: inherits vs_parent's, and its table.
source: vs_inherits is vs_parent extend { dimension: amt2 is amt * 2 }

#@ persist name="vs_child" storage="lake"
source: vs_child is vs_parent extend { dimension: amt3 is amt * 3 }

#@ persist name="vs_join" storage="lake"
source: vs_join is ev_vs_join extend {
  join_one: b is base_vs_joined on b.id = id
  measure: total is amt.sum()
}
`,
   "v1_colocated.malloy": `${HEAD}
given: ORG :: string
${root("vc_own")}
${base("vc_one")}
${root("vc_own_query")}

#@ persist name="vc_own"
source: vc_own is ev_vc_own extend {
  where: org = $ORG
  measure: total is amt.sum()
}

#@ persist name="vc_one"
source: vc_one is base_vc_one extend { measure: total is amt.sum() }

#@ persist name="vc_own_query"
source: vc_own_query is ev_vc_own_query -> { group_by: id, org, amt } extend {
  where: org = $ORG
}
`,
   "v1_import.malloy": `${HEAD}
import "base.malloy"

#@ persist name="vi_one" storage="lake"
source: vi_one is base_vi_one extend { measure: total is amt.sum() }
`,
};

type Path =
   | { kind: "simple" }
   | { kind: "refused_warning"; code: string; says: string }
   | { kind: "refused_source"; reason: string }
   | { kind: "inherits_annotation" }
   | { kind: "live" };

interface ShapeCase {
   name: string;
   model: string;
   source: string;
   path: Path;
   query?: string;
   /** Every served row is the caller's own org (false for a join's null side). */
   ownRowsOnly?: boolean;
   /**
    * Answered from a stored table through the served copy (default: when
    * built and simple). A refused extension of a materialized parent is
    * answered from the parent's table, which keeps the parent's own term.
    */
   fromTable?: boolean;
}

const AGG = "{ group_by: org; aggregate: n is count(), s is amt.sum() }";

const CASES: ShapeCase[] = [
   {
      name: "#(materialize) storage=, the term on the source itself",
      model: "mat_storage.malloy",
      source: "ms_own",
      path: { kind: "simple" },
   },
   {
      name: "#(materialize) storage=, query-shaped, the term in its own trailing extend",
      model: "mat_storage.malloy",
      source: "ms_own_query",
      path: { kind: "simple" },
   },
   {
      name: "#(materialize) colocated, the term on the source itself",
      model: "mat_colocated.malloy",
      source: "mc_own",
      path: { kind: "simple" },
   },
   {
      name: "#(materialize) storage=, the term on a base in the same file, one extend",
      model: "mat_storage.malloy",
      source: "ms_one",
      path: {
         kind: "refused_warning",
         code: "materialize-not-built",
         says: MOVE_WHERE,
      },
   },
   {
      name: "#(materialize) storage=, the term on a base two extend levels up",
      model: "mat_storage.malloy",
      source: "ms_two",
      path: {
         kind: "refused_warning",
         code: "materialize-not-built",
         says: MOVE_WHERE,
      },
   },
   {
      name: "#(materialize) storage=, the term on a base reached through a bare rename",
      model: "mat_storage.malloy",
      source: "ms_alias",
      path: {
         kind: "refused_warning",
         code: "materialize-not-built",
         says: MOVE_WHERE,
      },
   },
   {
      name: "#(materialize) storage=, the term on a materialized parent (an extension table)",
      model: "mat_storage.malloy",
      source: "ms_child",
      path: {
         kind: "refused_warning",
         code: "materialize-not-built",
         says: MOVE_WHERE,
      },
      fromTable: true,
   },
   {
      name: "#(materialize) storage=, the parent of that extension table (its own term)",
      model: "mat_storage.malloy",
      source: "ms_parent",
      path: { kind: "simple" },
   },
   {
      name: "#(materialize) colocated, the term on a base in the same file",
      model: "mat_colocated.malloy",
      source: "mc_one",
      path: {
         kind: "refused_warning",
         code: "materialize-not-built",
         says: MOVE_WHERE,
      },
   },
   {
      name: "#(materialize) storage=, the term on a base imported from another file",
      model: "mat_import.malloy",
      source: "mi_one",
      path: {
         kind: "refused_warning",
         code: "materialize-not-built",
         says: MOVE_WHERE,
      },
   },
   {
      name: "#(materialize) storage=, query-shaped over a scoped base",
      model: "mat_storage.malloy",
      source: "ms_query",
      path: { kind: "refused_source", reason: "given_in_persisted_query" },
   },
   {
      name: "#(materialize) storage=, the term on a joined source",
      model: "mat_storage.malloy",
      source: "ms_join",
      // Built (it has no term of its own); the join is not stored, so a
      // query through it is answered live, the joined source's term with it.
      path: { kind: "simple" },
      query: "run: ms_join -> { group_by: b.org; aggregate: n is count(), s is b.amt.sum() }",
      ownRowsOnly: false,
   },
   {
      name: "#(materialize) storage=, a given-reading dimension (no where:) on a base in the same file",
      model: "mat_storage.malloy",
      source: "ms_dim",
      path: {
         kind: "refused_warning",
         code: "materialize-not-built",
         says: "declare the given-reading dimension on the materialized source itself",
      },
      query: "run: ms_dim -> { group_by: mine; aggregate: n is count() }",
      ownRowsOnly: false,
   },
   {
      name: "v1 #@ persist storage=, the term on the source itself",
      model: "v1_storage.malloy",
      source: "vs_own",
      path: { kind: "simple" },
   },
   {
      name: "v1 #@ persist storage=, query-shaped, the term in its own trailing extend",
      model: "v1_storage.malloy",
      source: "vs_own_query",
      path: { kind: "simple" },
   },
   {
      name: "v1 #@ persist colocated, the term on the source itself",
      model: "v1_colocated.malloy",
      source: "vc_own",
      path: { kind: "simple" },
   },
   {
      name: "v1 #@ persist colocated, query-shaped, the term in its own trailing extend",
      model: "v1_colocated.malloy",
      source: "vc_own_query",
      path: { kind: "simple" },
   },
   {
      name: "v1 #@ persist storage=, the term on a base in the same file",
      model: "v1_storage.malloy",
      source: "vs_one",
      path: { kind: "refused_source", reason: "inherited_where_with_given" },
   },
   {
      name: "v1 #@ persist colocated, the term on a base in the same file",
      model: "v1_colocated.malloy",
      source: "vc_one",
      path: { kind: "refused_source", reason: "inherited_where_with_given" },
   },
   {
      name: "v1 #@ persist storage=, the term on a base imported from another file",
      model: "v1_import.malloy",
      source: "vi_one",
      path: { kind: "refused_source", reason: "inherited_where_with_given" },
   },
   {
      name: "v1 #@ persist storage=, a parent that writes its own term",
      model: "v1_storage.malloy",
      source: "vs_parent",
      path: { kind: "simple" },
   },
   {
      name: "v1 #@ persist storage=, an extension that inherits its parent's #@ persist (and table)",
      model: "v1_storage.malloy",
      source: "vs_inherits",
      path: { kind: "inherits_annotation" },
      fromTable: true,
   },
   {
      name: "v1 #@ persist storage=, an extension with its own #@ persist over a parent that writes the term",
      model: "v1_storage.malloy",
      source: "vs_child",
      path: { kind: "refused_source", reason: "inherited_where_with_given" },
      fromTable: true,
   },
   {
      name: "v1 #@ persist storage=, query-shaped over a scoped base",
      model: "v1_storage.malloy",
      source: "vs_query",
      path: { kind: "refused_source", reason: "given_in_persisted_query" },
   },
   {
      name: "v1 #@ persist storage=, the term on a joined source",
      model: "v1_storage.malloy",
      source: "vs_join",
      path: { kind: "refused_source", reason: "dynamic_joined_where" },
      query: "run: vs_join -> { group_by: b.org; aggregate: n is count(), s is b.amt.sum() }",
      ownRowsOnly: false,
   },
];

const CALLERS = ["acme", "globex", "acme", "globex"];

type MaterializePlans = Record<
   string,
   { leftOut?: { dimension: string }[] } | undefined
>;

const nameOf = (sourceID: string) => sourceID.split("@")[0];

function answered(outcome: Outcome) {
   if ("error" in outcome) throw new Error(outcome.error);
   return outcome.answer;
}

describe("given-reading where: by where it is written, through a real package", () => {
   let restore: () => void;
   let fixture: LoadedFixture;
   const live: Record<string, Outcome> = {};
   const zoneLive: Record<string, Record<string, unknown>[]> = {};

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      const loaded = await loadFixture(files);
      for (const c of CASES) {
         for (const ORG of ["acme", "globex"]) {
            live[`${c.source}/${ORG}`] = await loaded.run(
               c.model,
               c.query ?? `run: ${c.source} -> ${AGG}`,
               { ORG },
            );
         }
      }
      for (const query of [
         "run: ms_zone -> { group_by: ts_day; aggregate: n is count() }",
         "run: ms_zone -> { timezone: 'America/New_York'; group_by: ts_day; aggregate: n is count() }",
      ]) {
         zoneLive[query] = answered(
            await loaded.run("mat_storage.malloy", query),
         ).rows;
      }
      fixture = await loaded.bind();
   }, 300_000);

   afterAll(async () => {
      await fixture?.dispose();
      restore();
      await freshLoadPool();
   });

   for (const c of CASES) {
      it(`${c.name}: ${c.path.kind}`, async () => {
         const plans =
            (
               fixture.pkg.materialization() as unknown as {
                  materializePlans?: MaterializePlans;
               }
            ).materializePlans ?? {};
         const plan = Object.entries(plans).find(
            ([id]) => nameOf(id) === c.source,
         )?.[1];
         const built = fixture.built.some((b) => b.name === c.source);
         const warning = fixture.warnings.find((w) => w.subject === c.source);
         const refused = Object.values(
            fixture.pkg.getBuildPlan()?.refusedSources ?? {},
         ).find((r) => r.name === c.source);

         switch (c.path.kind) {
            case "simple":
               expect(built).toBe(true);
               expect(refused).toBeUndefined();
               if (c.model.startsWith("mat_")) expect(plan).toBeDefined();
               break;
            case "refused_warning":
               expect(built).toBe(false);
               expect(plan).toBeUndefined();
               expect(warning?.code).toBe(c.path.code);
               expect(warning?.message).toContain(c.path.says);
               break;
            case "refused_source":
               expect(built).toBe(false);
               expect(refused?.reason).toBe(c.path.reason as never);
               break;
            case "inherits_annotation":
               // Shares its parent's table, so it is neither refused nor
               // built under its own name.
               expect(refused).toBeUndefined();
               break;
            case "live":
               // Built, but the served copy does not re-root it.
               expect(built).toBe(true);
               break;
         }

         const query = c.query ?? `run: ${c.source} -> ${AGG}`;
         const served: Record<string, unknown>[][] = [];
         for (const ORG of CALLERS) {
            const before = answered(live[`${c.source}/${ORG}`]);
            const now = answered(await fixture.run(c.model, query, { ORG }));
            expect(rowDifference(before.rows, now.rows)).toBeUndefined();
            const fromTable =
               c.fromTable ??
               (c.path.kind === "simple" && c.source !== "ms_join");
            expect(now.servedBy).toBe(fromTable ? "materialized" : "live");
            if (fromTable) expect(now.via).toBe("served_copy");
            if (c.ownRowsOnly ?? true) {
               for (const row of now.rows) expect(row.org).toBe(ORG);
            }
            served.push(now.rows);
         }
         // Different callers, different answers; repeats equal the first.
         expect(rowDifference(served[0], served[1])).toBeDefined();
         expect(rowDifference(served[0], served[2])).toBeUndefined();
         expect(rowDifference(served[1], served[3])).toBeUndefined();
      }, 120_000);
   }

   it("#(materialize) storage=, a zone-dependent dimension on a base: stored as built, a timezone: query answered live", async () => {
      const plans =
         (
            fixture.pkg.materialization() as unknown as {
               materializePlans?: MaterializePlans;
            }
         ).materializePlans ?? {};
      const plan = Object.entries(plans).find(
         ([id]) => nameOf(id) === "ms_zone",
      )?.[1];
      expect(plan).toBeDefined();
      expect((plan?.leftOut ?? []).map((d) => d.dimension)).toEqual([]);
      expect(fixture.built.some((b) => b.name === "ms_zone")).toBe(true);
      const inDefaultZone =
         "run: ms_zone -> { group_by: ts_day; aggregate: n is count() }";
      const inNewYork =
         "run: ms_zone -> { timezone: 'America/New_York'; group_by: ts_day; aggregate: n is count() }";
      for (const [query, fromTable] of [
         [inDefaultZone, true],
         [inNewYork, false],
      ] as const) {
         const now = answered(await fixture.run("mat_storage.malloy", query));
         expect(rowDifference(zoneLive[query], now.rows)).toBeUndefined();
         expect(now.servedBy).toBe(fromTable ? "materialized" : "live");
      }
      // The zone moves the day boundary, so the two answers differ.
      expect(
         rowDifference(zoneLive[inDefaultZone], zoneLive[inNewYork]),
      ).toBeDefined();
   }, 120_000);
});

/**
 * Shapes a security review of the narrowing probed. A `timezone:` declared on
 * the source a query runs on (not in the query) must still decline a table
 * that stores a zone-dependent dimension, which the served copy computed in
 * the build's zone. A current-time dimension a base declares refuses the
 * source. A v1 table read through a chain of unannotated extensions (one
 * adding the given-reading `where:`), in its file or another, and a
 * multi-source `#@ persist` statement, answer as live for each caller.
 */
const reviewFiles: Record<string, string> = {
   "data/ev.csv":
      "id,org,amt\n1,acme,10\n2,acme,20\n3,globex,5\n4,globex,7\n5,globex,9\n6,initech,3\n",
   "v1a.malloy": `${HEAD}
given: ORG :: string
${root("pa")}
#@ persist name="pa" storage="lake"
source: pa is ev_pa extend { measure: total is amt.sum() }
source: pmid is pa extend { where: org = $ORG }
source: pchild2 is pmid extend { dimension: a2 is amt * 2 }

${root("pb")}
#@ persist name="pb"
source: pb is ev_pb extend { measure: total is amt.sum() }
source: pbmid is pb extend { where: org = $ORG }
source: pbchild is pbmid extend { dimension: a2 is amt * 2 }

${root("ms")}
#@ persist storage="lake"
source:
  ms1 is ev_ms extend { where: org = $ORG }
  ms2 is ms1 extend { dimension: a3 is amt * 3 }
`,
   "v1b.malloy": `${HEAD}
import "v1a.malloy"
source: pchild is pmid extend { dimension: a4 is amt * 4 }
source: pbchildx is pbmid extend { dimension: a4 is amt * 4 }
`,
   "zbase.malloy": `${HEAD}
source: ev_z2 is duckdb.sql("""
  SELECT id, org, amt, TIMESTAMP '2026-01-01 20:30:00' + INTERVAL (id) HOUR AS ts
  FROM read_csv_auto('data/ev.csv')
""")
source: zb2 is ev_z2 extend { dimension: ts_day is ts.day }
`,
   "zone.malloy": `${HEAD}
import "zbase.malloy"
source: ev_z is duckdb.sql("""
  SELECT id, org, amt, 'z' AS tag, TIMESTAMP '2026-01-01 20:30:00' + INTERVAL (id) HOUR AS ts
  FROM read_csv_auto('data/ev.csv')
""")
source: zb is ev_z extend { dimension: ts_day is ts.day }
#(materialize) storage="lake"
source: mz is zb extend { measure: total is amt.sum() }
source: mz_ny is mz extend { timezone: 'America/New_York' }
source: mz_ny_ext is mz_ny extend { dimension: k is 1 }

#(materialize) storage="lake"
source: mz2 is zb2 extend { measure: total is amt.sum() }
source: mz2_ny is mz2 extend { timezone: 'America/New_York' }

source: ev_z3 is duckdb.sql("""
  SELECT id, org, amt, 'z3' AS tag, TIMESTAMP '2026-01-01 20:30:00' + INTERVAL (id) HOUR AS ts
  FROM read_csv_auto('data/ev.csv')
""")
#(materialize) storage="lake"
source: mz3 is ev_z3 extend {
  dimension: ts_day is ts.day
  measure: total is amt.sum()
}
source: mz3_ny is mz3 extend { timezone: 'America/New_York' }
`,
   "now.malloy": `${HEAD}
source: ev_n is duckdb.sql("""
  SELECT id, org, amt, TIMESTAMP '2020-01-01 00:00:00' + INTERVAL (id * 3000) DAY AS ts
  FROM read_csv_auto('data/ev.csv')
""")
source: nb is ev_n extend { dimension: past is ts < now }
#(materialize) storage="lake"
source: mn is nb extend { measure: total is amt.sum() }
`,
};

const DAYS = (s: string) =>
   `run: ${s} -> { group_by: ts_day; aggregate: n is count() }`;

const REVIEW: {
   name: string;
   model: string;
   query: string;
   /** Answered from a stored table through the served copy. */
   fromTable: boolean;
   /** Queried per caller (ORG), each answer its own rows only. */
   perCaller: boolean;
}[] = [
   ...(
      [
         ["v1a.malloy", "pmid"],
         ["v1a.malloy", "pchild2"],
         ["v1b.malloy", "pchild"],
         ["v1a.malloy", "pbmid"],
         ["v1a.malloy", "pbchild"],
         ["v1b.malloy", "pbchildx"],
         ["v1a.malloy", "ms1"],
         ["v1a.malloy", "ms2"],
      ] as const
   ).map(([model, source]) => ({
      name: `v1, read through unannotated extensions: ${source}`,
      model,
      query: `run: ${source} -> ${AGG}`,
      fromTable: true,
      perCaller: true,
   })),
   {
      name: "#(materialize), a zone-dependent dimension on a base, no zone: from the table",
      model: "zone.malloy",
      query: DAYS("mz"),
      fromTable: true,
      perCaller: false,
   },
   {
      name: "#(materialize), the same, timezone: on the queried source (same file): live",
      model: "zone.malloy",
      query: DAYS("mz_ny"),
      fromTable: false,
      perCaller: false,
   },
   {
      name: "#(materialize), the same, timezone: on a source up the chain: live",
      model: "zone.malloy",
      query: DAYS("mz_ny_ext"),
      fromTable: false,
      perCaller: false,
   },
   {
      name: "#(materialize), the same over a base in another file, timezone: on the queried source: live",
      model: "zone.malloy",
      query: DAYS("mz2_ny"),
      fromTable: false,
      perCaller: false,
   },
   {
      name: "#(materialize), its own zone-dependent dimension, timezone: on the queried source: from the table, computed at read",
      model: "zone.malloy",
      query: DAYS("mz3_ny"),
      fromTable: true,
      perCaller: false,
   },
   {
      name: "#(materialize), a current-time dimension on a base: not built, live",
      model: "now.malloy",
      query: "run: mn -> { group_by: past; aggregate: n is count() }",
      fromTable: false,
      perCaller: false,
   },
];

describe("given-reading where: and caller-dependent dimensions: shapes a security review probed", () => {
   let restore: () => void;
   let fixture: LoadedFixture;
   const live: Record<string, Outcome> = {};
   const callersOf = (perCaller: boolean) =>
      perCaller ? CALLERS.map((ORG) => ({ ORG })) : [undefined];

   beforeAll(async () => {
      restore = installSettings({
         env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" },
      }).restore;
      await freshLoadPool();
      const loaded = await loadFixture(reviewFiles);
      for (const c of REVIEW) {
         for (const givens of callersOf(c.perCaller)) {
            live[`${c.query}/${givens?.ORG}`] = await loaded.run(
               c.model,
               c.query,
               givens,
            );
         }
      }
      fixture = await loaded.bind();
   }, 300_000);

   afterAll(async () => {
      await fixture?.dispose();
      restore();
      await freshLoadPool();
   });

   it("refuses the current-time dimension a base declares, at plan", () => {
      expect(fixture.built.some((b) => b.name === "mn")).toBe(false);
      expect(
         fixture.warnings.find((w) => w.subject === "mn")?.message,
      ).toContain(
         "declare the current-time dimension on the materialized source itself",
      );
   });

   it("the zone moves the answer, so a table read in the build's zone could not match", () => {
      const utc = answered(live[`${DAYS("mz")}/undefined`]).rows;
      const ny = answered(live[`${DAYS("mz_ny")}/undefined`]).rows;
      expect(rowDifference(utc, ny)).toBeDefined();
   });

   for (const c of REVIEW) {
      it(
         c.name,
         async () => {
            const answers: Record<string, unknown>[][] = [];
            for (const givens of callersOf(c.perCaller)) {
               const before = answered(live[`${c.query}/${givens?.ORG}`]);
               const now = answered(
                  await fixture.run(c.model, c.query, givens),
               );
               expect(rowDifference(before.rows, now.rows)).toBeUndefined();
               expect(now.servedBy).toBe(c.fromTable ? "materialized" : "live");
               if (c.fromTable) expect(now.via).toBe("served_copy");
               if (givens) {
                  for (const row of now.rows) expect(row.org).toBe(givens.ORG);
               }
               answers.push(now.rows);
            }
            if (c.perCaller) {
               expect(rowDifference(answers[0], answers[1])).toBeDefined();
               expect(rowDifference(answers[0], answers[2])).toBeUndefined();
               expect(rowDifference(answers[1], answers[3])).toBeUndefined();
            }
         },
         120_000,
      );
   }
});
