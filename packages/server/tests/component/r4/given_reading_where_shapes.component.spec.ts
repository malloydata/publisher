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
 * - kept base: the term is inherited from a base declared in the same file,
 *   through one or more `extend` (or rename) hops. The plan lists the base on
 *   `keptBases`, the build copy writes `true` in the base's text, and the
 *   served copy keeps the base's trailing `extend` blocks.
 * - refused: not built (a package warning, or a `refusedSources` entry), so
 *   the source answers live.
 * - live: built, but the served copy does not re-root it, so every query is
 *   answered live.
 *
 * A query-shaped source over a scoped base is refused by the eligibility gate
 * (`given_in_persisted_query`); a v1 `#@ persist` never takes the kept-base
 * path (an inherited term keeps the source live at read).
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import {
   freshLoadPool,
   loadFixture,
   type LoadedFixture,
   type Outcome,
} from "./conformance/package_harness";
import { rowDifference } from "./harness";

const HEAD = `##! experimental { persistence givens }`;
/** A root of its own per shape, so no two shapes' tables share a content address. */
const root = (x: string) =>
   `source: ev_${x} is duckdb.sql("SELECT id, org, amt, '${x}' AS tag FROM read_csv_auto('data/ev.csv')")`;
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
   | { kind: "kept_base"; bases: string[] }
   | { kind: "refused_warning"; code: string; says: string }
   | { kind: "refused_source"; reason: string }
   | { kind: "live" };

interface ShapeCase {
   name: string;
   model: string;
   source: string;
   path: Path;
   query?: string;
   /** Every served row is the caller's own org (false for a join's null side). */
   ownRowsOnly?: boolean;
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
      path: { kind: "kept_base", bases: ["base_ms_one"] },
   },
   {
      name: "#(materialize) storage=, the term on a base two extend levels up",
      model: "mat_storage.malloy",
      source: "ms_two",
      path: { kind: "kept_base", bases: ["mid_ms_two", "base_ms_two"] },
   },
   {
      name: "#(materialize) storage=, the term on a base reached through a bare rename",
      model: "mat_storage.malloy",
      source: "ms_alias",
      path: { kind: "kept_base", bases: ["alias_ms_alias", "base_ms_alias"] },
   },
   {
      name: "#(materialize) storage=, the term on a materialized parent (an extension table)",
      model: "mat_storage.malloy",
      source: "ms_child",
      path: { kind: "kept_base", bases: ["ms_parent"] },
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
      path: { kind: "kept_base", bases: ["base_mc_one"] },
   },
   {
      name: "#(materialize) storage=, the term on a base imported from another file",
      model: "mat_import.malloy",
      source: "mi_one",
      path: {
         kind: "refused_warning",
         code: "materialize-not-built",
         says: "cannot re-apply it at read",
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
      path: { kind: "kept_base", bases: ["dim_base"] },
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
      path: { kind: "live" },
   },
   {
      name: "v1 #@ persist colocated, the term on a base in the same file",
      model: "v1_colocated.malloy",
      source: "vc_one",
      path: { kind: "live" },
   },
   {
      name: "v1 #@ persist storage=, the term on a base imported from another file",
      model: "v1_import.malloy",
      source: "vi_one",
      path: { kind: "live" },
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
   { keptBases?: { sourceID: string }[] } | undefined
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
               fixture.pkg as unknown as {
                  materialization?: { materializePlans?: MaterializePlans };
               }
            ).materialization?.materializePlans ?? {};
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
            case "kept_base":
               expect(built).toBe(true);
               expect(refused).toBeUndefined();
               if (c.model.startsWith("mat_")) {
                  expect(plan).toBeDefined();
                  expect(
                     (plan?.keptBases ?? []).map((b) => nameOf(b.sourceID)),
                  ).toEqual(c.path.kind === "kept_base" ? c.path.bases : []);
               }
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
               (c.path.kind === "simple" || c.path.kind === "kept_base") &&
               c.source !== "ms_join";
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
});
