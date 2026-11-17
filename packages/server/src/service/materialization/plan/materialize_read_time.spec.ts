// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   materializePlansIn,
   sourcesIn,
   targetsIn,
} from "../../../test_helpers/materialization_test_fixtures";
import { describe, expect, it } from "bun:test";
import {
   planPackages,
   materializePlanNamed,
   notBuiltMessage,
   plannedSourceNamed,
} from "../../../test_helpers/materialization_fixture";
import { makeDigest, Runtime } from "@malloydata/malloy";
import { duckdbTestConnections } from "../../../test_helpers/incremental_test_harness";
import { addressSQL, buildSQLOf, computeSourceEntityId } from "./build_plan";
import {
   compilePackageBuildPlan,
   computePackageBuildPlan,
} from "./build_plan_compile";
import { buildCopyText } from "./materialize_build_copy";
import { generateBuildFile, givenReadTerms } from "./build_file_generator";
import { planModelMaterialize } from "./materialize_plan";
import { type MaterializePlan } from "../domain/table_plans";
import { admitGivenPositions } from "./persist_dynamic_terms";

/**
 * What a `#(materialize)` source leaves out of its build and computes at
 * read (`materialize_read_time.ts`, and the planner's refusals around it):
 * a given-reading `where:` of the source's own (P2), a caller-dependent
 * dimension (P9), and what a base declares of either, which refuses the
 * source. One section per shape.
 */

describe("a given-reading where: of the source's own (P2)", () => {
   /**
    * P2 on the `#(materialize)` build side (materialization v2, phase 3, PR 2): a
    * `where:` that reads a given is left out of the build and kept for the read,
    * and nothing else about the generated build moves. A current-time `where:`
    * is built as written and addressed over `now` unsubstituted, which is what
    * the rolling-window pull request has to keep so it moves no address.
    */

   const HEAD = `##! experimental { givens }
given:
  ORG_ID :: number is 1
source: raw is duckdb.sql("""
  select * from (values
    (1, 1, 'a', 10.5),
    (2, 2, 'b', 20.0),
    (3, 1, 'c', 5.25),
    (4, 2, 'zz', 7.0)
  ) as t(id, org_id, s, amount)
""")
`;

   const packages = planPackages({}, { keepIncrementalUnbuildable: true });
   const settings = packages.settings;

   /** The generated build source, presented under the author's sourceID. */
   function generated(c: Awaited<ReturnType<typeof compilePackageBuildPlan>>) {
      const [sourceID] = Object.keys(materializePlansIn(c) ?? {});
      const source = sourcesIn(c)[sourceID];
      if (!source) throw new Error("no #(materialize) source was planned");
      return { sourceID, source, target: targetsIn(c)?.[sourceID] };
   }

   const ORDERS = `${HEAD}
#(materialize) storage="lake"
source: orders is raw extend {
  where: org_id = $ORG_ID, s != 'zz'
  dimension: big is amount > 10
}
`;

   describe("P2: a given-reading where: is left out of the build", () => {
      it(
         "builds every caller's rows, applies the fixed terms, and keeps the given term for the read",
         async () => {
            const { pkg, c } = await packages.plan(ORDERS);
            const plan = Object.values(materializePlansIn(c) ?? {})[0];
            expect(plan.readTimeTerms?.map((t) => t.code)).toEqual([
               "org_id = $ORG_ID",
            ]);
            expect(givenReadTerms(plan)[0].givens).toEqual(["ORG_ID"]);
            expect(givenReadTerms(plan)[0].columns).toEqual(["org_id"]);

            const { sourceID, source, target } = generated(c);
            const sql = addressSQL(source, target);
            // The given's default (1) is not in the build; the fixed term is.
            expect(sql).not.toMatch(/"org_id"\s*=\s*1/);
            expect(sql).toContain("'zz'");

            // v1's machinery reads the term as stripped: the gate admits it.
            const classified = admitGivenPositions(source);
            expect(classified.ok).toBe(true);
            if (classified.ok) {
               expect(classified.terms.map((t) => t.code)).toEqual([
                  "org_id = $ORG_ID",
               ]);
               expect(classified.terms[0].columns).toEqual(["org_id"]);
            }

            const duckdb = await pkg.getMalloyConnection!("duckdb");
            const built = buildSQLOf(source, { entries: {} }, target);
            const rows = (
               await duckdb.runSQL(
                  `SELECT id, org_id FROM (${built}) t ORDER BY id`,
               )
            ).rows;
            expect(rows.map((r) => [Number(r.id), Number(r.org_id)])).toEqual([
               [1, 1],
               [2, 2],
               [3, 1],
            ]);
            expect(sourceID).toBe(plan.sourceID);
            expect(source.name).toBe("orders");
         },
         { timeout: 30000 },
      );

      it(
         "reports the term as a stripped term on the wire and does not refuse the source",
         async () => {
            const pkg = await packages.create(ORDERS);
            const { plan } = await computePackageBuildPlan(pkg);
            const wire = Object.values(plan?.sources ?? {}).find(
               (s) => s.name === "orders",
            );
            expect(wire?.strippedTerms).toEqual([
               { code: "org_id = $ORG_ID", givens: ["ORG_ID"] },
            ]);
            expect(
               Object.values(plan?.refusedSources ?? {}).map((r) => r.name),
            ).not.toContain("orders");
         },
         { timeout: 30000 },
      );

      it(
         "writes the build copy and the generated where: from the author's text",
         async () => {
            const { c } = await packages.plan(ORDERS);
            const plan = Object.values(materializePlansIn(c) ?? {})[0];
            // The text the package's model file holds.
            const result = buildCopyText(ORDERS, plan);
            if (!("copy" in result)) throw new Error(result.reason);
            expect(result.copy).toContain("where: true, s != 'zz'");
            expect(result.copy).not.toContain("$ORG_ID");
            const text = generateBuildFile(
               [plan],
               "m.malloy",
               undefined,
               new Map([
                  [plan.sourceID, "m.malloy.0.materialize-build.malloy"],
               ]),
            );
            expect(text).toBe(`##! experimental { persistence givens }
import { orders__mat__base is orders } from 'm.malloy.0.materialize-build.malloy'
import { ORG_ID } from 'm.malloy'

#@ persist name="orders" storage="lake"
source: orders__mat is orders__mat__base -> {
  select:
    id,
    org_id,
    s,
    amount,
    big
} extend {
  where: org_id = $ORG_ID
}
`);
         },
         { timeout: 30000 },
      );

      it(
         "imports a given the model imports from the file that declares it",
         async () => {
            const pkg = await packages.create({
               "g.malloy": HEAD,
               "m.malloy": `##! experimental { givens }
import { raw, ORG_ID } from 'g.malloy'
#(materialize) storage="lake"
source: orders is raw extend { where: org_id = $ORG_ID }
`,
            });
            const c = await compilePackageBuildPlan({
               ...pkg,
               getModelPaths: () => ["m.malloy"],
            });
            const { source, target } = generated(c);
            // The term reads `true` in the build copy; no org filter is built.
            expect(addressSQL(source, target)).not.toMatch(/"org_id"\s*=/);
            const plan = Object.values(materializePlansIn(c) ?? {})[0];
            expect(plan.givenDeclarations?.map((g) => g.name)).toEqual([
               "ORG_ID",
            ]);
            expect(plan.givenDeclarations?.[0].url).toEndWith("/g.malloy");
         },
         { timeout: 30000 },
      );

      it(
         "leaves a source with no given-reading where: exactly as it was",
         async () => {
            const { c } = await packages.plan(`${HEAD}
#(materialize) storage="lake"
source: orders is raw extend { where: s != 'zz' }
`);
            const plan = Object.values(materializePlansIn(c) ?? {})[0];
            expect(plan.readTimeTerms).toBeUndefined();
            expect(generateBuildFile([plan], "m.malloy")).toStartWith(
               "##! experimental.persistence\nimport { orders__mat__base is orders } from 'm.malloy'\n",
            );
         },
         { timeout: 30000 },
      );
   });

   describe("P2: what is refused rather than left out", () => {
      async function refusal(text: string): Promise<string> {
         const { c } = await packages.plan(text);
         // Nothing is planned for it, and so nothing is built.
         expect(Object.keys(materializePlansIn(c) ?? {})).toEqual([]);
         expect(
            Object.values(sourcesIn(c)).some((s) => s.name === "orders"),
         ).toBe(false);
         const { connections } = duckdbTestConnections();
         const runtime = new Runtime({
            urlReader: { readURL: async () => text },
            connections,
         });
         const model = await runtime
            .loadModel(new URL("file:///pkg/m.malloy"))
            .getModel();
         const { plans, refused } = planModelMaterialize(
            model._modelDef.contents as Record<string, unknown>,
         );
         expect(plans).toEqual([]);
         expect(refused).toHaveLength(1);
         return refused[0].reason;
      }

      it(
         "refuses a given term written on a base in another file",
         async () => {
            const pkg = await packages.create({
               "b.malloy": `${HEAD}
source: scoped is raw extend { where: org_id = $ORG_ID }
`,
               "m.malloy": `##! experimental { givens }
import "b.malloy"
#(materialize) storage="lake"
source: orders is scoped extend { dimension: big is amount > 10 }
`,
            });
            const c = await compilePackageBuildPlan(pkg);
            expect(Object.keys(materializePlansIn(c) ?? {})).toEqual([]);
            expect(
               (c.annotationFindings ?? []).find(
                  (f) =>
                     f.code === "materialize-not-built" &&
                     f.subject === "orders",
               )?.message,
            ).toContain(
               "move the given-reading `where:` onto the materialized source itself",
            );
         },
         { timeout: 30000 },
      );

      it(
         "refuses a given term over a field the table does not store",
         async () => {
            expect(
               await refusal(`${HEAD}
source: orgs is duckdb.sql("select 1 as org_id") extend { primary_key: org_id }
#(materialize) storage="lake"
source: orders is raw extend {
  join_one: orgs with org_id
  dimension: org is orgs.org_id
  where: org = $ORG_ID
}
`),
            ).toContain("does not store");
         },
         { timeout: 30000 },
      );

      it("refuses a term whose text is not found exactly once where the compiler placed it", () => {
         const text = `source: orders is raw extend {
  where: org_id = $ORG_ID
}
`;
         const plan: MaterializePlan = {
            sourceName: "orders",
            sourceID: "orders@file:///pkg/m.malloy",
            buildSourceName: "orders__mat",
            columns: [
               { dimension: "org_id", column: "org_id", computed: false },
            ],
            persist: { name: "orders", storage: "lake" },
            declaration: {
               url: "file:///pkg/m.malloy",
               range: {
                  start: { line: 0, character: 8 },
                  end: { line: 2, character: 1 },
               },
            },
            readTimeTerms: [
               {
                  kind: "given",
                  code: "org_id = $ORG_ID",
                  givens: ["ORG_ID"],
                  columns: ["org_id"],
                  // Not where the text is: a stale or misread location.
                  at: [
                     {
                        url: "file:///pkg/m.malloy",
                        range: {
                           start: { line: 0, character: 0 },
                           end: { line: 0, character: 6 },
                        },
                     },
                  ],
               },
            ],
         };
         const result = buildCopyText(text, plan);
         expect("copy" in result).toBe(false);
         if ("reason" in result) {
            expect(result.reason).toContain("could not be found");
         }
      });
   });

   describe("P2: the build copy takes out one source's terms, never another's", () => {
      /** The `materialize-not-built` message for `source`, if any. */
      function notBuilt(
         c: Awaited<ReturnType<typeof compilePackageBuildPlan>>,
         source: string,
      ): string | undefined {
         return (c.annotationFindings ?? []).find(
            (f) => f.code === "materialize-not-built" && f.subject === source,
         )?.message;
      }

      it(
         "keeps another source's given term in a build that joins it: the inlined join is refused",
         async () => {
            // One copy per source: were `a`'s term taken out of `b`'s build too,
            // `b` would store `aa.s` from every caller's `a` rows. The join is
            // inlined, so `b`'s build would read `a` (a reference join is not
            // built); a given read through an inlined join refuses `b` at plan
            // (P9), before the eligibility gate that refused it as
            // `given_in_persisted_query` would see its build.
            const text = `${HEAD}
#(materialize) storage="lake"
source: a is raw extend { where: org_id = $ORG_ID }
#(materialize) storage="lake" inline=["aa"]
source: b is raw -> { select: id, org_id } extend {
  join_one: aa is a on aa.id = id
  where: org_id = $ORG_ID
  dimension: a_s is aa.s
}
`;
            const { pkg, c } = await packages.plan(text);
            const ids = Object.fromEntries(
               Object.entries(sourcesIn(c)).map(([id, s]) => [s.name, id]),
            );
            expect(ids.b).toBeUndefined();
            expect(notBuilt(c, "b")).toContain(
               "inline= names `aa`, through which a given is read",
            );
            const { plan } = await computePackageBuildPlan(pkg);
            expect(plan?.refusedSources?.[ids.a]).toBeUndefined();
            // `a`'s own build still leaves its term out.
            expect(
               buildSQLOf(
                  sourcesIn(c)[ids.a],
                  { entries: {} },
                  targetsIn(c)?.[ids.a],
               ),
            ).not.toMatch(/"org_id"\s*=\s*1/);
         },
         { timeout: 30000 },
      );

      it(
         "refuses a source whose build reads a #@ persist source, rather than plan that source twice",
         async () => {
            const { c } =
               await packages.plan(`##! experimental { givens persistence }
given: ORG_ID :: number is 1
source: raw is duckdb.sql("select 1 as org_id, 'a' as s")
#@ persist name="base_t"
source: base is raw -> { select: org_id, s }
#(materialize) storage="lake"
source: orders is base -> { select: org_id, s } extend { where: org_id = $ORG_ID }
`);
            expect(Object.keys(materializePlansIn(c) ?? {})).toEqual([]);
            const nodes = c.builds.flatMap((b) => b.order);
            expect(
               nodes.every((id) => !id.includes(".materialize-build.")),
            ).toBe(true);
            expect(nodes.every((id) => sourcesIn(c)[id] !== undefined)).toBe(
               true,
            );
            expect(notBuilt(c, "orders")).toContain("#@ persist source (base)");
         },
         { timeout: 30000 },
      );

      it(
         "refuses a term the served copy would not keep (a parenthesised head), so no table is built for nothing",
         async () => {
            const { c } = await packages.plan(`${HEAD}
#(materialize) storage="lake"
source: orders is (raw extend { where: org_id = $ORG_ID }) extend {
  dimension: big is amount > 10
}
`);
            expect(Object.keys(materializePlansIn(c) ?? {})).toEqual([]);
            expect(notBuilt(c, "orders")).toContain("trailing `extend");
         },
         { timeout: 30000 },
      );
   });

   describe("P2 beside a rolling window: one read-time list, one classifier", () => {
      const DATED = `${HEAD}
source: dated is duckdb.sql("""
  select 1 as id, 1 as org_id, timestamp '2026-01-01' as t
""")
`;

      it(
         "plans a given term and a rolling window on one list, and leaves only the given term out of the build",
         async () => {
            const { c } = await packages.plan(`${DATED}
#(materialize) storage="lake"
source: orders is dated extend {
  where: t >= now - 30 days
  where: org_id = $ORG_ID
}
`);
            const plan = Object.values(materializePlansIn(c) ?? {})[0];
            expect(plan.readTimeTerms?.map((t) => t.kind).sort()).toEqual([
               "current_time",
               "given",
            ]);
            expect(givenReadTerms(plan).map((t) => t.code)).toEqual([
               "org_id = $ORG_ID",
            ]);
            const g = generated(c);
            const sql = buildSQLOf(g.source, { entries: {} }, g.target);
            // The window builds as written; the given term is left out.
            expect(sql).toMatch(/LOCALTIMESTAMP|CURRENT_TIMESTAMP/);
            expect(sql).not.toMatch(/"org_id"\s*=\s*1/);
         },
         { timeout: 30000 },
      );

      it(
         "refuses a term that reads both a given and the current time",
         async () => {
            const { c } = await packages.plan(`${DATED}
#(materialize) storage="lake"
source: orders is dated extend {
  where: org_id = $ORG_ID and t >= now - 30 days
}
`);
            expect(Object.keys(materializePlansIn(c) ?? {})).toEqual([]);
            const reason = (c.annotationFindings ?? []).find(
               (f) =>
                  f.code === "materialize-not-built" && f.subject === "orders",
            )?.message;
            expect(reason).toContain("reads both a given and the current time");
         },
         { timeout: 30000 },
      );
   });

   describe("one re-address: P2 adds none, P12 still moves only storage=", () => {
      it(
         "addresses a current-time where: over now unsubstituted, so a later literal now moves nothing",
         async () => {
            const text = `${HEAD}
source: dated is duckdb.sql("""select 1 as id, timestamp '2026-01-01' as t""")
#(materialize) storage="lake"
source: orders is dated extend { where: t >= now - 30 days }
`;
            const first = generated((await packages.plan(text)).c);
            const sql = addressSQL(first.source, first.target);
            expect(sql).toMatch(/LOCALTIMESTAMP|CURRENT_TIMESTAMP/);
            expect(sql).not.toMatch(/20\d\d-\d\d-\d\d \d\d:\d\d/);
            const second = generated((await packages.plan(text)).c);
            expect(addressSQL(second.source, second.target)).toBe(sql);
         },
         { timeout: 30000 },
      );

      it(
         "moves a storage= source with a stripped term once under the switch, by its destination alone",
         async () => {
            // The address reads the switch when it is computed, so each is
            // computed under its own setting.
            const addressUnder = async (env: Record<string, string>) => {
               settings({ PUBLISHER_PERSIST_STORAGE_MODE: "on", ...env });
               const { c } = await packages.plan(ORDERS);
               const g = generated(c);
               return {
                  sql: addressSQL(g.source, g.target),
                  id: computeSourceEntityId(
                     g.source,
                     c.connectionDigests,
                     g.target,
                  ),
                  digest: c.connectionDigests.duckdb,
                  plain: makeDigest(
                     c.connectionDigests.duckdb,
                     addressSQL(g.source, g.target),
                  ),
               };
            };
            const off = await addressUnder({});
            const on = await addressUnder({
               PUBLISHER_PERSIST_ADDRESS_INCLUDES_DESTINATION: "true",
            });
            // Same build SQL either way: the switch moves the address, not the build.
            expect(on.sql).toBe(off.sql);
            expect(off.id).toBe(off.plain);
            expect(on.id).toBe(
               makeDigest(on.digest, `${on.sql}\n-- stored in: lake`),
            );
            expect(on.id).not.toBe(off.id);
         },
         { timeout: 30000 },
      );
   });
});

describe("a given-reading term or dimension a base declares", () => {
   /**
    * A base's given-reading `where:` and given-reading dimensions on the
    * `#(materialize)` build side. Only a materialized source's own declaration
    * is left out of the build and re-applied at read: the served copy keeps that
    * declaration's trailing `extend` and no base's text. So a term the source
    * inherits from any base (one or several `extend` or rename hops up, or a
    * materialized parent) refuses the source at plan, as does a given-reading
    * dimension a base declares; the source is not built and answers live. A
    * zone-dependent dimension a base declares is stored as built.
    */

   const HEAD = `##! experimental { givens access_modifiers }
given:
  ORG_ID :: number is 1
source: raw is duckdb.sql("""
  select * from (values
    (1, 1, 'a', 10.5),
    (2, 2, 'b', 20.0),
    (3, 1, 'c', 5.25)
  ) as t(id, org_id, s, amount)
""")
`;

   /** What every refusal of an inherited given-reading `where:` says to do. */
   const MOVE_WHERE =
      "move the given-reading `where:` onto the materialized source itself";
   /** And of a given-reading dimension a base declares. */
   const MOVE_DIMENSION =
      "declare the given-reading dimension on the materialized source itself";

   const packages = planPackages({}, { keepIncrementalUnbuildable: true });

   const SCOPED = `${HEAD}
source: scoped is raw extend { where: org_id = $ORG_ID }
`;

   describe("a given-reading where: inherited from a base refuses the source", () => {
      for (const [shape, text] of [
         [
            "one extend up",
            `${SCOPED}
#(materialize) storage="lake"
source: orders is scoped extend { measure: n is count() }
`,
         ],
         [
            "several extend levels up",
            `${SCOPED}
source: mid is scoped extend { dimension: tag is 'x' }
source: upper_mid is mid extend { measure: total is amount.sum() }
#(materialize) storage="lake"
source: orders is upper_mid extend { measure: n is count() }
`,
         ],
         [
            "through a bare rename",
            `${SCOPED}
source: scoped2 is scoped
#(materialize) storage="lake"
source: orders is scoped2 extend { measure: n is count() }
`,
         ],
         [
            "with a term of the source's own beside it",
            `${HEAD.replace(
               "ORG_ID :: number is 1",
               "ORG_ID :: number is 1\n  MIN_AMT :: number is 0",
            )}
source: scoped is raw extend { where: org_id = $ORG_ID }
#(materialize) storage="lake"
source: orders is scoped extend { where: amount >= $MIN_AMT }
`,
         ],
         [
            "colocated",
            `${SCOPED}
#(materialize) name="orders"
source: orders is scoped extend { measure: n is count() }
`,
         ],
         [
            "from a base in another file",
            {
               "base.malloy": SCOPED,
               "m.malloy": `##! experimental.givens
import "base.malloy"
#(materialize) storage="lake"
source: orders is scoped extend { measure: n is count() }
`,
            },
         ],
      ] as const) {
         it(
            `is not planned or built: ${shape}`,
            async () => {
               const { c } = await packages.plan(text);
               expect(materializePlanNamed(c, "orders")).toBeUndefined();
               expect(plannedSourceNamed(c, "orders")).toBeUndefined();
               expect(notBuiltMessage(c, "orders")).toContain(MOVE_WHERE);
               expect(notBuiltMessage(c, "orders")).toContain(
                  "`where: org_id = $ORG_ID`",
               );
            },
            { timeout: 30000 },
         );
      }

      it(
         "refuses an extension of a materialized parent whose term it inherits, and plans the parent",
         async () => {
            const { c } = await packages.plan(`${HEAD}
#(materialize) storage="lake"
source: parent is raw extend { where: org_id = $ORG_ID }
#(materialize) storage="lake"
source: child is parent extend { dimension: tag is 'x' }
`);
            expect(materializePlanNamed(c, "child")).toBeUndefined();
            expect(notBuiltMessage(c, "child")).toContain(MOVE_WHERE);
            const parent = materializePlanNamed(c, "parent");
            expect(parent?.readTimeTerms?.map((t) => t.code)).toEqual([
               "org_id = $ORG_ID",
            ]);
         },
         { timeout: 30000 },
      );

      it(
         "still plans the term on the source itself, out of the build, whatever the given's default",
         async () => {
            const text = (org: number) => `${HEAD.replace(
               "ORG_ID :: number is 1",
               `ORG_ID :: number is ${org}`,
            )}
#(materialize) storage="lake"
source: orders is raw extend {
  where: org_id = $ORG_ID
  dimension: mine is org_id = $ORG_ID
  measure: n is count()
}
`;
            const sqls: string[] = [];
            for (const org of [1, 7]) {
               const { c } = await packages.plan(text(org));
               const p = materializePlanNamed(c, "orders");
               expect(p?.readTimeTerms?.map((t) => t.code)).toEqual([
                  "org_id = $ORG_ID",
               ]);
               expect(p?.leftOut?.map((d) => d.dimension)).toEqual(["mine"]);
               const sql = addressSQL(
                  plannedSourceNamed(c, "orders")!,
                  targetsIn(c)?.[p!.sourceID],
               );
               expect(sql).not.toMatch(new RegExp(`=\\s*${org}\\b`));
               expect(sql).not.toContain('"mine"');
               sqls.push(sql);
            }
            expect(sqls[1]).toBe(sqls[0]);
         },
         { timeout: 30000 },
      );
   });

   describe("a caller-dependent dimension a base declares", () => {
      it(
         "refuses the source when it reads a given",
         async () => {
            const { c } = await packages.plan(`${HEAD}
source: scoped is raw extend { dimension: mine is org_id = $ORG_ID }
#(materialize) storage="lake"
source: orders is scoped extend { measure: n is count() }
`);
            expect(materializePlanNamed(c, "orders")).toBeUndefined();
            expect(notBuiltMessage(c, "orders")).toContain(MOVE_DIMENSION);
         },
         { timeout: 30000 },
      );

      it(
         "refuses a term that reads its given through a base's dimension",
         async () => {
            const { c } = await packages.plan(`${HEAD}
source: scoped is raw extend {
  dimension: is_mine is org_id = $ORG_ID
  where: is_mine
}
#(materialize) storage="lake"
source: orders is scoped extend { measure: n is count() }
`);
            expect(materializePlanNamed(c, "orders")).toBeUndefined();
            expect(notBuiltMessage(c, "orders")).toContain(MOVE_DIMENSION);
         },
         { timeout: 30000 },
      );

      it(
         "stores a zone-dependent one as built, so the table is not marked zone-free for it",
         async () => {
            const { c } = await packages.plan(`${HEAD}
source: timed is duckdb.sql("select 1 as id, timestamp '2026-01-01 03:00:00' as ts")
source: base is timed extend { dimension: ts_day is ts.day }
#(materialize) storage="lake"
source: orders is base extend { measure: n is count() }
`);
            const p = materializePlanNamed(c, "orders");
            expect(p?.leftOut ?? []).toEqual([]);
            const day = p?.columns.find((col) => col.dimension === "ts_day");
            expect(day?.computed).toBe(true);
            expect(day?.zoneFree).toBeUndefined();
         },
         { timeout: 30000 },
      );
   });

   describe("the build copy takes out only a term the source's own declaration writes", () => {
      it("refuses a plan whose term lies in a base's declaration", () => {
         // Defense in depth: were the planner to place an inherited term on a
         // plan, the build copy would not find it in the source's declaration
         // and refuses, so the term is never written \`true\` in a base.
         const text = `source: scoped is raw extend {
  where: org_id = $ORG_ID
}
source: orders is scoped extend {
  measure: n is count()
}
`;
         const url = "file:///pkg/m.malloy";
         const plan: MaterializePlan = {
            sourceName: "orders",
            sourceID: `orders@${url}`,
            buildSourceName: "orders__mat",
            columns: [
               { dimension: "org_id", column: "org_id", computed: false },
            ],
            persist: { name: "orders", storage: "lake" },
            declaration: {
               url,
               range: {
                  start: { line: 3, character: 8 },
                  end: { line: 5, character: 1 },
               },
            },
            readTimeTerms: [
               {
                  kind: "given",
                  code: "org_id = $ORG_ID",
                  givens: ["ORG_ID"],
                  columns: ["org_id"],
                  at: [
                     {
                        url,
                        range: {
                           start: { line: 1, character: 9 },
                           end: { line: 1, character: 15 },
                        },
                     },
                     {
                        url,
                        range: {
                           start: { line: 1, character: 18 },
                           end: { line: 1, character: 25 },
                        },
                     },
                  ],
               },
            ],
         };
         expect(buildCopyText(text, plan)).toHaveProperty("reason");
      });
   });
});

describe("caller-dependent dimensions (P9)", () => {
   /**
    * P9 on the `#(materialize)` build side: a dimension whose value depends on
    * the caller (it reads a given, the current time or the query time zone) is
    * left out of the build, so the table never holds one caller's value, and the
    * served copy computes it at read. The probe compiler decides beside the IR
    * walks; a dimension it clears of the time zone is marked `zoneFree`.
    */

   const HEAD = `##! experimental { givens access_modifiers }
given:
  ORG_ID :: number is 1
source: raw is duckdb.sql("""
  select * from (values
    (1, 1, 'a', 10.5, TIMESTAMP '2026-01-01 03:00:00', DATE '2026-01-01'),
    (2, 2, 'b', 20.0, TIMESTAMP '2026-01-02 23:30:00', DATE '2026-01-02')
  ) as t(id, org_id, s, amount, ts, d)
""")
`;

   const packages = planPackages({}, { keepIncrementalUnbuildable: true });

   const ORDERS = `${HEAD}
source: base is raw extend { dimension: base_hour is hour(ts) }
#(materialize) storage="lake"
source: orders is base extend {
  dimension: mine is org_id = $ORG_ID
  dimension: label is pick 'me' when mine else 'other'
  internal dimension: imine is org_id = $ORG_ID
  dimension: hr is hour(ts)
  dimension: ts_month is ts.month
  dimension: d_month is d.month
  dimension: age is hours(ts to now)
  dimension: big is amount > 10
  measure: n is count()
}
`;

   describe("P9: caller-dependent dimensions are left out of the build", () => {
      it(
         "leaves out what reads a given, the current time or the zone, and stores the rest",
         async () => {
            const { pkg, c } = await packages.plan(ORDERS);
            const p = materializePlanNamed(c, "orders");
            expect(p).toBeDefined();
            expect(
               p?.leftOut?.map(({ dimension, reads }) => ({
                  dimension,
                  reads,
               })),
            ).toEqual([
               // `base_hour` is zone-dependent but declared on a base, whose
               // text the served copy does not keep: it is stored as built.
               { dimension: "mine", reads: ["given"] },
               { dimension: "label", reads: ["given"] },
               { dimension: "imine", reads: ["given"] },
               { dimension: "hr", reads: ["time_zone"] },
               { dimension: "ts_month", reads: ["time_zone"] },
               { dimension: "age", reads: ["current_time"] },
            ]);
            expect(
               p?.columns.map((col) =>
                  col.zoneFree ? `${col.column}*` : col.column,
               ),
            ).toEqual([
               "id",
               "org_id",
               "s",
               "amount",
               "ts",
               "d",
               "base_hour",
               "d_month*",
               "big*",
            ]);
            // The build selects none of them, so it reads no given and bakes no
            // default: the eligibility gate admits it.
            const source = Object.values(sourcesIn(c)).find(
               (s) => s.name === "orders",
            );
            const sql = addressSQL(source!, targetsIn(c)?.[p!.sourceID]);
            for (const left of ["mine", "label", "imine", "hr", "age"]) {
               expect(sql).not.toContain(`"${left}"`);
            }
            expect(sql).not.toMatch(/=\s*1\b/);
            const outcome = await computePackageBuildPlan(pkg);
            expect(outcome.plan?.refusedSources?.[p!.sourceID]).toBeUndefined();
            expect(outcome.sourceEligibility?.eligible).toContain(p!.sourceID);
         },
         { timeout: 30000 },
      );

      it(
         "refuses a given-reading dimension declared on a base in the same file",
         async () => {
            const { c } = await packages.plan(`${HEAD}
source: base is raw extend { dimension: mine is org_id = $ORG_ID }
source: mid is base extend { dimension: big is amount > 10 }
#(materialize) storage="lake"
source: orders is mid extend { measure: n is count() }
`);
            expect(materializePlanNamed(c, "orders")).toBeUndefined();
            expect(notBuiltMessage(c, "orders")).toContain(
               "declare the given-reading dimension on the materialized source itself",
            );
         },
         { timeout: 30000 },
      );

      it(
         "refuses a given-reading dimension declared on a base in another file",
         async () => {
            const pkg = await packages.create({
               "b.malloy": `${HEAD}
source: base is raw extend { dimension: mine is org_id = $ORG_ID }
`,
               "m.malloy": `##! experimental { givens access_modifiers }
import "b.malloy"
#(materialize) storage="lake"
source: orders is base extend { measure: n is count() }
`,
            });
            const c = await compilePackageBuildPlan(pkg);
            expect(materializePlanNamed(c, "orders")).toBeUndefined();
            expect(notBuiltMessage(c, "orders")).toContain(
               "the dimension `mine` reads a given and is not declared on this source",
            );
         },
         { timeout: 30000 },
      );

      it(
         "leaves out a dimension reading the given its own where: reads (the IR walk finds it)",
         async () => {
            const { c } = await packages.plan(`${HEAD}
#(materialize) storage="lake"
source: scoped is raw extend {
  where: org_id = $ORG_ID
  dimension: mine is org_id = $ORG_ID
  dimension: big is amount > 10
}
`);
            const p = materializePlanNamed(c, "scoped");
            expect(p?.leftOut).toEqual([
               { dimension: "mine", reads: ["given"] },
            ]);
            expect(p?.columns.map((col) => col.column)).not.toContain("mine");
            // P2 still leaves the term out of the build.
            expect(p?.readTimeTerms?.map((t) => t.code)).toEqual([
               "org_id = $ORG_ID",
            ]);
            expect(
               p?.columns.find((col) => col.column === "big")?.zoneFree,
            ).toBe(true);
         },
         { timeout: 30000 },
      );

      it(
         "without the probe compiler, the IR walks still leave out given and clock readers and clear nothing",
         async () => {
            const contents = await authorContents(ORDERS);
            const unprobed = planModelMaterialize(contents).plans.find(
               (q) => q.sourceName === "orders",
            )!;
            expect(unprobed.leftOut?.map((d) => d.dimension)).toEqual([
               "mine",
               "label",
               "imine",
               "age",
            ]);
            expect(unprobed.columns.some((col) => col.zoneFree)).toBe(false);
            // The zone-dependent ones are stored, as before P9: a timezone:
            // query over the table is then declined.
            expect(unprobed.columns.map((col) => col.column)).toContain("hr");
         },
         { timeout: 30000 },
      );

      it(
         "honours a probe answer the IR walks do not see: a given, the clock, or an unsettled zone",
         async () => {
            const contents = await authorContents(ORDERS);
            const sourceID = planModelMaterialize(contents).plans.find(
               (q) => q.sourceName === "orders",
            )!.sourceID;
            const planned = planModelMaterialize(
               contents,
               undefined,
               undefined,
               new Map([
                  [
                     sourceID,
                     new Map([
                        ["big", { givens: ["given/x"] }],
                        [
                           "d_month",
                           { givens: [], currentTime: true, timeZone: false },
                        ],
                        ["hr", { givens: [], currentTime: false }],
                     ]),
                  ],
               ]),
            ).plans.find((q) => q.sourceID === sourceID)!;
            expect(planned.leftOut).toContainEqual({
               dimension: "big",
               reads: ["given"],
            });
            expect(planned.leftOut).toContainEqual({
               dimension: "d_month",
               reads: ["current_time"],
            });
            // A zone the probe could not settle is neither left out nor cleared.
            const hr = planned.columns.find((col) => col.column === "hr");
            expect(hr).toBeDefined();
            expect(hr?.zoneFree).toBeUndefined();
         },
         { timeout: 30000 },
      );

      it(
         "refuses an inlined join through which a given is read",
         async () => {
            const { c } = await packages.plan(`${HEAD}
source: orgs is duckdb.sql("select 1 as org_id, 'x' as name") extend {
  primary_key: org_id
  dimension: is_mine is org_id = $ORG_ID
}
#(materialize) storage="lake" inline=["orgs"]
source: orders is raw extend {
  join_one: orgs with org_id
}
`);
            expect(materializePlanNamed(c, "orders")).toBeUndefined();
            expect(notBuiltMessage(c, "orders")).toContain(
               "inline= names `orgs`, through which a given is read",
            );
         },
         { timeout: 30000 },
      );
   });

   /** The compiled `contents` of a one-file model, for calling the planner directly. */
   async function authorContents(
      text: string,
   ): Promise<Record<string, unknown>> {
      const { Runtime, InMemoryURLReader, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const url = new URL("file:///p9/m.malloy");
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url.toString(), text]])),
         connections: new FixedConnectionMap(
            new Map([["duckdb", new DuckDBConnection("duckdb", ":memory:")]]),
            "duckdb",
         ),
      });
      const model = await runtime.loadModel(url).getModel();
      return model._modelDef.contents as Record<string, unknown>;
   }
});
