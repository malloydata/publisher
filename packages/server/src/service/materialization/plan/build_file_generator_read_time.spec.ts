// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { makeDigest, Runtime } from "@malloydata/malloy";
import {
   duckdbTestConnections,
   tempBuildPlanPackages,
} from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { addressSQL, buildSQLOf, computeSourceEntityId } from "./build_plan";
import {
   compilePackageBuildPlan,
   computePackageBuildPlan,
} from "./build_plan_compile";
import {
   buildCopyText,
   generateBuildFile,
   givenReadTerms,
   planModelMaterialize,
   type MaterializePlan,
} from "./build_file_generator";
import { admitGivenPositions } from "./persist_dynamic_terms";

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

const packages = tempBuildPlanPackages();
let restore: () => void;

function settings(env: Record<string, string> = {}) {
   restore?.();
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true", ...env },
   }).restore;
}

beforeEach(() => settings());
afterEach(async () => {
   restore?.();
   await packages.cleanup();
});

async function compiled(text: string) {
   const pkg = await packages.create(text);
   return { pkg, c: await compilePackageBuildPlan(pkg) };
}

/** The generated build source, presented under the author's sourceID. */
function generated(c: Awaited<ReturnType<typeof compilePackageBuildPlan>>) {
   const [sourceID] = Object.keys(c.materializePlans ?? {});
   const source = c.sources[sourceID];
   if (!source) throw new Error("no #(materialize) source was planned");
   return { sourceID, source, target: c.targets?.[sourceID] };
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
         const { pkg, c } = await compiled(ORDERS);
         const plan = Object.values(c.materializePlans ?? {})[0];
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
         const { c } = await compiled(ORDERS);
         const plan = Object.values(c.materializePlans ?? {})[0];
         // The text the package's model file holds.
         const result = buildCopyText(ORDERS, plan);
         if (!("copy" in result)) throw new Error(result.reason);
         expect(result.copy).toContain("where: true, s != 'zz'");
         expect(result.copy).not.toContain("$ORG_ID");
         const text = generateBuildFile(
            [plan],
            "m.malloy",
            undefined,
            new Map([[plan.sourceID, "m.malloy.0.materialize-build.malloy"]]),
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
         const plan = Object.values(c.materializePlans ?? {})[0];
         expect(plan.givenDeclarations?.map((g) => g.name)).toEqual(["ORG_ID"]);
         expect(plan.givenDeclarations?.[0].url).toEndWith("/g.malloy");
      },
      { timeout: 30000 },
   );

   it(
      "leaves a source with no given-reading where: exactly as it was",
      async () => {
         const { c } = await compiled(`${HEAD}
#(materialize) storage="lake"
source: orders is raw extend { where: s != 'zz' }
`);
         const plan = Object.values(c.materializePlans ?? {})[0];
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
      const { c } = await compiled(text);
      // Nothing is planned for it, and so nothing is built.
      expect(Object.keys(c.materializePlans ?? {})).toEqual([]);
      expect(Object.values(c.sources).some((s) => s.name === "orders")).toBe(
         false,
      );
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
      "refuses a given term written on a source this one extends",
      async () => {
         expect(
            await refusal(`${HEAD}
source: scoped is raw extend { where: org_id = $ORG_ID }
#(materialize) storage="lake"
source: orders is scoped extend { dimension: big is amount > 10 }
`),
         ).toContain("not on this source's own declaration");
      },
      { timeout: 30000 },
   );

   it(
      "refuses a given term over a field the table does not store",
      async () => {
         expect(
            await refusal(`${HEAD}
#(materialize) storage="lake"
source: orders is raw extend {
  private dimension: org is org_id
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
         columns: [{ dimension: "org_id", column: "org_id", computed: false }],
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
      "keeps another source's given term in a build that joins it, so the gate refuses that build",
      async () => {
         // One copy per source: were `a`'s term taken out of `b`'s build too,
         // `b` would store `aa.s` from every caller's `a` rows. The join is
         // inlined, so `b`'s build reads `a` (a reference join is not built).
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
         const { pkg, c } = await compiled(text);
         const ids = Object.fromEntries(
            Object.entries(c.sources).map(([id, s]) => [s.name, id]),
         );
         const bSQL = buildSQLOf(
            c.sources[ids.b],
            { entries: {} },
            c.targets?.[ids.b],
         );
         expect(bSQL).toMatch(/"org_id"\s*=\s*1/);
         const { plan } = await computePackageBuildPlan(pkg);
         expect(plan?.refusedSources?.[ids.b]?.reason).toBe(
            "given_in_persisted_query",
         );
         expect(plan?.refusedSources?.[ids.a]).toBeUndefined();
         // `a`'s own build still leaves its term out.
         expect(
            buildSQLOf(c.sources[ids.a], { entries: {} }, c.targets?.[ids.a]),
         ).not.toMatch(/"org_id"\s*=\s*1/);
      },
      { timeout: 30000 },
   );

   it(
      "refuses a source whose build reads a #@ persist source, rather than plan that source twice",
      async () => {
         const { c } = await compiled(`##! experimental { givens persistence }
given: ORG_ID :: number is 1
source: raw is duckdb.sql("select 1 as org_id, 'a' as s")
#@ persist name="base_t"
source: base is raw -> { select: org_id, s }
#(materialize) storage="lake"
source: orders is base -> { select: org_id, s } extend { where: org_id = $ORG_ID }
`);
         expect(Object.keys(c.materializePlans ?? {})).toEqual([]);
         const nodes = c.builds.flatMap((b) => b.order);
         expect(nodes.every((id) => !id.includes(".materialize-build."))).toBe(
            true,
         );
         expect(nodes.every((id) => c.sources[id] !== undefined)).toBe(true);
         expect(notBuilt(c, "orders")).toContain("#@ persist source (base)");
      },
      { timeout: 30000 },
   );

   it(
      "refuses a term the served copy would not keep (a parenthesised head), so no table is built for nothing",
      async () => {
         const { c } = await compiled(`${HEAD}
#(materialize) storage="lake"
source: orders is (raw extend { where: org_id = $ORG_ID }) extend {
  dimension: big is amount > 10
}
`);
         expect(Object.keys(c.materializePlans ?? {})).toEqual([]);
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
         const { c } = await compiled(`${DATED}
#(materialize) storage="lake"
source: orders is dated extend {
  where: t >= now - 30 days
  where: org_id = $ORG_ID
}
`);
         const plan = Object.values(c.materializePlans ?? {})[0];
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
         const { c } = await compiled(`${DATED}
#(materialize) storage="lake"
source: orders is dated extend {
  where: org_id = $ORG_ID and t >= now - 30 days
}
`);
         expect(Object.keys(c.materializePlans ?? {})).toEqual([]);
         const reason = (c.annotationFindings ?? []).find(
            (f) => f.code === "materialize-not-built" && f.subject === "orders",
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
         const first = generated((await compiled(text)).c);
         const sql = addressSQL(first.source, first.target);
         expect(sql).toMatch(/LOCALTIMESTAMP|CURRENT_TIMESTAMP/);
         expect(sql).not.toMatch(/20\d\d-\d\d-\d\d \d\d:\d\d/);
         const second = generated((await compiled(text)).c);
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
            settings({ PERSIST_STORAGE_MODE: "on", ...env });
            const { c } = await compiled(ORDERS);
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
