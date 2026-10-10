// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The gate's safety property, asserted WITHOUT reference to where a given sits.
//
// `classifyDynamicTerms` decides by position, reading `query.givenUsage` — a
// summary of what the pipeline REFERENCES. The property that actually matters is
// what the build BAKES, and those two are not the same question. They have
// already diverged once: a given bound as a source ARGUMENT
// (`pp(x is $ORG_ID) -> …`) is substituted without being referenced, which the
// marker reads as empty, and it took a bespoke walk to catch. One known
// counterexample makes the equivalence empirical rather than structural, and the
// next one gets found the same way — by someone thinking to look for it.
//
// So this asks the question directly, with an oracle that needs no knowledge of
// IR shape, of join pruning, or of which positions exist: compile each source
// twice with DIFFERENT given defaults and compare the build SQL. If the SQL
// differs, the build substituted a value, and the artifact is one caller's
// slice. That is the definition, not a proxy for it.
//
// The invariant, one direction only: **a source whose build SQL depends on a
// given's value must be refused.** The converse is deliberately not asserted —
// the gate over-refuses on purpose (a given in a declared dimension, a join's
// `on:`, or the `where:` of a joined source that is not itself materialized is
// refused although the build leaves it out), and over-refusal costs coverage
// while under-refusal serves one tenant's rows to another.
//
// A new shape belongs in CASES below whether or not anyone knows which way it
// goes. That is the point: the oracle answers, rather than the author.
import type { FixedConnectionMap } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   compilePersistSources,
   duckdbTestConnections,
} from "./incremental_test_harness";
import { classifyDynamicTerms } from "./persist_dynamic_terms";

let connections: FixedConnectionMap;

beforeAll(() => {
   ({ connections } = duckdbTestConnections());
});

/**
 * The model preamble, with the given's default as the only variable. Everything
 * a case can reach is declared here so a case is one `#@ persist` source.
 */
function preamble(givenDefault: string): string {
   return `##! experimental { persistence givens parameters }
given:
  ORG_ID :: number is ${givenDefault}
source: raw is duckdb.sql("""SELECT * FROM (VALUES (1,7,'a'),(2,9,'b'),(1,8,'c')) AS t(org_id, user_id, s)""")
source: vis is duckdb.sql("""SELECT * FROM (VALUES (1,7),(2,9)) AS v(org_id, user_id)""")
source: scoped is raw extend { where: org_id = $ORG_ID }
source: pp(x::number is 0) is raw extend { where: org_id = x }
#@ persist name="grants" storage=lake
source: grants is vis -> { select: * } extend { where: org_id = $ORG_ID }
`;
}

const CASES: Record<string, string> = {
   // A join to a materialized given-scoped source, which the gate admits: the
   // join is not in the build, and the joined source's binding re-applies its
   // own term per caller.
   "a join to a materialized given-scoped source": `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: g is grants on user_id = g.user_id
  dimension: visible is g.user_id is not null
}`,

   // The same join, read BY the persisted query: the grant table's term is now
   // in the build, so the artifact holds one caller's grants.
   "a materialized given-scoped source read by the persisted query": `#@ persist name="p" storage=lake
source: p is raw extend {
  join_one: g is grants on user_id = g.user_id
} -> { group_by: s, granted is g.user_id is not null; aggregate: n is count() }`,

   "extend-block where:": `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend { where: org_id = $ORG_ID }`,

   "inside the persisted query": `#@ persist name="p" storage=lake
source: p is raw -> { where: org_id = $ORG_ID; select: * }`,

   "in a group_by": `#@ persist name="p" storage=lake
source: p is raw -> { group_by: s, mine is org_id = $ORG_ID; aggregate: n is count() }`,

   "inherited and read by the query": `#@ persist name="p" storage=lake
source: p is scoped -> { group_by: s; aggregate: n is count() }`,

   "inherited through extend": `#@ persist name="p" storage=lake
source: p is scoped extend { where: s != 'zzz' }`,

   "a declared dimension": `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend { dimension: mine is org_id = $ORG_ID }`,

   "a join's on: condition": `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: v is vis on v.user_id = user_id and v.org_id = $ORG_ID
}`,

   "a joined source's own where:": `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: v is (vis extend { where: org_id = $ORG_ID }) on v.user_id = user_id
}`,

   // The pair that decides the gate's one narrowing: the same join, read and
   // unread by the persisted query.
   "a given-scoped join the query does not read": `#@ persist name="p" storage=lake
source: p is raw extend { join_one: g is scoped on g.user_id = user_id } -> {
  aggregate: c is count()
}`,

   "a given-scoped join the query reads": `#@ persist name="p" storage=lake
source: p is raw extend { join_one: g is scoped on g.user_id = user_id } -> {
  group_by: gs is g.s
  aggregate: c is count()
}`,

   // The shape that made the marker empirical: bound as a source argument, so
   // substituted without ever being referenced.
   "bound as a source argument": `#@ persist name="p" storage=lake
source: p is pp(x is $ORG_ID) -> { select: * }`,

   "no given at all": `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend { where: s = 'a' }`,
};

/** The source's build SQL under one given default, or null if it cannot build. */
async function buildSQLUnder(
   body: string,
   givenDefault: string,
): Promise<string | null> {
   const { sources } = await compilePersistSources(
      connections,
      `${preamble(givenDefault)}\n${body}`,
   );
   const source = sources["p"];
   if (!source) return null;
   try {
      return await source.getSQL();
   } catch {
      // A source that cannot build under a value cannot be materialized under
      // it either; treated as "depends on the value" below, which is the
      // fail-closed reading.
      return null;
   }
}

describe("the build's dependence on a given's value decides the gate", () => {
   for (const [name, body] of Object.entries(CASES)) {
      it(`${name}: refused whenever the build SQL moves with the value`, async () => {
         const underOne = await buildSQLUnder(body, "1");
         const underTwo = await buildSQLUnder(body, "2");

         // Byte-identical build SQL under two different values means the value
         // reached none of it. Anything else — including a source that builds
         // under one value and not the other — means it did.
         const bakesTheValue =
            underOne === null || underTwo === null || underOne !== underTwo;

         const { sources } = await compilePersistSources(
            connections,
            `${preamble("1")}\n${body}`,
         );
         const refused = !classifyDynamicTerms(sources["p"]).ok;

         if (bakesTheValue) {
            expect(refused).toBe(true);
         }
         // No assertion on the other branch: the gate over-refuses by design.
      });
   }

   it("a join to a materialized given-scoped source does not bake and is admitted, and reading it in the query bakes and is refused", async () => {
      // The loop above only asserts one direction. The admission this shape
      // carries is the converse, so pin both halves for it explicitly.
      const verdict = async (name: string) => {
         const body = CASES[name];
         const a = await buildSQLUnder(body, "1");
         const b = await buildSQLUnder(body, "2");
         const { sources } = await compilePersistSources(
            connections,
            `${preamble("1")}\n${body}`,
         );
         return {
            bakes: a === null || b === null || a !== b,
            admitted: classifyDynamicTerms(sources["p"]).ok,
         };
      };
      expect(
         await verdict("a join to a materialized given-scoped source"),
      ).toEqual({ bakes: false, admitted: true });
      expect(
         await verdict(
            "a materialized given-scoped source read by the persisted query",
         ),
      ).toEqual({ bakes: true, admitted: false });
   });

   it("covers both halves, so the invariant is not vacuously satisfied", async () => {
      // A suite where nothing bakes would pass every case above while asserting
      // nothing. Pin that the corpus exercises the branch that carries the
      // safety property, and the one that would catch a gate refusing
      // everything.
      const verdicts = await Promise.all(
         Object.values(CASES).map(async (body) => {
            const a = await buildSQLUnder(body, "1");
            const b = await buildSQLUnder(body, "2");
            return a === null || b === null || a !== b;
         }),
      );
      expect(verdicts).toContain(true);
      expect(verdicts).toContain(false);
   });
});
