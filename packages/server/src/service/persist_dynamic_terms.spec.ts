// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Where a given sits decides whether a source can be materialized into a
// storage destination and served per caller. Each case here pairs the
// classification with the BUILD SQL that justifies it: a position is admitted
// only when the given's value is provably absent from the artifact, so every
// admitted case asserts that absence rather than trusting the classifier's own
// account of it.
import type { FixedConnectionMap, PersistSource } from "@malloydata/malloy";
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
 * A default on the given is deliberate throughout: without one the baked shapes
 * cannot compile at all (`Given 'ORG_ID' has no value and no default`), so the
 * refusals they exercise would be unreachable and the tests would pass for the
 * wrong reason.
 */
const HEAD = `##! experimental { persistence givens }
given:
  ORG_ID :: number is 1
source: raw is duckdb.sql("""SELECT * FROM (VALUES (1,7,'a'),(2,9,'b'),(1,8,'c')) AS t(org_id, user_id, s)""")
source: vis is duckdb.sql("""SELECT * FROM (VALUES (1,7),(2,9)) AS v(org_id, user_id)""")
source: scoped is raw extend { where: org_id = $ORG_ID }
`;

async function persistSource(body: string): Promise<PersistSource> {
   const { sources } = await compilePersistSources(
      connections,
      `${HEAD}\n${body}`,
   );
   const source = sources["p"];
   expect(source).toBeDefined();
   return source;
}

/** The build SQL, whitespace-collapsed, or the error persistence raised. */
async function buildSQL(source: PersistSource): Promise<string> {
   try {
      return (await source.getSQL()).replace(/\s+/g, " ");
   } catch (err) {
      return `THREW: ${err instanceof Error ? err.message : String(err)}`;
   }
}

describe("classifyDynamicTerms: positions that strip", () => {
   it("admits an extend-block where:, and the build carries no trace of it", async () => {
      const source = await persistSource(
         `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend { where: org_id = $ORG_ID }`,
      );
      // The assertion that makes the admission mean something: the value the
      // given would have taken is NOT in the artifact, so the rows are every
      // caller's and the term is the reader's to apply.
      expect(await buildSQL(source)).not.toContain('org_id"=1');

      const result = classifyDynamicTerms(source);
      expect(result).toEqual({
         ok: true,
         terms: [
            {
               code: "org_id = $ORG_ID",
               givens: ["ORG_ID"],
               columns: ["org_id"],
            },
         ],
      });
   });

   it("admits a filter inherited through extend, which is how a shared scope is written", async () => {
      const source = await persistSource(
         `#@ persist name="p" storage=lake
source: p is scoped extend { where: s != 'zzz' }`,
      );
      expect(await buildSQL(source)).not.toContain('org_id"=1');

      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(true);
      // `filterList` accumulates through `extend`, so the base's term arrives
      // here as the source's own and is stripped like one. The source's OWN
      // static term is not returned: only a term that reads a given has to be
      // re-applied with a caller's value, and a static one rides the serve
      // shape's ordinary filter re-emission whether or not this pass names it.
      if (result.ok) {
         expect(result.terms).toEqual([
            {
               code: "org_id = $ORG_ID",
               givens: ["ORG_ID"],
               columns: ["org_id"],
            },
         ]);
      }
   });

   it("reports no terms for a source with only a static where:", async () => {
      const source = await persistSource(
         `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend { where: s = 'a' }`,
      );
      expect(classifyDynamicTerms(source)).toEqual({ ok: true, terms: [] });
   });
});

describe("classifyDynamicTerms: positions the build bakes", () => {
   it("refuses a given inside the persisted query, naming the value it baked", async () => {
      const source = await persistSource(
         `#@ persist name="p" storage=lake
source: p is raw -> { where: org_id = $ORG_ID; select: * }`,
      );
      // The refusal's justification, asserted rather than assumed: the
      // default's value IS in the artifact, so no read-time term can undo it.
      expect(await buildSQL(source)).toContain('org_id"=1');

      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe("given_in_persisted_query");
   });

   it("refuses a given in a group_by, which lands as a projected column", async () => {
      const source = await persistSource(
         `#@ persist name="p" storage=lake
source: p is raw -> { group_by: s, mine is org_id = $ORG_ID; aggregate: n is count() }`,
      );
      expect(await buildSQL(source)).toContain('org_id"=1');

      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe("given_in_persisted_query");
   });

   it("refuses a query over a given-scoped input source", async () => {
      // The shape `mz-storage/05-givens-refused` uses, and the reason it stays
      // refused after this change: the persisted QUERY reads the input's
      // filter, so the predicate is in the build and `filterList` is empty —
      // there is nothing left to re-apply at read.
      const source = await persistSource(
         `#@ persist name="p" storage=lake
source: p is scoped -> { group_by: s; aggregate: n is count() }`,
      );
      expect(await buildSQL(source)).toContain('org_id"=1');

      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe("given_in_persisted_query");
   });
});

describe("classifyDynamicTerms: read-time positions v1 does not admit", () => {
   // These three are NOT in the build either — each case asserts that — so none
   // is a leak today. They are refused because admitting them is a separate
   // decision about what the serve shape reproduces, and this pass refuses what
   // it has not proven. If that decision is taken, these are the tests that
   // change, and the build-SQL assertions beside them are why it is safe to.
   it("refuses a given in a declared dimension", async () => {
      const source = await persistSource(
         `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend { dimension: mine is org_id = $ORG_ID }`,
      );
      expect(await buildSQL(source)).not.toContain('org_id"=1');

      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) {
         expect(result.reason).toBe("dynamic_projection");
         expect(result.detail).toContain("mine");
      }
   });

   it("refuses a given in a join's on: condition", async () => {
      const source = await persistSource(
         `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: v is vis on v.user_id = user_id and v.org_id = $ORG_ID
}`,
      );
      expect(await buildSQL(source)).not.toContain('org_id"=1');

      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) {
         expect(result.reason).toBe("dynamic_join");
         // The alias the author wrote, not the joined source's `sql://` identity.
         expect(result.detail).toContain("'v'");
      }
   });

   it("refuses a given-scoped source reached through a join", async () => {
      const source = await persistSource(
         `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: v is (vis extend { where: org_id = $ORG_ID }) on v.user_id = user_id
}`,
      );
      expect(await buildSQL(source)).not.toContain('org_id"=1');

      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe("dynamic_joined_where");
   });
});
