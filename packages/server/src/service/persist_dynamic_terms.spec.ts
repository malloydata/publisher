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

describe("classifyDynamicTerms: read-time positions the serve shape cannot reproduce", () => {
   // These are NOT in the build either — each case asserts that — so none is a
   // leak at build time. They are refused because the serve shape has no way to
   // re-bind the given per caller in that position.
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

   it("refuses a given-scoped source joined through a refinement rather than by name", async () => {
      const source = await persistSource(
         `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: v is (vis extend { where: org_id = $ORG_ID }) on v.user_id = user_id
}`,
      );
      expect(await buildSQL(source)).not.toContain('org_id"=1');

      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) {
         expect(result.reason).toBe("dynamic_joined_where");
         expect(result.detail).toContain("refinement");
      }
   });
});

describe("classifyDynamicTerms: a join to a materialized given-scoped source", () => {
   // The per-user visibility idiom: a persisted source joins a grant table that
   // is itself persisted and scoped by the caller's givens, and a dimension
   // decides visibility by null-checking the join. The join is not in the build,
   // and at read it is re-emitted only against the grant table's own binding,
   // which re-applies the grant table's terms per caller.
   const GRANTS = `
source: grants_raw is duckdb.sql("""SELECT * FROM (VALUES (1,7,'a'),(2,9,'b')) AS g(org_id, user_id, s)""")
#@ persist name="grants" storage=lake
source: grants is grants_raw -> { select: * } extend {
  where: org_id = $ORG_ID and user_id = $USER_ID
}
`;
   const HEAD_USER = HEAD.replace(
      "ORG_ID :: number is 1",
      "ORG_ID :: number is 1\n  USER_ID :: number is 7",
   );

   async function persistSourceWithGrants(
      body: string,
   ): Promise<PersistSource> {
      const { sources } = await compilePersistSources(
         connections,
         `${HEAD_USER}${GRANTS}\n${body}`,
      );
      const source = sources["p"];
      expect(source).toBeDefined();
      return source;
   }

   it("admits it, and records the grant table's terms against the alias", async () => {
      const source = await persistSourceWithGrants(
         `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  where: org_id = $ORG_ID
  join_one: g is grants on s = g.s
  dimension:
    visible is g.s is not null
    shown is pick s when visible else '[Hidden]'
}`,
      );
      // The artifact is the relation alone: neither the grant table nor either
      // given's default reaches the build.
      const sql = await buildSQL(source);
      expect(sql).not.toContain("AS g(org_id, user_id, s)");
      expect(sql).not.toContain('user_id"=7');
      expect(sql).not.toContain('org_id"=1');

      expect(classifyDynamicTerms(source)).toEqual({
         ok: true,
         terms: [
            {
               code: "org_id = $ORG_ID",
               givens: ["ORG_ID"],
               columns: ["org_id"],
            },
         ],
         joinedTerms: [
            {
               alias: "g",
               source: "grants",
               terms: [
                  {
                     code: "org_id = $ORG_ID and user_id = $USER_ID",
                     givens: ["ORG_ID", "USER_ID"],
                     columns: ["org_id", "user_id"],
                  },
               ],
            },
         ],
      });
   });

   it("carries every term the grant table accumulates through extend", async () => {
      // `filterList` accumulates through `extend`, so a grant table declared over
      // a scoped extension re-applies BOTH terms. Recording only its own would
      // report a narrower scope than its binding applies.
      const source = await persistSourceWithGrants(
         `source: org_grants is grants_raw -> { select: * } extend { where: org_id = $ORG_ID }
#@ persist name="user_grants" storage=lake
source: user_grants is org_grants extend { where: user_id = $USER_ID }
#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: g is user_grants on s = g.s
}`,
      );
      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(true);
      if (result.ok) {
         expect(result.joinedTerms?.[0]?.terms.map((t) => t.code)).toEqual([
            "org_id = $ORG_ID",
            "user_id = $USER_ID",
         ]);
      }
   });

   it("admits a join_many to the grant table on the same terms", async () => {
      // A join_many can fan out, and the live query fans out identically: the
      // serve shape re-emits the same keyword over rows the live join would read.
      const source = await persistSourceWithGrants(
         `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_many: g is grants on s = g.s
}`,
      );
      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(true);
   });

   it("refuses the join when the grant table is not persisted", async () => {
      const source = await persistSourceWithGrants(
         `source: live_grants is grants_raw -> { select: * } extend {
  where: user_id = $USER_ID
}
#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: g is live_grants on s = g.s
}`,
      );
      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) {
         expect(result.reason).toBe("dynamic_joined_where");
         expect(result.detail).toContain("'live_grants'");
         expect(result.detail).toContain("storage destination");
      }
   });

   it("refuses the join when the grant table is persisted colocated, without storage=", async () => {
      // Only a storage binding is on the serve shape, so a colocated grant table
      // leaves the join nothing to be re-emitted against.
      const source = await persistSourceWithGrants(
         `#@ persist name="colo_grants"
source: colo_grants is grants_raw -> { select: * } extend {
  where: user_id = $USER_ID
}
#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: g is colo_grants on s = g.s
}`,
      );
      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe("dynamic_joined_where");
   });

   it("refuses the join when the grant table's own build would bake a given", async () => {
      const source = await persistSourceWithGrants(
         `#@ persist name="baked_grants" storage=lake
source: baked_grants is grants_raw -> { where: user_id = $USER_ID; select: * }
#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: g is baked_grants on s = g.s
}`,
      );
      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) {
         expect(result.reason).toBe("dynamic_joined_where");
         expect(result.detail).toContain("BUILT");
      }
   });

   it("holds the grant table's own joins to the same rule, transitively", async () => {
      const chain = `#@ persist name="teams" storage=lake
source: teams is grants_raw -> { select: * } extend { where: user_id = $USER_ID }
#@ persist name="team_grants" storage=lake
source: team_grants is grants_raw -> { select: * } extend {
  where: org_id = $ORG_ID
  join_one: t is teams on s = t.s
}
#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: g is team_grants on s = g.s
}`;
      const admitted = classifyDynamicTerms(
         await persistSourceWithGrants(chain),
      );
      expect(admitted.ok).toBe(true);
      if (admitted.ok) {
         expect(admitted.joinedTerms?.map((j) => [j.alias, j.source])).toEqual([
            ["g", "team_grants"],
            ["g.t", "teams"],
         ]);
      }

      // The same chain with the second hop left unmaterialized: the grant table
      // could not itself be admitted, so neither can the source joining it.
      const refused = classifyDynamicTerms(
         await persistSourceWithGrants(
            chain.replace('#@ persist name="teams" storage=lake\n', ""),
         ),
      );
      expect(refused.ok).toBe(false);
      if (!refused.ok) {
         expect(refused.reason).toBe("dynamic_joined_where");
         expect(refused.detail).toContain("'teams'");
      }
   });

   it("still refuses a dimension that reads a given itself beside the join", async () => {
      const source = await persistSourceWithGrants(
         `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: g is grants on s = g.s
  dimension: mine is g.s is not null or user_id = $USER_ID
}`,
      );
      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe("dynamic_projection");
   });

   it("still refuses a given in the join's on: condition", async () => {
      const source = await persistSourceWithGrants(
         `#@ persist name="p" storage=lake
source: p is raw -> { select: * } extend {
  join_one: g is grants on s = g.s and g.user_id = $USER_ID
}`,
      );
      const result = classifyDynamicTerms(source);
      expect(result.ok).toBe(false);
      if (!result.ok) expect(result.reason).toBe("dynamic_join");
   });
});
