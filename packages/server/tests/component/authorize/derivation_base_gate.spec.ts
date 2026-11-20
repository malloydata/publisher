// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A gate inherited through `->` filters the base's rows BEFORE the derivation
 * runs. For `Z is X -> { ... }` with `#(access_filter)` on `X`, each caller's
 * answer from `Z` is what `Z`'s query gives over only the rows `X`'s gate
 * admits for that caller: its aggregates, `all()`, renames, `limit` and window
 * functions see those rows and no others.
 *
 * Every expectation is computed by hand from the base rows, filtered first,
 * and compared as a string (rows sorted), per caller. Until 0.17.0 the gate
 * was grafted onto the derivation's output columns instead: a derivation that
 * renamed a column to the gate's field name was filtered on the wrong values,
 * `all()`, ranks and limits were computed over every caller's rows, and a gate
 * on a computed dimension denied every caller.
 */
import { describe, expect, it } from "bun:test";
import type { Model } from "../../../src/service/model";
import {
   expectAccessDenied,
   queryRows,
   withAuthorizeModel,
} from "../../../src/test_helpers/authorize_fixture";

// id, category, region, amount
const ROWS = `SELECT * FROM (VALUES
  (1, 'a', 'x', 1), (2, 'a', 'y', 2), (3, 'a', 'x', 4),
  (4, 'b', 'x', 8), (5, 'b', 'z', 16), (6, 'c', 'y', 32)
) AS t(id, category, region, amount)`;

const MODEL = `##! experimental.givens
given:
  GROUPS :: string[]
  SUB :: string
  ROLES :: string[]

source: raw is duckdb.sql("""${ROWS}""") extend {
  dimension: tenant is concat(category, '-T')
  measure: n is count()
  measure: amt is amount.sum()
}

#(access_filter) tenant in $GROUPS
source: gated is raw extend {}

// P6-03: the gate reads a computed dimension the derivation groups by.
source: inherits is gated -> { group_by: tenant, region; aggregate: n, amt }

// A rename onto the gate's field name: the output's tenant holds regions.
source: renamed is gated -> { group_by: tenant is region; aggregate: n }

source: shares is gated -> {
  group_by: category
  aggregate: n, share is n / all(n)
}

source: top2 is gated -> {
  group_by: tenant, region
  aggregate: amt
  order_by: amt desc
  limit: 2
}

source: ranked is gated -> {
  group_by: tenant, region
  aggregate: amt
  calculate: r is rank()
  order_by: amt desc
}

// A chain: the gate is two derivations down.
source: chained is inherits -> {
  group_by: tenant
  aggregate: total is amt.sum(), share is amt.sum() / all(amt.sum())
}

query: named_shares is shares -> { group_by: category, share }

source: cats is duckdb.sql("""SELECT * FROM (VALUES ('a'), ('b'), ('c')) AS t(category)""")

#(access_filter) category = $SUB
source: gated_sub is raw extend {}
source: by_sub is gated_sub -> {
  group_by: category
  aggregate: n, share is n / all(n)
}

#(authorize) 'admin' in $ROLES
source: locked is raw extend {}
source: locked_d is locked -> { group_by: category; aggregate: n }

// A given-reading where: on the base (not a gate) filters first in Malloy.
source: scoped is raw extend { where: tenant in $GROUPS }
source: scoped_d is scoped -> {
  group_by: category
  aggregate: n, share is n / all(n)
}

// A gate the derivation declares itself filters its own output, as before.
#(access_filter) tenant in $GROUPS
source: own_gated is raw -> {
  group_by: tenant
  aggregate: n, share is n / all(n)
}
`;

/** Rows as one string, sorted, so the order a query returns them in is moot. */
function canon(rows: Array<Record<string, unknown>>): string {
   return JSON.stringify(rows.map((r) => JSON.stringify(r)).sort());
}

const rowsOf = (
   model: Model,
   query: string,
   givens: Record<string, unknown>,
): Promise<string> => queryRows(model, query, givens).then(canon);

async function withModel(body: (model: Model) => Promise<void>) {
   await withAuthorizeModel(MODEL, async ({ model, error }) => {
      expect(error).toBeUndefined();
      await body(model);
   });
}

const A = { GROUPS: ["a-T"] };
const B = { GROUPS: ["b-T"] };
const AB = { GROUPS: ["a-T", "b-T"] };
const NONE = { GROUPS: ["nobody"] };

describe("a gate inherited through -> filters the base's rows first", () => {
   it("P6-03: a gate on a computed dimension answers each caller's own rows", async () => {
      await withModel(async (model) => {
         const q = "run: inherits -> { group_by: tenant, region, n, amt }";
         expect(await rowsOf(model, q, A)).toBe(
            canon([
               { tenant: "a-T", region: "x", n: 2, amt: 5 },
               { tenant: "a-T", region: "y", n: 1, amt: 2 },
            ]),
         );
         expect(await rowsOf(model, q, B)).toBe(
            canon([
               { tenant: "b-T", region: "x", n: 1, amt: 8 },
               { tenant: "b-T", region: "z", n: 1, amt: 16 },
            ]),
         );
         expect(await rowsOf(model, q, AB)).toBe(
            canon([
               { tenant: "a-T", region: "x", n: 2, amt: 5 },
               { tenant: "a-T", region: "y", n: 1, amt: 2 },
               { tenant: "b-T", region: "x", n: 1, amt: 8 },
               { tenant: "b-T", region: "z", n: 1, amt: 16 },
            ]),
         );
         expect(await rowsOf(model, q, NONE)).toBe(canon([]));
      });
   });

   it("an inline reader of the derivation answers the same rows", async () => {
      await withModel(async (model) => {
         const q =
            "source: inherits__live is inherits extend {}\nrun: inherits__live -> { group_by: tenant, region, n, amt }";
         expect(await rowsOf(model, q, A)).toBe(
            canon([
               { tenant: "a-T", region: "x", n: 2, amt: 5 },
               { tenant: "a-T", region: "y", n: 1, amt: 2 },
            ]),
         );
      });
   });

   it("a rename onto the gate's field name filters the base, not the renamed values", async () => {
      await withModel(async (model) => {
         const q = "run: renamed -> { group_by: tenant, n }";
         expect(await rowsOf(model, q, A)).toBe(
            canon([
               { tenant: "x", n: 2 },
               { tenant: "y", n: 1 },
            ]),
         );
         expect(await rowsOf(model, q, B)).toBe(
            canon([
               { tenant: "x", n: 1 },
               { tenant: "z", n: 1 },
            ]),
         );
         // A caller naming a region sees nothing: the gate never reads regions.
         expect(await rowsOf(model, q, { GROUPS: ["x"] })).toBe(canon([]));
      });
   });

   it("all() sees only the caller's rows", async () => {
      await withModel(async (model) => {
         const q = "run: shares -> { group_by: category, n, share }";
         expect(await rowsOf(model, q, A)).toBe(
            canon([{ category: "a", n: 3, share: 1 }]),
         );
         expect(await rowsOf(model, q, AB)).toBe(
            canon([
               { category: "a", n: 3, share: 0.6 },
               { category: "b", n: 2, share: 0.4 },
            ]),
         );
         expect(await rowsOf(model, q, NONE)).toBe(canon([]));
      });
   });

   it("limit and rank() see only the caller's rows", async () => {
      await withModel(async (model) => {
         expect(
            await rowsOf(
               model,
               "run: top2 -> { group_by: tenant, region, amt }",
               A,
            ),
         ).toBe(
            canon([
               { tenant: "a-T", region: "x", amt: 5 },
               { tenant: "a-T", region: "y", amt: 2 },
            ]),
         );
         expect(
            await rowsOf(
               model,
               "run: ranked -> { group_by: tenant, region, amt, r }",
               A,
            ),
         ).toBe(
            canon([
               { tenant: "a-T", region: "x", amt: 5, r: 1 },
               { tenant: "a-T", region: "y", amt: 2, r: 2 },
            ]),
         );
      });
   });

   it("a chain of two derivations applies the gate at the innermost base", async () => {
      await withModel(async (model) => {
         const q = "run: chained -> { group_by: tenant, total, share }";
         expect(await rowsOf(model, q, A)).toBe(
            canon([{ tenant: "a-T", total: 7, share: 1 }]),
         );
         expect(await rowsOf(model, q, B)).toBe(
            canon([{ tenant: "b-T", total: 24, share: 1 }]),
         );
         expect(await rowsOf(model, q, NONE)).toBe(canon([]));
      });
   });

   it("a model named query over the derivation answers the caller's rows", async () => {
      await withModel(async (model) => {
         expect(await rowsOf(model, "run: named_shares", AB)).toBe(
            canon([
               { category: "a", share: 0.6 },
               { category: "b", share: 0.4 },
            ]),
         );
      });
   });

   it("a caller's join to the derivation filters the derivation's base first", async () => {
      await withModel(async (model) => {
         const q =
            "run: cats extend { join_one: s is shares on category = s.category } -> { group_by: category; aggregate: sh is s.share.max() }";
         expect(await rowsOf(model, q, A)).toBe(
            canon([
               { category: "a", sh: 1 },
               { category: "b", sh: null },
               { category: "c", sh: null },
            ]),
         );
         expect(await rowsOf(model, q, AB)).toBe(
            canon([
               { category: "a", sh: 0.6 },
               { category: "b", sh: 0.4 },
               { category: "c", sh: null },
            ]),
         );
      });
   });

   it("a caller's own extension and named query over the derivation answer the caller's rows", async () => {
      await withModel(async (model) => {
         const expected = canon([
            { category: "a", share: 0.6 },
            { category: "b", share: 0.4 },
         ]);
         expect(
            await rowsOf(
               model,
               "source: mine is shares extend {}\nrun: mine -> { group_by: category, share }",
               AB,
            ),
         ).toBe(expected);
         expect(
            await rowsOf(
               model,
               "query: q is shares -> { group_by: category, share }\nrun: q",
               AB,
            ),
         ).toBe(expected);
      });
   });

   it("a scalar given gate filters the base first", async () => {
      await withModel(async (model) => {
         const q = "run: by_sub -> { group_by: category, n, share }";
         expect(await rowsOf(model, q, { SUB: "b" })).toBe(
            canon([{ category: "b", n: 2, share: 1 }]),
         );
         expect(await rowsOf(model, q, { SUB: "none" })).toBe(canon([]));
      });
   });

   it("an inherited #(authorize) lock admits or denies the derivation", async () => {
      await withModel(async (model) => {
         const q = "run: locked_d -> { group_by: category, n }";
         expect(await rowsOf(model, q, { ROLES: ["admin"] })).toBe(
            canon([
               { category: "a", n: 3 },
               { category: "b", n: 2 },
               { category: "c", n: 1 },
            ]),
         );
         await expectAccessDenied(queryRows(model, q, { ROLES: ["viewer"] }));
      });
   });

   it("a given-reading where: on the base filters first, as before", async () => {
      await withModel(async (model) => {
         const q = "run: scoped_d -> { group_by: category, n, share }";
         expect(await rowsOf(model, q, A)).toBe(
            canon([{ category: "a", n: 3, share: 1 }]),
         );
      });
   });

   it("a caller's inline derivation of the gated source filters first, as before", async () => {
      await withModel(async (model) => {
         const q =
            "run: gated -> { group_by: category; aggregate: n, share is n / all(n) }";
         expect(await rowsOf(model, q, A)).toBe(
            canon([{ category: "a", n: 3, share: 1 }]),
         );
      });
   });

   it("a gate the derivation declares itself filters its output, as before", async () => {
      await withModel(async (model) => {
         const q = "run: own_gated -> { group_by: tenant, n, share }";
         expect(await rowsOf(model, q, A)).toBe(
            canon([{ tenant: "a-T", n: 3, share: 0.5 }]),
         );
      });
   });

   it("a caller who supplies no given is denied", async () => {
      await withModel(async (model) => {
         await expectAccessDenied(
            queryRows(model, "run: inherits -> { group_by: tenant }", {}),
         );
      });
   });
});
