// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What each accepted gate shape does to a real query, through `Model.create`
 * and `getQueryResults` against DuckDB. The two routes answer different
 * questions and deny differently:
 *
 *  - `#(access_filter)` is a row filter: a caller it admits nowhere gets a
 *    200 with zero rows.
 *  - `#(authorize)` is a lock: a caller it refuses gets a 403, decided before
 *    the query compiles, never a fabricated zero.
 *
 * Every assertion is on rows or on the thrown error, never on a parse: a
 * test that only checks the model loaded passes just as happily when a lock
 * grafts a filter instead. What loads at all is `load_validation.spec.ts`.
 */
import { describe, expect, it } from "bun:test";
import { BadRequestError } from "../../../src/errors";
import {
   buildGatedModel,
   countFor,
   expectAccessDenied,
   expectRefused,
   idsFor,
   queryRows,
   withAuthorizeModel,
   withGivensFlag,
} from "../../../src/test_helpers/authorize_fixture";

/** `withAuthorizeModel` for a model that must load clean. */
async function withModel(
   text: string,
   body: (model: Parameters<typeof countFor>[0]) => Promise<void>,
): Promise<void> {
   await withAuthorizeModel(withGivensFlag(text), async ({ model, error }) => {
      expect(error).toBeUndefined();
      await body(model);
   });
}

const ALL_ACCOUNTS = [1, 2, 3, 4, 5, 6];

// ---------------------------------------------------------------------------
// #(access_filter)
// ---------------------------------------------------------------------------

describe("#(access_filter) filters rows per caller", () => {
   it("on the declaring source: each caller sees their rows, an empty list sees none (200, not 403)", async () => {
      await withModel(
         `given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {}
`,
         async (model) => {
            expect(
               await idsFor(model, "gated_parent", { GROUPS: [1] }),
            ).toEqual([1, 2]);
            expect(
               await idsFor(model, "gated_parent", { GROUPS: [2] }),
            ).toEqual([3, 4]);
            expect(await countFor(model, "gated_parent", { GROUPS: [] })).toBe(
               0,
            );
         },
      );
   });

   it.each([
      [
         "`in` on a backticked column",
         "GROUPS :: string[]",
         "`cost center` in $GROUPS",
         { GROUPS: ["cc1"] },
         [1, 3],
         { GROUPS: ["cc2"] },
         [2, 5],
      ],
      [
         "`=` against a scalar given with no default",
         "REGION :: string",
         "region = $REGION",
         { REGION: "east" },
         [1, 2, 4],
         { REGION: "west" },
         [3, 5, 6],
      ],
   ])("%s", async (_label, given, gate, a, aIds, b, bIds) => {
      await withModel(
         `given:\n  ${given}\n\n#(access_filter) ${gate}\nsource: X is duckdb.table('accounts') extend {}\n`,
         async (model) => {
            expect(await idsFor(model, "X", a)).toEqual(aIds);
            expect(await idsFor(model, "X", b)).toEqual(bIds);
         },
      );
   });

   it("`in` on a dotted join path", async () => {
      await withModel(
         `given:
  GROUPS :: string[]

#(access_filter) child.name in $GROUPS
source: X is duckdb.table('accounts') extend {
   join_one: child is duckdb.table('child') on child_id = child.id
}
`,
         async (model) => {
            const ids = async (g: string) =>
               (
                  await queryRows(
                     model,
                     "run: X -> { select: id, child.name; order_by: id }",
                     { GROUPS: [g] },
                  )
               ).map((r) => r.id);
            expect(await ids("north")).toEqual([1, 2, 3]);
            expect(await ids("south")).toEqual([4, 5, 6]);
         },
      );
   });

   it("ANDs its terms, whether written as one `and` or as two notes", async () => {
      // `org_id in $GROUPS` alone admits 1,2,3 for org1; `region = $REGION`
      // alone admits 3,5,6 for west. Only [3] satisfies both: AND, not OR,
      // and not the first term's own admission.
      await withModel(
         `given:
  GROUPS :: string[]
  REGION :: string

#(access_filter) org_id in $GROUPS
#(access_filter) region = $REGION
source: two_notes is duckdb.table('accounts') extend {}

#(access_filter) org_id in $GROUPS and region = $REGION
source: one_line is duckdb.table('accounts') extend {}
`,
         async (model) => {
            for (const source of ["two_notes", "one_line"]) {
               expect(
                  await idsFor(model, source, {
                     GROUPS: ["org1"],
                     REGION: "east",
                  }),
               ).toEqual([1, 2]);
               expect(
                  await idsFor(model, source, {
                     GROUPS: ["org1"],
                     REGION: "west",
                  }),
               ).toEqual([3]);
               expect(
                  await idsFor(model, source, {
                     GROUPS: ["orgX"],
                     REGION: "east",
                  }),
               ).toEqual([]);
            }
         },
      );
   });

   it("grafts onto an inline-SQL source, not just a table", async () => {
      // A `duckdb.sql(...)` source is a sql_select struct. The lock never
      // needs a graft target, so only the filter proves this struct kind.
      await withModel(
         `given:
  ORGS :: string[]

#(access_filter) org_id in $ORGS
source: inline_src is duckdb.sql("select id, org_id from accounts") extend {}
`,
         async (model) => {
            expect(
               await idsFor(model, "inline_src", { ORGS: ["org1"] }),
            ).toEqual([1, 2, 3]);
            expect(
               await idsFor(model, "inline_src", { ORGS: ["org1", "org2"] }),
            ).toEqual(ALL_ACCOUNTS);
            expect(await idsFor(model, "inline_src", { ORGS: [] })).toEqual([]);
         },
      );
   });

   it("a block-list note attributed to a sibling still ANDs into that sibling's own second note", async () => {
      // Malloy puts the block note and `b`'s item note both on `b`; the
      // block note is attributed to `a`, but `b` still enforces both.
      await withModel(
         `given:
  GROUPS :: number[]
  OWNERS :: number[]

#(access_filter) org_id in $GROUPS
source:
  a is duckdb.table('orgtable') extend {},
  #(access_filter) owner in $OWNERS
  b is a extend {}
`,
         async (model) => {
            expect(await idsFor(model, "a", { GROUPS: [1] })).toEqual([1, 2]);
            expect(
               await idsFor(model, "b", { GROUPS: [1], OWNERS: [1] }),
            ).toEqual([2]);
            expect(
               await idsFor(model, "b", { GROUPS: [1], OWNERS: [999] }),
            ).toEqual([]);
         },
      );
   });
});

// ---------------------------------------------------------------------------
// #(authorize)
// ---------------------------------------------------------------------------

describe("#(authorize) admits or refuses the caller outright", () => {
   it("`'literal' = $GIVEN` and `$GIVEN = 'literal'` decide identically", async () => {
      for (const body of ["'admin' = $ROLE", "$ROLE = 'admin'"]) {
         await withModel(
            `given:\n  ROLE :: string\n\n#(authorize) ${body}\nsource: X is duckdb.table('accounts') extend {}\n`,
            async (model) => {
               expect(await idsFor(model, "X", { ROLE: "admin" })).toEqual(
                  ALL_ACCOUNTS,
               );
               await expectRefused(model, "X", { ROLE: "user" });
            },
         );
      }
   });

   it("an AGGREGATE over a refused caller is a 403, not a fabricated zero or NULL", async () => {
      await withModel(
         `given:
  ROLE :: string

#(authorize) 'admin' = $ROLE
source: X is duckdb.table('accounts') extend {
   measure: total is amount.sum()
}
`,
         async (model) => {
            for (const q of ["aggregate: total", "aggregate: n is count()"]) {
               await expectAccessDenied(
                  queryRows(model, `run: X -> { ${q} }`, { ROLE: "user" }),
               );
            }
         },
      );
   });

   it("membership is exact and case-sensitive, and a null or missing binding is a 403 rather than a 500", async () => {
      // The decision is exact-match TypeScript, not warehouse collation
      // (MySQL's default is case-insensitive). A `null` reaches the decision
      // as a real value; an unguarded `.some` on it would be a 500.
      await withModel(
         `given:
  GROUPS :: string[]

#(authorize) 'finance' in $GROUPS
source: X is duckdb.table('accounts') extend {}
`,
         async (model) => {
            expect(await idsFor(model, "X", { GROUPS: ["finance"] })).toEqual(
               ALL_ACCOUNTS,
            );
            expect(
               await idsFor(model, "X", { GROUPS: ["sales", "finance"] }),
            ).toEqual(ALL_ACCOUNTS);
            await expectRefused(model, "X", { GROUPS: ["Finance"] });
            await expectRefused(model, "X", { GROUPS: ["sales"] });
            await expectRefused(model, "X", { GROUPS: null });
            await expectRefused(model, "X", {});
         },
      );
   });

   it("`false` loads as a deny-all that refuses every caller, aggregates included", async () => {
      await withModel(
         `#(authorize) false\nsource: locked_out is duckdb.table('orgtable') extend {}\n`,
         async (model) => {
            expect(model.getAuthorize("locked_out")).toEqual(["false"]);
            await expectAccessDenied(countFor(model, "locked_out"));
         },
      );
   });

   it("`true` serves every row to a caller supplying nothing", async () => {
      await withModel(
         `given:\n  UNUSED :: string\n\n#(authorize) true\nsource: X is duckdb.table('accounts') extend {}\n`,
         async (model) => {
            expect(await idsFor(model, "X", {})).toEqual(ALL_ACCOUNTS);
         },
      );
   });

   it("survives `extend {}`: the derivation admits and refuses as its base does", async () => {
      await withModel(
         `given:
  ROLE :: string[]

#(authorize) 'finance' in $ROLE
source: base is duckdb.table('accounts') extend {}

source: mine is base extend {}
`,
         async (model) => {
            expect(await idsFor(model, "mine", { ROLE: ["finance"] })).toEqual(
               ALL_ACCOUNTS,
            );
            await expectRefused(model, "mine", { ROLE: ["sales"] });
         },
      );
   });
});

// ---------------------------------------------------------------------------
// Both routes on one source
// ---------------------------------------------------------------------------

describe("the lock runs first, then the filter narrows", () => {
   it("a caller the lock refuses never sees the filter's zero rows", async () => {
      await withModel(
         `given:
  ROLE :: string
  ORGS :: string[]

#(authorize) 'admin' = $ROLE
#(access_filter) org_id in $ORGS
source: X is duckdb.table('accounts') extend {}
`,
         async (model) => {
            expect(
               await idsFor(model, "X", { ROLE: "admin", ORGS: ["org1"] }),
            ).toEqual([1, 2, 3]);
            // Lock passes, filter matches nothing: 200, zero rows.
            expect(
               await idsFor(model, "X", { ROLE: "admin", ORGS: ["nope"] }),
            ).toEqual([]);
            // Lock fails: 403, whatever the filter would have done.
            await expectRefused(model, "X", { ROLE: "user", ORGS: ["org1"] });
            await expectRefused(model, "X", { ROLE: "user", ORGS: ["nope"] });
         },
      );
   });

   it("on an inline-SQL source too", async () => {
      await withModel(
         `given:
  GROUPS :: string[]
  ORGS :: string[]

#(authorize) 'finance' in $GROUPS
#(access_filter) org_id in $ORGS
source: inline_src is duckdb.sql("select id, org_id from accounts") extend {}
`,
         async (model) => {
            await expectRefused(model, "inline_src", {
               GROUPS: [],
               ORGS: ["org1"],
            });
            expect(
               await idsFor(model, "inline_src", {
                  GROUPS: ["finance"],
                  ORGS: ["org1"],
               }),
            ).toEqual([1, 2, 3]);
         },
      );
   });

   it("a lock over a derivation whose route cache is shared with the filter is still refused, never served empty", async () => {
      // The fail-closed sentinel is synthesized on BOTH routes for one struct
      // and the walk is filter-first. Without the route in the gate-shape
      // cache key, the lock would read the filter's `row_level` entry, graft
      // `where: false`, and answer 200 with zero rows.
      await withModel(
         `given:
  ROLE :: string
  ORGS :: string[]

#(authorize) 'admin' = $ROLE
#(access_filter) org_id in $ORGS
source: base is duckdb.table('accounts') extend {}

source: derived is base extend {}
`,
         async (model) => {
            await expectRefused(model, "derived", {
               ROLE: "user",
               ORGS: ["org1"],
            });
            await expectRefused(model, "base", {
               ROLE: "user",
               ORGS: ["org1"],
            });
         },
      );
   });
});

// ---------------------------------------------------------------------------
// Inheritance: derivation carries a gate, a join never does, and an own note
// replaces only its own route's inherited gate.
// ---------------------------------------------------------------------------

describe("inheritance", () => {
   const MODEL = `given:
  GROUPS :: number[]
  NEVER :: number[]
  EVERYONE :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {}

source: child is gated_parent extend {}

#(access_filter) org_id in $NEVER
source: child_own is gated_parent extend {}

#(access_filter) org_id in $EVERYONE
source: child_relaxed is gated_parent extend {}

source: joiner is duckdb.table('orgtable') extend {
   join_one: gp is gated_parent on id = gp.id
}

source: qchild is gated_parent -> { group_by: id, org_id, val; aggregate: n is count() }
`;

   it("a plain `extend {}` and a query-source derivation both carry the base's filter", async () => {
      await withModel(MODEL, async (model) => {
         for (const source of ["child", "qchild"]) {
            expect(await countFor(model, source, { GROUPS: [1] })).toBe(2);
            expect(await countFor(model, source, { GROUPS: [] })).toBe(0);
         }
         // Defined in the caller's own text, too.
         expect(
            (
               await queryRows(
                  model,
                  "source: mine is gated_parent extend {}\nrun: mine -> { aggregate: n is count() }",
                  { GROUPS: [1] },
               )
            )[0].n,
         ).toBe(2);
      });
   });

   it("a child's own filter REPLACES the inherited one: not OR (narrowing), not AND (relaxing)", async () => {
      await withModel(MODEL, async (model) => {
         // OR would still admit the org-1 rows GROUPS names.
         expect(
            await countFor(model, "child_own", { GROUPS: [1], NEVER: [] }),
         ).toBe(0);
         // AND would deny everything: GROUPS matches no row. The locked-base
         // + curated-re-exposure idiom depends on this direction.
         expect(
            await countFor(model, "child_relaxed", {
               GROUPS: [999],
               EVERYONE: [1, 2],
            }),
         ).toBe(4);
      });
   });

   it("a gate reached only through join_one: never fires: the joining source stays ungated", async () => {
      await withModel(MODEL, async (model) => {
         // No GROUPS at all: a propagated gate would deny or fail unbound.
         expect(await countFor(model, "joiner")).toBe(4);
      });
   });

   it("a child's own #(access_filter) replaces only the filter: the inherited lock still decides", async () => {
      await withModel(
         `given:
  GROUPS :: number[]
  NEVER :: number[]
  ROLE :: string[]

#(access_filter) org_id in $GROUPS
#(authorize) 'finance' in $ROLE
source: base is duckdb.table('orgtable') extend {}

#(access_filter) org_id in $NEVER
source: own_filter is base extend {}
`,
         async (model) => {
            expect(
               await countFor(model, "own_filter", {
                  NEVER: [1],
                  ROLE: ["finance"],
               }),
            ).toBe(2);
            await expectAccessDenied(
               countFor(model, "own_filter", { NEVER: [1], ROLE: ["sales"] }),
            );
            // The base's filter is replaced, not ORed back in.
            expect(
               await countFor(model, "own_filter", {
                  NEVER: [],
                  ROLE: ["finance"],
               }),
            ).toBe(0);
         },
      );
   });

   it("a child's own #(authorize) replaces only the lock: the inherited filter still applies", async () => {
      await withModel(
         `given:
  GROUPS :: number[]
  ROLE :: string[]

#(access_filter) org_id in $GROUPS
source: base is duckdb.table('orgtable') extend {}

#(authorize) 'finance' in $ROLE
source: own_lock is base extend {}
`,
         async (model) => {
            expect(
               await countFor(model, "own_lock", {
                  GROUPS: [1],
                  ROLE: ["finance"],
               }),
            ).toBe(2);
            expect(
               await countFor(model, "own_lock", {
                  GROUPS: [999],
                  ROLE: ["finance"],
               }),
            ).toBe(0);
            await expectAccessDenied(
               countFor(model, "own_lock", { GROUPS: [1], ROLE: ["sales"] }),
            );
         },
      );
   });

   it("except: the gated column then rename: another onto its name denies rather than filtering on the wrong column", async () => {
      // The graft compiles (a field named org_id exists again), but
      // filter_binding_guard proves it no longer binds the declaring
      // source's column. See tests/component/filter_binding_guard.component.spec.ts for
      // the full variant matrix.
      const gated = await buildGatedModel(`given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {}

source: w_misbind is gated_parent extend { except: org_id } extend { rename: org_id is owner }
`);
      try {
         await expectAccessDenied(
            queryRows(
               gated.model,
               "run: w_misbind -> { group_by: id; aggregate: n is count() }",
               { GROUPS: [1] },
            ),
         );
      } finally {
         await gated.duckdb.close();
      }
   });
});

// ---------------------------------------------------------------------------
// `#(authorize) true`: a curated extension re-opens a locked base. `true`
// sheds only the LOCK it inherits; an inherited filter stays.
// ---------------------------------------------------------------------------

describe("the `true` admit-all sentinel", () => {
   it("a `false` base refuses every caller; its `true` extension admits all", async () => {
      await withModel(
         `#(authorize) false
source: base is duckdb.table('parent') extend {}

#(authorize) true
source: reopened is base extend {}
`,
         async (model) => {
            await expectRefused(model, "base");
            expect(await idsFor(model, "reopened")).toEqual([1, 2, 3, 4]);
         },
      );
   });

   it.each([
      [
         "an inherited lock is shed; the inherited filter still narrows",
         `#(authorize) 'finance' in $ROLE\n#(access_filter) org_id in $GROUPS\nsource: base is duckdb.table('parent') extend {}\n\n#(authorize) true\nsource: reopened is base extend {}`,
         { ROLE: ["sales"] },
      ],
      [
         "a base with only a filter: the filter still narrows",
         `#(access_filter) org_id in $GROUPS\nsource: base is duckdb.table('parent') extend {}\n\n#(authorize) true\nsource: reopened is base extend {}`,
         {},
      ],
      [
         "an INHERITED `true` beside the source's own filter",
         `#(authorize) true\nsource: base is duckdb.table('parent') extend {}\n\n#(access_filter) org_id in $GROUPS\nsource: reopened is base extend {}`,
         {},
      ],
   ])("%s", async (_label, body, extra) => {
      await withModel(
         `given:\n  ROLE :: string[]\n  GROUPS :: number[]\n\n${body}\n`,
         async (model) => {
            expect(
               await idsFor(model, "reopened", { ...extra, GROUPS: [1] }),
            ).toEqual([1, 2]);
            expect(
               await idsFor(model, "reopened", { ...extra, GROUPS: [999] }),
            ).toEqual([]);
         },
      );
   });

   it("a source's own `true` beside its own filter: the lock is lifted, the filter still decides", async () => {
      await withModel(
         `given:
  ORGS :: number[]

#(authorize) false
source: base is duckdb.table('parent') extend {}

#(authorize) true
#(access_filter) org_id in $ORGS
source: reopened is base extend {}
`,
         async (model) => {
            await expectRefused(model, "base", { ORGS: [1] });
            expect(await idsFor(model, "reopened", { ORGS: [1] })).toEqual([
               1, 2,
            ]);
            expect(await idsFor(model, "reopened", { ORGS: [999] })).toEqual(
               [],
            );
         },
      );
   });

   it("re-opens an `extend` derivation but NOT a query-source one: the base's `false` stays in its conjunction", async () => {
      await withModel(
         `#(authorize) false
source: base is duckdb.table('parent') extend {}

#(authorize) true
source: reopened is base extend {}

#(authorize) true
source: derived is base -> { select: id }
`,
         async (model) => {
            expect(await idsFor(model, "reopened")).toEqual([1, 2, 3, 4]);
            await expectRefused(model, "derived");
         },
      );
   });
});

// ---------------------------------------------------------------------------
// Caller-submitted text may not declare a gate: a caller-minted
// `#(access_filter) true` would replace the author's gate under own-wins.
// ---------------------------------------------------------------------------

describe("a caller cannot mint a gate", () => {
   it.each(["access_filter", "authorize"])(
      "#(%s) true in query text is refused before compile, and the author's gate stays in force",
      async (route) => {
         await withModel(
            `given:
  GROUPS :: string[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}
`,
            async (model) => {
               const err = await queryRows(
                  model,
                  `#(${route}) true\nrun: X -> { select: id }`,
                  { GROUPS: ["org1"] },
               ).then(
                  () => undefined,
                  (e: unknown) => e,
               );
               expect(err).toBeInstanceOf(BadRequestError);
               expect(String(err)).toMatch(/not permitted in caller-submitted/);
               expect(await idsFor(model, "X", { GROUPS: ["org1"] })).toEqual([
                  1, 2, 3,
               ]);
            },
         );
      },
   );
});
