// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Syntax conformance for `#(authorize)` — real compiled Malloy models against
 * real DuckDB, through the same `Model.create` / `getQueryResults` path
 * production uses (same idiom as `row_level_authorize.integration.spec.ts`'s
 * "load-time scoping" `createModel` helper — duplicated here, not imported,
 * since that file is owned elsewhere).
 *
 * Group A exercises every accepted grammar shape end to end (it filters, and
 * filters correctly). Group B exercises every shape the grammar now refuses
 * outright at load, asserting the specific `AuthorizeGrammarRejectionCause`
 * named in the error — this used to be a report of surprising, inconsistent
 * behavior across a Malloy-arbitrary-boolean gate; the narrow grammar makes
 * every one of these a predictable load-time refusal instead, so there is no
 * longer anything to observe and report on beyond that.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError, ModelCompilationError } from "../errors";
import { AuthorizeGrammarError } from "./authorize_grammar";
import { Model } from "./model";

const SEED_SQL = `
CREATE OR REPLACE TABLE accounts (
   id INTEGER, org_id VARCHAR, region VARCHAR, amount INTEGER,
   owner VARCHAR, "cost center" VARCHAR, child_id INTEGER
);
INSERT INTO accounts VALUES
   (1, 'org1', 'east', 100, 'alice', 'cc1', 1),
   (2, 'org1', 'east', 200, 'bob',   'cc2', 1),
   (3, 'org1', 'west',  50, 'alice', 'cc1', 1),
   (4, 'org2', 'east', 300, 'carol', 'cc3', 2),
   (5, 'org2', 'west', 400, 'dave',  'cc2', 2),
   (6, 'org2', 'west', 150, 'eve',   'cc3', 2);
CREATE OR REPLACE TABLE child (id INTEGER, name VARCHAR);
INSERT INTO child VALUES (1, 'north'), (2, 'south');
`;

async function newDuckdb(): Promise<DuckDBConnection> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   for (const stmt of SEED_SQL.trim()
      .split(";")
      .filter((s) => s.trim())) {
      await duckdb.runSQL(stmt.trim() + ";");
   }
   return duckdb;
}

/** Load `text` through the REAL `Model.create` against a fresh seeded
 *  DuckDB. Caller is responsible for `duckdb.close()` / `fs.rmSync(dir)`. */
async function createModel(
   text: string,
): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
   const duckdb = await newDuckdb();
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "authz-syntax-"));
   fs.writeFileSync(
      path.join(dir, "m.malloy"),
      text.includes("experimental.givens")
         ? text
         : `##! experimental.givens\n\n${text}`,
   );
   const model = await Model.create(
      "test-pkg",
      dir,
      "m.malloy",
      new Map<string, Connection>([["duckdb", duckdb]]),
   );
   return { model, duckdb, dir };
}

function compilationErrorOf(model: Model): Error | undefined {
   return (model as unknown as { compilationError?: Error }).compilationError;
}

async function cleanup(duckdb: DuckDBConnection, dir: string): Promise<void> {
   await duckdb.close();
   fs.rmSync(dir, { recursive: true, force: true });
}

const SELECT_COLS = "id, org_id, region, amount, owner, `cost center`";

async function rowsFor(
   model: Model,
   sourceName: string,
   givens: Record<string, unknown>,
   selectCols = SELECT_COLS,
): Promise<ReadonlyArray<Record<string, unknown>>> {
   const result = await model.getQueryResults(
      undefined,
      undefined,
      `run: ${sourceName} -> { select: ${selectCols}; order_by: id }`,
      {},
      true,
      givens as never,
   );
   return result.compactResult as unknown as ReadonlyArray<
      Record<string, unknown>
   >;
}

function ids(rows: ReadonlyArray<Record<string, unknown>>): number[] {
   return rows.map((r) => Number(r.id)).sort((a, b) => a - b);
}

/** The lock's denial: a 403, never a 200 carrying zero rows. */
async function expectRefused(
   model: Model,
   sourceName: string,
   givens: Record<string, unknown>,
   selectCols = SELECT_COLS,
): Promise<void> {
   await expect(
      rowsFor(model, sourceName, givens, selectCols),
   ).rejects.toBeInstanceOf(AccessDeniedError);
}

// ---------------------------------------------------------------------------
// Group A — accepted shapes
// ---------------------------------------------------------------------------

describe("authorize syntax conformance — Group A (accepted shapes)", () => {
   it("row-level `in` — array membership, differs per principal", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const org1 = await rowsFor(model, "X", { GROUPS: ["org1"] });
         const org2 = await rowsFor(model, "X", { GROUPS: ["org2"] });
         expect(ids(org1)).toEqual([1, 2, 3]);
         expect(ids(org2)).toEqual([4, 5, 6]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("row-level `in` on a backticked column, differs per principal", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(access_filter) \`cost center\` in $GROUPS
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const cc1 = await rowsFor(model, "X", { GROUPS: ["cc1"] });
         const cc2 = await rowsFor(model, "X", { GROUPS: ["cc2"] });
         expect(ids(cc1)).toEqual([1, 3]);
         expect(ids(cc2)).toEqual([2, 5]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("row-level `=` — scalar given with NO default, differs per principal", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  REGION :: string

#(access_filter) region = $REGION
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const east = await rowsFor(model, "X", { REGION: "east" });
         const west = await rowsFor(model, "X", { REGION: "west" });
         expect(ids(east)).toEqual([1, 2, 4]);
         expect(ids(west)).toEqual([3, 5, 6]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("row-level `in` on a dotted JOIN path", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(access_filter) child.name in $GROUPS
source: X is duckdb.table('accounts') extend {
   join_one: child is duckdb.table('child') on child_id = child.id
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const north = await rowsFor(
            model,
            "X",
            { GROUPS: ["north"] },
            "id, org_id, region, amount, owner, `cost center`, child.name",
         );
         const south = await rowsFor(
            model,
            "X",
            { GROUPS: ["south"] },
            "id, org_id, region, amount, owner, `cost center`, child.name",
         );
         expect(ids(north)).toEqual([1, 2, 3]);
         expect(ids(south)).toEqual([4, 5, 6]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("source-level `'literal' = $GIVEN` — scalar given, admits every row or refuses outright", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  ROLE :: string

#(authorize) 'admin' = $ROLE
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const admin = await rowsFor(model, "X", { ROLE: "admin" });
         expect(ids(admin)).toEqual([1, 2, 3, 4, 5, 6]);
         await expectRefused(model, "X", { ROLE: "user" });
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("source-level `'literal' in $GIVEN` — list given, admits when the literal is a member and refuses otherwise", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  ROLES :: string[]

#(authorize) 'admin' in $ROLES
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const admin = await rowsFor(model, "X", { ROLES: ["admin", "user"] });
         expect(ids(admin)).toEqual([1, 2, 3, 4, 5, 6]);
         await expectRefused(model, "X", { ROLES: ["user"] });
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a conjunction of two row-level terms joined by `and`", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]
  REGION :: string

#(access_filter) org_id in $GROUPS and region = $REGION
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const a = await rowsFor(model, "X", {
            GROUPS: ["org1"],
            REGION: "east",
         });
         const b = await rowsFor(model, "X", {
            GROUPS: ["org2"],
            REGION: "west",
         });
         expect(ids(a)).toEqual([1, 2]);
         expect(ids(b)).toEqual([5, 6]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a source that inherits the gate via `extend` — same filtering as base", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}

source: Y is X extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const xRows = await rowsFor(model, "X", { GROUPS: ["org1"] });
         const yRows = await rowsFor(model, "Y", { GROUPS: ["org1"] });
         expect(ids(xRows)).toEqual([1, 2, 3]);
         expect(ids(yRows)).toEqual(ids(xRows));
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`#(authorize) true` loads with no compilation error and serves every row to a caller supplying nothing", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  UNUSED :: string

#(authorize) true
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const rows = await rowsFor(model, "X", {});
         expect(ids(rows)).toEqual([1, 2, 3, 4, 5, 6]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("empty array given — fail-closed, zero rows", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const rows = await rowsFor(model, "X", { GROUPS: [] });
         expect(ids(rows)).toEqual([]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

// ---------------------------------------------------------------------------
// Group B — refused shapes. Every case now aborts model LOAD with an
// `AuthorizeGrammarError` naming its `rejectionCause` — there is no
// request-time ambiguity left to observe, unlike the retired
// arbitrary-Malloy-boolean form this suite used to document.
// ---------------------------------------------------------------------------

async function expectRejectionCause(
   text: string,
   cause: string,
): Promise<void> {
   const { model, duckdb, dir } = await createModel(text);
   try {
      const err = compilationErrorOf(model);
      expect(err).toBeInstanceOf(AuthorizeGrammarError);
      expect((err as AuthorizeGrammarError).rejectionCause).toBe(
         cause as never,
      );
   } finally {
      await cleanup(duckdb, dir);
   }
}

describe("authorize syntax conformance — Group B (refused shapes)", () => {
   it("`not (org_id in $GROUPS)` — negation is refused (compound_boolean)", async () => {
      await expectRejectionCause(
         `
given:
  GROUPS :: string[]

#(authorize) not (org_id in $GROUPS)
source: X is duckdb.table('accounts') extend {}
`,
         "compound_boolean",
      );
   });

   it("`org_id in $GROUPS or region = $REGION` — disjunction is refused (compound_boolean)", async () => {
      await expectRejectionCause(
         `
given:
  GROUPS :: string[]
  REGION :: string

#(access_filter) org_id in $GROUPS or region = $REGION
source: X is duckdb.table('accounts') extend {}
`,
         "compound_boolean",
      );
   });

   it("`region != $REGION` — negated operator is refused", async () => {
      await expectRejectionCause(
         `
given:
  REGION :: string

#(access_filter) region != $REGION
source: X is duckdb.table('accounts') extend {}
`,
         "negated_operator",
      );
   });

   it("`amount > $AMOUNTMIN` — a bare comparison operator is refused", async () => {
      await expectRejectionCause(
         `
given:
  AMOUNTMIN :: number

#(access_filter) amount > $AMOUNTMIN
source: X is duckdb.table('accounts') extend {}
`,
         "comparison_operator",
      );
   });

   it("`org_id = $GROUPS` — a scalar operator against a list-typed given is an arity mismatch", async () => {
      await expectRejectionCause(
         `
given:
  GROUPS :: string[]

#(access_filter) org_id = $GROUPS
source: X is duckdb.table('accounts') extend {}
`,
         "operator_arity_mismatch",
      );
   });

   it("`owner in $ROLE` — `in` against a scalar given is an arity mismatch", async () => {
      await expectRejectionCause(
         `
given:
  ROLE :: string

#(access_filter) owner in $ROLE
source: X is duckdb.table('accounts') extend {}
`,
         "operator_arity_mismatch",
      );
   });

   it("`upper(region) = $REGION` — a function call is not a field path", async () => {
      await expectRejectionCause(
         `
given:
  REGION :: string

#(authorize) upper(region) = $REGION
source: X is duckdb.table('accounts') extend {}
`,
         "left_not_field_path",
      );
   });

   it("`1 = 1` — two constants, neither side names a field path or given reference", async () => {
      await expectRejectionCause(
         `
given:
  UNUSED :: string

#(authorize) 1 = 1
source: X is duckdb.table('accounts') extend {}
`,
         "left_not_field_path",
      );
   });

   it("`region like $PAT` — `like` is not a recognized operator", async () => {
      await expectRejectionCause(
         `
given:
  PAT :: string

#(authorize) region like $PAT
source: X is duckdb.table('accounts') extend {}
`,
         "malformed_body",
      );
   });

   it("`region is not null` — the `not` token is refused as a compound boolean", async () => {
      await expectRejectionCause(
         `
given:
  UNUSED :: string

#(authorize) region is not null
source: X is duckdb.table('accounts') extend {}
`,
         "compound_boolean",
      );
   });

   it("`amount + 1 > $AMOUNTMIN` — arithmetic is refused as a comparison operator", async () => {
      await expectRejectionCause(
         `
given:
  AMOUNTMIN :: number

#(authorize) amount + 1 > $AMOUNTMIN
source: X is duckdb.table('accounts') extend {}
`,
         "comparison_operator",
      );
   });

   it("`true and org_id in $GROUPS` — `true` only sheds the sentinel form; alongside an `and`, it is an ordinary malformed first term", async () => {
      await expectRejectionCause(
         `
given:
  GROUPS :: string[]

#(authorize) true and org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}
`,
         "malformed_body",
      );
   });

   it("`region = $REGION` where `$REGION` has a declared default is refused, unrelated to the grammar (G4)", async () => {
      // Unlike every case above, this is not a grammar rejection — the body
      // is perfectly legal. `validateSourceLineGateGivenUsage`'s G4 refuses a
      // gate that reads a given declared WITH a default, because a caller
      // who omits it would otherwise be served rows off the author's default
      // rather than being denied. See `source_line_authorize_integration.spec.ts`.
      const { model, duckdb, dir } = await createModel(`
given:
  REGION :: string is 'east'

#(access_filter) region = $REGION
source: X is duckdb.table('accounts') extend {
}
`);
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeDefined();
         expect(err).not.toBeInstanceOf(AuthorizeGrammarError);
         expect(err?.message).toMatch(/declared with a default/);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

// ---------------------------------------------------------------------------
// Group C — a repeated `#(authorize)` on one source is legal and AND's.
// ---------------------------------------------------------------------------

const TWO_NOTES = `
given:
  GROUPS :: string[]
  REGION :: string

#(access_filter) org_id in $GROUPS
#(access_filter) region = $REGION
source: X is duckdb.table('accounts') extend {}
`;

const TWO_NOTES_AND_EQUIVALENT = `${TWO_NOTES}
#(access_filter) org_id in $GROUPS and region = $REGION
source: Y is duckdb.table('accounts') extend {}
`;

describe("authorize syntax conformance — Group C (repeated #(authorize) — conjunction)", () => {
   it("two #(authorize) notes on one source are accepted and BOTH are enforced", async () => {
      const { model, duckdb, dir } = await createModel(TWO_NOTES);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const rows = await rowsFor(model, "X", {
            GROUPS: ["org1"],
            REGION: "east",
         });
         expect(ids(rows)).toEqual([1, 2]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a caller admitted by neither note gets zero rows; admitted by only one, only the rows the other admits too", async () => {
      const { model, duckdb, dir } = await createModel(TWO_NOTES);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();

         // Neither term admits: org_id in $GROUPS fails for every row.
         const neither = await rowsFor(model, "X", {
            GROUPS: ["orgX"],
            REGION: "east",
         });
         expect(ids(neither)).toEqual([]);

         // org_id in $GROUPS alone would admit [1, 2, 3]; region = $REGION
         // alone would admit [3, 5, 6]. Only [3] satisfies both — proof this
         // is AND, not the first term's own admission.
         const onlyOneTermWouldAdmitTheRest = await rowsFor(model, "X", {
            GROUPS: ["org1"],
            REGION: "west",
         });
         expect(ids(onlyOneTermWouldAdmitTheRest)).toEqual([3]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a two-note gate and its one-line `and` equivalent admit the SAME rows for every caller (not the same filterText string)", async () => {
      const { model, duckdb, dir } = await createModel(
         TWO_NOTES_AND_EQUIVALENT,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const cases: Array<{ GROUPS: string[]; REGION: string }> = [
            { GROUPS: ["org1"], REGION: "east" },
            { GROUPS: ["org1"], REGION: "west" },
            { GROUPS: ["org2"], REGION: "east" },
            { GROUPS: ["orgX"], REGION: "east" },
         ];
         let sawNonEmptyCase = false;
         for (const givens of cases) {
            const xRows = await rowsFor(model, "X", givens);
            const yRows = await rowsFor(model, "Y", givens);
            expect(ids(yRows)).toEqual(ids(xRows));
            if (xRows.length > 0) sawNonEmptyCase = true;
         }
         // Without this, X and Y could both filter down to zero rows for
         // EVERY case above and the loop's equality assertions would still
         // pass — proving nothing about the two spellings actually admitting
         // the same rows, only that both are equally broken.
         expect(sawNonEmptyCase).toBe(true);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

// ---------------------------------------------------------------------------
// Group D — cross-NOTE coherence. The four checks that used to be safe
// within one body (`duplicate_given`, `duplicate_field_path`,
// `mixed_scope_body`, the `false` deny-all sentinel) must now see a
// declaring source's terms as a SET across its notes, since repeats are
// legal — see `assertAuthorizeGrammarTermsCoherent`.
// ---------------------------------------------------------------------------

describe("authorize syntax conformance — Group D (cross-note coherence)", () => {
   it("duplicate_given across two notes on one source is refused", async () => {
      await expectRejectionCause(
         `
given:
  G :: string

#(access_filter) a = $G
#(access_filter) b = $G
source: X is duckdb.table('accounts') extend {}
`,
         "duplicate_given",
      );
   });

   it("duplicate_field_path across two notes on one source is refused", async () => {
      await expectRejectionCause(
         `
given:
  A :: string
  B :: string

#(access_filter) region = $A
#(access_filter) region = $B
source: X is duckdb.table('accounts') extend {}
`,
         "duplicate_field_path",
      );
   });

   it("`#(authorize) false` plus any sibling note on the same source is refused", async () => {
      await expectRejectionCause(
         `
given:
  REGION :: string

#(authorize) false
#(access_filter) region = $REGION
source: X is duckdb.table('accounts') extend {}
`,
         "deny_all_with_sibling",
      );
   });

   // Regression guard for the scoping rule `assertAuthorizeGrammarValid`
   // depends on: coherence runs PER GROUP, never over `groups.flat()`. A
   // query-source base's own gate and its separately-resolved composite
   // member's own gate are two DIFFERENT declaring sources — two groups
   // that AND — and here they deliberately reuse the SAME given ($GROUPS)
   // and mix row-level scope, which would misfire as `duplicate_given` (or
   // worse) if the two groups were ever flattened together before this
   // check.
   it("a query-source base gate and its composite member's own gate — two groups, same given — still LOADS", async () => {
      const { model, duckdb, dir } = await createModel(`
##! experimental.composite_sources

given:
  GROUPS :: string[]

#(access_filter) region in $GROUPS
source: member_a is duckdb.table('accounts') extend {}

source: member_b is duckdb.sql("select id, org_id from accounts") extend {}

#(access_filter) org_id in $GROUPS
source: combo is compose(member_a, member_b)

source: qs is combo -> { group_by: id, region }
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

// ---------------------------------------------------------------------------
// Group E — `#(authorize)`: the LOCK. It answers "may this caller reach this
// source at all", so its denial is a 403 and it is decided BEFORE the
// caller's query compiles. It composes with `#(access_filter)` by running
// first: pass the lock, then see your rows.
// ---------------------------------------------------------------------------

async function expectModelCompilationError(
   text: string,
   messagePattern: RegExp,
): Promise<void> {
   const { model, duckdb, dir } = await createModel(text);
   try {
      const err = compilationErrorOf(model);
      expect(err).toBeInstanceOf(ModelCompilationError);
      expect(err?.message).toMatch(messagePattern);
   } finally {
      await cleanup(duckdb, dir);
   }
}

describe("authorize syntax conformance — Group E (#(authorize))", () => {
   it("survives `source: mine is base extend {}` — inherits through extend", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  ROLE :: string[]

#(authorize) 'finance' in $ROLE
source: base is duckdb.table('accounts') extend {}

source: mine is base extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const admitted = await rowsFor(model, "mine", { ROLE: ["finance"] });
         expect(ids(admitted)).toEqual([1, 2, 3, 4, 5, 6]);
         await expectRefused(model, "mine", { ROLE: ["sales"] });
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("runs before the #(access_filter) on the same source — a caller it does not admit gets 403, and never the filter's zero rows", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]
  ROLE :: string[]

#(access_filter) org_id in $GROUPS
#(authorize) 'finance' in $ROLE
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         // Both admit: the row-level gate's own org_id filtering still
         // applies on top of the caller-identity check.
         const both = await rowsFor(model, "X", {
            GROUPS: ["org1"],
            ROLE: ["finance"],
         });
         expect(ids(both)).toEqual([1, 2, 3]);
         // Filter satisfied, lock NOT satisfied — 403. The two denials are
         // deliberately different responses, which is the whole point of
         // splitting the routes.
         await expectRefused(model, "X", {
            GROUPS: ["org1"],
            ROLE: ["sales"],
         });
         // Lock satisfied, filter matches nothing — 200 with zero rows.
         const orgDenied = await rowsFor(model, "X", {
            GROUPS: ["org-nowhere"],
            ROLE: ["finance"],
         });
         expect(ids(orgDenied)).toEqual([]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`#(authorize) false` loads clean and refuses every caller", async () => {
      const { model, duckdb, dir } = await createModel(`
#(authorize) false
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectRefused(model, "X", {});
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`#(authorize) true` loads and admits every caller", async () => {
      const { model, duckdb, dir } = await createModel(`
#(authorize) true
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const rows = await rowsFor(model, "X", {});
         expect(ids(rows)).toEqual([1, 2, 3, 4, 5, 6]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`#(authorize) true` alongside a sibling note ON THE SAME ROUTE is refused (admit_all_with_sibling)", async () => {
      await expectRejectionCause(
         `
given:
  GROUPS :: string[]

#(authorize) true
#(authorize) 'finance' in $GROUPS
source: X is duckdb.table('accounts') extend {}
`,
         "admit_all_with_sibling",
      );
   });

   it("`#(authorize) true` alongside a row-level `#(access_filter)` is LEGAL and the row filter still runs", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) true
#(access_filter) org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const org1 = await rowsFor(model, "X", { GROUPS: ["org1"] });
         const org2 = await rowsFor(model, "X", { GROUPS: ["org2"] });
         expect(ids(org1)).toEqual([1, 2, 3]);
         expect(ids(org2)).toEqual([4, 5, 6]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`#(authorize) false` alongside `#(authorize) org_id in $GROUPS` is refused (deny_all_with_sibling)", async () => {
      await expectRejectionCause(
         `
given:
  GROUPS :: string[]

#(authorize) false
#(access_filter) org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}
`,
         "deny_all_with_sibling",
      );
   });

   it("a row-level term in a #(authorize) body is refused (row_level_term_in_authorize)", async () => {
      await expectRejectionCause(
         `
given:
  REGION :: string

#(authorize) region = $REGION
source: X is duckdb.table('accounts') extend {}
`,
         "row_level_term_in_authorize",
      );
   });

   // The mirror. Without it a source-level body on the filter route grafts as
   // a constant predicate, and a caller it excludes is served zero rows
   // instead of being refused.
   it("a source-level term in a #(access_filter) body is refused (source_level_term_in_access_filter)", async () => {
      await expectRejectionCause(
         `
given:
  GROUPS :: string[]

#(access_filter) 'finance' in $GROUPS
source: X is duckdb.table('accounts') extend {}
`,
         "source_level_term_in_access_filter",
      );
   });

   it("both sentinels are refused on #(access_filter), each naming the lock form", async () => {
      for (const spelling of ["false", "true"]) {
         await expectRejectionCause(
            `
#(access_filter) ${spelling}
source: X is duckdb.table('accounts') extend {}
`,
            "sentinel_in_access_filter",
         );
      }
   });

   it('`#(authorize) "x"` draws the same legacy-string refusal as `#(authorize) "x"`', async () => {
      await expectModelCompilationError(
         `
given:
  ROLE :: string

#(authorize) "$ROLE = 'admin'"
source: X is duckdb.table('accounts') extend {}
`,
         /string form.*no longer accepted/is,
      );
   });

   it("`#(authorize)` on a field is refused as misplaced", async () => {
      await expectModelCompilationError(
         `
given:
  ROLE :: string[]

source: X is duckdb.table('accounts') extend {
   #(authorize) 'finance' in $ROLE
   dimension: d is org_id
}
`,
         /never enforced.*#\(authorize\)/is,
      );
   });

   it("`#(authorize)` on a `query:` statement is refused as misplaced", async () => {
      await expectModelCompilationError(
         `
given:
  ROLE :: string[]

source: X is duckdb.table('accounts') extend {}

#(authorize) 'finance' in $ROLE
query: q is X -> { select: id }
`,
         /never enforced.*#\(authorize\)/is,
      );
   });

   it("file-level `##(authorize)` is refused as misplaced", async () => {
      await expectModelCompilationError(
         `
given:
  ROLE :: string[]

##(authorize) 'finance' in $ROLE

source: X is duckdb.table('accounts') extend {}
`,
         /file level/i,
      );
   });

   it("is rejected in caller-submitted Malloy text", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               `#(authorize) 'x' in $GROUPS\nrun: X -> { select: id }`,
               {},
               true,
               { GROUPS: ["org1"] } as never,
            ),
         ).rejects.toThrow(/not permitted in caller-submitted/);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a caller-submitted `#(authorize) true` is rejected too — the highest-value forgery in the system, since `true` is the one spelling that turns a gate OFF", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               `#(authorize) true\nrun: X -> { select: id }`,
               {},
               true,
               { GROUPS: ["org1"] } as never,
            ),
         ).rejects.toThrow(/not permitted in caller-submitted/);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   // Through the REAL query path, not the rejecter in isolation: a caller who
   // mints `#(access_filter) true` over a gated source would otherwise replace
   // the author's gate under per-route own-wins. The forgery pattern is
   // stem-based, so it had to move in the same commit as the route names.
   it.each(["access_filter", "authorize", "authorize"])(
      "a caller query minting #(%s) is refused before it reaches the compiler",
      async (route) => {
         const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(access_filter) org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}
`);
         try {
            expect(compilationErrorOf(model)).toBeUndefined();
            await expect(
               model.getQueryResults(
                  undefined,
                  undefined,
                  `#(${route}) true\nrun: X -> { select: id }`,
                  {},
                  true,
                  { GROUPS: ["org1"] } as never,
               ),
            ).rejects.toThrow(/not permitted in caller-submitted/);
            // And the author's gate is still the one in force.
            expect(
               ids(await rowsFor(model, "X", { GROUPS: ["org1"] })),
            ).toEqual([1, 2, 3]);
         } finally {
            await cleanup(duckdb, dir);
         }
      },
   );

   // Each of these five spellings reads as an attempt at `#(authorize)`
   // but Malloy does not route it there — a naive implementation loads clean
   // and serves every row. See `authorize.spec.ts`'s
   // `collectAuthorizeNearMisses` unit tests for the per-spelling routing
   // table this end-to-end sweep exercises.
   const TYPO_SPELLINGS: ReadonlyArray<[string, string]> = [
      ["sourceauthorize", "#(sourceauthorize) 'finance' in $ROLE"],
      ["authorize-source", "#(authorize-source) 'finance' in $ROLE"],
      ["SOURCE_AUTHORIZE (case)", "#(SOURCE_AUTHORIZE) 'finance' in $ROLE"],
      ["# (authorize) (motly)", "# (authorize) 'finance' in $ROLE"],
   ];

   for (const [label, annotation] of TYPO_SPELLINGS) {
      it(`\`${label}\` fails the load rather than loading inert`, async () => {
         const { model, duckdb, dir } = await createModel(`
given:
  ROLE :: string[]

${annotation}
source: X is duckdb.table('accounts') extend {}
`);
         try {
            const err = compilationErrorOf(model);
            expect(err).toBeInstanceOf(ModelCompilationError);
            // Pin the actual near-miss refusal, not just SOME load failure —
            // AuthorizeGrammarError also extends ModelCompilationError, so the
            // instance check alone stays green even if the wrong path fired.
            expect(err).not.toBeInstanceOf(AuthorizeGrammarError);
            expect(err?.message).toMatch(/never enforced/i);
            expect(err?.message).toContain("#(authorize)");
         } finally {
            await cleanup(duckdb, dir);
         }
      });
   }
});

// ---------------------------------------------------------------------------
// Group F — the two routes side by side
// ---------------------------------------------------------------------------

/**
 * The lock and the filter answer different questions, so a source may carry
 * both and neither sheds the other. The per-route scoping that lets them share
 * a given is what makes the pair usable at all. Rows, not parse success, are
 * what catch a regression here.
 */
describe("authorize syntax conformance — Group F (lock and filter together)", () => {
   it("a lock and a filter on one source both apply", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]
  ORGS :: string[]

#(authorize) 'finance' in $GROUPS
#(access_filter) org_id in $ORGS
source: base is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const rows = await rowsFor(model, "base", {
            GROUPS: ["finance"],
            ORGS: ["org1"],
         });
         const widened = await rowsFor(model, "base", {
            GROUPS: ["finance"],
            ORGS: ["org1", "org2"],
         });
         // Admitted by the lock, then narrowed by the filter.
         expect(ids(rows).length).toBeGreaterThan(0);
         expect(ids(widened).length).toBeGreaterThan(ids(rows).length);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("an extension's own `#(authorize) true` sheds the base's LOCK and leaves the inherited filter standing", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]
  ORGS :: string[]

#(authorize) 'finance' in $GROUPS
#(access_filter) org_id in $ORGS
source: base is duckdb.table('accounts') extend {}

#(authorize) true
source: ext is base extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         // Not in `finance`: the base's lock would refuse, the `true` opens it.
         // The inherited filter still narrows, so this is not every row.
         const rows = await rowsFor(model, "ext", {
            GROUPS: [],
            ORGS: ["org1"],
         });
         const widened = await rowsFor(model, "ext", {
            GROUPS: [],
            ORGS: ["org1", "org2"],
         });
         expect(ids(rows).length).toBeGreaterThan(0);
         expect(ids(widened).length).toBeGreaterThan(ids(rows).length);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("the same given may be reused across the two routes — scoping is per route", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) 'finance' in $GROUPS
#(access_filter) org_id in $GROUPS
source: base is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

// ---------------------------------------------------------------------------
// Group F — the lock's RESPONSE, end to end. Every assertion here is on rows
// or on the thrown error, never on a parse: the whole subject of the route
// split is what a denied caller receives, and a test that asserts the parse
// passes just as happily when the gate grafts a filter instead.
// ---------------------------------------------------------------------------

describe("authorize syntax conformance — Group F (the lock answers 403)", () => {
   it("an AGGREGATE over a locked source is refused, not answered with a fabricated zero", async () => {
      // The case the route split exists for. Grafted as `where: false`,
      // `SELECT sum(amount) ... WHERE FALSE` is one row of NULL and
      // `count()` is `0` — an answer about data the caller was refused.
      const { model, duckdb, dir } = await createModel(`
given:
  ROLE :: string

#(authorize) 'admin' = $ROLE
source: X is duckdb.table('accounts') extend {
   measure: total is amount.sum()
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         for (const q of ["aggregate: total", "aggregate: n"]) {
            await expect(
               model.getQueryResults(
                  undefined,
                  undefined,
                  `run: X -> { ${q} }`,
                  {},
                  true,
                  { ROLE: "user" } as never,
               ),
            ).rejects.toBeInstanceOf(AccessDeniedError);
         }
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("the lock runs first: a caller it refuses never sees the filter's zero rows", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  ROLE :: string
  ORGS :: string[]

#(authorize) 'admin' = $ROLE
#(access_filter) org_id in $ORGS
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         expect(
            ids(await rowsFor(model, "X", { ROLE: "admin", ORGS: ["org1"] })),
         ).toEqual([1, 2, 3]);
         // Lock passes, filter matches nothing: 200 with zero rows.
         expect(
            ids(await rowsFor(model, "X", { ROLE: "admin", ORGS: ["nope"] })),
         ).toEqual([]);
         // Lock fails: 403, whatever the filter would have done.
         await expectRefused(model, "X", { ROLE: "user", ORGS: ["org1"] });
         await expectRefused(model, "X", { ROLE: "user", ORGS: ["nope"] });
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`$GIVEN = 'literal'` and `'literal' = $GIVEN` decide identically", async () => {
      // The graft compiles the author's own text, so both operand orders
      // reach the decision.
      for (const body of ["'admin' = $ROLE", "$ROLE = 'admin'"]) {
         const { model, duckdb, dir } = await createModel(`
given:
  ROLE :: string

#(authorize) ${body}
source: X is duckdb.table('accounts') extend {}
`);
         try {
            expect(compilationErrorOf(model)).toBeUndefined();
            expect(ids(await rowsFor(model, "X", { ROLE: "admin" }))).toEqual([
               1, 2, 3, 4, 5, 6,
            ]);
            await expectRefused(model, "X", { ROLE: "user" });
         } finally {
            await cleanup(duckdb, dir);
         }
      }
   });

   it("a null given binding is a 403, not a 500", async () => {
      // `null` reaches the decision as a real value rather than an absent
      // one, so an unguarded `(null).some(...)` would be a TypeError — and a
      // TypeError is a 500, which is a different response class entirely.
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) 'finance' in $GROUPS
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectRefused(model, "X", { GROUPS: null });
         await expectRefused(model, "X", {});
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("comparison does not case-fold — `'Finance'` is not `'finance'`", async () => {
      // A source-shaped gate on the pre-flip row route was compared with
      // warehouse collation, and MySQL's default is case-insensitive. The
      // decision is exact-match TypeScript now, which is a real behavior
      // change for those tenants and fails closed.
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) 'finance' in $GROUPS
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectRefused(model, "X", { GROUPS: ["Finance"] });
         expect(
            ids(await rowsFor(model, "X", { GROUPS: ["finance"] })),
         ).toEqual([1, 2, 3, 4, 5, 6]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a lock on a source whose base is unreadable is refused, never served empty", async () => {
      // B1: the fail-closed `["false"]` sentinel is synthesized on BOTH
      // routes for the same struct, and the two routes are walked
      // filter-first. Without the route in `resolveGateShape`'s cache key the
      // filter walk's `row_level` entry answers the lock's lookup, the lock
      // grafts `where: false`, and the caller gets 200 with zero rows on a
      // source they may not reach. Reversing CANONICAL_AUTHORIZE_ROUTES must
      // leave this green.
      const { model, duckdb, dir } = await createModel(`
given:
  ROLE :: string
  ORGS :: string[]

#(authorize) 'admin' = $ROLE
#(access_filter) org_id in $ORGS
source: base is duckdb.table('accounts') extend {}

source: derived is base extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectRefused(model, "derived", {
            ROLE: "user",
            ORGS: ["org1"],
         });
         await expectRefused(model, "base", { ROLE: "user", ORGS: ["org1"] });
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});
