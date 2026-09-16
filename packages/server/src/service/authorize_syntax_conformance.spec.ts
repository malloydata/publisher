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
import { ModelCompilationError } from "../errors";
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

// ---------------------------------------------------------------------------
// Group A — accepted shapes
// ---------------------------------------------------------------------------

describe("authorize syntax conformance — Group A (accepted shapes)", () => {
   it("row-level `in` — array membership, differs per principal", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) org_id in $GROUPS
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

#(authorize) \`cost center\` in $GROUPS
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

#(authorize) region = $REGION
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

#(authorize) child.name in $GROUPS
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

   it("source-level `'literal' = $GIVEN` — scalar given, admits all or none", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  ROLE :: string

#(authorize) 'admin' = $ROLE
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const admin = await rowsFor(model, "X", { ROLE: "admin" });
         const user = await rowsFor(model, "X", { ROLE: "user" });
         expect(ids(admin)).toEqual([1, 2, 3, 4, 5, 6]);
         expect(ids(user)).toEqual([]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("source-level `'literal' in $GIVEN` — list given, admits when the literal is a member", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  ROLES :: string[]

#(authorize) 'admin' in $ROLES
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const admin = await rowsFor(model, "X", { ROLES: ["admin", "user"] });
         const user = await rowsFor(model, "X", { ROLES: ["user"] });
         expect(ids(admin)).toEqual([1, 2, 3, 4, 5, 6]);
         expect(ids(user)).toEqual([]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a conjunction of two row-level terms joined by `and`", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]
  REGION :: string

#(authorize) org_id in $GROUPS and region = $REGION
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

#(authorize) org_id in $GROUPS
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

   it("empty array given — fail-closed, zero rows", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) org_id in $GROUPS
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

#(authorize) org_id in $GROUPS or region = $REGION
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

#(authorize) region != $REGION
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

#(authorize) amount > $AMOUNTMIN
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

#(authorize) org_id = $GROUPS
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

#(authorize) owner in $ROLE
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

   it("bare boolean literals `true`/`false` no longer parse (no operator, no given)", async () => {
      await expectRejectionCause(
         `
given:
  UNUSED :: string

#(authorize) true
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

#(authorize) region = $REGION
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

#(authorize) org_id in $GROUPS
#(authorize) region = $REGION
source: X is duckdb.table('accounts') extend {}
`;

const TWO_NOTES_AND_EQUIVALENT = `${TWO_NOTES}
#(authorize) org_id in $GROUPS and region = $REGION
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

#(authorize) a = $G
#(authorize) b = $G
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

#(authorize) region = $A
#(authorize) region = $B
source: X is duckdb.table('accounts') extend {}
`,
         "duplicate_field_path",
      );
   });

   it("mixed_scope_body across two notes — a row-level note and a source-level note on the same source", async () => {
      await expectRejectionCause(
         `
given:
  REGION :: string
  ROLE :: string

#(authorize) region = $REGION
#(authorize) 'admin' = $ROLE
source: X is duckdb.table('accounts') extend {}
`,
         "mixed_scope_body",
      );
   });

   it("`#(authorize) false` plus any sibling note on the same source is refused", async () => {
      await expectRejectionCause(
         `
given:
  REGION :: string

#(authorize) false
#(authorize) region = $REGION
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

#(authorize) region in $GROUPS
source: member_a is duckdb.table('accounts') extend {}

source: member_b is duckdb.sql("select id, org_id from accounts") extend {}

#(authorize) org_id in $GROUPS
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
// Group E — `#(source-authorize)`: a rule about the CALLER rather than the
// row, that ANDs with any row-level `#(authorize)` gate rather than
// bypassing it. Same load path (`Model.create`), same enforcement mechanism
// (a graft) as `#(authorize)` — the only new machinery is the second route
// and its body restriction.
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

describe("authorize syntax conformance — Group E (#(source-authorize))", () => {
   it("survives `source: mine is base extend {}` — inherits through extend", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  ROLE :: string[]

#(source-authorize) 'finance' in $ROLE
source: base is duckdb.table('accounts') extend {}

source: mine is base extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const admitted = await rowsFor(model, "mine", { ROLE: ["finance"] });
         const denied = await rowsFor(model, "mine", { ROLE: ["sales"] });
         expect(ids(admitted)).toEqual([1, 2, 3, 4, 5, 6]);
         expect(ids(denied)).toEqual([]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("grafts as its own entry and ANDs with a row-level #(authorize) gate — a caller it does not admit gets 200 with ZERO ROWS", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]
  ROLE :: string[]

#(authorize) org_id in $GROUPS
#(source-authorize) 'finance' in $ROLE
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
         // Row-level gate satisfied, source-authorize NOT satisfied — 200,
         // zero rows, not an error. Asserted on the ACTUAL ROWS, not filter
         // text, since the whole point is that the AND is enforced, not
         // merely declared.
         const roleDenied = await rowsFor(model, "X", {
            GROUPS: ["org1"],
            ROLE: ["sales"],
         });
         expect(ids(roleDenied)).toEqual([]);
         // Source-authorize satisfied, row-level gate NOT satisfied — also
         // zero rows.
         const orgDenied = await rowsFor(model, "X", {
            GROUPS: ["org-nowhere"],
            ROLE: ["finance"],
         });
         expect(ids(orgDenied)).toEqual([]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`#(source-authorize) false` is accepted and behaves identically to `#(authorize) false`", async () => {
      const sourceAuthorizeFalse = await createModel(`
#(source-authorize) false
source: X is duckdb.table('accounts') extend {}
`);
      const authorizeFalse = await createModel(`
#(authorize) false
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(sourceAuthorizeFalse.model)).toBeUndefined();
         expect(compilationErrorOf(authorizeFalse.model)).toBeUndefined();
         const viaSourceAuthorize = await rowsFor(
            sourceAuthorizeFalse.model,
            "X",
            {},
         );
         const viaAuthorize = await rowsFor(authorizeFalse.model, "X", {});
         expect(ids(viaSourceAuthorize)).toEqual([]);
         expect(ids(viaSourceAuthorize)).toEqual(ids(viaAuthorize));
      } finally {
         await cleanup(sourceAuthorizeFalse.duckdb, sourceAuthorizeFalse.dir);
         await cleanup(authorizeFalse.duckdb, authorizeFalse.dir);
      }
   });

   it("`#(source-authorize) false` alongside `#(authorize) org_id in $GROUPS` is refused (deny_all_with_sibling)", async () => {
      await expectRejectionCause(
         `
given:
  GROUPS :: string[]

#(source-authorize) false
#(authorize) org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}
`,
         "deny_all_with_sibling",
      );
   });

   it("a row-level term in a #(source-authorize) body is refused (row_level_term_in_source_authorize)", async () => {
      await expectRejectionCause(
         `
given:
  REGION :: string

#(source-authorize) region = $REGION
source: X is duckdb.table('accounts') extend {}
`,
         "row_level_term_in_source_authorize",
      );
   });

   it('`#(source-authorize) "x"` draws the same legacy-string refusal as `#(authorize) "x"`', async () => {
      await expectModelCompilationError(
         `
given:
  ROLE :: string

#(source-authorize) "$ROLE = 'admin'"
source: X is duckdb.table('accounts') extend {}
`,
         /string form.*no longer accepted/is,
      );
   });

   it("`#(source-authorize)` on a field is refused as misplaced", async () => {
      await expectModelCompilationError(
         `
given:
  ROLE :: string[]

source: X is duckdb.table('accounts') extend {
   #(source-authorize) 'finance' in $ROLE
   dimension: d is org_id
}
`,
         /never enforced.*#\(source-authorize\)/is,
      );
   });

   it("`#(source-authorize)` on a `query:` statement is refused as misplaced", async () => {
      await expectModelCompilationError(
         `
given:
  ROLE :: string[]

source: X is duckdb.table('accounts') extend {}

#(source-authorize) 'finance' in $ROLE
query: q is X -> { select: id }
`,
         /never enforced.*#\(source-authorize\)/is,
      );
   });

   it("file-level `##(source-authorize)` is refused as misplaced", async () => {
      await expectModelCompilationError(
         `
given:
  ROLE :: string[]

##(source-authorize) 'finance' in $ROLE

source: X is duckdb.table('accounts') extend {}
`,
         /file level/i,
      );
   });

   it("is rejected in caller-submitted Malloy text", async () => {
      const { model, duckdb, dir } = await createModel(`
given:
  GROUPS :: string[]

#(authorize) org_id in $GROUPS
source: X is duckdb.table('accounts') extend {}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               `#(source-authorize) 'x' in $GROUPS\nrun: X -> { select: id }`,
               {},
               true,
               { GROUPS: ["org1"] } as never,
            ),
         ).rejects.toThrow(/not permitted in caller-submitted/);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   // Each of these five spellings reads as an attempt at `#(source-authorize)`
   // but Malloy does not route it there — a naive implementation loads clean
   // and serves every row. See `authorize.spec.ts`'s
   // `collectAuthorizeNearMisses` unit tests for the per-spelling routing
   // table this end-to-end sweep exercises.
   const TYPO_SPELLINGS: ReadonlyArray<[string, string]> = [
      ["source_authorize", "#(source_authorize) 'finance' in $ROLE"],
      ["sourceauthorize", "#(sourceauthorize) 'finance' in $ROLE"],
      ["authorize-source", "#(authorize-source) 'finance' in $ROLE"],
      ["SOURCE-AUTHORIZE (case)", "#(SOURCE-AUTHORIZE) 'finance' in $ROLE"],
      [
         "# (source-authorize) (motly)",
         "# (source-authorize) 'finance' in $ROLE",
      ],
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
            expect(err?.message).toContain("#(source-authorize)");
         } finally {
            await cleanup(duckdb, dir);
         }
      });
   }
});
