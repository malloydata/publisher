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
