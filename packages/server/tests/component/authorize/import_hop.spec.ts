// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(authorize)` when the gated source arrives through an `import`.
 *
 * The gate is enforced by grafting its condition onto the gated source inside
 * a clone of the model, recompiling, then proving the condition landed. That
 * works only when the recompile RE-DERIVES the run target from
 * `contents[graftTarget]`. It fails whenever the recompile reuses a pre-graft
 * snapshot of the struct — and an `import` is exactly what produces one:
 * `import-statement.js` marks every imported entry `exported: false`, and
 * `named-source.js` INLINES an unexported entry's struct at the reference
 * site instead of emitting a name. The graft then lands on a `contents` entry
 * nothing consults, the proof correctly reports the condition missing, and an
 * AUTHORIZED caller is denied — byte-identical to a real denial.
 *
 * No fixture anywhere else puts a gated source behind an `import`, which is
 * why this shipped broken. Every test here uses two files for that reason;
 * collapsing one into a single file makes it pass while testing nothing.
 */
import type { GivenValue } from "@malloydata/malloy";
import { unrecordedAudit } from "../../../src/service/access_record";
import type { Model, ModelQueryResults } from "../../../src/service/model";
import { describe, expect, it } from "bun:test";
import { loadAuthorizeModel } from "../../../src/test_helpers/authorize_fixture";

/** The gated source, in its own file so importers get an UNEXPORTED entry. */
const GIVEN_ONLY_GATE = `##! experimental.givens

#(secure)
given:
  ROLE :: string

#(authorize) 'analyst' = $ROLE
source: gated is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

const ROW_FIELD_GATE = `##! experimental.givens

#(secure)
given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

/** `compactResult` rows from `runQuery`, as this file's count shape. */
function countRows(result: unknown): { c: number }[] {
   return (result as { compactResult?: unknown }).compactResult as {
      c: number;
   }[];
}

/**
 * The aggregate count out of a query run on a notebook's path, with the
 * generated `sql`, so a test can assert the gate reached the warehouse and not
 * merely the IR.
 */
function notebookCount(result: ModelQueryResults): {
   count: number;
   sql: string;
} {
   const rows = countRows(result);
   if (rows.length !== 1) {
      throw new Error(`expected one aggregate row, got ${rows.length}`);
   }
   return { count: rows[0].c, sql: result.result.sql ?? "" };
}

/** Run `query` on the notebook's own path with `givens`, as the query route does. */
async function runOnNotebook(
   model: Model,
   query: string,
   givens: Record<string, GivenValue>,
): Promise<{ count: number; sql: string }> {
   return notebookCount(
      await model.runQuery(unrecordedAudit(), { query, givens }),
   );
}

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations).
describe("deprecated: .malloynb — a notebook cell that imports a gated source and runs it in the SAME cell", () => {
   // The cell's query runs on the notebook's own path, so the gated source
   // reaches the run target through the cell's `import`: the import hop the
   // graft has to follow.
   it("ADMITS an authorized caller (given-only gate)", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "gate.malloy": GIVEN_ONLY_GATE,
            "nb.malloynb": `>>>malloy
##! experimental.givens
import "gate.malloy"
run: gated -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         const result = await runOnNotebook(
            model,
            "run: gated -> { aggregate: c }",
            { ROLE: "analyst" },
         );
         // 4, not 0: an admitted given-only gate filters nothing away.
         expect(result.count).toBe(4);
      } finally {
         await cleanup();
      }
   });

   it("filters to the caller's rows (row-field gate)", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "gate.malloy": ROW_FIELD_GATE,
            "nb.malloynb": `>>>malloy
##! experimental.givens
import "gate.malloy"
run: gated -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         // 2, not 4: the `2` is what proves the condition bound to `org_id`
         // rather than merely attaching somewhere and admitting everything.
         const mineCell = await runOnNotebook(
            model,
            "run: gated -> { aggregate: c }",
            { GROUPS: [1] },
         );
         expect(mineCell.count).toBe(2);
         // The gate reached the WAREHOUSE, not just the IR — the proof the
         // in-process checks structurally cannot give.
         expect(mineCell.sql).toMatch(/where/i);
         expect(mineCell.sql).toMatch(/org_id/i);

         // Empty membership matches no row — a 200 with zero, not a 403.
         const none = await runOnNotebook(
            model,
            "run: gated -> { aggregate: c }",
            { GROUPS: [] },
         );
         expect(none.count).toBe(0);
      } finally {
         await cleanup();
      }
   });
});

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations).
describe("deprecated: .malloynb — the graft never escapes the request that made it", () => {
   it("gives two callers with different givens their own row sets from the SAME cell", async () => {
      // A graft built for one request must not be seen by the next: whoever
      // queried first must not decide what everyone after them sees.
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "gate.malloy": ROW_FIELD_GATE,
            "nb.malloynb": `>>>malloy
##! experimental.givens
import "gate.malloy"
run: gated -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      const cell = "run: gated -> { aggregate: c }";
      try {
         expect((await runOnNotebook(model, cell, { GROUPS: [1] })).count).toBe(
            2,
         );
         // Runs second. A stacked or retained filter from the call above shows
         // up here as 0 instead of 4, and a lost graft shows up as 4 instead
         // of 2 on a repeat.
         expect(
            (await runOnNotebook(model, cell, { GROUPS: [1, 2] })).count,
         ).toBe(4);
         expect((await runOnNotebook(model, cell, { GROUPS: [1] })).count).toBe(
            2,
         );
      } finally {
         await cleanup();
      }
   });

   it("leaves an UNGATED source in the same cell unfiltered", async () => {
      // Notebook-side precision control, the sibling of the named-query one
      // below: the gate must reach its own source and no other.
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "gate.malloy": ROW_FIELD_GATE,
            "plain.malloy": `source: plain is duckdb.table('orgtable') extend {
  measure: c is count()
}
`,
            "nb.malloynb": `>>>malloy
##! experimental.givens
import "gate.malloy"
import "plain.malloy"
run: plain -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         const cell = await runOnNotebook(
            model,
            "run: plain -> { aggregate: c }",
            { GROUPS: [1] },
         );
         expect(cell.count).toBe(4);
         expect(cell.sql).not.toMatch(/org_id/i);
      } finally {
         await cleanup();
      }
   });
});

describe("a query_source built on an imported gated source", () => {
   it("aggregates over the CALLER's rows, not every tenant's", async () => {
      // A gated `query_source` aggregates over the rows the caller may see.
      //
      // Worth stating because an earlier version of this change claimed the
      // opposite: this is NOT a behaviour change to any previously-working
      // shape. `graftIntoNamedQuerySnapshots` only touches a stored query
      // whose `structRef` is an INLINED struct, which happens exactly when the
      // source arrived unexported through an `import`. A locally-declared
      // source is exported, so its named query stores the NAME `per_org` and
      // the sweep skips it -- measured identical with and without the sweep.
      // The imported shape below did not previously produce a different
      // number either; it produced a 403.
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "gate.malloy": ROW_FIELD_GATE,
            "dash.malloy": `##! experimental.givens
import "gate.malloy"

source: per_org is gated -> { group_by: org_id; aggregate: n is count() }

query: tile is per_org -> { aggregate: c is count() }
`,
         },
         "dash.malloy",
      );
      try {
         // Seed data holds two orgs. A caller scoped to one sees ONE group.
         const one = await model.runQuery(unrecordedAudit(), {
            queryName: "tile",
            givens: { GROUPS: [1] },
         });
         expect(countRows(one)).toEqual([{ c: 1 }]);

         // Scoped to both, the same tile counts two -- so the number tracks
         // the caller rather than being uniformly filtered or uniformly not.
         const both = await model.runQuery(unrecordedAudit(), {
            queryName: "tile",
            givens: { GROUPS: [1, 2] },
         });
         expect(countRows(both)).toEqual([{ c: 2 }]);
      } finally {
         await cleanup();
      }
   });
});

describe("a model-level named query over an imported gated source", () => {
   // The documented single-query dashboard shape (`docs/dashboards.md`):
   // `import` + `# artifact` on a model-level `query:`. Pre-existing, not a
   // regression — the stored `NamedQueryDef` holds its own pre-graft
   // `structRef` snapshot, which `query-reference.js` reuses verbatim.
   it("filters to the caller's rows rather than denying", async () => {
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "gate.malloy": ROW_FIELD_GATE,
            "dash.malloy": `##! experimental.givens
import "gate.malloy"

query: tile is gated -> { aggregate: c }
`,
         },
         "dash.malloy",
      );
      try {
         const result = await model.runQuery(unrecordedAudit(), {
            queryName: "tile",
            givens: { GROUPS: [2] },
         });
         expect(countRows(result)).toEqual([{ c: 2 }]);
      } finally {
         await cleanup();
      }
   });

   it("leaves an UNGATED imported source's named query untouched", async () => {
      // Precision control: passes before and after the fix. Proves the graft
      // reaches the gated query only, rather than spraying every named query
      // that happens to read an imported source.
      const { model, cleanup } = await loadAuthorizeModel(
         {
            "gate.malloy": ROW_FIELD_GATE,
            "plain.malloy": `source: plain is duckdb.table('orgtable') extend {
  measure: c is count()
}
`,
            "dash.malloy": `##! experimental.givens
import "gate.malloy"
import "plain.malloy"

query: gated_tile is gated -> { aggregate: c }
query: plain_tile is plain -> { aggregate: c }
`,
         },
         "dash.malloy",
      );
      try {
         const result = await model.runQuery(unrecordedAudit(), {
            queryName: "plain_tile",
            givens: { GROUPS: [1] },
         });
         // All 4 rows: the gate on the sibling source must not reach here.
         expect(countRows(result)).toEqual([{ c: 4 }]);
      } finally {
         await cleanup();
      }
   });
});
