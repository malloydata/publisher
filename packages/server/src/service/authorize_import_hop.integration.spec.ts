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
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { type Connection } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Model } from "./model";

const SEED_SQL = `
CREATE OR REPLACE TABLE orgtable (id INTEGER, org_id INTEGER, owner INTEGER, val VARCHAR);
INSERT INTO orgtable VALUES
   (1, 1, 2, 'a'), (2, 1, 1, 'b'), (3, 2, 1, 'c'), (4, 2, 2, 'd');
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

async function createModelWithFiles(
   files: Record<string, string>,
   entryFileName: string,
): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
   const duckdb = await newDuckdb();
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "authz-import-hop-"));
   for (const [fileName, text] of Object.entries(files)) {
      fs.writeFileSync(path.join(dir, fileName), text);
   }
   const model = await Model.create(
      "test-pkg",
      dir,
      entryFileName,
      new Map<string, Connection>([["duckdb", duckdb]]),
   );
   return { model, duckdb, dir };
}

async function cleanup(duckdb: DuckDBConnection, dir: string): Promise<void> {
   await duckdb.close();
   fs.rmSync(dir, { recursive: true, force: true });
}

/** The gated source, in its own file so importers get an UNEXPORTED entry. */
const GIVEN_ONLY_GATE = `##! experimental.givens

given:
  ROLE :: string

#(authorize) $ROLE = 'analyst'
source: gated is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

const ROW_FIELD_GATE = `##! experimental.givens

given:
  GROUPS :: number[]

#(authorize) org_id in $GROUPS
source: gated is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

/** `compactResult` rows from `getQueryResults`, as this file's count shape. */
function countRows(result: unknown): { c: number }[] {
   return (result as { compactResult?: unknown }).compactResult as {
      c: number;
   }[];
}

/**
 * The aggregate count out of a notebook cell, whose `result` is JSON text
 * holding Malloy's nested cell encoding. Also returns the generated `sql`, so
 * a test can assert the gate reached the warehouse and not merely the IR.
 */
function notebookCell(cell: unknown): { count: number; sql: string } {
   const raw = (cell as { result?: string }).result;
   if (!raw) throw new Error("notebook cell returned no result");
   const parsed = JSON.parse(raw) as {
      sql?: string;
      data?: {
         array_value?: Array<{
            record_value?: Array<{ number_value?: number }>;
         }>;
      };
   };
   const rows = parsed.data?.array_value ?? [];
   if (rows.length !== 1) {
      throw new Error(`expected one aggregate row, got ${rows.length}`);
   }
   const count = rows[0].record_value?.[0]?.number_value;
   if (typeof count !== "number") {
      throw new Error("aggregate cell was not a number");
   }
   return { count, sql: parsed.sql ?? "" };
}

describe("a notebook cell that imports a gated source and runs it in the SAME cell", () => {
   // The blocking regression. There is no earlier code cell to graft
   // against, so the cell's own scope is the graft target and the bind goes
   // through `_loadQueryFromQueryDef` — which never recompiles, so it never
   // re-derives the run target from the grafted `contents`.
   it("ADMITS an authorized caller (given-only gate)", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
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
         const result = await model.executeNotebookCell(0, undefined, false, {
            ROLE: "analyst",
         });
         // 4, not 0: an admitted given-only gate filters nothing away.
         expect(notebookCell(result).count).toBe(4);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("filters to the caller's rows (row-field gate)", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
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
         const mine = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [1],
         });
         const mineCell = notebookCell(mine);
         expect(mineCell.count).toBe(2);
         // The gate reached the WAREHOUSE, not just the IR — the proof the
         // in-process checks structurally cannot give.
         expect(mineCell.sql).toMatch(/where/i);
         expect(mineCell.sql).toMatch(/org_id/i);

         // Empty membership matches no row — a 200 with zero, not a 403.
         const none = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [],
         });
         expect(notebookCell(none).count).toBe(0);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("the graft never escapes the request that made it", () => {
   it("gives two callers with different givens their own row sets from the SAME cell", async () => {
      // `cell.runnable` is memoized across requests. Grafting the cell's
      // queryDef IN PLACE would poison that cache: whoever queried first
      // would decide what everyone after them sees. The queryDef is deep-
      // cloned per request for exactly this reason, and this is the test that
      // would catch losing that clone.
      const { model, duckdb, dir } = await createModelWithFiles(
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
         expect(
            notebookCell(
               await model.executeNotebookCell(0, undefined, false, {
                  GROUPS: [1],
               }),
            ).count,
         ).toBe(2);
         // Runs second, against the same memoized runnable. A stacked or
         // retained filter from the call above shows up here as 0 instead of
         // 4, and a lost graft shows up as 4 instead of 2 on a repeat.
         expect(
            notebookCell(
               await model.executeNotebookCell(0, undefined, false, {
                  GROUPS: [1, 2],
               }),
            ).count,
         ).toBe(4);
         expect(
            notebookCell(
               await model.executeNotebookCell(0, undefined, false, {
                  GROUPS: [1],
               }),
            ).count,
         ).toBe(2);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("leaves an UNGATED source in the same cell unfiltered", async () => {
      // Notebook-side precision control, the sibling of the named-query one
      // below: the gate must reach its own source and no other.
      const { model, duckdb, dir } = await createModelWithFiles(
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
         const cell = notebookCell(
            await model.executeNotebookCell(0, undefined, false, {
               GROUPS: [1],
            }),
         );
         expect(cell.count).toBe(4);
         expect(cell.sql).not.toMatch(/org_id/i);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("a model-level named query over an imported gated source", () => {
   // The documented single-query dashboard shape (`docs/dashboards.md`):
   // `import` + `# artifact` on a model-level `query:`. Pre-existing, not a
   // regression — the stored `NamedQueryDef` holds its own pre-graft
   // `structRef` snapshot, which `query-reference.js` reuses verbatim.
   it("filters to the caller's rows rather than denying", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
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
         const result = await model.getQueryResults(
            undefined,
            "tile",
            undefined,
            undefined,
            false,
            { GROUPS: [2] },
         );
         expect(countRows(result)).toEqual([{ c: 2 }]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("leaves an UNGATED imported source's named query untouched", async () => {
      // Precision control: passes before and after the fix. Proves the graft
      // reaches the gated query only, rather than spraying every named query
      // that happens to read an imported source.
      const { model, duckdb, dir } = await createModelWithFiles(
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
         const result = await model.getQueryResults(
            undefined,
            "plain_tile",
            undefined,
            undefined,
            false,
            { GROUPS: [1] },
         );
         // All 4 rows: the gate on the sibling source must not reach here.
         expect(countRows(result)).toEqual([{ c: 4 }]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});
