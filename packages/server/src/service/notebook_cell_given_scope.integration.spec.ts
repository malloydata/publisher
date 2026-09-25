// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Harness pattern from authorize_import_hop.integration.spec.ts.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { MalloyError, type Connection } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError } from "../errors";
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
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "nb-given-scope-"));
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

function cellCount(cell: unknown): number {
   const raw = (cell as { result?: string }).result;
   if (!raw) throw new Error("notebook cell returned no result");
   const parsed = JSON.parse(raw) as {
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
   return count;
}

const PLAIN = `source: plain is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

/** Row-filter gate: forwarding GROUPS lets the caller scope the rows they see. */
const GROUPS_GATE = `##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

/** Lock gate: forwarding ROLE either admits or 403s. */
const ROLE_LOCK = `##! experimental.givens

given:
  ROLE :: string

#(authorize) 'admin' = $ROLE
source: locked is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

describe("notebook cells receive only the givens in their own scope", () => {
   it("(1) pre-import cell with GROUPS supplied runs — the given isn't declared yet, so the value is dropped rather than 400ing", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "plain.malloy": PLAIN,
            "gate.malloy": GROUPS_GATE,
            "nb.malloynb": `>>>malloy
import "plain.malloy"
run: plain -> { aggregate: c }
>>>malloy
##! experimental.givens
import "gate.malloy"
run: gated -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         const result = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [1],
         });
         expect(cellCount(result)).toBe(4);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("(2) a typo'd given 400s on both the pre-import and the post-import cell", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "plain.malloy": PLAIN,
            "gate.malloy": GROUPS_GATE,
            "nb.malloynb": `>>>malloy
import "plain.malloy"
run: plain -> { aggregate: c }
>>>malloy
##! experimental.givens
import "gate.malloy"
run: gated -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         for (const cellIndex of [0, 1]) {
            const rejection = model.executeNotebookCell(
               cellIndex,
               undefined,
               false,
               { NOtaGiven: 1 },
            );
            await expect(rejection).rejects.toBeInstanceOf(MalloyError);
            await expect(rejection).rejects.toThrow(/NOtaGiven/);
         }
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("(3) a gate-only given (never on the model's own surface) is still dropped, cellDeclared or not", async () => {
      // ROLE is gate-only (never on the surface); a lock two hops deep always
      // denies, so the point is that it 403s rather than 400ing on ROLE.
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "deep_base.malloy": `##! experimental.givens

given:
  ROLE :: string

#(authorize) 'analyst' = $ROLE
source: deep_locked is duckdb.table('orgtable') extend {
  measure: c is count()
}
`,
            "deep_mid.malloy": `import "deep_base.malloy"

source: deep_ext is deep_locked extend {}
`,
            "nb.malloynb": `>>>malloy
import "deep_mid.malloy"
run: deep_ext -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         await expect(
            model.executeNotebookCell(0, undefined, false, {
               ROLE: "analyst",
            }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("(4) post-import cell binds GROUPS: [1] filters, [] returns 0, and the lock admits/denies", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "gate.malloy": GROUPS_GATE,
            "lock.malloy": ROLE_LOCK,
            "nb.malloynb": `>>>malloy
##! experimental.givens
import "gate.malloy"
import "lock.malloy"
run: gated -> { aggregate: c }
>>>malloy
run: locked -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         const mine = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [1],
         });
         expect(cellCount(mine)).toBe(2);

         const none = await model.executeNotebookCell(0, undefined, false, {
            GROUPS: [],
         });
         expect(cellCount(none)).toBe(0);

         const admitted = await model.executeNotebookCell(1, undefined, false, {
            ROLE: "admin",
         });
         expect(cellCount(admitted)).toBe(4);
         await expect(
            model.executeNotebookCell(1, undefined, false, { ROLE: "guest" }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("(5) a cell between `import plain` and `import groups` runs without GROUPS; a later cell filters", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "plain.malloy": PLAIN,
            "gate.malloy": GROUPS_GATE,
            "nb.malloynb": `>>>malloy
import "plain.malloy"
run: plain -> { aggregate: c }
>>>malloy
run: plain -> { aggregate: c }
>>>malloy
##! experimental.givens
import "gate.malloy"
run: gated -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         const middle = await model.executeNotebookCell(1, undefined, false, {
            GROUPS: [1],
         });
         expect(cellCount(middle)).toBe(4);

         const last = await model.executeNotebookCell(2, undefined, false, {
            GROUPS: [1],
         });
         expect(cellCount(last)).toBe(2);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   // TENANT is declared twice: defaulted in tenant_base (read by a plain
   // `where:`), undefaulted in tenant_gate (read by a gate). Cell 0 reaches
   // tenant_base two hops deep, so TENANT is in its declared set but not on its
   // surface. Dropping it there would bind the default; it must 400 instead.
   it("(6) a given declared deep in a cell but surfaced only by a later import 400s, never default-binds", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "tenant_base.malloy": `##! experimental.givens

given:
  TENANT :: number is 1

source: rows is duckdb.table('orgtable') extend {
  where: org_id = $TENANT
  measure: c is count()
}
`,
            "tenant_hub.malloy": `import "tenant_base.malloy"

source: visible_rows is rows extend {}
`,
            "tenant_gate.malloy": `##! experimental.givens

given:
  TENANT :: number

#(access_filter) org_id = $TENANT
source: gated_tenant is duckdb.table('orgtable') extend {
  measure: c is count()
}
`,
            "nb.malloynb": `>>>malloy
import "tenant_hub.malloy"
run: visible_rows -> { aggregate: c }
>>>malloy
##! experimental.givens
import { TENANT, gated_tenant } from "tenant_gate.malloy"
run: gated_tenant -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         const rejection = model.executeNotebookCell(0, undefined, false, {
            TENANT: 2,
         });
         await expect(rejection).rejects.toBeInstanceOf(MalloyError);
         await expect(rejection).rejects.toThrow(/TENANT/);

         const later = await model.executeNotebookCell(1, undefined, false, {
            TENANT: 2,
         });
         expect(cellCount(later)).toBe(2);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   const TENANT_BASE = `##! experimental.givens

given:
  TENANT :: number is 1

source: rows is duckdb.table('orgtable') extend {
  where: org_id = $TENANT
  measure: c is count()
}
`;

   // The surface and the runtime bind by the alias; the cell's registry keeps
   // the declaration name, so a name-only match would drop T and bind the default.
   it("(6a) a given imported under an alias binds the caller's value, not the default", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "tenant_base.malloy": TENANT_BASE,
            "nb.malloynb": `>>>malloy
##! experimental.givens
import { T is TENANT, rows } from "tenant_base.malloy"
run: rows -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         const result = await model.executeNotebookCell(0, undefined, false, {
            T: 2,
         });
         expect(cellCount(result)).toBe(2);
         const none = await model.executeNotebookCell(0, undefined, false, {
            T: 3,
         });
         expect(cellCount(none)).toBe(0);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("(6b) an alias surfaced only by a later cell still 400s on an earlier cell that reaches the given deep", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "tenant_base.malloy": TENANT_BASE,
            "tenant_hub.malloy": `import "tenant_base.malloy"

source: visible_rows is rows extend {}
`,
            "nb.malloynb": `>>>malloy
import "tenant_hub.malloy"
run: visible_rows -> { aggregate: c }
>>>malloy
##! experimental.givens
import { T is TENANT } from "tenant_base.malloy"
run: visible_rows -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         const rejection = model.executeNotebookCell(0, undefined, false, {
            T: 3,
         });
         await expect(rejection).rejects.toBeInstanceOf(MalloyError);
         await expect(rejection).rejects.toThrow(/'T'/);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("(7) same-cell import+run still gates", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "gate.malloy": GROUPS_GATE,
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
            GROUPS: [1],
         });
         expect(cellCount(result)).toBe(2);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("(8) a lock cell ahead of a later import still 403s, with the wrong role or none", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "plain.malloy": PLAIN,
            "lock.malloy": ROLE_LOCK,
            "nb.malloynb": `>>>malloy
##! experimental.givens
import "lock.malloy"
run: locked -> { aggregate: c }
>>>malloy
import "plain.malloy"
run: plain -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         await expect(
            model.executeNotebookCell(0, undefined, false, {
               ROLE: "guest",
            }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
         await expect(model.executeNotebookCell(0)).rejects.toBeInstanceOf(
            AccessDeniedError,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("(9) a leading markdown cell doesn't shift index alignment", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "plain.malloy": PLAIN,
            "gate.malloy": GROUPS_GATE,
            "nb.malloynb": `>>>markdown
# Notes
>>>malloy
import "plain.malloy"
run: plain -> { aggregate: c }
>>>malloy
##! experimental.givens
import "gate.malloy"
run: gated -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         const markdown = await model.executeNotebookCell(0);
         expect(markdown.type).toBe("markdown");

         const preImport = await model.executeNotebookCell(
            1,
            undefined,
            false,
            { GROUPS: [1] },
         );
         expect(cellCount(preImport)).toBe(4);

         const postImport = await model.executeNotebookCell(
            2,
            undefined,
            false,
            { GROUPS: [1] },
         );
         expect(cellCount(postImport)).toBe(2);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("(10) a `#(secure)` given, the shape a host injects on every run, is dropped before its import and binds after it", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "plain.malloy": PLAIN,
            "gate.malloy": `##! experimental.givens

#(secure)
given: GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('orgtable') extend {
  measure: c is count()
}
`,
            "nb.malloynb": `>>>malloy
import "plain.malloy"
run: plain -> { aggregate: c }
>>>malloy
##! experimental.givens
import "gate.malloy"
run: gated -> { aggregate: c }
`,
         },
         "nb.malloynb",
      );
      try {
         const preImport = await model.executeNotebookCell(
            0,
            undefined,
            false,
            { GROUPS: [1] },
         );
         expect(cellCount(preImport)).toBe(4);

         const mine = await model.executeNotebookCell(1, undefined, false, {
            GROUPS: [1],
         });
         expect(cellCount(mine)).toBe(2);

         const none = await model.executeNotebookCell(1, undefined, false, {
            GROUPS: [],
         });
         expect(cellCount(none)).toBe(0);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});
