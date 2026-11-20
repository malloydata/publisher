// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// DEPRECATED FEATURE COVERAGE — delete together with
// `.malloynb` notebooks (see RELEASE_NOTES deprecations). Every test in
// this file is about that deprecated feature.
//
// Each cell's query runs on the notebook's own path, as the query route runs
// it: every cell compiles into one model, so the notebook's givens are one
// set, not a set per cell.

// Harness pattern from tests/component/authorize/import_hop.spec.ts.
import { unrecordedAudit } from "../../src/service/access_record";
import { type GivenValue, MalloyError } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { AccessDeniedError } from "../../src/errors";
import type { Model, ModelQueryResults } from "../../src/service/model";
import {
   createTempModel,
   seededDuckdb,
   type TempModel,
} from "../../src/test_helpers/temp_package";

const SEED_SQL = `
CREATE OR REPLACE TABLE orgtable (id INTEGER, org_id INTEGER, owner INTEGER, val VARCHAR);
INSERT INTO orgtable VALUES
   (1, 1, 2, 'a'), (2, 1, 1, 'b'), (3, 2, 1, 'c'), (4, 2, 2, 'd');
`;

async function createModelWithFiles(
   files: Record<string, string>,
   entryFileName: string,
): Promise<TempModel> {
   return createTempModel(files, entryFileName, {
      name: "test-pkg",
      duckdb: await seededDuckdb(SEED_SQL),
   });
}

/**
 * Run cell `cellIndex`'s query on the notebook's own path: its last `run:` to
 * the end of the cell, as the Console sends it.
 */
async function runCell(
   model: Model,
   cellIndex: number,
   givens?: Record<string, GivenValue>,
): Promise<ModelQueryResults> {
   const cell = (await model.getNotebook()).notebookCells?.[cellIndex];
   const text = cell?.text ?? "";
   const at = text.lastIndexOf("run:");
   if (at < 0) throw new Error(`cell ${cellIndex} runs no query`);
   return model.runQuery(unrecordedAudit(), { query: text.slice(at), givens });
}

function cellCount(result: ModelQueryResults): number {
   const rows = result.compactResult as { c: number }[];
   if (rows.length !== 1) {
      throw new Error(`expected one aggregate row, got ${rows.length}`);
   }
   return rows[0].c;
}

const PLAIN = `source: plain is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

/** Row-filter gate: forwarding GROUPS lets the caller scope the rows they see. */
const GROUPS_GATE = `##! experimental.givens

#(secure)
given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

/** Lock gate: forwarding ROLE either admits or 403s. */
const ROLE_LOCK = `##! experimental.givens

#(secure)
given:
  ROLE :: string

#(authorize) 'admin' = $ROLE
source: locked is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

describe("a notebook's givens, on the notebook's own path", () => {
   it("(1) a cell whose query does not read GROUPS runs with it supplied: GROUPS is on the notebook's surface", async () => {
      const { model, cleanup } = await createModelWithFiles(
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
         const result = await runCell(model, 0, { GROUPS: [1] });
         expect(cellCount(result)).toBe(4);
      } finally {
         await cleanup();
      }
   });

   it("(2) a typo'd given 400s on both cells, and a gated cell missing its gate's given 403s first, as a query does", async () => {
      const { model, cleanup } = await createModelWithFiles(
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
         const typos: [number, Record<string, GivenValue>][] = [
            [0, { NOtaGiven: 1 }],
            [1, { NOtaGiven: 1, GROUPS: [1] }],
         ];
         for (const [cellIndex, givens] of typos) {
            const rejection = runCell(model, cellIndex, givens);
            await expect(rejection).rejects.toBeInstanceOf(MalloyError);
            await expect(rejection).rejects.toThrow(/NOtaGiven/);
         }
         // The gate reads GROUPS, unsupplied: the opaque denial, before the
         // typo is reported.
         await expect(
            runCell(model, 1, { NOtaGiven: 1 }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup();
      }
   });

   it("(3) a gate-only given (never on the model's own surface) is still dropped", async () => {
      // ROLE is gate-only (never on the surface); a lock two hops deep always
      // denies, so the point is that it 403s rather than 400ing on ROLE.
      const { model, cleanup } = await createModelWithFiles(
         {
            "deep_base.malloy": `##! experimental.givens

#(secure)
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
            runCell(model, 0, { ROLE: "analyst" }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup();
      }
   });

   it("(4) a gated cell binds GROUPS: [1] filters, [] returns 0, and the lock admits/denies", async () => {
      const { model, cleanup } = await createModelWithFiles(
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
         const mine = await runCell(model, 0, { GROUPS: [1] });
         expect(cellCount(mine)).toBe(2);

         const none = await runCell(model, 0, { GROUPS: [] });
         expect(cellCount(none)).toBe(0);

         const admitted = await runCell(model, 1, { ROLE: "admin" });
         expect(cellCount(admitted)).toBe(4);
         await expect(
            runCell(model, 1, { ROLE: "guest" }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup();
      }
   });

   const TENANT_BASE = `##! experimental.givens

#(secure)
given:
  TENANT :: number is 1

source: rows is duckdb.table('orgtable') extend {
  where: org_id = $TENANT
  measure: c is count()
}
`;

   // The surface and the runtime bind by the alias; the registry keeps the
   // declaration name, so a name-only match would drop T and bind the default.
   it("(6a) a given imported under an alias binds the caller's value, not the default", async () => {
      const { model, cleanup } = await createModelWithFiles(
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
         const result = await runCell(model, 0, { T: 2 });
         expect(cellCount(result)).toBe(2);
         const none = await runCell(model, 0, { T: 3 });
         expect(cellCount(none)).toBe(0);
      } finally {
         await cleanup();
      }
   });

   // Every cell compiles into one model, so the alias a later cell imports is
   // on the surface for an earlier cell that reaches the given deep.
   it("(6b) an alias surfaced only by a later cell binds the caller's value on an earlier cell that reaches the given deep", async () => {
      const { model, cleanup } = await createModelWithFiles(
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
         expect(cellCount(await runCell(model, 0, { T: 3 }))).toBe(0);
         expect(cellCount(await runCell(model, 0, { T: 2 }))).toBe(2);
      } finally {
         await cleanup();
      }
   });

   it("(7) same-cell import+run still gates", async () => {
      const { model, cleanup } = await createModelWithFiles(
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
         const result = await runCell(model, 0, { GROUPS: [1] });
         expect(cellCount(result)).toBe(2);
      } finally {
         await cleanup();
      }
   });

   it("(8) a lock cell ahead of a later import still 403s, with the wrong role or none", async () => {
      const { model, cleanup } = await createModelWithFiles(
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
            runCell(model, 0, { ROLE: "guest" }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
         await expect(runCell(model, 0)).rejects.toBeInstanceOf(
            AccessDeniedError,
         );
      } finally {
         await cleanup();
      }
   });

   it("(10) a `#(secure)` given, the shape a host injects on every run, is ignored by a cell that does not read it and binds where it is read", async () => {
      const { model, cleanup } = await createModelWithFiles(
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
         const preImport = await runCell(model, 0, { GROUPS: [1] });
         expect(cellCount(preImport)).toBe(4);

         const mine = await runCell(model, 1, { GROUPS: [1] });
         expect(cellCount(mine)).toBe(2);

         const none = await runCell(model, 1, { GROUPS: [] });
         expect(cellCount(none)).toBe(0);
      } finally {
         await cleanup();
      }
   });
});

// #1241 in a notebook: HIDE is on no cell's surface, and the gate reading it is
// reached only through the hub's `mid_gated`.
const OG_BASE = `##! experimental.givens

#(secure)
given:
  HIDE :: string is 'none'

source: ungated_deep is duckdb.table('orgtable') extend {
  where: val != $HIDE
  measure: c is count()
}
`;

const OG_GATE = `##! experimental.givens

#(secure)
given:
  HIDE :: string

#(access_filter) val = $HIDE
source: deep_gated is duckdb.table('orgtable') extend {
  measure: c is count()
}
`;

const OG_HUB = `import { ungated_deep } from "og_base.malloy"
import "og_gate.malloy"

source: mid_ungated is ungated_deep extend {}
source: mid_gated is deep_gated extend {}
query: q_mid is mid_ungated -> { aggregate: c }
`;

const OG_NOTEBOOK = `>>>malloy
import "plain.malloy"
run: plain -> { aggregate: c }
>>>malloy
import "og_hub.malloy"
run: mid_ungated -> { aggregate: c }
>>>malloy
run: q_mid
`;

describe("a notebook cell that reads an off-surface gate given", () => {
   const files = {
      "plain.malloy": PLAIN,
      "og_base.malloy": OG_BASE,
      "og_gate.malloy": OG_GATE,
      "og_hub.malloy": OG_HUB,
      "nb.malloynb": OG_NOTEBOOK,
   };

   it("400s instead of binding the where: given's default", async () => {
      const { model, cleanup } = await createModelWithFiles(
         files,
         "nb.malloynb",
      );
      try {
         const err = await runCell(model, 1, { HIDE: "a" }).catch(
            (e: unknown) => e,
         );
         expect(err).toBeInstanceOf(MalloyError);
         expect((err as Error).message).toMatch(/unknown given 'HIDE'/);
      } finally {
         await cleanup();
      }
   });

   it("refuses a cell running a named query that reaches it by name", async () => {
      const { model, cleanup } = await createModelWithFiles(
         files,
         "nb.malloynb",
      );
      try {
         await expect(runCell(model, 2, { HIDE: "a" })).rejects.toBeInstanceOf(
            AccessDeniedError,
         );
      } finally {
         await cleanup();
      }
   });

   it("still drops it for a cell that never reads it", async () => {
      const { model, cleanup } = await createModelWithFiles(
         files,
         "nb.malloynb",
      );
      try {
         const result = await runCell(model, 0, { HIDE: "a" });
         expect(cellCount(result)).toBe(4);
      } finally {
         await cleanup();
      }
   });
});
