// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Verification suite for the `#(partition)` row-filter graft — the read-time
 * half of the feature (`gate_classification.ts`'s `resolvePartitionGraftEntries`,
 * wired into `Model.probeEntryPointGates`/`authorizeAndBindRunnable`).
 *
 * Covers exactly the shapes the legacy `#(filter)` text-injection path cannot
 * reach (a named query invoked by `queryName` alone, a notebook cell), the
 * bypass posture (`bypassAuthorize` skips the graft, `bypassFilters` must
 * NOT), composition with `#(authorize)`, and the publish-time composite
 * refusal. Follows `row_level_authorize.integration.spec.ts`'s conventions
 * (`Model.create` against a real DuckDB connection, `firstCellResultValue`
 * for notebook cells).
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   type Connection,
   type ModelDef,
   type QueryMaterializer,
   type SourceDef,
} from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError } from "../errors";
import { PartitionAnnotationError } from "./partition_annotation";
import { Model } from "./model";

const SEED_SQL = `
CREATE OR REPLACE TABLE tenant_rows (tenant VARCHAR, val INTEGER);
INSERT INTO tenant_rows VALUES ('acme', 1), ('acme', 2), ('globex', 3);
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

async function createModel(
   text: string,
   fileName = "m.malloy",
): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
   const duckdb = await newDuckdb();
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "partition-graft-"));
   fs.writeFileSync(path.join(dir, fileName), text);
   const model = await Model.create(
      "test-pkg",
      dir,
      fileName,
      new Map<string, Connection>([["duckdb", duckdb]]),
   );
   return { model, duckdb, dir };
}

function compilationErrorOf(model: Model): Error | undefined {
   return (model as unknown as { compilationError?: Error }).compilationError;
}

function firstCellResultValue(resultJson: string): unknown {
   const parsed = JSON.parse(resultJson);
   const row = parsed?.data?.array_value?.[0]?.record_value?.[0];
   return row?.number_value ?? null;
}

const PARTITIONED_MODEL = `##! experimental.givens

given:
  TENANT :: string

#(partition) tenant = $TENANT
source: X is duckdb.table('tenant_rows') extend {
   measure: n is count()
}

query: q is X -> { aggregate: n is count() }
`;

describe("#(partition) graft — read paths", () => {
   it("filters a plain source read to the caller's slice", async () => {
      const { model, duckdb, dir } = await createModel(PARTITIONED_MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const acme = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            { TENANT: "acme" },
         );
         expect((acme.compactResult as unknown as { n: number }[])[0].n).toBe(
            2,
         );
         const globex = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            { TENANT: "globex" },
         );
         expect((globex.compactResult as unknown as { n: number }[])[0].n).toBe(
            1,
         );
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — filters a named query invoked by queryName ALONE — the legacy #(filter) text-injection path cannot see this shape at all", async () => {
      const { model, duckdb, dir } = await createModel(PARTITIONED_MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            "q",
            undefined,
            {},
            true,
            { TENANT: "acme" },
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            2,
         );
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("filters an ad-hoc caller-declared derivation of the partitioned source", async () => {
      const { model, duckdb, dir } = await createModel(PARTITIONED_MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "source: mine is X extend {}\nrun: mine -> { aggregate: n is count() }",
            {},
            true,
            { TENANT: "acme" },
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            2,
         );
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — filters a notebook cell that declares and runs the partitioned source", async () => {
      const duckdb = await newDuckdb();
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "partition-graft-nb-"));
      try {
         fs.writeFileSync(
            path.join(dir, "nb.malloynb"),
            `>>>malloy
##! experimental.givens

given:
  TENANT :: string

#(partition) tenant = $TENANT
source: X is duckdb.table('tenant_rows') extend {
   measure: n is count()
}

run: X -> { aggregate: n is count() }
`,
         );
         const model = await Model.create(
            "test-pkg",
            dir,
            "nb.malloynb",
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.executeNotebookCell(0, undefined, false, {
            TENANT: "acme",
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — the graft is ABSENT under bypassAuthorize (index-time scan reads every partition in one pass)", async () => {
      const { model, duckdb, dir } = await createModel(PARTITIONED_MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: X -> { aggregate: n is count() }",
            {},
            true,
            {}, // no TENANT supplied at all — would fail to bind if grafted
            undefined,
            undefined,
            "full",
            /* bypassAuthorize */ true,
         );
         // All 3 seed rows, across both tenants — unfiltered.
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            3,
         );
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — bypassFilters (the legacy #(filter) control) does NOT skip the partition predicate", async () => {
      const { model, duckdb, dir } = await createModel(PARTITIONED_MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         for (const bypassFilters of [false, true]) {
            const result = await model.getQueryResults(
               undefined,
               undefined,
               "run: X -> { aggregate: n is count() }",
               {},
               bypassFilters,
               { TENANT: "acme" },
            );
            expect(
               (result.compactResult as unknown as { n: number }[])[0].n,
            ).toBe(2);
         }
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

describe("#(partition) graft — composes with #(authorize)", () => {
   const COMBINED_MODEL = `##! experimental.givens

given:
  TENANT :: string
  ROLE :: string

#(partition) tenant = $TENANT
#(authorize) $ROLE = 'analyst'
source: Y is duckdb.table('tenant_rows') extend {
   measure: n is count()
}
`;

   it("both predicates apply conjunctively — an admitted role still sees only its own tenant slice", async () => {
      const { model, duckdb, dir } = await createModel(COMBINED_MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: Y -> { aggregate: n is count() }",
            {},
            true,
            { TENANT: "acme", ROLE: "analyst" },
         );
         // Same narrowed count as the partition-only source above — the
         // authorize gate did not widen or replace the partition filter.
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            2,
         );
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("a role the authorize gate rejects sees zero rows regardless of a valid tenant — every gate is a live WHERE now, and partition composes with it rather than replacing it", async () => {
      // `#(authorize) $ROLE = 'analyst'` grafts as `where: $ROLE = 'analyst'`
      // — a whole-source boolean condition, not a column-keyed row filter —
      // so a mismatched ROLE makes that WHERE false for every row, same as
      // any other row-level authorize gate (see this file's header on the
      // "constant gate dimensions" posture in `row_level_authorize
      // .integration.spec.ts`). The point pinned here is that this composes
      // WITH the partition filter rather than one replacing the other.
      const { model, duckdb, dir } = await createModel(COMBINED_MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: Y -> { aggregate: n is count() }",
            {},
            true,
            { TENANT: "acme", ROLE: "guest" },
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            0,
         );
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — a partition given absent from the model's given surface denies rather than admitting unfiltered", async () => {
      const { model, duckdb, dir } = await createModel(
         `#(partition) tenant = $UNDECLARED
source: X is duckdb.table('tenant_rows') extend {
   measure: n is count()
}
`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: X -> { aggregate: n is count() }",
               {},
               true,
               {},
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

describe("#(partition) — publish-time composite refusal", () => {
   it("CRITICAL — a composite source that itself declares #(partition) refuses to load", async () => {
      const { model, duckdb, dir } = await createModel(
         `##! experimental.composite_sources

source: a is duckdb.table('tenant_rows') extend { measure: n is count() }
source: b is duckdb.table('tenant_rows') extend { measure: n is count() }

#(partition) tenant = $TENANT
source: combo is compose(a, b)
`,
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(PartitionAnnotationError);
         expect(err?.message).toMatch(/composite/i);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("CRITICAL — a composite source whose MEMBER declares #(partition) refuses to load", async () => {
      // Measured before the refusal existed: `run: combo` returned all three
      // rows under `TENANT: 'acme'` while `run: marked` correctly returned
      // two, and forcing the marked branch with `group_by: marked_flag` still
      // returned three. A member's marker is silently dropped, so the
      // composite reads every partition — unlike a marker on the composite
      // itself, which denies loudly.
      const { model, duckdb, dir } = await createModel(
         `##! experimental { composite_sources givens }

given:
  TENANT :: string

#(partition) tenant = $TENANT
source: marked is duckdb.table('tenant_rows') extend {
   measure: n is count()
   dimension: marked_flag is 1
}

source: openm is duckdb.table('tenant_rows') extend { measure: n is count() }

source: combo is compose(marked, openm)
`,
      );
      try {
         const err = compilationErrorOf(model);
         expect(err).toBeInstanceOf(PartitionAnnotationError);
         expect(err?.message).toMatch(/member "marked"/);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });

   it("does NOT refuse a non-composite query-source derived from a composite base with no partition marker of its own", async () => {
      // Guards against a false positive: `assertPartitionAnnotationsValid` only
      // inspects TOP-LEVEL composite `modelDef.contents` entries, so a
      // query-source over an unmarked composite must load cleanly.
      const { model, duckdb, dir } = await createModel(
         `##! experimental.composite_sources

source: a is duckdb.table('tenant_rows') extend { measure: n is count() }
source: b is duckdb.table('tenant_rows') extend { measure: n is count() }

source: combo is compose(a, b)

source: mine is combo -> { aggregate: n is count() }
`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

describe("#(partition) — storage/pre-aggregation routing veto (HALF 2c)", () => {
   interface ModelInternals {
      queryEntryPointHasRowLevelGate(runnable: {
         getPreparedQuery(): Promise<unknown>;
      }): Promise<boolean>;
   }

   it("CRITICAL — queryEntryPointHasRowLevelGate returns true for a partition-only entry point (no #(authorize) at all)", async () => {
      const { model, duckdb, dir } = await createModel(PARTITIONED_MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const modelDef = (model as unknown as { modelDef?: ModelDef })
            .modelDef;
         const struct = modelDef?.contents["X"] as SourceDef;
         expect(struct).toBeDefined();
         // A minimal runnable stub: `resolveRunTargetStruct` reads
         // `getPreparedQuery()._query.structRef` / `._modelDef`.
         const runnable: {
            getPreparedQuery(): Promise<unknown>;
         } = {
            getPreparedQuery: async () => ({
               _query: { structRef: struct },
               _modelDef: modelDef,
            }),
         };
         const internals = model as unknown as ModelInternals;
         const blocksRouting = await internals.queryEntryPointHasRowLevelGate(
            runnable as unknown as QueryMaterializer,
         );
         expect(blocksRouting).toBe(true);
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});
