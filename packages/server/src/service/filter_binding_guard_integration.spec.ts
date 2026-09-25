// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Coverage for `./filter_binding_guard` — the fail-closed guard against
 * `docs/authorize.md`'s former "Known hole": a filter (a grafted
 * `#(access_filter)` condition, a plain author `where:`, a join's own
 * filter, or a `#(filter)` injection) resolves the field it reads by NAME,
 * late, against whatever struct it ends up attached to. A `rename:` that
 * frees up the same name — directly, or via `except:`/`accept:`/`include`
 * dropping the original then a later block reusing the name — rebinds the
 * filter to a DIFFERENT physical column instead of failing to compile.
 *
 * Every test here is written against the REAL `Model.create` /
 * `Model.getQueryResults` path (`createModel`) except where the shape is
 * deliberately unexpressible at its one entry point, which instead uses
 * `buildGatedModel` to skip `Model.create`'s load-time preflight — same
 * convention, and same two helpers (copied rather than imported, this
 * file's own convention already documented in
 * `source_line_authorize_integration.spec.ts`), as that file.
 *
 * `org_id`/`owner` are deliberately DIFFERENT per row (see `SEED_SQL`) so a
 * misbound query returns a DIFFERENT row set than the correctly-bound query,
 * not merely an empty one — the asymmetry is what proves misbinding rather
 * than mere denial, and also lets a genuinely-served (non-misbound) query be
 * told apart from an accidentally-passing one.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   MalloyConfig,
   MalloyError,
   modelDefToModelInfo,
   Runtime,
   type Connection,
   type GivenValue,
   type ModelDef,
} from "@malloydata/malloy";
import { describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AccessDeniedError } from "../errors";
import { malloyGivenToApi, type MalloyGiven } from "./given";
import { Model } from "./model";
import { Package } from "./package";

const ROOT = "file:///filter-binding-guard-tests/";

const SEED_SQL = `
CREATE OR REPLACE TABLE orgtable (id INTEGER, org_id INTEGER, owner INTEGER, val VARCHAR, note VARCHAR);
INSERT INTO orgtable VALUES
   (1, 1, 2, 'a', 'x'), (2, 1, 1, 'b', 'y'), (3, 2, 1, 'c', 'a'), (4, 2, 2, 'd', 'b');

CREATE OR REPLACE TABLE childtable (id INTEGER, org_id INTEGER);
INSERT INTO childtable VALUES (1, 1), (2, 1), (3, 2), (4, 2);

CREATE OR REPLACE TABLE decoytable (id INTEGER, org_id INTEGER);
INSERT INTO decoytable VALUES (1, 1), (2, 1), (3, 1), (4, 1);
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
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "filter-binding-guard-"));
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

async function createModel(
   text: string,
   fileName = "m.malloy",
): Promise<{ model: Model; duckdb: DuckDBConnection; dir: string }> {
   return createModelWithFiles({ [fileName]: text }, fileName);
}

/**
 * Load `files` through the REAL production path — `Package.create`, which
 * dispatches to the package-load worker pool and constructs every model via
 * `Model.fromSerialized` — rather than the bare `Model.create`
 * `createModelWithFiles` uses. This is the ONLY path that pushes down
 * `Package.applySiblingModelResolverToModels`, so it is the one that can
 * actually prove the sibling-model lookup: a `Model` built
 * directly, outside a `Package`, never gets a resolver at all, and would
 * pass even a broken implementation vacuously.
 *
 * The worker compiles in a SEPARATE process with its own fresh `:memory:`
 * DuckDB, so a table this process seeded via `runSQL` is invisible there —
 * every fixture below reads its rows from an inline `duckdb.sql("select …
 * union all select …")` literal instead of `duckdb.table(...)`, the same
 * workaround `row_level_authorize.integration.spec.ts`'s own worker-pool test
 * uses.
 */
async function createPackageWithFiles(
   files: Record<string, string>,
): Promise<{ pkg: Package; duckdb: DuckDBConnection; dir: string }> {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   const dir = fs.mkdtempSync(
      path.join(os.tmpdir(), "filter-binding-guard-pkg-"),
   );
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: "test-pkg" }),
   );
   for (const [fileName, text] of Object.entries(files)) {
      fs.writeFileSync(path.join(dir, fileName), text);
   }
   const connections = new FixedConnectionMap(
      new Map<string, Connection>([["duckdb", duckdb]]),
      "duckdb",
   );
   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(() => connections);
   const pkg = await Package.create("env", "test-pkg", dir, malloyConfig);
   return { pkg, duckdb, dir };
}

/** The `orgtable` fixture's rows, as an inline literal a package-load
 *  worker's own separate DuckDB can read without a shared connection. */
const ORGROWS_SQL =
   "select 1 as id, 1 as org_id, 2 as owner, 'a' as val, 'x' as note " +
   "union all select 2, 1, 1, 'b', 'y' " +
   "union all select 3, 2, 1, 'c', 'a' " +
   "union all select 4, 2, 2, 'd', 'b'";

/** The `childtable` fixture's rows, as the same inline-literal workaround
 *  {@link ORGROWS_SQL} documents. */
const CHILDROWS_SQL =
   "select 1 as id, 1 as org_id " +
   "union all select 2, 1 " +
   "union all select 3, 2 " +
   "union all select 4, 2";

async function expectDeniedViaPackage(
   pkg: Package,
   modelPath: string,
   queryText: string,
   givens: Record<string, GivenValue> = {},
): Promise<void> {
   const model = pkg.getModel(modelPath);
   expect(model).toBeDefined();
   await expect(
      model!.getQueryResults(undefined, undefined, queryText, {}, true, givens),
   ).rejects.toBeInstanceOf(AccessDeniedError);
}

async function expectServesWithCountViaPackage(
   pkg: Package,
   modelPath: string,
   queryText: string,
   expected: number,
   givens: Record<string, GivenValue> = {},
): Promise<void> {
   const model = pkg.getModel(modelPath);
   expect(model).toBeDefined();
   const result = await model!.getQueryResults(
      undefined,
      undefined,
      queryText,
      {},
      true,
      givens,
   );
   expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
      expected,
   );
}

function compilationErrorOf(model: Model): Error | undefined {
   return (model as unknown as { compilationError?: Error }).compilationError;
}

async function cleanup(duckdb: DuckDBConnection, dir?: string): Promise<void> {
   await duckdb.close();
   if (dir) fs.rmSync(dir, { recursive: true, force: true });
}

/** {@link cleanup}, for a `Package.create`d package — releases the
 *  package's OWN `MalloyConfig` connections (which the package-load worker
 *  pool opens independently of the raw `duckdb` connection the test handed
 *  it) before closing that raw connection, so a package test does not leak
 *  a worker-side connection every run. */
async function cleanupPackage(
   pkg: Package,
   duckdb: DuckDBConnection,
   dir?: string,
): Promise<void> {
   await pkg.getMalloyConfig().releaseConnections();
   await cleanup(duckdb, dir);
}

/** A notebook cell's `result` is Malloy's own wire-format JSON, not the
 *  `compactResult` shape `getQueryResults` returns — same helper
 *  `row_level_authorize.integration.spec.ts` keeps its own copy of. */
function firstCellResultValue(resultJson: string): unknown {
   const parsed = JSON.parse(resultJson);
   const row = parsed?.data?.array_value?.[0]?.record_value?.[0];
   return (
      row?.number_value ??
      row?.string_value ??
      row?.boolean_value ??
      row?.timestamp_value ??
      null
   );
}

/** Assert `queryText` denies with `AccessDeniedError` — the shared shape for
 *  every caller-text misbind variant below. */
async function expectDenied(
   model: Model,
   queryText: string,
   givens: Record<string, GivenValue> = {},
): Promise<void> {
   await expect(
      model.getQueryResults(undefined, undefined, queryText, {}, true, givens),
   ).rejects.toBeInstanceOf(AccessDeniedError);
}

/** Assert `queryText` serves and its lone aggregate `n` equals `expected` —
 *  the negative-case shape. */
async function expectServesWithCount(
   model: Model,
   queryText: string,
   expected: number,
   givens: Record<string, GivenValue> = {},
): Promise<void> {
   const result = await model.getQueryResults(
      undefined,
      undefined,
      queryText,
      {},
      true,
      givens,
   );
   expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
      expected,
   );
}

/**
 * Build a `Model` from real compiled Malloy IR while skipping `Model.create`'s
 * pre-flight `validateAuthorizeProbes` call — see
 * `source_line_authorize_integration.spec.ts`'s identical helper (that file
 * owns the canonical version; duplicated per its own documented convention).
 * Used ONLY for gates deliberately unexpressible at the one entry point
 * under test, so the test reaches request time instead of aborting at load.
 */
async function buildGatedModel(
   text: string,
): Promise<{ model: Model; duckdb: DuckDBConnection }> {
   const duckdb = await newDuckdb();
   const modelPath = "m.malloy";
   const fullText = text.includes("experimental.givens")
      ? text
      : `##! experimental.givens\n\n${text}`;
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}${modelPath}`, fullText]]),
   );
   const runtime = new Runtime({
      urlReader,
      connections: new FixedConnectionMap(
         new Map<string, Connection>([["duckdb", duckdb]]),
         "duckdb",
      ),
   });
   const mm = runtime.loadModel(new URL(`${ROOT}${modelPath}`), {
      importBaseURL: new URL(ROOT),
   });
   const compiled = await mm.getModel();
   const modelDef = (compiled as unknown as { _modelDef: ModelDef })._modelDef;
   const modelInfo = modelDefToModelInfo(modelDef);
   const malloyGivens = Array.from(
      compiled.givens.values(),
   ) as unknown as MalloyGiven[];
   const givens =
      malloyGivens.length > 0 ? malloyGivens.map(malloyGivenToApi) : undefined;
   const model = new Model(
      "test-pkg",
      modelPath,
      {},
      "model",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      mm as any,
      modelDef,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      givens as any,
      modelInfo,
   );
   (model as unknown as { setGateRuntime(r: Runtime): void }).setGateRuntime(
      runtime,
   );
   return { model, duckdb };
}

describe("filter binding guard — #(access_filter) row-level gate stays bound to its original field", () => {
   it("a row filter stays bound when the entry point is re-extended (except+rename, model-declared)", async () => {
      const { model, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: w_misbind is gated_parent extend { except: org_id } extend { rename: org_id is owner }
`);
      try {
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: w_misbind -> { group_by: id; aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb);
      }
   });

   it("a row filter stays bound when the entry point is re-extended (accept+rename, model-declared)", async () => {
      const { model, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: w_misbind is gated_parent extend { accept: id, val, n, owner } extend { rename: org_id is owner }
`);
      try {
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: w_misbind -> { group_by: id; aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb);
      }
   });

   it("a row filter stays bound through a transitive dimension reference (except+rename)", async () => {
      const { model, duckdb } = await buildGatedModel(`
given:
  GROUPS :: number[]

#(access_filter) authorized
source: gated_parent is duckdb.table('orgtable') extend {
   dimension: authorized is org_id in $GROUPS
   measure: n is count()
}

source: w_misbind is gated_parent extend { except: org_id } extend { rename: org_id is owner }
`);
      try {
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: w_misbind -> { group_by: id; aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb);
      }
   });

   it("a row filter stays bound when the caller declares a named derivation (except+rename, caller-text)", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               `
source: mine is gated_parent extend { except: org_id } extend { rename: org_id is owner }
run: mine -> { group_by: id; aggregate: n is count() }
`,
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a row filter stays bound across an import boundary (except+rename, package-derived)", async () => {
      const base = `##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}
`;
      const derived = `
import "base.malloy"

source: w_misbind is gated_parent extend { except: org_id } extend { rename: org_id is owner }
`;
      const { model, duckdb, dir } = await createModelWithFiles(
         { "base.malloy": base, "derived.malloy": derived },
         "derived.malloy",
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: w_misbind -> { group_by: id; aggregate: n is count() }",
               {},
               true,
               { GROUPS: [1] },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("renaming a field that no filter reads still serves (negative)", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: renamed_unrelated is gated_parent extend { rename: label is val }
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: renamed_unrelated -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            2,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a query_source entry point still serves (negative)", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: qchild is gated_parent -> { group_by: id, org_id, val; aggregate: n is count() }
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: qchild -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            2,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("`#(authorize)` lock, which reads no field, is unaffected by a rename on the entry point", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  ROLE :: string

#(authorize) $ROLE = 'analyst'
source: locked is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: locked_renamed is locked extend { rename: owner_id is owner }
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const ok = await model.getQueryResults(
            undefined,
            undefined,
            "run: locked_renamed -> { aggregate: n is count() }",
            {},
            true,
            { ROLE: "analyst" },
         );
         expect((ok.compactResult as unknown as { n: number }[])[0].n).toBe(4);

         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: locked_renamed -> { aggregate: n is count() }",
               {},
               true,
               { ROLE: "guest" },
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("filter binding guard — a joined source's own inherited filter stays bound to its original field", () => {
   it("a joined source's own where: filter stays bound when the join member is re-extended (except+rename)", async () => {
      const { model, duckdb, dir } = await createModel(`
source: base_filtered is duckdb.table('orgtable') extend {
   where: org_id = 1
   measure: n is count()
}

source: base_misbind is base_filtered extend { except: org_id } extend { rename: org_id is owner }

source: joiner is duckdb.table('orgtable') extend {
   join_one: j is base_misbind on id = j.id
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: joiner -> { aggregate: n is count() }",
               {},
               true,
               {},
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a plain author where: filter with no rename still serves through a join (negative)", async () => {
      const { model, duckdb, dir } = await createModel(`
source: base_filtered is duckdb.table('orgtable') extend {
   where: org_id = 1
   measure: n is count()
}

source: joiner is duckdb.table('orgtable') extend {
   join_one: j is base_filtered on id = j.id
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: joiner -> { aggregate: n is count() }",
            {},
            true,
            {},
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            4,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("filter binding guard — #(filter) injection stays bound to its original field", () => {
   it("an injected #(filter) where: stays bound when the caller re-extends the dimension (except+rename)", async () => {
      const { model, duckdb, dir } = await createModel(`
#(filter) dimension=val type=equal
source: filtered_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               `
run: filtered_parent extend { except: val } extend { rename: val is note } -> { group_by: id; aggregate: n is count() }
`,
               { val: "a" },
               false,
               {},
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("an unrenamed dimension still serves the injected where: (negative)", async () => {
      const { model, duckdb, dir } = await createModel(`
#(filter) dimension=val type=equal
source: filtered_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: filtered_parent -> { aggregate: n is count() }",
            { val: "a" },
            false,
            {},
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            1,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   // The `#(filter)`-tagged dimension itself (`safe_col`) is never renamed —
   // only the column its OWN expression reads (`val`) is. A check that
   // compares just `[filter.dimension]` sees `safe_col`'s own definition
   // (`is val`) unchanged on both sides and calls it identical; it never
   // walks into what `val` itself now resolves to on the executed struct,
   // where `except: val; rename: val is note` has quietly rebound it to a
   // different physical column.
   it("a derived #(filter) dimension whose underlying column is rebound by the caller must deny", async () => {
      const { model, duckdb, dir } = await createModel(`
#(filter) dimension=safe_col type=equal
source: filtered_parent is duckdb.table('orgtable') extend {
   dimension: safe_col is val
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: filtered_parent extend { except: val } extend { rename: val is note } -> { group_by: id; aggregate: n is count() }",
               { safe_col: "a" },
               false,
               {},
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("an unrenamed underlying column still serves the derived #(filter) dimension (negative)", async () => {
      const { model, duckdb, dir } = await createModel(`
#(filter) dimension=safe_col type=equal
source: filtered_parent is duckdb.table('orgtable') extend {
   dimension: safe_col is val
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: filtered_parent -> { aggregate: n is count() }",
            { safe_col: "a" },
            false,
            {},
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            1,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("except:-ing an injected filter's own dimension, with nothing renamed onto it, surfaces Malloy's own compile error, not a 403", async () => {
      const { model, duckdb, dir } = await createModel(`
#(filter) dimension=val type=equal
source: filtered_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: filtered_parent extend { except: val } -> { aggregate: n is count() }",
               { val: "a" },
               false,
               {},
            ),
         ).rejects.toBeInstanceOf(MalloyError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   // `resolveFilterSource` returns the FIRST name in `filterMap` it finds
   // walking from the run target — and `filterMap` (`source_extraction.ts`)
   // stores an entry under a DERIVED source's own name too, whenever it
   // inherits a `#(filter)` note through `.inherits`, not only under the
   // name that originally declared it. A caller who names the derivation
   // (`source: mine is filtered_parent extend { ... }`) makes `mine` that
   // first hit, so `declaringSourceName` becomes `mine` itself — comparing
   // `mine`'s own (already-misbound) field space to itself is vacuously
   // "identical" no matter how the caller renamed things, unless the
   // declaring struct is re-resolved to the TRUE ancestor that wrote the
   // annotation.
   it("stays bound when the caller declares a NAMED derivation, not just an anonymous extend (except+rename)", async () => {
      const { model, duckdb, dir } = await createModel(`
#(filter) dimension=val type=equal
source: filtered_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               `
source: mine is filtered_parent extend { except: val } extend { rename: val is note }
run: mine -> { group_by: id; aggregate: n is count() }
`,
               { val: "a" },
               false,
               {},
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   // Same misbind, but the MODEL FILE itself declares the renamed derivation
   // (`mine`) — `filterMap` (`source_extraction.ts`) stores an entry under
   // `mine`'s own name too, since it inherits the note through `.inherits`.
   // Querying `mine` DIRECTLY, with no further caller-side derivation at all,
   // makes `resolveFilterSource` return "mine" on its very first check —
   // `declaringSourceName` becomes `mine`, comparing `mine`'s own (already
   // misbound, right there in the model file) field space to itself.
   it("stays bound when the MODEL declares the misbound derivation and it is queried directly (except+rename)", async () => {
      const { model, duckdb, dir } = await createModel(`
#(filter) dimension=val type=equal
source: filtered_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: mine is filtered_parent extend { except: val } extend { rename: val is note }
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: mine -> { group_by: id; aggregate: n is count() }",
               { val: "a" },
               false,
               {},
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a notebook cell's injected #(filter) stays bound when the cell re-extends the dimension (except+rename)", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "nb.malloynb": `>>>malloy
#(filter) dimension=val type=equal
source: filtered_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

>>>malloy
run: filtered_parent extend { except: val } extend { rename: val is note } -> { group_by: id; aggregate: n is count() }
`,
         },
         "nb.malloynb",
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.executeNotebookCell(1, { val: "a" }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

/**
 * The headline vector: a CALLER writes the misbind directly in `run:` text,
 * on an anonymous `extend` of the entry point — no separate `source:`
 * declaration at all, model-declared or caller-declared. This is a distinct
 * code path from every test above: those all name a NEW source (whether in
 * the model or the query text), which the compiler registers with its own
 * identity; a bare `run: gated_parent extend { … } -> { … }` never gets one.
 *
 * The redeclared field here is a fresh `dimension:` set to a CONSTANT chosen
 * to satisfy the gate (`dimension: org_id is 1`, or `is $TENANT` for a
 * scalar given) rather than a `rename:` of an unrelated existing column —
 * the caller does not even need a column whose real values happen to
 * cooperate; they just declare one that always does. `rename:` variants are
 * included too, alongside the `dimension:` ones, both as a single `extend {
 * … }` block and as two chained ones, since Malloy accepts both shapes.
 */
describe("filter binding guard — caller-text inline extend, no separate source: declaration (headline vector)", () => {
   const MODEL_TEXT = `##! experimental.givens

given:
  GROUPS :: number[]
  TENANT :: string

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

#(access_filter) note = $TENANT
source: gated_parent2 is duckdb.table('orgtable') extend {
   measure: n is count()
}

source: base_filtered is duckdb.table('orgtable') extend {
   where: org_id = 1
   measure: n is count()
}

source: childtable_src is duckdb.table('childtable') extend {
   primary_key: id
}

source: decoy_src is duckdb.table('decoytable') extend {
   primary_key: id
}

#(access_filter) child.org_id in $GROUPS
source: gated_joiner is duckdb.table('orgtable') extend {
   join_one: child is childtable_src on id = child.id
   measure: n is count()
}
`;

   it.each([
      [
         "except+dimension, single block",
         "except: org_id; dimension: org_id is 1",
      ],
      [
         "except+dimension, chained",
         "except: org_id } extend { dimension: org_id is 1",
      ],
      [
         "accept+dimension, single block",
         "accept: id, val, n, owner, note; dimension: org_id is 1",
      ],
      [
         "accept+dimension, chained",
         "accept: id, val, n, owner, note } extend { dimension: org_id is 1",
      ],
      [
         "except+rename, single block",
         "except: org_id; rename: org_id is owner",
      ],
      [
         "except+rename, chained",
         "except: org_id } extend { rename: org_id is owner",
      ],
   ])(
      "a row filter (array given, direct column) stays bound when the entry point is re-extended (%s)",
      async (_label, extendBody) => {
         const { model, duckdb, dir } = await createModel(MODEL_TEXT);
         try {
            expect(compilationErrorOf(model)).toBeUndefined();
            await expectDenied(
               model,
               `run: gated_parent extend { ${extendBody} } -> { group_by: id; aggregate: n is count() }`,
               { GROUPS: [1] },
            );
         } finally {
            await cleanup(duckdb, dir);
         }
      },
   );

   it.each([
      [
         "except+dimension, single block",
         "except: note; dimension: note is $TENANT",
      ],
      [
         "except+dimension, chained",
         "except: note } extend { dimension: note is $TENANT",
      ],
   ])(
      "a row filter (scalar given, direct column) stays bound when the entry point is re-extended (%s)",
      async (_label, extendBody) => {
         const { model, duckdb, dir } = await createModel(MODEL_TEXT);
         try {
            expect(compilationErrorOf(model)).toBeUndefined();
            await expectDenied(
               model,
               `run: gated_parent2 extend { ${extendBody} } -> { aggregate: n is count() }`,
               { TENANT: "anything" },
            );
         } finally {
            await cleanup(duckdb, dir);
         }
      },
   );

   it.each([
      [
         "except+dimension, single block",
         "except: org_id; dimension: org_id is 1",
      ],
      [
         "except+dimension, chained",
         "except: org_id } extend { dimension: org_id is 1",
      ],
      [
         "except+rename, single block",
         "except: org_id; rename: org_id is owner",
      ],
      [
         "except+rename, chained",
         "except: org_id } extend { rename: org_id is owner",
      ],
   ])(
      "a plain where: filter with NO access_filter annotation stays bound when the entry point is re-extended (%s)",
      async (_label, extendBody) => {
         const { model, duckdb, dir } = await createModel(MODEL_TEXT);
         try {
            expect(compilationErrorOf(model)).toBeUndefined();
            await expectDenied(
               model,
               `run: base_filtered extend { ${extendBody} } -> { aggregate: n is count() }`,
            );
         } finally {
            await cleanup(duckdb, dir);
         }
      },
   );

   it.each([
      [
         "single block",
         "except: child; join_one: child is decoy_src on id = child.id",
      ],
      [
         "chained",
         "except: child } extend { join_one: child is decoy_src on id = child.id",
      ],
   ])(
      "a joined-field access_filter gate stays bound when the caller replaces the join (%s)",
      async (_label, extendBody) => {
         const { model, duckdb, dir } = await createModel(MODEL_TEXT);
         try {
            expect(compilationErrorOf(model)).toBeUndefined();
            // `decoy_src` maps every id's `org_id` to 1 — a legit join would
            // deny 2 of the 4 rows (org_id=2 in the real `childtable`); a
            // misbind that swaps in `decoy_src` would serve all 4.
            await expectDenied(
               model,
               `run: gated_joiner extend { ${extendBody} } -> { aggregate: n is count() }`,
               { GROUPS: [1] },
            );
         } finally {
            await cleanup(duckdb, dir);
         }
      },
   );

   it("include does not free the gated name at all — Malloy itself refuses the redeclare (documented, not a guard gap)", async () => {
      // `include { private: … }` only changes VISIBILITY (malloy-gotchas-modeling:
      // "include public/internal/private" vs "extend except/accept/rename"),
      // it does not remove the field the way `except:` does — so there is no
      // name to redeclare, and Malloy itself refuses with "Cannot redefine"
      // before this guard would ever get a chance to run. Confirms "include
      // only where Malloy accepts it" means never, for this exact attack.
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "m.malloy": `##! experimental.givens
##! experimental.access_modifiers

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}
`,
         },
         "m.malloy",
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const res = model.getQueryResults(
            undefined,
            undefined,
            "run: gated_parent include { private: org_id } extend { dimension: org_id is 1 } -> { aggregate: n is count() }",
            {},
            true,
            { GROUPS: [1] },
         );
         await expect(res).rejects.toThrow(/Cannot redefine/);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("renaming a field no filter reads still serves through an inline extend (negative)", async () => {
      const { model, duckdb, dir } = await createModel(MODEL_TEXT);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectServesWithCount(
            model,
            "run: gated_parent extend { rename: label is val } -> { aggregate: n is count() }",
            2,
            { GROUPS: [1] },
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("an unmodified direct query still serves (negative)", async () => {
      const { model, duckdb, dir } = await createModel(MODEL_TEXT);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectServesWithCount(
            model,
            "run: base_filtered -> { aggregate: n is count() }",
            2,
         );
         await expectServesWithCount(
            model,
            "run: gated_joiner -> { aggregate: n is count() }",
            2,
            { GROUPS: [1] },
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("filter binding guard — a caller's own fresh where: in an inline extend", () => {
   it("serves for an unannotated, unfiltered source", async () => {
      const { model, duckdb, dir } = await createModel(`
source: plain_src is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectServesWithCount(
            model,
            "run: plain_src extend { where: id = 1 } -> { aggregate: n is count() }",
            1,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("serves for a source with its own plain where: filter", async () => {
      const { model, duckdb, dir } = await createModel(`
source: base_filtered is duckdb.table('orgtable') extend {
   where: org_id = 1
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectServesWithCount(
            model,
            "run: base_filtered extend { where: id = 1 } -> { aggregate: n is count() }",
            1,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("serves for a #(access_filter)-gated source", async () => {
      const { model, duckdb, dir } = await createModel(`##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectServesWithCount(
            model,
            "run: gated_parent extend { where: id = 1 } -> { aggregate: n is count() }",
            1,
            { GROUPS: [1] },
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("serves for a #(filter)-injected source", async () => {
      const { model, duckdb, dir } = await createModel(`
#(filter) dimension=note type=equal
source: filtered_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: filtered_parent extend { where: id = 1 } -> { aggregate: n is count() }",
            { note: "x" },
            false,
            {},
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            1,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("serves for the notebook-cell equivalent", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "nb.malloynb": `>>>malloy
##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}

>>>malloy
run: gated_parent extend { where: id = 1 } -> { aggregate: n is count() }
`,
         },
         "nb.malloynb",
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.executeNotebookCell(1, {}, false, {
            GROUPS: [1],
         });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(1);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("filter binding guard — the bypassAuthorize (trusted) path", () => {
   it("still denies a misbound plain where: filter even when #(authorize) gate evaluation is bypassed", async () => {
      const { model, duckdb, dir } = await createModel(`
source: base_filtered is duckdb.table('orgtable') extend {
   where: org_id = 1
   measure: n is count()
}
`);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: base_filtered extend { except: org_id } extend { rename: org_id is owner } -> { aggregate: n is count() }",
               {},
               undefined,
               {},
               undefined,
               undefined,
               "full",
               true,
            ),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("filter binding guard — #(filter) cross-file notebook import fallback", () => {
   // Loaded via `Package.create`: the innocent case below needs the
   // sibling-model lookup to serve at all, so proving it through a bare
   // `Model.create` (no `Package`, no resolver) would pass vacuously.
   const FILES = {
      "base.malloy": `
#(filter) dimension=org_id type=equal
source: base_src is duckdb.sql("${ORGROWS_SQL}") extend {
   measure: n is count()
}
`,
      "child.malloy": `
import "base.malloy"

source: child_src is base_src extend { except: org_id } extend { rename: org_id is owner }
`,
      "child_unmodified.malloy": `
import "base.malloy"

source: child_src is base_src extend {}
`,
      "child_dimension_misbind.malloy": `
import "base.malloy"

source: child_src is base_src extend { except: org_id } extend { dimension: org_id is owner }
`,
      "nb.malloynb": `>>>malloy
import "child.malloy"

>>>malloy
run: child_src -> { aggregate: n is count() }
`,
      "nb_unmodified.malloynb": `>>>malloy
import "child_unmodified.malloy"

>>>malloy
run: child_src -> { aggregate: n is count() }
`,
      "nb_dimension_misbind.malloynb": `>>>malloy
import "child_dimension_misbind.malloy"

>>>malloy
run: child_src -> { aggregate: n is count() }
`,
   };

   // A MODEL-declared misbind (the model file itself, not the caller's query
   // text, does the `except:`/`rename:`), spread across an IMPORT boundary
   // and run from a notebook cell — the one shape `findFilterAnnotationDeclaringSource`
   // can genuinely fail to resolve on its own (a notebook's per-cell compile
   // does not always carry an imported base source forward as its own
   // `modelDef.contents` entry) and needs the sibling-model lookup to reach
   // `base.malloy`'s real declaration. Reaching it must not admit THIS
   // shape merely because it cannot tell it apart from the innocent one —
   // the field-binding check that runs against the sibling's struct is what
   // still denies it.
   it("denies a misbound derivation declared across an import, run directly from a notebook cell", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles(FILES);
      try {
         const model = pkg.getModel("nb.malloynb");
         expect(model).toBeDefined();
         await expect(
            model!.executeNotebookCell(1, { org_id: "2" }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });

   it("an unmodified cross-file inheritance still serves from a notebook cell (negative)", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles(FILES);
      try {
         const model = pkg.getModel("nb_unmodified.malloynb");
         expect(model).toBeDefined();
         const result = await model!.executeNotebookCell(1, { org_id: "2" });
         expect(result.result).toBeDefined();
         expect(firstCellResultValue(result.result!)).toBe(2);
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });

   // Same shape as the misbind above, but redeclaring with `dimension:` (a
   // brand-new computed field) instead of `rename:` (a pure alias). The
   // redeclared field's intrinsic `.name` is `org_id` again, same as the
   // original — name equality alone cannot tell this apart from the
   // genuinely untouched column; `assertFilterDimensionBindsToDeclaringSource`
   // catches it once the sibling lookup resolves the real declarer, by
   // comparing `child_src`'s own (redeclared) expression to `base.malloy`'s.
   it("denies a misbind that redeclares with dimension: instead of rename:, run from a notebook cell", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles(FILES);
      try {
         const model = pkg.getModel("nb_dimension_misbind.malloynb");
         expect(model).toBeDefined();
         await expect(
            model!.executeNotebookCell(1, { org_id: "2" }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });
});

describe("filter binding guard — an inherited filter from a file not independently in modelDef.contents", () => {
   // `a.malloy` declares the filter; `b.malloy` imports it fully and derives
   // `derived` from it (unmodified); the served model imports ONLY `{
   // derived }` from `b.malloy` — a selective import — so `base` (declared in
   // `a.malloy`) is never itself an enumerable `modelDef.contents` entry for
   // this compile, even though `derived`'s inherited filter still carries
   // `a.malloy`'s own URL as its parse location. A freshness rule that infers
   // "caller's own text" from "this URL names no `modelDef.contents` entry"
   // cannot tell that apart from a genuinely inherited condition whose
   // declaring file was simply never promoted — it must instead identify the
   // CALLER's text positively, by comparing against the URL this specific
   // request actually compiled under.
   it("a plain where: inherited across a selectively-imported file still denies when the caller misbinds it", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "a.malloy": `
source: base is duckdb.table('orgtable') extend {
   where: org_id = 1
   measure: n is count()
}
`,
            "b.malloy": `
import "a.malloy"

source: derived is base extend {}
`,
            "m.malloy": `
import { derived } from "b.malloy"
`,
         },
         "m.malloy",
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectDenied(
            model,
            "run: derived extend { except: org_id } extend { rename: org_id is owner } -> { aggregate: n is count() }",
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a #(access_filter)-gated source inherited across a selectively-imported file still denies when the caller misbinds it", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "a.malloy": `##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: base is duckdb.table('orgtable') extend {
   measure: n is count()
}
`,
            "b.malloy": `
import "a.malloy"

source: derived is base extend {}
`,
            "m.malloy": `
import { derived } from "b.malloy"
import { GROUPS } from "a.malloy"
`,
         },
         "m.malloy",
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectDenied(
            model,
            "run: derived extend { except: org_id } extend { rename: org_id is owner } -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   // A direct, UNMODIFIED query against the selectively-imported derivation
   // still serves: `derived`'s `referenceID`/`sourceID` survive uncleared
   // (nothing was derived further), so `resolveDeclaredSource`'s
   // `sourceRegistry` link resolves straight to `base`'s own struct object —
   // no location-based fallback, and no freshness question, ever arises.
   //
   // The moment a caller adds ANY `extend` at all, Malloy clears that
   // reference (see `resolveDeclaredSource`'s own doc). Through a bare
   // `Model.create` (no `Package`, no sibling resolver — as this test still
   // uses), that used to be an unqualified deny even for a harmless extend,
   // since there was no structural link left to prove an untouched
   // inherited filter binds. Item 1's `Package.applySiblingModelResolverToModels`
   // closes that gap for the real, production path: the describe block below
   // (loaded via `Package.create`) proves a harmless caller extend on this
   // exact shape now serves.
   it("an unmodified direct query against the selectively-imported derived source still serves (negative)", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "a.malloy": `
source: base is duckdb.table('orgtable') extend {
   where: org_id = 1
   measure: n is count()
}
`,
            "b.malloy": `
import "a.malloy"

source: derived is base extend {}
`,
            "m.malloy": `
import { derived } from "b.malloy"
`,
         },
         "m.malloy",
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectServesWithCount(
            model,
            "run: derived -> { aggregate: n is count() }",
            2,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("filter binding guard — an unresolvable executed struct denies rather than skips", () => {
   // `assertGraftedGatesBind` used to `return` silently when the executed
   // struct couldn't be resolved, treating "can't tell" as "nothing to
   // check" — the one caller in this module for which that reasoning does
   // NOT hold: it is the sole authoritative proof that a grafted
   // row-security condition still binds, so skipping it serves the gated
   // query with the gate unverified. Forces that exact path by making
   // `resolveRunTargetStruct` (which the real compile never actually fails
   // this way) return an unresolvable struct.
   it("denies a gated query when the executed struct cannot be resolved", async () => {
      const { model, duckdb, dir } = await createModel(
         `##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend { measure: n is count() }
`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const realModelDef = (model as unknown as { modelDef: unknown })
            .modelDef;
         const spy = spyOn(
            model as unknown as {
               resolveRunTargetStruct: () => Promise<unknown>;
            },
            "resolveRunTargetStruct",
         ).mockResolvedValue({
            // Keeps `modelDef` real (unlike the catch-all failure path,
            // which nulls everything together) — this isolates an executed
            // struct that specifically failed to resolve, not a wholesale
            // resolution failure every other caller of
            // `resolveRunTargetStruct` already denies on.
            struct: undefined,
            modelDef: realModelDef,
            compositeResolvedSourceDef: undefined,
            compiledUrl: undefined,
            queryLocation: undefined,
         });
         try {
            await expectDenied(
               model,
               "run: gated_parent -> { aggregate: n is count() }",
               { GROUPS: [1] },
            );
         } finally {
            spy.mockRestore();
         }
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("filter binding guard — resolved through the package's sibling model", () => {
   // Loaded via `Package.create`, never a bare `Model.create` — the sibling
   // lookup is pushed down by `Package.applySiblingModelResolverToModels`
   // and is simply absent otherwise, so only the real production path can
   // prove it. `a.malloy` declares `base`/`gated_parent`; `b.malloy` imports
   // it fully and derives `derived is … extend {}` (unmodified); the served
   // model imports ONLY `{ derived }` — a selective import, so `base`/
   // `gated_parent` is never independently an enumerable `modelDef.contents`
   // entry of the served compile. The package still compiled `a.malloy` on
   // its own, though, and that is exactly what the sibling resolver reaches.
   const SEL_PLAIN = {
      "a.malloy": `
source: base is duckdb.sql("${ORGROWS_SQL}") extend {
   where: org_id = 1
   measure: n is count()
}
`,
      "b.malloy": `
import "a.malloy"

source: derived is base extend {}
`,
      "m.malloy": `
import { derived } from "b.malloy"
`,
   };
   const SEL_GATED = {
      "a.malloy": `##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.sql("${ORGROWS_SQL}") extend {
   measure: n is count()
}
`,
      "b.malloy": `
import "a.malloy"

source: derived is gated_parent extend {}
`,
      "m.malloy": `
import { derived } from "b.malloy"
import { GROUPS } from "a.malloy"
`,
   };

   it("an unmodified direct run against a selectively-imported gated source now serves", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles(SEL_GATED);
      try {
         await expectServesWithCountViaPackage(
            pkg,
            "m.malloy",
            "run: derived -> { aggregate: n is count() }",
            2,
            { GROUPS: [1] },
         );
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });

   it("a caller extend renaming an UNRELATED field now serves", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles(SEL_PLAIN);
      try {
         await expectServesWithCountViaPackage(
            pkg,
            "m.malloy",
            "run: derived extend { rename: label is val } -> { aggregate: n is count() }",
            2,
         );
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });

   it("a caller extend adding an UNRELATED dimension now serves", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles(SEL_PLAIN);
      try {
         await expectServesWithCountViaPackage(
            pkg,
            "m.malloy",
            "run: derived extend { dimension: d is 1 } -> { aggregate: n is count() }",
            2,
         );
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });

   it("a caller's own FRESH where: alongside the inherited filter now serves", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles(SEL_PLAIN);
      try {
         await expectServesWithCountViaPackage(
            pkg,
            "m.malloy",
            "run: derived extend { where: val = 'a' } -> { aggregate: n is count() }",
            1,
         );
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });

   it("a caller extend adding an UNRELATED dimension on a gated source now serves", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles(SEL_GATED);
      try {
         await expectServesWithCountViaPackage(
            pkg,
            "m.malloy",
            "run: derived extend { dimension: d is 1 } -> { aggregate: n is count() }",
            2,
            { GROUPS: [1] },
         );
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });

   it("a caller misbind on the same selectively-imported plain-where shape still denies", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles(SEL_PLAIN);
      try {
         await expectDeniedViaPackage(
            pkg,
            "m.malloy",
            "run: derived extend { except: org_id } extend { rename: org_id is owner } -> { aggregate: n is count() }",
         );
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });

   it("a caller misbind on the same selectively-imported gated shape still denies", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles(SEL_GATED);
      try {
         await expectDeniedViaPackage(
            pkg,
            "m.malloy",
            "run: derived extend { except: org_id } extend { rename: org_id is owner } -> { aggregate: n is count() }",
            { GROUPS: [1] },
         );
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });
});

describe("filter binding guard — freshness never claimed for a named run", () => {
   // `run: q1` / `queryName: "q1"` compiles no caller/cell text of its own —
   // `_query.location.url` there is just the MODEL FILE's URL, the same file
   // that declares the inherited `where:`. Passing that URL through as
   // `compiledUrl` would let a misbind baked into the model's OWN declared
   // query claim freshness merely for sharing a file with the query that
   // runs it — `resolveRunTargetStruct` now only ever passes an `internal://`
   // URL, so a named run gets no freshness claim at all and falls through to
   // the ordinary (deny-by-default) path.
   it("a named query over a model-declared plain-where misbind still denies", async () => {
      const { model, duckdb, dir } = await createModel(
         `
source: base_filtered is duckdb.table('orgtable') extend {
   where: org_id = 1
   measure: n is count()
}
query: q1 is base_filtered extend { except: org_id } extend { rename: org_id is owner } -> { aggregate: n is count() }
`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(undefined, "q1", undefined, {}, true, {}),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a named query over a model-declared #(access_filter) misbind still denies", async () => {
      const { model, duckdb, dir } = await createModel(
         `##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend { measure: n is count() }
query: q1 is gated_parent extend { except: org_id } extend { rename: org_id is owner } -> { aggregate: n is count() }
`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expect(
            model.getQueryResults(undefined, "q1", undefined, {}, true, {
               GROUPS: [1],
            }),
         ).rejects.toBeInstanceOf(AccessDeniedError);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("a model-declared query with its own fresh where: still serves as a named query", async () => {
      const { model, duckdb, dir } = await createModel(
         `
source: base_filtered is duckdb.table('orgtable') extend {
   where: org_id = 1
   measure: n is count()
}
query: q2 is base_filtered extend { where: val = 'a' } -> { aggregate: n is count() }
`,
      );
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            "q2",
            undefined,
            {},
            true,
            {},
         );
         expect((result.compactResult as unknown as { n: number }[])[0].n).toBe(
            1,
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("filter binding guard — a derived source's own unrelated annotation never stands in for the base's gate note", () => {
   // The gate note lives in `a.malloy`, padded well below line 1 so its own
   // line number is unambiguously LATER than anything `b.malloy` declares.
   // `w` (in `b.malloy`) carries a render tag of its own — nothing to do
   // with the gate — directly above its `source:` line, at a much EARLIER
   // line number than the gate note. A position comparison that ignores
   // FILE identity (or that considers a non-gate note as a candidate owner
   // at all) can mistake `w`'s own tag for having "authored" the inherited
   // gate merely for sitting at a lower line, and then compare `w`'s own
   // (unmodified-looking) fields to themselves — passing a misbind that
   // should deny.
   const GATE_FILE = `##! experimental.givens

given:
  GROUPS :: number[]

// padding
// padding
// padding
// padding
// padding
// padding
#(access_filter) org_id in $GROUPS
source: gated_parent is duckdb.table('orgtable') extend {
   measure: n is count()
}
`;
   const GATE_FILE_SQL = GATE_FILE.replace(
      "duckdb.table('orgtable')",
      `duckdb.sql("${ORGROWS_SQL}")`,
   );
   const DERIVED_TAG_ABOVE = `import "a.malloy"
# line_chart
source: w is gated_parent extend { except: org_id } extend { rename: org_id is owner }
`;
   const DERIVED_TAG_BELOW = `import "a.malloy"




# line_chart
source: w is gated_parent extend { except: org_id } extend { rename: org_id is owner }
`;

   it("denies in-process (Model.create) when the derived source's own tag sits at an EARLIER line than the gate note", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         { "a.malloy": GATE_FILE, "b.malloy": DERIVED_TAG_ABOVE },
         "b.malloy",
      );
      try {
         await expectDenied(
            model,
            "run: w -> { group_by: id; aggregate: n is count() }",
            { GROUPS: [1] },
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("denies in-process (Model.create) when the derived source's own tag sits at a LATER line than the gate note (control)", async () => {
      const { model, duckdb, dir } = await createModelWithFiles(
         { "a.malloy": GATE_FILE, "b.malloy": DERIVED_TAG_BELOW },
         "b.malloy",
      );
      try {
         await expectDenied(
            model,
            "run: w -> { group_by: id; aggregate: n is count() }",
            { GROUPS: [1] },
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("denies via Package.create when the derived source's own tag sits at an EARLIER line than the gate note", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles({
         "a.malloy": GATE_FILE_SQL,
         "b.malloy": DERIVED_TAG_ABOVE,
      });
      try {
         await expectDeniedViaPackage(
            pkg,
            "b.malloy",
            "run: w -> { group_by: id; aggregate: n is count() }",
            { GROUPS: [1] },
         );
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });

   it("denies for a selective import with no sibling resolver and no structural link to the declaring source", async () => {
      // `m.malloy` selectively imports just `w` (never `gated_parent`) and
      // `GROUPS`, so `gated_parent` is not independently a `modelDef.contents`
      // entry of the served compile — the gate-scoped note search and the
      // structural walk both find nothing, and a bare `Model.create` (no
      // `Package`) supplies no sibling resolver either. Every resolution
      // mechanism this guard has genuinely fails here, which is exactly the
      // case an unfiltered fallback (a struct comparing its own unrelated
      // `# line_chart` tag to itself) would otherwise paper over.
      const { model, duckdb, dir } = await createModelWithFiles(
         {
            "a.malloy": GATE_FILE,
            "b.malloy": DERIVED_TAG_ABOVE,
            "m.malloy": `import { w } from "b.malloy"
import { GROUPS } from "a.malloy"
`,
         },
         "m.malloy",
      );
      try {
         await expectDenied(
            model,
            "run: w -> { group_by: id; aggregate: n is count() }",
            { GROUPS: [1] },
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});

describe("filter binding guard — an inline join's own where: is not denied for merely sitting inside the parent's own span", () => {
   // `child_src`'s declaration SPAN textually contains the joined member
   // `j`'s own `where:` (it is written inline, inside `child_src`'s
   // `extend { ... }` block) — but the filter belongs to `j`, not to
   // `child_src`, which has no `org_id`/`val` field of its own at all. A
   // sibling-compile candidate accepted on span CONTAINMENT alone (with no
   // check that the candidate's own `filterList` actually holds this
   // condition) mistakes the parent for the declarer and denies a query
   // that never misbound anything.
   it("serves via Package.create for a single-file inline join with its own where:", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles({
         "m.malloy": `
source: child_src is duckdb.sql("${CHILDROWS_SQL}") extend {
   join_one: j is duckdb.sql("${ORGROWS_SQL}") extend { where: val = 'a' } on id = j.id
   measure: n is count()
}
`,
      });
      try {
         await expectServesWithCountViaPackage(
            pkg,
            "m.malloy",
            "run: child_src -> { aggregate: n is count() }",
            4,
         );
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });

   it("serves via Package.create for the same shape declared in a second file, queried via import", async () => {
      const { pkg, duckdb, dir } = await createPackageWithFiles({
         "a.malloy": `
source: pj is duckdb.sql("${CHILDROWS_SQL}") extend {
   join_one: j is duckdb.sql("${ORGROWS_SQL}") extend { where: val = 'a' } on id = j.id
   measure: n is count()
}
`,
         "m.malloy": `import "a.malloy"\n`,
      });
      try {
         await expectServesWithCountViaPackage(
            pkg,
            "m.malloy",
            "run: pj -> { aggregate: n is count() }",
            4,
         );
      } finally {
         await cleanupPackage(pkg, duckdb, dir);
      }
   });
});

describe("filter binding guard — a caller-written join into a row-gated source", () => {
   // `base`/`plain_child` are ungated; `gated_child` carries the row filter.
   // The caller writes the `join_one:` itself (never a model-declared join),
   // so `Model.probeCallerJoinGates` grafts the condition onto the CALLER's
   // own join struct, which `assertGraftedGatesBind` must bind-check against
   // `gated_child`'s declaring struct via `locateCallerJoin` — the entry
   // point (`base`) itself carries no gate at all, so this exercises a path
   // distinct from every other test in this file.
   const MODEL = `##! experimental.givens

given:
  GROUPS :: number[]

#(access_filter) org_id in $GROUPS
source: gated_child is duckdb.table('orgtable') extend {
   primary_key: id
   measure: n is count()
}

source: plain_child is duckdb.table('orgtable') extend {
   primary_key: id
}

source: base is duckdb.table('orgtable') extend {
   primary_key: id
}
`;

   it("(a) a caller join into the gated source serves, correctly filtered", async () => {
      const { model, duckdb, dir } = await createModel(MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: base extend { join_one: g is gated_child on id = g.id } -> { group_by: id, gorg is g.org_id; order_by: id }",
            {},
            true,
            { GROUPS: [1] },
         );
         expect(result.compactResult).toEqual([
            { id: 1, gorg: 1 },
            { id: 2, gorg: 1 },
            { id: 3, gorg: null },
            { id: 4, gorg: null },
         ]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("(b) a caller join misbinding the gated field inside the join's own extend denies", async () => {
      const { model, duckdb, dir } = await createModel(MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         await expectDenied(
            model,
            "run: base extend { join_one: g is gated_child extend { except: org_id } extend { rename: org_id is owner } on id = g.id } -> { group_by: id, gorg is g.org_id; order_by: id }",
            { GROUPS: [1] },
         );
      } finally {
         await cleanup(duckdb, dir);
      }
   });

   it("(c) an ungated caller join still serves", async () => {
      const { model, duckdb, dir } = await createModel(MODEL);
      try {
         expect(compilationErrorOf(model)).toBeUndefined();
         const result = await model.getQueryResults(
            undefined,
            undefined,
            "run: base extend { join_one: p is plain_child on id = p.id } -> { group_by: id, porg is p.org_id; order_by: id }",
            {},
            true,
            {},
         );
         expect(result.compactResult).toEqual([
            { id: 1, porg: 1 },
            { id: 2, porg: 1 },
            { id: 3, porg: 2 },
            { id: 4, porg: 2 },
         ]);
      } finally {
         await cleanup(duckdb, dir);
      }
   });
});
