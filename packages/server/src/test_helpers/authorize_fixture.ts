// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One fixture for every `#(authorize)` / `#(access_filter)` spec: an
 * in-memory DuckDB seeded with every table those specs read, a model loaded
 * through the real `Model.create`, and the `buildGatedModel` escape hatch for
 * the few tests that must reach request time past a deliberately broken gate.
 *
 * The seed tables are disjoint, so one seed serves them all:
 *
 *  - `customers` (id, name, region): 1 'a' us-west, 2 'b' us-east.
 *  - `nested_cols`: STRUCT / ARRAY / ARRAY-of-STRUCT columns, one row.
 *  - `salaries` / `departments`: docs/authorize.md's worked example.
 *  - `parent` (id, org_id, child_id, val): org 1 is ids 1,2; org 2 is ids
 *    3,4. `child_id` 10 (ids 1,3) joins `childtable.name` 'bob', 20 'other'.
 *  - `orgtable` (id, org_id, owner, val): (1,1,2) (2,1,1) (3,2,1) (4,2,2) —
 *    `org_id` and `owner` differ per row so a misbound column shows.
 *  - `accounts` (id, org_id, region, amount, owner, `cost center`,
 *    child_id): org1 is ids 1-3, org2 is ids 4-6; east is 1,2,4.
 *    `child` names child_id 1 'north', 2 'south'.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   modelDefToModelInfo,
   Runtime,
   type Connection,
   type GivenValue,
   type ModelDef,
   type ModelMaterializer,
   type QueryMaterializer,
   type SourceDef,
} from "@malloydata/malloy";
import fs from "fs";
import os from "os";
import path from "path";
import { AccessDeniedError } from "../errors";
import { malloyGivenToApi, type MalloyGiven } from "../service/given";
import { Model } from "../service/model";
import { seededDuckdb as seededTempDuckdb } from "./temp_package";

export const AUTHORIZE_SEED_SQL = `
CREATE OR REPLACE TABLE customers (id INTEGER, name VARCHAR, region VARCHAR);
INSERT INTO customers VALUES (1, 'a', 'us-west'), (2, 'b', 'us-east');
CREATE OR REPLACE TABLE nested_cols (
   id INTEGER, rec STRUCT(a INTEGER), arr INTEGER[], arr_rec STRUCT(b INTEGER)[]
);
INSERT INTO nested_cols VALUES (1, {'a': 1}, [1, 2, 3], [{'b': 2}]);
CREATE OR REPLACE TABLE salaries (id INTEGER, department_id INTEGER, department VARCHAR, salary INTEGER);
INSERT INTO salaries VALUES (1, 10, 'eng', 100), (2, 20, 'sales', 90);
CREATE OR REPLACE TABLE departments (id INTEGER, name VARCHAR);
INSERT INTO departments VALUES (10, 'eng'), (20, 'sales');
CREATE OR REPLACE TABLE parent (id INTEGER, org_id INTEGER, child_id INTEGER, val VARCHAR);
INSERT INTO parent VALUES (1, 1, 10, 'a'), (2, 1, 20, 'b'), (3, 2, 10, 'c'), (4, 2, 20, 'd');
CREATE OR REPLACE TABLE childtable (id INTEGER, name VARCHAR);
INSERT INTO childtable VALUES (10, 'bob'), (20, 'other');
CREATE OR REPLACE TABLE orgtable (id INTEGER, org_id INTEGER, owner INTEGER, val VARCHAR);
INSERT INTO orgtable VALUES (1, 1, 2, 'a'), (2, 1, 1, 'b'), (3, 2, 1, 'c'), (4, 2, 2, 'd');
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

/** A fresh in-memory DuckDB carrying {@link AUTHORIZE_SEED_SQL}. */
export function seededDuckdb(): Promise<DuckDBConnection> {
   return seededTempDuckdb(AUTHORIZE_SEED_SQL);
}

export function connectionsFor(
   duckdb: DuckDBConnection,
): Map<string, Connection> {
   return new Map<string, Connection>([["duckdb", duckdb]]);
}

/** Prefix `##! experimental.givens` unless the text already sets a flag. */
export function withGivensFlag(text: string): string {
   return text.includes("##!") ? text : `##! experimental.givens\n\n${text}`;
}

export interface LoadedAuthorizeModel {
   model: Model;
   duckdb: DuckDBConnection;
   dir: string;
   /** The load's compilation error, if any. */
   error: Error | undefined;
   cleanup: () => Promise<void>;
}

/**
 * Load a model through the real `Model.create` from a temp directory.
 * `files` is one model's text (written as `m.malloy`) or a map of relative
 * paths to contents, loading `entry` (the second argument, or
 * `opts.entry`). Texts are written verbatim.
 */
export async function loadAuthorizeModel(
   files: string | Record<string, string>,
   entryOrOpts: string | { entry?: string; duckdb?: DuckDBConnection } = {},
): Promise<LoadedAuthorizeModel> {
   const opts =
      typeof entryOrOpts === "string" ? { entry: entryOrOpts } : entryOrOpts;
   const fileMap = typeof files === "string" ? { "m.malloy": files } : files;
   const entry = opts.entry ?? "m.malloy";
   const duckdb = opts.duckdb ?? (await seededDuckdb());
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "authorize-model-"));
   for (const [name, text] of Object.entries(fileMap)) {
      fs.mkdirSync(path.dirname(path.join(dir, name)), { recursive: true });
      fs.writeFileSync(path.join(dir, name), text);
   }
   const model = await Model.create(
      "test-pkg",
      dir,
      entry,
      connectionsFor(duckdb),
   );
   return {
      model,
      duckdb,
      dir,
      error: compilationErrorOf(model),
      cleanup: async () => {
         if (!opts.duckdb) await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      },
   };
}

/** Load, run `body`, and always clean up. */
export async function withAuthorizeModel<T>(
   files: string | Record<string, string>,
   body: (loaded: LoadedAuthorizeModel) => Promise<T>,
   entry?: string,
): Promise<T> {
   const loaded = await loadAuthorizeModel(files, { entry });
   try {
      return await body(loaded);
   } finally {
      await loaded.cleanup();
   }
}

export function compilationErrorOf(model: Model): Error | undefined {
   return (model as unknown as { compilationError?: Error }).compilationError;
}

/** `compactResult` rows of an ad-hoc query through `getQueryResults`. */
export async function queryRows(
   model: Model,
   query: string,
   givens: Record<string, unknown> = {},
): Promise<Array<Record<string, unknown>>> {
   const result = await model.getQueryResults(
      undefined,
      undefined,
      query,
      {},
      true,
      givens as Record<string, GivenValue>,
   );
   return result.compactResult as unknown as Array<Record<string, unknown>>;
}

/** `count()` over `source` for a caller. */
export async function countFor(
   model: Model,
   source: string,
   givens: Record<string, unknown> = {},
): Promise<number> {
   const rows = await queryRows(
      model,
      `run: ${source} -> { aggregate: n is count() }`,
      givens,
   );
   return Number(rows[0].n);
}

/** Sorted ids of `source` visible to a caller. */
export async function idsFor(
   model: Model,
   source: string,
   givens: Record<string, unknown> = {},
): Promise<number[]> {
   const rows = await queryRows(
      model,
      `run: ${source} -> { select: id; order_by: id }`,
      givens,
   );
   return rows.map((r) => Number(r.id)).sort((a, b) => a - b);
}

/** The lock's denial on `source`: a 403, never a 200 with zero rows. */
export async function expectRefused(
   model: Model,
   source: string,
   givens: Record<string, unknown> = {},
): Promise<void> {
   await expectAccessDenied(
      queryRows(model, `run: ${source} -> { select: id }`, givens),
   );
}

export async function expectAccessDenied(
   promise: Promise<unknown>,
): Promise<AccessDeniedError> {
   const err = await promise.then(
      () => undefined,
      (e: unknown) => e,
   );
   if (!(err instanceof AccessDeniedError)) {
      throw new Error(
         `expected AccessDeniedError, got ${err === undefined ? "a result" : String(err)}`,
      );
   }
   return err;
}

/** Messages and `sourceName`s of `logger.warn` calls matching `pattern`. */
export function warnedSources(
   calls: ReadonlyArray<ReadonlyArray<unknown>>,
   pattern: string,
): string[] {
   return calls
      .filter((c) => String(c[0]).includes(pattern))
      .map((c) =>
         String((c[1] as { sourceName?: string } | undefined)?.sourceName),
      );
}

// ---------------------------------------------------------------------------
// buildGatedModel: request-time behaviour past a deliberately broken gate.
// ---------------------------------------------------------------------------

const ROOT = "file:///authorize-tests/";

/** Private `Model` members the request-time specs reach past the public
 *  surface (same idiom as `authorize_gate_walk.spec.ts`'s `GateWalker`). */
export interface ModelInternals {
   setGateRuntime(runtime: Runtime): void;
   authorizeAndBindRunnable(
      runnable: QueryMaterializer,
      givens: Record<string, GivenValue>,
      options?: {
         recompile?: (mm: ModelMaterializer) => QueryMaterializer;
         bypassAuthorize?: boolean;
      },
   ): Promise<QueryMaterializer>;
   resolveGraftTarget(
      struct: SourceDef,
      originModelDef: ModelDef,
      graftModelDef: ModelDef,
   ): string | undefined;
   resolveGateShape(
      entry: {
         label: string;
         exprs: string[];
         selfContained: boolean;
         struct?: SourceDef;
      },
      originModelDef: ModelDef,
      graftScope:
         | {
              modelDef: ModelDef;
              materializer: ModelMaterializer;
              cacheScope: string;
           }
         | undefined,
   ): Promise<{ shape: string }>;
   queryEntryPointHasRowLevelGate(runnable: {
      getPreparedQuery(): Promise<unknown>;
   }): Promise<boolean>;
   modelDef?: ModelDef;
   modelMaterializer: ModelMaterializer;
}

export interface GatedModel {
   model: Model;
   internals: ModelInternals;
   mm: ModelMaterializer;
   duckdb: DuckDBConnection;
}

function compileRuntime(
   files: Record<string, string>,
   entry: string,
   duckdb: DuckDBConnection,
): { runtime: Runtime; mm: ModelMaterializer } {
   const urlReader = new InMemoryURLReader(
      new Map(Object.entries(files).map(([p, t]) => [`${ROOT}${p}`, t])),
   );
   const runtime = new Runtime({
      urlReader,
      connections: new FixedConnectionMap(connectionsFor(duckdb), "duckdb"),
   });
   const mm = runtime.loadModel(new URL(`${ROOT}${entry}`), {
      importBaseURL: new URL(ROOT),
   });
   return { runtime, mm };
}

/**
 * A `Model` built from REAL compiled Malloy IR that skips `Model.create`'s
 * load-time validation (`validateAuthorizeProbes`, the misplaced-annotation
 * scan) and never populates `sources`/`queries`, so `hasAuthorize()` and
 * `effectiveAuthorizeFor` are inert on it. Only for tests whose gate is
 * deliberately unexpressible at the one entry point under test, or that
 * drive the graft machinery directly; everything else uses
 * {@link loadAuthorizeModel}. `text` gets the givens flag via
 * {@link withGivensFlag}; pass `files` + `entry` for an import chain.
 */
export async function buildGatedModel(
   text: string | Record<string, string>,
   opts: { duckdb?: DuckDBConnection; entry?: string } = {},
): Promise<GatedModel> {
   const duckdb = opts.duckdb ?? (await seededDuckdb());
   const entry = opts.entry ?? "m.malloy";
   const files =
      typeof text === "string" ? { [entry]: withGivensFlag(text) } : text;
   const { runtime, mm } = compileRuntime(files, entry, duckdb);
   const compiled = await mm.getModel();
   const modelDef = (compiled as unknown as { _modelDef: ModelDef })._modelDef;
   const malloyGivens = Array.from(
      compiled.givens.values(),
   ) as unknown as MalloyGiven[];
   const givens =
      malloyGivens.length > 0 ? malloyGivens.map(malloyGivenToApi) : undefined;
   const model = new Model(
      "test-pkg",
      entry,
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
      modelDefToModelInfo(modelDef),
   );
   const internals = model as unknown as ModelInternals;
   internals.setGateRuntime(runtime);
   return { model, internals, mm, duckdb };
}

/**
 * A stub runnable carrying its OWN `_modelDef`, compiled from `text` under a
 * separate file URL — what append-scope `/compile`'s synthetic
 * `__compile_check.malloy` recompile produces: a struct sharing no object
 * identity, `sourceID` or note identity with the on-disk model's copy.
 */
export async function buildEphemeralRunnable(
   text: string,
   sourceName: string,
   duckdb: DuckDBConnection,
): Promise<{ getPreparedQuery(): Promise<unknown> }> {
   const entry = "__compile_check.malloy";
   const { mm } = compileRuntime(
      { [entry]: withGivensFlag(text) },
      entry,
      duckdb,
   );
   const compiled = await mm.getModel();
   const modelDef = (compiled as unknown as { _modelDef: ModelDef })._modelDef;
   const structRef = modelDef.contents[sourceName] as SourceDef;
   return {
      getPreparedQuery: async () => ({
         _query: { structRef },
         _modelDef: modelDef,
      }),
   };
}

/** Bind `queryText` through the real `authorizeAndBindRunnable`. */
async function bind(
   gated: Pick<GatedModel, "internals" | "mm">,
   queryText: string,
   givens: Record<string, GivenValue>,
): Promise<QueryMaterializer> {
   return gated.internals.authorizeAndBindRunnable(
      gated.mm.loadRestrictedQuery(queryText),
      givens,
      { recompile: (m) => m.loadRestrictedQuery(queryText) },
   );
}

/** The SQL the bound runnable would issue: proof of what the graft did. */
export async function boundSql(
   gated: Pick<GatedModel, "internals" | "mm">,
   queryText: string,
   givens: Record<string, GivenValue> = {},
): Promise<string> {
   return (await bind(gated, queryText, givens)).getSQL({ givens });
}

export async function boundRows(
   gated: Pick<GatedModel, "internals" | "mm">,
   queryText: string,
   givens: Record<string, GivenValue> = {},
): Promise<ReadonlyArray<Record<string, unknown>>> {
   const result = await (
      await bind(gated, queryText, givens)
   ).run({ rowLimit: 1000, givens });
   return result.data.value;
}
