// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelMaterializer,
   type PersistSource,
} from "@malloydata/malloy";

/**
 * Shared real-compile harness for the incremental specs (declaration,
 * policy-by-way-of-declaration, apply, compiler contract, address stability).
 * All of them compile single-file models against an in-memory DuckDB — the
 * compile target only; nothing here runs a delta — and the boilerplate to do
 * that is identical, so it lives once.
 */

const ROOT = "file:///spec/";

/** One in-memory DuckDB plus the FixedConnectionMap the Runtime needs. */
export function duckdbTestConnections(): {
   duckdb: DuckDBConnection;
   connections: FixedConnectionMap;
} {
   const duckdb = new DuckDBConnection("duckdb", ":memory:");
   return {
      duckdb,
      connections: new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      ),
   };
}

/**
 * Load a single-file model, keeping the Runtime that loaded it.
 *
 * {@link loadTestModel} discards the Runtime, which is right for anything
 * model-level. Runtime-level APIs need it back — `getBuildTargets` lives on
 * Runtime rather than on Model because it resolves a connection digest per
 * source, which only the connection map can answer.
 */
export function loadTestRuntime(
   connections: FixedConnectionMap,
   model: string,
): { runtime: Runtime; materializer: ModelMaterializer } {
   const urlReader = new InMemoryURLReader(
      new Map([[`${ROOT}m.malloy`, model]]),
   );
   const runtime = new Runtime({ urlReader, connections });
   return {
      runtime,
      materializer: runtime.loadModel(new URL(`${ROOT}m.malloy`), {
         importBaseURL: new URL(ROOT),
      }),
   };
}

/** Load a single-file model; the materializer compiles on demand. */
export function loadTestModel(
   connections: FixedConnectionMap,
   model: string,
): ModelMaterializer {
   return loadTestRuntime(connections, model).materializer;
}

/**
 * Compile a single-file model, returning its persist sources by name plus the
 * materializer (for specs that contrast a source's getSQL() against what a
 * compiled QUERY over it would do).
 */
export async function compilePersistSources(
   connections: FixedConnectionMap,
   model: string,
): Promise<{
   sources: Record<string, PersistSource>;
   materializer: ModelMaterializer;
}> {
   const materializer = loadTestModel(connections, model);
   const compiled = await materializer.getModel();
   const sources: Record<string, PersistSource> = {};
   for (const source of Object.values(compiled.getBuildPlan().sources)) {
      sources[source.name] = source;
   }
   return { sources, materializer };
}
