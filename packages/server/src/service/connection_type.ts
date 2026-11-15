// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The configured type of a resolved Malloy connection (`postgres`, `duckdb`,
 * `ducklake`, ...), which the build plan's `storage=` gate reads
 * (materialization/domain/source_connection_type.ts).
 *
 * A Malloy connection carries only its dialect, which does not tell a DuckDB
 * connection from a DuckLake one, so the type is recorded where each
 * connection is resolved: the environment's from its configuration
 * (connection.ts), the package's DuckDB sandbox as `duckdb` (package.ts), and,
 * in the package-load worker, from the metadata the main thread sends.
 */

const recorded = new WeakMap<object, string>();

/** Test seam layers: connection name -> the type to record instead. */
const overrides: Map<string, string>[] = [];

/**
 * Test seam: record each connection named in `types` as the type it maps to,
 * until the returned restore is called. For fixtures whose DuckDB connection
 * stands in for a warehouse a `storage=` build reads: the conformance harness
 * (test_helpers/conformance) builds such a table from the package sandbox by
 * hand, and some component specs fake a Postgres passthrough over a DuckDB
 * one. Recorded on the main thread, which also answers the package-load
 * worker's connection metadata, so both plan paths see it. Layers stack;
 * the latest wins.
 */
export function recordConnectionsAsForTests(
   types: Record<string, string>,
): () => void {
   const layer = new Map(Object.entries(types));
   overrides.push(layer);
   return () => {
      const at = overrides.indexOf(layer);
      if (at >= 0) overrides.splice(at, 1);
   };
}

function overrideOf(connection: object): string | undefined {
   const name = (connection as { name?: unknown }).name;
   if (typeof name !== "string") return undefined;
   for (let i = overrides.length - 1; i >= 0; i--) {
      const type = overrides[i].get(name);
      if (type !== undefined) return type;
   }
   return undefined;
}

/** Record `connection`'s configured type; returns `connection`. */
export function recordConnectionType<T extends object>(
   connection: T,
   type: string | undefined,
): T {
   const recordedType = overrideOf(connection) ?? type;
   if (recordedType) recorded.set(connection, recordedType);
   return connection;
}

/** The type recorded for `connection`, or undefined when none was. */
export function connectionTypeOf(
   connection: object | null | undefined,
): string | undefined {
   return connection ? recorded.get(connection) : undefined;
}

/**
 * The recorded type of each resolved connection, by name, for the build plan
 * (`CompiledBuildPlan.connectionTypes`). A connection with no recorded type is
 * left out.
 */
export function resolveConnectionTypes(
   connections: ReadonlyMap<string, object>,
): Record<string, string> {
   const types: Record<string, string> = {};
   for (const [name, connection] of connections) {
      const type = connectionTypeOf(connection);
      if (type !== undefined) types[name] = type;
   }
   return types;
}
