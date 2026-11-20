// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which connection types a `storage=` build can read a source from, and why a
 * source on any other is refused. The types are the configured ones
 * (`postgres`, `duckdb`, `ducklake`, ...), recorded per resolved connection
 * by `service/connection_type.ts`, since a dialect does not tell DuckDB from
 * DuckLake.
 */

/** Source connection types the `storage=` build's query passthrough reads. */
export const PASSTHROUGH_SOURCE_TYPES = [
   "bigquery",
   "snowflake",
   "postgres",
] as const;

/**
 * Whether a `storage=` build can read a source on a connection of `type`: a
 * passthrough warehouse, or a DuckLake catalog, which builds into the same
 * catalog (decided at build against the destination; another catalog is
 * refused there). Anything else, the package's DuckDB sandbox among them,
 * cannot be passed through.
 */
export function storageBuildReadsConnectionType(type: string): boolean {
   return (
      (PASSTHROUGH_SOURCE_TYPES as readonly string[]).includes(type) ||
      type === "ducklake"
   );
}

/**
 * Why a `storage=` source whose connection the build cannot read is refused
 * (`storage_source_unsupported`), for the plan's gate and the build's
 * fail-closed check alike: `type` is the connection's configured type, or
 * undefined when the environment holds no connection of that name (the
 * package's sandbox). `ducklake` reaches here only from the build, for a
 * catalog other than the destination's.
 */
export function unreadableSourceConnectionMessage(
   source: { name: string; connectionName: string },
   type: string | undefined,
): string {
   // A `publisher` connection is a proxy to the host the package is
   // published to, with no credentials here: neither fix below applies on
   // this server, and the host builds the source.
   if (type === "publisher") {
      return (
         `Source '${source.name}' cannot be materialized into a storage ` +
         `destination on this server: its connection ` +
         `'${source.connectionName}' is a 'publisher' connection, a proxy to ` +
         `the host the package is published to; this server holds no ` +
         `warehouse credentials for it. A 'storage=' build runs on that host, not on ` +
         `this server. The source is answered live here.`
      );
   }
   const what =
      type === undefined
         ? `is not one of the environment's connections (the package's own ` +
           `DuckDB sandbox is not)`
         : type === "ducklake"
           ? `is a DuckLake catalog other than the destination's`
           : `is a '${type}' connection`;
   return (
      `Source '${source.name}' cannot be materialized into a storage ` +
      `destination: its connection '${source.connectionName}' ${what}, ` +
      `which cannot be passed through to a 'storage=' build (it reads ` +
      `${PASSTHROUGH_SOURCE_TYPES.join(", ")} sources, and a DuckLake ` +
      `source into its own catalog only). ` +
      `The source is not built and is answered live. Drop 'storage=' to ` +
      `build it in its own connection, or put the source on a warehouse ` +
      `connection.`
   );
}

/**
 * Why a `storage=` source on a DuckLake connection that carries author SQL is
 * refused (`storage_source_unsupported`): its build runs on a read-write
 * session on its own catalog, which runs only SQL Malloy generated.
 */
export function catalogAuthorSqlMessage(source: {
   name: string;
   connectionName: string;
}): string {
   return (
      `Source '${source.name}' cannot be materialized: its connection ` +
      `'${source.connectionName}' is a DuckLake catalog, whose tables are ` +
      `built inside the catalog by SQL Malloy generates, and the source ` +
      `carries SQL its author wrote (an .sql() source or a sql_* ` +
      `expression, its own or a joined source's) or reads a file or URL as a ` +
      `table. The source is not built and is answered live. Express it in ` +
      `Malloy over the catalog's tables to build it.`
   );
}
