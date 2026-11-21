// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The shape of one connection type's entry in the registry (`index.ts`): what
 * Publisher knows about a connection by its configured type (`postgres`,
 * `ducklake`, ...), as opposed to what Malloy's dialect for it can do
 * (`materialization/domain/dialect.ts`, keyed by Malloy's `dialectName`).
 *
 * The two keys differ: `duckdb`, `motherduck` and `ducklake` share the
 * dialect `duckdb`, and Redshift connects under the `postgres` type. A branch
 * keyed on the connection type reads this registry; one keyed on the dialect
 * reads the capability table. Neither is derived from the other at a branch.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../../api";
import type { ConnectionOrigin } from "../../config";
import type { openProxy } from "../proxy";

/** A connection's configured type, as the API spells it. */
export type ConnectionType = NonNullable<
   components["schemas"]["Connection"]["type"]
>;

/** A connection type's static attributes, as the API reports them. */
export type ConnectionAttributes =
   components["schemas"]["ConnectionAttributes"];

/** A connection as the API carries it. */
export type ApiConnection = components["schemas"]["Connection"];

/** A connection past assembly's name check. */
export type NamedConnection = ApiConnection & { name: string };

/** The Malloy core connection entry a connection assembles into. */
export type CoreConnectionEntry = {
   is: string;
   [key: string]: unknown;
};

/** A database a DuckDB connection attaches, as the API carries it. */
export type AttachedDatabase = components["schemas"]["AttachedDatabase"];

/**
 * How a database of this type is attached to a DuckDB connection
 * (`duckdbConnection.attachedDatabases[]`, whose `type` names it).
 */
export interface AttachedDatabaseDialect {
   /**
    * Attach `attachedDb` read-only under its name, creating any secret it
    * reads its credentials from before the ATTACH.
    */
   attach(
      connection: DuckDBConnection,
      attachedDb: AttachedDatabase,
   ): Promise<void>;
   /** A connection test's probe of the database attached under `name`. */
   test(connection: DuckDBConnection, name: string): Promise<void>;
   /**
    * The secret an attach under `name` creates, as the DROP SECRET that
    * detaches it names it; absent when the attach leaves none behind.
    */
   secretName?(name: string): string;
}

/** A source warehouse type that exposes a native DuckDB query-passthrough. */
export type FederatedSourceType = "bigquery" | "snowflake" | "postgres";

/**
 * The result of federating a source connection onto a build session: the
 * `handle` a passthrough table function needs to reach it —
 *  - bigquery: the resolved project id (`bigquery_query('<project>', '<sql>')`),
 *  - snowflake: the created secret name (`snowflake_query('<sql>', '<secret>')`),
 *  - postgres: the ATTACH alias (`postgres_query('<alias>', '<sql>')`).
 */
export interface FederatedHandle {
   handle: string;
   sourceType: FederatedSourceType;
   /**
    * Releases what the federation holds outside the DuckDB session (the SSH
    * tunnel of a proxied Postgres source), which disposing the session does not
    * close. The build session calls this in its `finally`.
    */
   close?: () => Promise<void>;
}

/**
 * The source connection's configuration needed to federate it onto a build
 * session. `name` is a caller-supplied, already-safe base for the secret/alias
 * identifiers; the type-specific config carries the credentials.
 */
export interface FederationConfig {
   name: string;
   bigqueryConnection?: components["schemas"]["BigqueryConnection"];
   snowflakeConnection?: components["schemas"]["SnowflakeConnection"];
   postgresConnection?: components["schemas"]["PostgresConnection"];
   /**
    * The connection's proxy. A build must tunnel a proxied Postgres source as
    * the query path does, or the passthrough dials the database host the
    * bastion exists to keep unreachable.
    */
   proxy?: components["schemas"]["ConnectionProxy"];
}

/** Injection seam for tests: how a proxied federation opens its tunnel. */
export interface FederationDeps {
   openProxy?: typeof openProxy;
}

/**
 * How a build reads a source of this type through its warehouse's native
 * query-passthrough, so the warehouse computes the rows.
 */
export interface PassthroughDialect {
   /**
    * Establish the source's credentials on a build-scoped DuckDB session
    * (federateSourceForPassthrough).
    */
   federate(
      connection: DuckDBConnection,
      config: FederationConfig,
      deps: FederationDeps,
   ): Promise<FederatedHandle>;
   /**
    * `innerSQL` as the passthrough call that runs it through `handle`, as
    * {@link federate} returned it (wrapPassthrough).
    */
   wrap(handle: string, innerSQL: string): string;
}

/** One connection type's entry in the registry. */
export interface ConnectionDialect {
   /** The type this entry is for, as its registry key. */
   readonly type: ConnectionType;
   /**
    * What the API reports for a connection of this type before it is opened:
    * its Malloy dialect, whether it pools, persists and streams. Undefined
    * for a type whose attributes are only known once it resolves
    * (`publisher`, which names another server's connection).
    */
   readonly attributes: Readonly<ConnectionAttributes> | undefined;
   /**
    * The type's own shape checks, run at config load after the checks every
    * connection gets (connection_config.ts, validateConnectionShape). Absent
    * for a type with none of its own (postgres, mysql).
    */
   readonly validateShape?: (
      connection: ApiConnection,
      origin: ConnectionOrigin,
   ) => void;
   /**
    * The Malloy core connection entry a connection of this type assembles
    * into; `environmentPath` resolves a relative file a DuckDB-family
    * connection names.
    */
   readonly coreEntry: (
      connection: NamedConnection,
      environmentPath: string,
   ) => CoreConnectionEntry;
   /** How a database of this type is attached; absent when it cannot be. */
   readonly attached?: AttachedDatabaseDialect;
   /** How a build reads a source of this type; absent when it cannot. */
   readonly passthrough?: PassthroughDialect;
}
