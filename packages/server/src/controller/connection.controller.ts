// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { lazyCounter } from "../telemetry";
import { Connection, RunSQLOptions, TableSourceDef } from "@malloydata/malloy";
import { PersistSQLResults } from "@malloydata/malloy/connection";
import { components } from "../api";
import {
   getMaxQueryRows,
   getMaxResponseBytes,
   getQueryTimeoutMs,
} from "../config";
import {
   BadRequestError,
   ConnectionError,
   ConnectionNotFoundError,
   databaseAccessFailure,
   statementRejection,
   InvalidArgumentError,
   PackageNotFoundError,
   PayloadTooLargeError,
   TableNotFoundError,
   UnconfiguredConnectionError,
} from "../errors";
import { recordQueryCapExceeded } from "../query_cap_metrics";
import { logger } from "../logger";
import { assertSafePackageName } from "../path_safety";
import { redactConnectionSecretShapes } from "../pg_helpers";
import { runWithQueryTimeout } from "../query_timeout";
import { testConnectionConfig } from "../service/connection";
import {
   toPublicConnection,
   toPublicConnections,
} from "../service/connection_public_view";
import { validateDuckdbApiSurface } from "../service/connection_config";
import { ConnectionService } from "../service/connection_service";
import {
   getSchemasForConnection,
   listTablesForSchema,
   validateAzureUrl,
} from "../service/db_utils";
import {
   mergeQueryMetadata,
   mintCorrelationId,
   parseQueryClass,
   parseSuppliedQueryMetadata,
   queryMetadataViolations,
   type QueryClass,
   type QueryMetadata,
} from "../service/query_metadata";
import type { Environment } from "../service/environment";
import { EnvironmentStore } from "../service/environment_store";
import { isStreamingConnection, streamSqlWithBudget } from "../stream_helpers";

type ApiConnection = components["schemas"]["Connection"];
type ApiConnectionStatus = components["schemas"]["ConnectionStatus"];
type ApiSqlSource = components["schemas"]["SqlSource"];
type ApiTable = components["schemas"]["Table"];
type ApiQueryData = components["schemas"]["QueryData"];
type ApiTemporaryTable = components["schemas"]["TemporaryTable"];
type ApiSchema = components["schemas"]["Schema"];

function validateAzureAttachedDatabases(connectionConfig: ApiConnection): void {
   if (connectionConfig.type !== "duckdb") return;
   const attachedDbs =
      connectionConfig.duckdbConnection?.attachedDatabases || [];
   for (const db of attachedDbs) {
      if (db.type !== "azure" || !db.azureConnection) continue;
      const { authType, sasUrl, fileUrl } = db.azureConnection;
      if (authType === "sas_token" && sasUrl) {
         validateAzureUrl(sasUrl, `"${db.name}" sasUrl`);
      } else if (authType === "service_principal" && fileUrl) {
         validateAzureUrl(fileUrl, `"${db.name}" fileUrl`);
      }
   }
}

function validateAdminAuthoredConnection(
   connectionName: string,
   connectionConfig: ApiConnection,
): void {
   if (connectionName === "duckdb" || connectionConfig.name === "duckdb") {
      throw new BadRequestError(
         "Connection name 'duckdb' is reserved for per-package sandboxes. Choose a different name for environment-level DuckDB connections (e.g. 'shared_duckdb').",
      );
   }

   try {
      validateDuckdbApiSurface(connectionConfig);
   } catch (error) {
      throw new BadRequestError((error as Error).message);
   }

   // The connection's author is right here, so tell them: a property the
   // contract rejects (e.g. a hyphen in `cost-centre`) would otherwise be
   // dropped at dispatch and surface only as a metric. Config LOAD warns
   // instead (see assembleEnvironmentConnections): a tag must never fail an
   // environment.
   for (const field of ["queryMetadata", "queryMetadataEnforced"] as const) {
      const violations = queryMetadataViolations(connectionConfig[field]);
      if (violations.length > 0) {
         throw new BadRequestError(
            `Connection "${connectionName}" ${field} is invalid: ${violations.join("; ")}`,
         );
      }
   }
}

/**
 * BigQuery's driver signals failure by RETURNING `error.message` instead of
 * throwing, discarding the structured `code: 404` the Google client gave it
 * (`@malloydata/db-bigquery` fetchTableSchema). That text is the only surviving
 * signal, so a missing table is told apart from a broken connection by matching
 * BigQuery's own API wording.
 *
 * Deliberately not generalized. Postgres reports a missing table as the generic
 * "Unable to read schema.", and Snowflake's DESCRIBE TABLE conflates absence
 * with denial ("does not exist or not authorized"); guessing would be worse
 * than leaving them 502. (On the sqlSource, sqlQuery and sqlTemporaryTable
 * paths a Postgres rejection carries a SQLSTATE, which statementRejection
 * reads; getTable's Postgres driver returns text and drops the code.)
 */
const BIGQUERY_NOT_FOUND = /^Not found: (Table|Dataset)\b/;

/**
 * A path that cannot name a table in this dialect at all -- BigQuery needs
 * `dataset.table`, so a bare `sales` is rejected before any lookup happens. The
 * caller's argument is malformed, which is a 400, not a 404. Reached constantly
 * while a path is being typed.
 */
const BIGQUERY_IMPROPER_PATH = /^Improper table path\b/;

/**
 * DuckDB -- the sandbox, and the Azure and DuckLake connections built on it --
 * rejects rather than resolving empty, so a missing table arrives as a thrown
 * catalog error. These two cover the three shapes it produces for an absent
 * object: a missing table, a missing schema behind a table lookup, and a missing
 * catalog on a three-part path.
 *
 * Deliberately not `Catalog Error: .* does not exist`: DuckDB words a missing
 * EXTENSION the same way ("Table Function with name read_csv does not exist!"),
 * which is a misconfigured deployment. A 404 would hide a broken Azure or
 * DuckLake connection behind the one status nobody investigates.
 *
 * Likewise no pattern for `IO Error: No files found that match the pattern`:
 * for DuckLake or an Azure blob an absent file means corruption or a
 * misconfigured mount, not a mistyped name.
 */
const DUCKDB_TABLE_NOT_FOUND =
   /^Catalog Error: Table with name .+ does not exist/;
const DUCKDB_CATALOG_NOT_FOUND = /^Binder Error: Catalog .+ does not exist/;

function driverErrorToPublisherError(message: string): Error {
   if (
      BIGQUERY_NOT_FOUND.test(message) ||
      DUCKDB_TABLE_NOT_FOUND.test(message) ||
      DUCKDB_CATALOG_NOT_FOUND.test(message)
   ) {
      return new TableNotFoundError(message);
   }
   if (BIGQUERY_IMPROPER_PATH.test(message)) {
      return new InvalidArgumentError(message);
   }
   return new ConnectionError(message);
}

/**
 * Shared by the two schema-introspection catches. Both take a driver failure
 * that may be a thrown Error, a thrown string, or an already-classified error,
 * and answer with the narrowest error the message supports.
 */
function classifyDriverFailure(error: unknown): Error {
   if (
      error instanceof TableNotFoundError ||
      error instanceof InvalidArgumentError
   ) {
      return error;
   }
   const message =
      error instanceof Error
         ? error.message
         : typeof error === "string"
           ? error
           : JSON.stringify(error);
   return (
      databaseAccessFailure(error) ??
      statementRejection(error) ??
      driverErrorToPublisherError(message)
   );
}

/**
 * A failure running SQL the caller sent: a 502 with the driver's text logged
 * and generalized, unless the database was unreachable (502 with
 * `reason: CONNECTION_FAILED`), rejected the credentials (424 with
 * `reason: CONNECTION_AUTH_FAILED`), or -- Postgres only -- rejected the
 * statement itself (400 with the driver's message, or 404 for a table it does
 * not have).
 */
function sqlRunFailure(error: unknown): Error {
   // Already classified (an exhausted pool), with a message written for the
   // caller.
   if (error instanceof ConnectionError) return error;
   return (
      databaseAccessFailure(error) ??
      statementRejection(error) ??
      new ConnectionError((error as Error).message)
   );
}

/**
 * A raw statement, shaped so the connection's driver can read its rows.
 *
 * The Malloy Postgres driver hands back `rows[i].row` from both `runSQL` and
 * `runSQLStream`, because a Malloy Postgres query projects one `row` JSON
 * column. Raw SQL has no such column, so every row came back `undefined`: a
 * `null` per row on the buffered path, and a 502 on the streaming path Postgres
 * takes, where the byte cap measures each row. Wrapping the statement in
 * `row_to_json` supplies the column, as `probeSelect` and the introspection
 * queries already do. Trailing semicolons are dropped, and the statement sits
 * on its own lines so a trailing `--` comment cannot swallow the `)`. Other
 * dialects return rows as plain objects and get the statement unchanged.
 */
export function rawSqlForDriver(
   dialectName: string | undefined,
   sqlStatement: string,
): string {
   if (dialectName !== "postgres") return sqlStatement;
   let end = sqlStatement.length;
   while (end > 0 && /[\s;]/.test(sqlStatement[end - 1])) end -= 1;
   return `SELECT row_to_json(__q) AS "row" FROM (\n${sqlStatement.slice(0, end)}\n) AS __q`;
}

/**
 * Schema listing lets most driver failures through as they are (a 500), but
 * an unreachable database or rejected credentials answer the same as on every
 * other route that reaches the database.
 */
async function withDatabaseAccessClassified<T>(
   list: () => Promise<T>,
): Promise<T> {
   try {
      return await list();
   } catch (error) {
      throw databaseAccessFailure(error) ?? error;
   }
}

const CONNECTION_TYPES = new Set<string>([
   "postgres",
   "bigquery",
   "snowflake",
   "trino",
   "databricks",
   "mysql",
   "sqlserver",
   "duckdb",
   "motherduck",
   "ducklake",
   "publisher",
]);

const connectionOperations = lazyCounter(
   "publisher_connection_operations_total",
   "Connection create/update/delete/test requests. Labels: operation ('create'|'update'|'delete'|'test'), type (the connection type, 'unknown' when absent or unrecognised), outcome ('success'|'failed'|'refused'). 'failed' is a test that ran and could not connect; 'refused' threw (a 4xx or 5xx).",
);

/** Run a connection operation and count it once, however it ends. */
async function metered<T>(
   operation: "create" | "update" | "delete" | "test",
   type: unknown,
   run: () => Promise<T>,
   outcomeOf: (result: T) => string = () => "success",
): Promise<T> {
   const label = {
      operation,
      type:
         typeof type === "string" && CONNECTION_TYPES.has(type)
            ? type
            : "unknown",
   };
   try {
      const result = await run();
      connectionOperations().add(1, { ...label, outcome: outcomeOf(result) });
      return result;
   } catch (error) {
      connectionOperations().add(1, { ...label, outcome: "refused" });
      throw error;
   }
}

export class ConnectionController {
   private environmentStore: EnvironmentStore;
   private connectionService: ConnectionService;
   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
      this.connectionService = new ConnectionService(environmentStore);
   }

   /**
    * Gets the appropriate Malloy connection for a given connection name.
    * For DuckDB connections, retrieves from package level; for others, from environment level.
    */
   private getApiConnectionForLookup(
      environment: Environment,
      connectionName: string,
   ): ApiConnection {
      if (connectionName === "duckdb") {
         return {
            name: "duckdb",
            type: "duckdb",
            duckdbConnection: { attachedDatabases: [] },
         };
      }
      return environment.getApiConnection(connectionName);
   }

   /**
    * A connection's default per-query metadata, or null when it declares none.
    * Fails open: metadata is observability, so a connection whose config can't be
    * read contributes no default rather than failing the query the caller asked
    * for.
    */
   private async connectionQueryMetadata(
      environmentName: string,
      connectionName: string,
   ): Promise<{
      default: QueryMetadata | null;
      enforced: QueryMetadata | null;
   }> {
      try {
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const connection = this.getApiConnectionForLookup(
            environment,
            connectionName,
         );
         return {
            default: connection.queryMetadata ?? null,
            enforced: connection.queryMetadataEnforced ?? null,
         };
      } catch (error) {
         // Fails open, but logged: the enforced layer is lost here and no
         // metric covers it.
         logger.debug("No query-metadata layers for connection", {
            connectionName,
            error,
         });
         return { default: null, enforced: null };
      }
   }

   /**
    * The Malloy connection a route runs against. Failures are classified here,
    * before any route's own catch, because resolving a connection can already
    * reach the database (a Postgres SSH tunnel opens on first lookup).
    *
    * A name the environment does not configure is a 404, as on the listing
    * routes: the caller typed it. The 424 `CONNECTION_NOT_FOUND` is for a
    * model naming a deleted connection, which the caller did not choose.
    */
   private async getMalloyConnection(
      environmentName: string,
      connectionName: string,
      packageName?: string,
   ): Promise<Connection> {
      try {
         return await this.lookupMalloyConnection(
            environmentName,
            connectionName,
            packageName,
         );
      } catch (error) {
         if (error instanceof UnconfiguredConnectionError) {
            throw new ConnectionNotFoundError(
               `Connection ${connectionName} not found`,
            );
         }
         throw databaseAccessFailure(error) ?? error;
      }
   }

   private async lookupMalloyConnection(
      environmentName: string,
      connectionName: string,
      packageName?: string,
   ): Promise<Connection> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );

      // "duckdb" is the per-package sandbox (the name is reserved at the
      // environment level), so the caller must say which package to use.
      if (connectionName === "duckdb") {
         const packages = await environment.listPackages();
         if (packageName) {
            // A package this server does not hold is a missing resource, so
            // 404 -- including when it holds no packages at all. Routers treat
            // a 404 as "this worker no longer serves that package" and
            // re-resolve; a 400 leaves them routing to the same stale worker.
            const known = packages.some((p) => p.name === packageName);
            if (!known) {
               throw new PackageNotFoundError(
                  `Package "${packageName}" not found in environment "${environmentName}"`,
               );
            }
            const pkg = await environment.getPackage(packageName);
            return await pkg.getMalloyConnection(connectionName);
         }
         if (packages.length === 0) {
            // Surfaces the standard "connection not found".
            return await environment.getMalloyConnection(connectionName);
         }
         if (packages.length === 1) {
            const onlyPackage = packages[0].name;
            if (!onlyPackage) {
               throw new ConnectionError("Package name is undefined", {
                  callerSafe: true,
               });
            }
            const pkg = await environment.getPackage(onlyPackage);
            return await pkg.getMalloyConnection(connectionName);
         }
         throw new BadRequestError(
            `Ambiguous "duckdb" connection lookup: environment "${environmentName}" has multiple packages, ` +
               `and the "duckdb" sandbox exists once per package. Name one of: ${packages
                  .map((p) => p.name)
                  .filter(Boolean)
                  .join(", ")}. ` +
               `Over MCP pass it as the packageName argument; over REST use ` +
               `/environments/${environmentName}/packages/{packageName}/connections/duckdb/...`,
         );
      } else {
         return await environment.getMalloyConnection(connectionName);
      }
   }

   /**
    * Fetches a table's schema via the Malloy connection's fetchTableSchema,
    * returning an ApiTable with columns and the raw source JSON.
    */
   private async fetchTable(
      malloyConnection: Connection,
      tableKey: string,
      tablePath: string,
   ): Promise<ApiTable> {
      try {
         const source = await (
            malloyConnection as Connection & {
               fetchTableSchema: (
                  tableKey: string,
                  tablePath: string,
               ) => Promise<TableSourceDef | string | undefined>;
            }
         ).fetchTableSchema(tableKey, tablePath);
         if (!source) {
            throw new TableNotFoundError(`Table ${tablePath} not found`);
         }
         // BigQueryConnection returns `error.message` as a string on failure instead of throwing.
         if (typeof source === "string") {
            // The SQL Server connector answers an object the server reports
            // missing (its error 208) with this exact text.
            if (source === `Table ${tablePath} not found`) {
               throw new TableNotFoundError(source);
            }
            // Malloy's Postgres and Databricks drivers return a failed lookup
            // as `Error fetching schema for <path>: <driver message>`, and the
            // SQL Server connector as `Error fetching schema for table <path>:
            // <number>: <driver message>`, the number being SQL Server's (0
            // for a failure before the server answered). Both drop the error's
            // code. The driver message is still matched like any other
            // code-less one, so a dead database answers 502 CONNECTION_FAILED
            // here as on every other route.
            const driverMessage = lookupFailureDriverMessage(source, tablePath);
            const accessFailure =
               driverMessage === undefined
                  ? undefined
                  : databaseAccessFailure(new Error(driverMessage));
            throw accessFailure ?? driverErrorToPublisherError(source);
         }

         return {
            source: JSON.stringify(source),
            resource: tablePath,
            columns: (source.fields || []).map((f) => ({
               name: f.name,
               type: f.type,
            })),
         };
      } catch (error) {
         // Where most not-founds land: every driver but BigQuery rejects
         // rather than returning a message, so the checks above never see them.
         const classified = classifyDriverFailure(error);
         if (
            classified instanceof TableNotFoundError ||
            classified instanceof InvalidArgumentError
         ) {
            // A caller's bad reference, not a fault: warn, so a mistyped path
            // cannot fill the error log while it is being typed.
            logger.warn("table not resolvable", {
               tableKey,
               tablePath,
               reason: classified.constructor.name,
            });
            throw classified;
         }
         // A connection problem is logged once, at warn, where it is mapped.
         if (databaseAccessFailure(classified)) throw classified;
         logger.error("fetchTableSchema error", {
            error,
            tableKey,
            tablePath,
         });
         throw classified;
      }
   }

   public async getConnection(
      environmentName: string,
      connectionName: string,
   ): Promise<ApiConnection> {
      if (!environmentName || !connectionName) {
         throw new BadRequestError("Connection payload is required");
      }
      // The in-memory API connection carries `attributes`; the DB row stores
      // raw config and FK columns, which are not the ApiConnection shape.
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      return toPublicConnection(environment.getApiConnection(connectionName));
   }

   public async listConnections(
      environmentName: string,
   ): Promise<ApiConnection[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      return toPublicConnections(environment.listApiConnections());
   }

   // Lists schemas (namespaces) available in a connection.
   // For "duckdb", the per-package sandbox, packageName disambiguates which
   // package's DuckDB to browse in a multi-package environment.
   public async listSchemas(
      environmentName: string,
      connectionName: string,
      packageName?: string,
   ): Promise<ApiSchema[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const connection = this.getApiConnectionForLookup(
         environment,
         connectionName,
      );
      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );

      return withDatabaseAccessClassified(() =>
         getSchemasForConnection(connection, malloyConnection),
      );
   }

   // Lists tables available in a schema. For postgres the schema is usually "public".
   // packageName disambiguates per-package "duckdb" lookups (see listSchemas).
   public async listTables(
      environmentName: string,
      connectionName: string,
      schemaName: string,
      tableNames?: string[],
      packageName?: string,
   ): Promise<ApiTable[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const connection = this.getApiConnectionForLookup(
         environment,
         connectionName,
      );
      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );

      return withDatabaseAccessClassified(() =>
         listTablesForSchema(
            connection,
            schemaName,
            malloyConnection,
            tableNames,
         ),
      );
   }

   public async getConnectionSqlSource(
      environmentName: string,
      connectionName: string,
      sqlStatement: string,
      packageName?: string,
   ): Promise<ApiSqlSource> {
      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );
      try {
         const schema = await (
            malloyConnection as Connection & {
               fetchSelectSchema: (params: {
                  connection: string;
                  selectStr: string;
               }) => Promise<unknown>;
            }
         ).fetchSelectSchema({
            connection: connectionName,
            selectStr: sqlStatement,
         });

         // BigQueryConnection returns `error.message` as a string on failure instead of throwing.
         if (typeof schema === "string") {
            throw driverErrorToPublisherError(schema);
         }

         return {
            source: JSON.stringify(schema),
         };
      } catch (error) {
         // Same classification as fetchTable, so an absent reference is not a
         // 502 and a thrown string keeps its message.
         throw classifyDriverFailure(error);
      }
   }

   public async getTable(
      environmentName: string,
      connectionName: string,
      schemaName: string,
      tablePath: string,
      packageName?: string,
   ): Promise<ApiTable> {
      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const connection = this.getApiConnectionForLookup(
         environment,
         connectionName,
      );

      if (connection.type === "ducklake") {
         if (tablePath.split(".").length === 1) {
            // db-publisher sends `tablePath.split('.')[0]` as the schema, so `table('t')` arrives as schema `t`.
            tablePath =
               schemaName === tablePath
                  ? `${connectionName}.${tablePath}`
                  : `${connectionName}.${schemaName}.${tablePath}`;
         } else if (
            tablePath.split(".").length === 2 &&
            !tablePath.startsWith(connectionName)
         ) {
            // tablePath is schemaName.tableName but missing connection prefix
            tablePath = `${connectionName}.${tablePath}`;
         }
         // If tablePath already has 3+ parts or starts with connection name, use as-is
      }

      if (connection.type === "duckdb") {
         const attachedDbs =
            connection.duckdbConnection?.attachedDatabases || [];
         const azureDb = attachedDbs.find(
            (db) =>
               db.type === "azure" &&
               db.name === schemaName &&
               db.azureConnection,
         );
         if (azureDb && azureDb.azureConnection) {
            // Reconstruct the full SAS URL for the specific file
            const azureConn = azureDb.azureConnection;
            const baseUrl =
               azureConn.authType === "sas_token"
                  ? azureConn.sasUrl
                  : azureConn.fileUrl;
            if (baseUrl) {
               // Extract the file name from tablePath (e.g., "a.aircraft.parquet" -> "aircraft.parquet")
               const fileName = tablePath.includes(".")
                  ? tablePath.split(".").slice(1).join(".")
                  : tablePath;
               // Replace the file portion in the base URL with the specific file name
               const urlParts = baseUrl.split("?");
               const basePath = urlParts[0];
               const queryString = urlParts[1] ? `?${urlParts[1]}` : "";
               // Replace the last path segment (or glob) with the actual file name
               const dirPath = basePath.substring(
                  0,
                  basePath.lastIndexOf("/") + 1,
               );
               const fullFileUrl = `${dirPath}${fileName}${queryString}`;

               const table = await this.fetchTable(
                  malloyConnection,
                  fileName,
                  fullFileUrl,
               );
               return { ...table, resource: tablePath };
            }
         }
      }

      const tableKey = tablePath.split(".").pop();
      if (!tableKey) {
         throw new Error(`Invalid tablePath: ${tablePath}`);
      }

      return this.fetchTable(malloyConnection, tableKey, tablePath);
   }

   public async getConnectionQueryData(
      environmentName: string,
      connectionName: string,
      sqlStatement: string,
      options: string,
      packageName?: string,
      /**
       * The request's per-query metadata fields, unvalidated — this controller is
       * the boundary that turns a bad bag into a 400 rather than letting the
       * connector refuse the statement at dispatch.
       */
      metadata?: { queryMetadata?: unknown; queryClass?: unknown },
   ): Promise<ApiQueryData> {
      // Express parses repeated query parameters (?sqlStatement=a&sqlStatement=b)
      // and array-shaped JSON bodies as `string[]`, not `string`. The route
      // handlers up-cast for TypeScript's benefit, so re-validate here at the
      // controller boundary before forwarding to the connector.
      if (typeof sqlStatement !== "string") {
         throw new BadRequestError("sqlStatement must be a string");
      }
      if (
         options !== undefined &&
         options !== null &&
         typeof options !== "string"
      ) {
         throw new BadRequestError("options must be a string");
      }

      // Shed load before any disk / DB work. The env-fetch is cached, so this
      // is an O(1) read on the hot path.
      const environmentForAdmission =
         await this.environmentStore.getEnvironment(environmentName, false);
      environmentForAdmission.assertCanAdmitQuery();

      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );

      let runSQLOptions: RunSQLOptions = {};
      if (options) {
         // JSON.parse also yields `null`, `42`, `"foo"`, `[1,2,3]`. Anything
         // but a plain object would crash on `.rowLimit` (null) or be mutated
         // and passed to the connector.
         let parsed: unknown;
         try {
            parsed = JSON.parse(options);
         } catch {
            throw new BadRequestError("options must be valid JSON");
         }
         if (
            parsed === null ||
            typeof parsed !== "object" ||
            Array.isArray(parsed)
         ) {
            throw new BadRequestError(
               'options must be a JSON object (e.g. {"rowLimit": 100})',
            );
         }
         runSQLOptions = parsed as RunSQLOptions;
      }
      if (runSQLOptions.abortSignal) {
         // A caller-supplied abortSignal is not supported.
         logger.info("Clearing unsupported abortSignal");
         runSQLOptions.abortSignal = undefined;
      }

      // Per-query metadata. Validated here, not clamped: a raw-SQL caller gets a
      // 400 telling it which property is wrong instead of a statement the
      // connector refuses at dispatch. `options` is forwarded as RunSQLOptions, so
      // a bag can also arrive inside it — validate that one too, and let the
      // documented field win.
      const suppliedMetadata =
         metadata?.queryMetadata ?? runSQLOptions.queryMetadata;
      let requestMetadata: QueryMetadata | undefined;
      let queryClass: QueryClass | undefined;
      try {
         requestMetadata = parseSuppliedQueryMetadata(suppliedMetadata);
         queryClass = parseQueryClass(metadata?.queryClass);
      } catch (error) {
         throw new BadRequestError((error as Error).message);
      }
      const connectionLayers = await this.connectionQueryMetadata(
         environmentName,
         connectionName,
      );
      const resolvedMetadata = mergeQueryMetadata({
         connection: connectionLayers.default,
         enforced: connectionLayers.enforced,
         request: requestMetadata,
         context: {
            // Raw SQL against a connection is platform maintenance unless the
            // caller says otherwise — it is not a modeled query.
            queryClass: queryClass ?? "ops",
            environment: environmentName,
            package: packageName,
            correlationId: mintCorrelationId(),
         },
      });
      runSQLOptions.queryMetadata = resolvedMetadata.metadata;
      const queryCorrelationId = resolvedMetadata.metadata?.query_id ?? null;

      // Two layered caps:
      //   - Row cap (PUBLISHER_MAX_QUERY_ROWS), pushed to the driver as
      //     rowLimit = min(callerLimit, cap+1). cap+1 rows back is the
      //     sentinel for overflow, answered 413. 0 disables it.
      //   - Byte cap (PUBLISHER_MAX_RESPONSE_BYTES), the real memory bound,
      //     since one 10 MB JSON column defeats the row cap. Streaming
      //     connections only: buffered `runSQL` has already allocated.
      // Connectors are trusted to honor rowLimit.
      const maxRows = getMaxQueryRows();
      const maxBytes = getMaxResponseBytes();
      if (maxRows > 0) {
         runSQLOptions.rowLimit = Math.min(
            runSQLOptions.rowLimit ?? maxRows + 1,
            maxRows + 1,
         );
      }

      const driverSql = rawSqlForDriver(
         malloyConnection.dialectName,
         sqlStatement,
      );

      // Streaming connections go through streamSqlWithBudget so the byte cap
      // fires mid-stream; byte counting on the buffered path would be theatre.
      //
      // runWithQueryTimeout turns a hung query into a 504, and its signal is
      // passed as abortSignal so the driver call is actually cancelled.
      if (isStreamingConnection(malloyConnection)) {
         const streamed = await runWithQueryTimeout(async (signal) => {
            const optionsWithSignal: RunSQLOptions = {
               ...runSQLOptions,
               abortSignal: signal,
            };
            try {
               return await streamSqlWithBudget(
                  malloyConnection,
                  driverSql,
                  optionsWithSignal,
                  { maxRows, maxBytes },
               );
            } catch (error) {
               // Already classified, with a message written for the caller.
               if (
                  error instanceof PayloadTooLargeError ||
                  error instanceof ConnectionError
               )
                  throw error;
               // If the timer fired, runWithQueryTimeout replaces this with
               // a QueryTimeoutError, so this matters only for real driver
               // failures.
               throw sqlRunFailure(error);
            }
         }, getQueryTimeoutMs());
         return { data: JSON.stringify(streamed), queryCorrelationId };
      }

      const result = await runWithQueryTimeout(async (signal) => {
         const optionsWithSignal: RunSQLOptions = {
            ...runSQLOptions,
            abortSignal: signal,
         };
         try {
            return await malloyConnection.runSQL(driverSql, optionsWithSignal);
         } catch (error) {
            throw sqlRunFailure(error);
         }
      }, getQueryTimeoutMs());

      if (maxRows > 0 && result.rows.length > maxRows) {
         // Counted on the buffered path too, so the metric covers every
         // connector, not just streaming ones.
         recordQueryCapExceeded("rows", "connection_sql");
         throw new PayloadTooLargeError(
            `Query returned more than ${maxRows} rows. Refine the query (add a LIMIT or more selective WHERE) or raise PUBLISHER_MAX_QUERY_ROWS.`,
         );
      }

      return { data: JSON.stringify(result), queryCorrelationId };
   }

   public async getConnectionTemporaryTable(
      environmentName: string,
      connectionName: string,
      sqlStatement: string,
      packageName?: string,
   ): Promise<ApiTemporaryTable> {
      // Re-validated for the same reason as in getConnectionQueryData.
      if (typeof sqlStatement !== "string") {
         throw new BadRequestError("sqlStatement must be a string");
      }

      // `manifestTemporaryTable` runs a real `CREATE TEMPORARY TABLE AS`, so
      // a back-pressured publisher must 503 these like ad-hoc SELECTs.
      const environmentForAdmission =
         await this.environmentStore.getEnvironment(environmentName, false);
      environmentForAdmission.assertCanAdmitQuery();

      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );

      // `manifestTemporaryTable` takes no abortSignal, so the call is raced
      // against the timeout signal: the response returns 504 at the deadline,
      // but the DDL keeps running in the database (cancelling it needs abort
      // support in `@malloydata/db-*`).
      //
      // runWithQueryTimeout re-emits any rejection after the timer fired as a
      // QueryTimeoutError, so the abort reason is re-thrown unwrapped.
      return await runWithQueryTimeout(async (signal) => {
         try {
            const tableName = await Promise.race([
               (malloyConnection as PersistSQLResults).manifestTemporaryTable(
                  sqlStatement,
               ),
               new Promise<never>((_resolve, reject) => {
                  if (signal.aborted) {
                     reject(signal.reason);
                     return;
                  }
                  signal.addEventListener(
                     "abort",
                     () => reject(signal.reason),
                     { once: true },
                  );
               }),
            ]);
            return { table: JSON.stringify(tableName) };
         } catch (error) {
            // The timeout wrapper converts this; do not bury it in a
            // ConnectionError.
            if (signal.aborted) throw error;
            throw sqlRunFailure(error);
         }
      }, getQueryTimeoutMs());
   }

   public async testConnectionConfiguration(
      input: ApiConnection & { config?: ApiConnection },
   ): Promise<ApiConnectionStatus> {
      const config = input?.config ?? input;
      return metered(
         "test",
         config?.type,
         () => this.testConfiguration(input),
         (status) => (status.status === "ok" ? "success" : "failed"),
      );
   }

   private async testConfiguration(
      input: ApiConnection & { config?: ApiConnection },
   ): Promise<ApiConnectionStatus> {
      // Some clients wrap the payload as { config: <connection> }; unwrap.
      const connectionConfig: ApiConnection =
         input.config && typeof input.config === "object"
            ? input.config
            : input;

      if (
         !connectionConfig ||
         typeof connectionConfig !== "object" ||
         Object.keys(connectionConfig).length === 0
      ) {
         throw new BadRequestError(
            "Connection configuration is required and cannot be empty",
         );
      }

      if (!connectionConfig.type || typeof connectionConfig.type !== "string") {
         throw new BadRequestError(
            "Connection type is required and must be a string",
         );
      }

      // duckdb/ducklake derive a `<name>.duckdb` filename, so an unsafe name
      // is a 400 rather than a failed test. Other types accept any name; an
      // empty name falls through to the service's missing-name failure.
      if (
         (connectionConfig.type === "duckdb" ||
            connectionConfig.type === "ducklake") &&
         connectionConfig.name
      ) {
         assertSafePackageName(connectionConfig.name);
      }

      try {
         return await testConnectionConfig(connectionConfig);
      } catch (error) {
         // The driver's text is the payload, not a leak: the caller supplied
         // this config and is being told why it does not work. Unlike the
         // 500/502 bodies the mapper generalizes, nothing in scope belongs to
         // another tenant or the server; host, port and user came in this
         // request.
         return {
            status: "failed",
            // Defence in depth, NOT the redaction point: testConnectionConfig
            // resolves failures into a ConnectionStatus, so this fires only if
            // it throws before its own catch-all (untestable without a module
            // mock). The redaction that matters is in service/connection.ts.
            errorMessage: redactConnectionSecretShapes(
               `Connection test failed: ${
                  error instanceof Error ? error.message : String(error)
               }`,
            ),
         };
      }
   }

   public async addConnection(
      environmentName: string,
      connectionName: string,
      connectionConfig: ApiConnection,
   ): Promise<{ message: string }> {
      return metered("create", connectionConfig?.type, () =>
         this.createConnection(
            environmentName,
            connectionName,
            connectionConfig,
         ),
      );
   }

   private async createConnection(
      environmentName: string,
      connectionName: string,
      connectionConfig: ApiConnection,
   ): Promise<{ message: string }> {
      if (!connectionConfig || typeof connectionConfig !== "object") {
         throw new BadRequestError("Connection configuration is required");
      }

      if (!connectionConfig.type) {
         throw new BadRequestError("Connection type is required");
      }

      validateAzureAttachedDatabases(connectionConfig);
      validateAdminAuthoredConnection(connectionName, connectionConfig);

      logger.info(
         `Creating connection "${connectionName}" in environment "${environmentName}"`,
      );

      await this.connectionService.addConnection(
         environmentName,
         connectionName,
         connectionConfig,
      );

      return {
         message: `Connection "${connectionName}" created successfully`,
      };
   }

   public async updateConnection(
      environmentName: string,
      connectionName: string,
      connection: Partial<ApiConnection>,
   ): Promise<{ message: string }> {
      return metered("update", connection?.type, () =>
         this.applyConnectionUpdate(
            environmentName,
            connectionName,
            connection,
         ),
      );
   }

   private async applyConnectionUpdate(
      environmentName: string,
      connectionName: string,
      connection: Partial<ApiConnection>,
   ): Promise<{ message: string }> {
      if (!connection || typeof connection !== "object") {
         throw new BadRequestError("Connection payload is required");
      }

      validateAzureAttachedDatabases(connection);
      validateAdminAuthoredConnection(connectionName, connection);

      logger.info(
         `Updating connection "${connectionName}" in environment "${environmentName}"`,
      );

      await this.connectionService.updateConnection(
         environmentName,
         connectionName,
         connection,
      );

      return {
         message: `Connection "${connectionName}" updated successfully`,
      };
   }

   public async deleteConnection(
      environmentName: string,
      connectionName: string,
   ): Promise<{ message: string }> {
      return metered("delete", undefined, () =>
         this.removeConnection(environmentName, connectionName),
      );
   }

   private async removeConnection(
      environmentName: string,
      connectionName: string,
   ): Promise<{ message: string }> {
      logger.info(
         `Deleting connection "${connectionName}" from environment "${environmentName}"`,
      );

      await this.connectionService.deleteConnection(
         environmentName,
         connectionName,
      );

      return {
         message: `Connection "${connectionName}" deleted successfully`,
      };
   }
}

/**
 * The driver's own message inside a table lookup's failure text, or undefined
 * when the text is not one of the shapes the drivers produce.
 */
export function lookupFailureDriverMessage(
   source: string,
   tablePath: string,
): string | undefined {
   const prefix = `Error fetching schema for ${tablePath}: `;
   if (source.startsWith(prefix)) return source.slice(prefix.length);
   const sqlserverPrefix = `Error fetching schema for table ${tablePath}: `;
   if (source.startsWith(sqlserverPrefix)) {
      return source.slice(sqlserverPrefix.length).replace(/^\d+: /, "");
   }
   return undefined;
}
