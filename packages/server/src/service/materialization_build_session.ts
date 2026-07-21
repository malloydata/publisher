import { DuckDBConnection } from "@malloydata/db-duckdb";
import path from "node:path";
import type { components } from "../api";
import { BadRequestError } from "../errors";
import { logger } from "../logger";
import { quoteIdentifier, quoteTablePath } from "./quoting";
import {
   attachDuckLakeReadWrite,
   escapeSQL,
   federateSourceForPassthrough,
   type FederatedSourceType,
} from "./connection";

type ApiConnection = components["schemas"]["Connection"];
type WireColumn = components["schemas"]["Column"];

/** Source warehouse types the native query-passthrough build supports. */
const PASSTHROUGH_SOURCE_TYPES: readonly FederatedSourceType[] = [
   "bigquery",
   "snowflake",
   "postgres",
];

/** Destination (storage) connection types the build can materialize INTO. */
const STORAGE_DESTINATION_TYPES = ["ducklake", "duckdb"] as const;

/**
 * Wrap a source-dialect SELECT as a native per-engine query-passthrough call so
 * a build-scoped DuckDB session can execute it against the source warehouse and
 * stream back only result-sized rows. The compiled SELECT is embedded verbatim
 * as a SQL string literal (single-quote-escaped) — the warehouse computes; the
 * base tables never enter DuckDB. `handle` is the per-engine reference
 * {@link federateSourceForPassthrough} established on the session (a projectId,
 * a secret name, or an ATTACH alias). Argument ORDER differs per engine, which
 * is the whole reason this is centralized.
 */
export function wrapPassthrough(
   sourceType: FederatedSourceType,
   handle: string,
   innerSQL: string,
): string {
   const sql = `'${escapeSQL(innerSQL)}'`;
   const h = `'${escapeSQL(handle)}'`;
   switch (sourceType) {
      case "bigquery":
         // bigquery_query(<project/db>, <sql>) — secret resolved by bq:// scope.
         return `SELECT * FROM bigquery_query(${h}, ${sql})`;
      case "postgres":
         // postgres_query(<attached-db alias>, <sql>).
         return `SELECT * FROM postgres_query(${h}, ${sql})`;
      case "snowflake":
         // snowflake_query(<sql>, <secret name>) — SQL FIRST for this engine.
         return `SELECT * FROM snowflake_query(${sql}, ${h})`;
      default: {
         // Exhaustiveness: a new FederatedSourceType must add a case above.
         const exhaustive: never = sourceType;
         throw new BadRequestError(
            `Unsupported passthrough source type: ${String(exhaustive)}`,
         );
      }
   }
}

/** Narrow a connection's declared type to a supported passthrough source type. */
export function passthroughSourceType(
   sourceConnection: ApiConnection,
): FederatedSourceType {
   const type = sourceConnection.type;
   if ((PASSTHROUGH_SOURCE_TYPES as readonly string[]).includes(type ?? "")) {
      return type as FederatedSourceType;
   }
   throw new BadRequestError(
      `Cannot materialize a '${type}' source into a storage destination: the ` +
         `native query-passthrough build supports source connections of type ` +
         `${PASSTHROUGH_SOURCE_TYPES.join(", ")} only. (A DuckDB/DuckLake source ` +
         `is already local and needs no passthrough.)`,
   );
}

/** Result of building one source into a storage destination. */
export interface StorageBuildResult {
   /** The connection the physical table now lives in (the destination). */
   storageConnectionName: string;
   /** Authoritative DuckDB column schema, captured post-build via DESCRIBE. */
   schema: WireColumn[];
}

/**
 * Build one persist source into a `storage=` destination via native
 * query-passthrough CTAS, on a *build-scoped* DuckDB session that is created,
 * used, and disposed here — so the read-write destination attach and the source
 * credential federation exist ONLY for the build's duration and NEVER on the
 * serve connection (the least-privilege boundary the design requires).
 *
 * Steps: create an in-memory DuckDB session → attach the destination read-write
 * → federate the source's credentials on demand → `CREATE OR REPLACE TABLE`
 * (qualified with the destination catalog) whose FROM is the source passthrough
 * → capture the built table's authoritative schema (DESCRIBE) → dispose the
 * session (releasing every credential/attach). DuckLake's catalog swap is
 * transactional, so the replace is atomic. The compiled SELECT (source dialect)
 * is unchanged; only where it executes and where the result lands move.
 *
 * @returns the destination connection name and the captured authoritative
 *   schema, both recorded on the manifest entry for the serve transform.
 */
export async function buildSourceIntoStorage(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   sourceConnection: ApiConnection;
   /** The source-dialect compiled SELECT (v0's getSQL output), verbatim. */
   buildSQL: string;
   /** Logical, unquoted physical table path (may carry a container path). */
   physicalTableName: string;
   /**
    * Logical name to expose as a convenience VIEW over the (content-addressed)
    * physical table, for operators poking at the DuckLake directly. Best-effort:
    * publisher's own reads/writes always address `physicalTableName`, never the
    * view. Omitted, or equal to `physicalTableName`, means no view.
    */
   logicalViewName?: string;
   environmentPath: string;
}): Promise<StorageBuildResult> {
   const {
      destinationName,
      destinationConnection,
      sourceConnection,
      buildSQL,
      physicalTableName,
      logicalViewName,
      environmentPath,
   } = params;

   assertSupportedDestination(destinationName, destinationConnection);
   const sourceType = passthroughSourceType(sourceConnection);

   // A dedicated, disposable build session. Credentials and the read-write
   // destination attach live only here, only for this build.
   const session = new DuckDBConnection(`build_${destinationName}`, ":memory:");
   try {
      await attachDestinationReadWrite(
         session,
         destinationName,
         destinationConnection,
         environmentPath,
      );

      if (destinationConnection.type === "ducklake") {
         // Write-path: disable DuckLake row inlining for this build session so
         // materialized rows land as Parquet in the data store, NOT inlined into
         // the catalog database. Materializations are generally large, and in a
         // shared/multitenant catalog inlining would push tenant data into the
         // catalog Postgres. Session-scoped (this build only) — it does not
         // mutate the shared catalog's persisted options.
         await session.runSQL("SET ducklake_default_data_inlining_row_limit=0");
      }

      const federated = await federateSourceForPassthrough(
         session,
         sourceType,
         sourceFederationConfig(sourceConnection),
      );

      // The CTAS target MUST be qualified with the destination catalog (the
      // attach alias) — an unqualified name lands in the build session's own
      // in-memory catalog, not the DuckLake/DuckDB store, so the rows would be
      // captured for the schema and then vanish with the session. DuckLake's
      // catalog swap is transactional, so CREATE OR REPLACE is atomic — no
      // staging/rename dance needed on this path.
      const target = quoteTablePath(
         `${destinationName}.${physicalTableName}`,
         "duckdb",
      );
      const passthrough = wrapPassthrough(
         sourceType,
         federated.handle,
         buildSQL,
      );

      await session.runSQL(
         `CREATE OR REPLACE TABLE ${target} AS (${passthrough})`,
      );
      // Capture the authoritative schema from the freshly-built table — the
      // serve transform declares exactly this, and the compiler does not
      // type-check a virtual source's declared columns.
      const schema = await describeTable(session, target);

      // Best-effort operator convenience: point the logical name at this
      // generation's physical table via a view. Publisher never reads or writes
      // through it (it always addresses the hashed physical name), so a failure
      // here — e.g. the logical name collides with a real object — is logged and
      // ignored rather than failing the build.
      if (logicalViewName && logicalViewName !== physicalTableName) {
         try {
            await session.runSQL(
               logicalViewSql(
                  destinationName,
                  logicalViewName,
                  physicalTableName,
               ),
            );
         } catch (err) {
            logger.warn(
               "Failed to create the logical convenience view over a " +
                  "materialized table (non-fatal; publisher reads the physical name)",
               {
                  destinationName,
                  logicalViewName,
                  physicalTableName,
                  error: err instanceof Error ? err.message : String(err),
               },
            );
         }
      }

      return { storageConnectionName: destinationName, schema };
   } finally {
      // Dispose releases the session's secrets and attaches — nothing federated
      // or read-write survives the build.
      try {
         await session.close();
      } catch (err) {
         logger.warn(
            "Failed to close build session (leaked in-memory session)",
            {
               destinationName,
               error: err instanceof Error ? err.message : String(err),
            },
         );
      }
   }
}

/**
 * DDL for the operator convenience view: `CREATE OR REPLACE VIEW` exposing the
 * logical name over the content-addressed physical table, both qualified with
 * the destination catalog and quoted for DuckDB. Pure (no I/O) so the SQL shape
 * is unit-testable; execution is best-effort in {@link buildSourceIntoStorage}.
 */
export function logicalViewSql(
   destinationName: string,
   logicalViewName: string,
   physicalTableName: string,
): string {
   const view = quoteTablePath(
      `${destinationName}.${logicalViewName}`,
      "duckdb",
   );
   const table = quoteTablePath(
      `${destinationName}.${physicalTableName}`,
      "duckdb",
   );
   return `CREATE OR REPLACE VIEW ${view} AS SELECT * FROM ${table}`;
}

/** Attach the destination connection read-write on the build session. */
async function attachDestinationReadWrite(
   session: DuckDBConnection,
   destinationName: string,
   destinationConnection: ApiConnection,
   environmentPath: string,
): Promise<void> {
   if (destinationConnection.type === "ducklake") {
      const cfg = destinationConnection.ducklakeConnection;
      if (!cfg) {
         throw new BadRequestError(
            `Storage destination '${destinationName}' is type 'ducklake' but ` +
               `has no ducklakeConnection config.`,
         );
      }
      await attachDuckLakeReadWrite(session, destinationName, cfg);
      return;
   }
   // Plain DuckDB destination: attach its database file read-write. The file
   // path is derived the same way connection assembly derives it.
   const dbPath = path.join(environmentPath, `${destinationName}.duckdb`);
   await session.runSQL(
      `ATTACH '${escapeSQL(dbPath)}' AS ${quoteIdentifier(destinationName, "duckdb")}`,
   );
}

/** Throw a clean 422 unless the destination is a supported storage type. */
function assertSupportedDestination(
   destinationName: string,
   destinationConnection: ApiConnection,
): void {
   const type = destinationConnection.type ?? "";
   if (!(STORAGE_DESTINATION_TYPES as readonly string[]).includes(type)) {
      throw new BadRequestError(
         `Storage destination '${destinationName}' is type '${type}', but ` +
            `'storage=' destinations must be one of ` +
            `${STORAGE_DESTINATION_TYPES.join(", ")}.`,
      );
   }
}

/**
 * The subset of a source connection's config the passthrough federation needs,
 * plus a sanitized `name` base for the secret/alias it creates on the session.
 */
function sourceFederationConfig(sourceConnection: ApiConnection): {
   name: string;
   bigqueryConnection?: components["schemas"]["BigqueryConnection"];
   snowflakeConnection?: components["schemas"]["SnowflakeConnection"];
   postgresConnection?: components["schemas"]["PostgresConnection"];
} {
   return {
      name: sourceConnection.name ?? "src",
      bigqueryConnection: sourceConnection.bigqueryConnection,
      snowflakeConnection: sourceConnection.snowflakeConnection,
      postgresConnection: sourceConnection.postgresConnection,
   };
}

/**
 * Read the authoritative column schema of a just-built table with `DESCRIBE`,
 * mapping DuckDB's `column_name`/`column_type` rows to the wire {@link Column}
 * shape the manifest carries. This is the schema the serve transform declares.
 */
async function describeTable(
   session: DuckDBConnection,
   quotedTablePath: string,
): Promise<WireColumn[]> {
   const result = await session.runSQL(`DESCRIBE ${quotedTablePath}`);
   const rows = Array.isArray(result) ? result : (result.rows ?? []);
   const columns: WireColumn[] = [];
   for (const row of rows as Record<string, unknown>[]) {
      const name = row.column_name;
      const type = row.column_type;
      if (typeof name === "string" && typeof type === "string") {
         columns.push({ name, type });
      }
   }
   return columns;
}
