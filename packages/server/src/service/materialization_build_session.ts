import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   type PersistSource,
   Runtime,
} from "@malloydata/malloy";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import type { components } from "../api";
import { BadRequestError, MaterializationEligibilityError } from "../errors";
import { logger } from "../logger";
import { errMessage } from "../utils";
import { quoteIdentifier, quoteManifestTablePath } from "./quoting";
import { projectToPublicColumns } from "./build_plan";
import {
   attachDuckLakeReadWrite,
   escapeSQL,
   federateSourceForPassthrough,
   type FederatedSourceType,
} from "./connection";
import {
   assertServesInDuckDB,
   type ServeBinding,
} from "./materialization_serve_transform";

type ApiConnection = components["schemas"]["Connection"];

/**
 * The process-wide session the build-time servability gate compiles against.
 * Created on first use and deliberately never disposed: it holds no attach and
 * no credentials, and recreating it per build was the single largest source of
 * per-build memory growth.
 */
let sharedGateSession: DuckDBConnection | undefined;
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
         `${PASSTHROUGH_SOURCE_TYPES.join(", ")} only.`,
   );
}

/**
 * A build-scoped DuckDB session on its OWN in-memory instance, plus a disposer
 * that closes it and removes the throwaway working directory that pins its
 * instance identity.
 *
 * Isolation is load-bearing for multi-tenant safety. `@malloydata/db-duckdb`
 * pools DuckDB instances in a process-global cache keyed by a "share key" that
 * deliberately EXCLUDES the connection name, and it never gives a `:memory:`
 * primary a private instance — so build sessions built with identical default
 * config get pooled onto ONE shared in-memory instance whenever their lifetimes
 * overlap (notably with the long-lived serve connection). Pooled together, they
 * collide on their transient ATTACH aliases: two builds (or two environments
 * with same-named-but-different-credential source/destination connections) both
 * `ATTACH … AS orders_pg` / `AS lake` on the shared instance and one clobbers
 * the other — a cross-tenant read (source) or WRITE (read-write destination).
 * DuckDB itself isolates SEPARATE instances cleanly; the collision is purely an
 * artifact of the shared pool.
 *
 * Fix: give each session a UNIQUE `workingDirectory`, which is part of the share
 * key, so it lands its OWN in-memory instance — its own catalog, secrets, and
 * attaches, torn down on close. Crucially `databasePath` stays exactly
 * `:memory:` (NOT a temp file): a `:memory:` primary lets a DuckLake attach
 * auto-initialize a fresh catalog, whereas a file primary does not. The working
 * directory is only there to make the share key unique — nothing is written to
 * it (every real path the build uses is absolute), so it stays empty and is
 * removed on dispose (best-effort; a leftover empty dir is benign).
 */
export function createIsolatedBuildSession(sessionName: string): {
   session: DuckDBConnection;
   dispose: () => Promise<void>;
} {
   const workDir = mkdtempSync(path.join(os.tmpdir(), "malloy-build-"));
   // The disposer that owns removing workDir does not exist until this function
   // returns, so a throw from the constructor would strand the directory with
   // nothing left holding a reference to it.
   let session: DuckDBConnection;
   try {
      session = new DuckDBConnection(sessionName, ":memory:", workDir);
   } catch (err) {
      rmSync(workDir, { recursive: true, force: true });
      throw err;
   }
   const dispose = async () => {
      // `close()` alone does not end the session. It refcount-decrements the
      // DuckDBInstance, but the node-api Connection holds a C++ ClientContext that
      // keeps the refcount above zero — so the in-memory database survives, and with
      // it the destination's ATTACH and the source's federated credentials. Measured:
      // 2 idle Postgres backends stranded per build, freed only at process exit,
      // where the whole point of a per-build session is that they exist only for
      // the build.
      //
      // db-duckdb cannot do this itself: 0.0.389 disconnected unconditionally and
      // had to be reverted (PR #2793) because malloy's translate layer holds
      // weak_ptrs to the C++ Connection and the language server segfaulted. A build
      // session is different in the way that matters — server-owned, single-use, and
      // nothing outside this function ever sees it — so it can release what a
      // general-purpose connection cannot. Reached through a cast because the handle
      // is library-internal; if db-duckdb ever grows an explicit hard-close, use it.
      const nodeConnection = (
         session as unknown as { connection?: { disconnectSync?: () => void } }
      ).connection;
      try {
         await session.close();
      } catch (err) {
         logger.warn("Failed to close build session (leaked session)", {
            sessionName,
            error: err instanceof Error ? err.message : String(err),
         });
      }
      try {
         nodeConnection?.disconnectSync?.();
      } catch (err) {
         logger.warn("Failed to disconnect build session (leaked connection)", {
            sessionName,
            error: err instanceof Error ? err.message : String(err),
         });
      }
      try {
         rmSync(workDir, { recursive: true, force: true });
      } catch (err) {
         logger.warn("Failed to remove build session working directory", {
            sessionName,
            workDir,
            error: err instanceof Error ? err.message : String(err),
         });
      }
   };
   return { session, dispose };
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
 * Steps: create a private build session → attach the destination read-write
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
   environmentPath: string;
}): Promise<StorageBuildResult> {
   const {
      destinationName,
      destinationConnection,
      sourceConnection,
      buildSQL,
      physicalTableName,
      environmentPath,
   } = params;

   assertSupportedDestination(destinationName, destinationConnection);
   const sourceType = passthroughSourceType(sourceConnection);

   // A dedicated, disposable build session on its OWN in-memory instance, so its
   // read-write destination attach and federated source credentials cannot be
   // pooled onto — or collide with — any other build/serve connection (see
   // createIsolatedBuildSession).
   const { session, dispose } = createIsolatedBuildSession(
      `build_${destinationName}`,
   );
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
      // (private, throwaway) in-memory catalog, not the DuckLake/DuckDB store,
      // so the rows would be captured for the schema and then vanish with the
      // session. DuckLake's
      // catalog swap is transactional, so CREATE OR REPLACE is atomic — no
      // staging/rename dance needed on this path.
      // quoteManifestTablePath, NOT quoteTablePath: the serve side renders this same
      // path with the manifest rule, which passes an already-quoted name through
      // unchanged. Quoting unconditionally here would create a table whose NAME
      // contains the author's quote characters while the serve side asks for the
      // unquoted-inner form — and because that mismatch surfaces at shape RUN time,
      // past the compile-time live fallback, every query on the source 400s. For an
      // unquoted name the two functions are identical, so nothing else moves.
      const target = quoteManifestTablePath(
         `${destinationName}.${physicalTableName}`,
         "duckdb",
      );
      const passthrough = wrapPassthrough(
         sourceType,
         federated.handle,
         buildSQL,
      );

      // Capture the authoritative schema from the freshly-built table — the
      // serve transform declares exactly this, and the compiler does not
      // type-check a virtual source's declared columns.
      const schema = await createTableAndDescribe(session, target, passthrough);

      return { storageConnectionName: destinationName, schema };
   } finally {
      // Dispose closes the private instance (releasing every secret + attach —
      // nothing federated or read-write survives the build) and removes its
      // throwaway working directory.
      await dispose();
   }
}

/**
 * Build a CHAINED `storage=` source by reading its already-materialized
 * upstream(s) from the destination store — the "stack on the parent" path —
 * instead of recomputing the upstream from raw against the source
 * warehouse. Reuses the parent's work and is consistent-by-construction
 * (the downstream is a pure function of the parent's STORED rows).
 *
 * The `transientModel` (assembled by the caller from the serve-shape rebind
 * machinery, see {@link buildChainedStorageBuildModel}) rebinds every upstream to
 * a virtual source on THIS destination and re-declares the downstream as a
 * persist source over them. We compile it against the build session (the only
 * connection it references is the attached destination), read the downstream
 * persist source's `getSQL({ virtualMap })` — the exact mirror of the warehouse
 * build's `getSQL`, only the SQL now reads the attached lake tables — and CTAS
 * the result into the destination. No source federation, no passthrough: nothing
 * leaves DuckDB. The `virtualMap` maps each upstream's handle to its (quoted)
 * physical lake path.
 *
 * Session lifecycle mirrors {@link buildSourceIntoStorage} exactly (create →
 * attach read-write → build → dispose), so no read-write attach survives the
 * build, and the same authoritative-schema capture applies.
 *
 * @returns the destination connection name and the captured authoritative schema.
 */
export async function buildDownstreamIntoStorage(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   /** Transient rebind model: upstream virtuals + persist-annotated downstream. */
   transientModel: string;
   /** The downstream source's Malloy name, to locate it in the transient plan. */
   downstreamName: string;
   /** connectionName → handle → quoted physical path for the upstream virtuals. */
   virtualMap: Map<string, Map<string, string>>;
   /** Logical, unquoted physical table path for the downstream's own table. */
   physicalTableName: string;
   environmentPath: string;
}): Promise<StorageBuildResult> {
   const {
      destinationName,
      destinationConnection,
      transientModel,
      downstreamName,
      virtualMap,
      physicalTableName,
      environmentPath,
   } = params;

   assertSupportedDestination(destinationName, destinationConnection);

   const { session, dispose } = createIsolatedBuildSession(
      `build_${destinationName}`,
   );
   try {
      await attachDestinationReadWrite(
         session,
         destinationName,
         destinationConnection,
         environmentPath,
      );
      if (destinationConnection.type === "ducklake") {
         // Same rationale as buildSourceIntoStorage: keep materialized rows out
         // of the shared catalog database.
         await session.runSQL("SET ducklake_default_data_inlining_row_limit=0");
      }

      // Compile the transient rebind model against the build session. Its only
      // connection is the attached destination; the upstream virtual sources
      // declare their captured schema, so compiling reads no tables (schema-on-
      // faith) — the CTAS below runs the real SQL against the attached lake.
      const root = "file:///chained-build/";
      const url = `${root}m.malloy`;
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(new Map([[url, transientModel]])),
         connections: new FixedConnectionMap(
            new Map([[destinationName, session]]),
            destinationName,
         ),
      });
      // Compiling the transient model is the only SHAPE step in this function:
      // a failure means the downstream cannot be expressed over its rebound
      // parents, which is a legitimate reason to fall back and recompute from
      // raw. Everything around it — the attach, the CTAS, the DESCRIBE — is
      // infrastructure, where falling back would just fail the same way against
      // the same destination. Marking the shape failures lets the caller tell
      // them apart.
      let model;
      try {
         model = await runtime
            .loadModel(new URL(url), { importBaseURL: new URL(root) })
            .getModel();
      } catch (err) {
         throw new MaterializationEligibilityError({
            message: `Chained build model did not compile over the rebound parents: ${errMessage(err)}`,
         });
      }
      const plan = model.getBuildPlan();
      let downstream: PersistSource | undefined;
      for (const ps of Object.values(plan.sources)) {
         if (ps.name === downstreamName) {
            downstream = ps;
            break;
         }
      }
      if (!downstream) {
         // The downstream didn't survive as a persist source — its definition
         // references something the rebind model doesn't provide (a parent
         // refinement not carried, a live leaf). The caller falls back.
         throw new MaterializationEligibilityError({
            message:
               `Chained build model did not yield a persist source named ` +
               `'${downstreamName}' (the downstream references something the ` +
               `rebound parents don't provide).`,
         });
      }
      // The downstream's materialization SQL, over the rebound parents — DuckDB
      // dialect, reading the attached lake tables via the virtualMap. Project to
      // the public columns so a hidden (`except:` / access-restricted) downstream
      // column is not materialized — same rationale as the single-source build:
      // keeping hidden values out of the store at rest.
      const sql = projectToPublicColumns(
         downstream,
         downstream.getSQL({ virtualMap }),
      );

      // Same write/read mirror as the single-source build above.
      const target = quoteManifestTablePath(
         `${destinationName}.${physicalTableName}`,
         "duckdb",
      );
      const schema = await createTableAndDescribe(session, target, sql);

      return { storageConnectionName: destinationName, schema };
   } finally {
      await dispose();
   }
}

/**
 * Build-time servability gate (the portable-DuckDB eligibility check, deferred
 * from the pre-build pass because it needs the POST-build schema): compile the
 * source's serve-shape in DuckDB against the captured schema and REFUSE the
 * build if it does not compile. The served table lives in DuckDB, so a source
 * authored against a warehouse must have a DuckDB-portable served shape — this
 * turns a serve-time execution error into a build-time refusal (HTTP 422),
 * running as part of stage→validate, the same step that captured the schema.
 *
 * The compile is schema-on-faith (the compiler does not type-check a virtual
 * source's columns and no SQL runs), so a throwaway in-memory DuckDB connection
 * suffices — no attach, no table read, nothing federated.
 *
 * @throws {MaterializationEligibilityError} (HTTP 422) if the shape can't compile.
 */
export async function assertStorageServeShapeCompiles(params: {
   destinationName: string;
   sourceName: string;
   virtualHandle: string;
   physicalTableName: string;
   schema: WireColumn[];
}): Promise<void> {
   const { destinationName, sourceName, virtualHandle, physicalTableName } =
      params;
   const binding: ServeBinding = {
      sourceName,
      connectionName: destinationName,
      virtualHandle,
      tablePath: `${destinationName}.${physicalTableName}`,
      schema: params.schema
         .filter((c) => c.name && c.type)
         .map((c) => ({ name: c.name as string, type: c.type as string })),
   };
   // ONE session for the process, not one per build. The build and GC sessions
   // need their own instance because they ATTACH a destination read-write and
   // federate customer credentials; this gate does neither — it compiles a
   // throwaway serve shape against a captured schema — so it has no cross-tenant
   // collision surface to isolate. A fresh instance per build cost ~5.9MB of RSS
   // per build on the production image, roughly three quarters of the build
   // path's total growth, and it is never reclaimed.
   //
   // The risk of sharing is a shared CATALOG: every compile declares a virtual
   // source into it, so a later shape could in principle pass on declarations
   // left by an earlier one. Pinned in the spec — refusals still refuse after 25
   // successful compiles, a refusal does not poison the session, and the same
   // handle recompiled with a different schema sees the new one.
   sharedGateSession ??= createIsolatedBuildSession("gate_shared").session;
   await assertServesInDuckDB(
      sourceName,
      binding,
      new FixedConnectionMap(
         new Map([[destinationName, sharedGateSession]]),
         destinationName,
      ),
   );
}

/** DDL to drop a storage table by its recorded name, catalog-qualified for DuckDB. */
export function dropStorageTableSql(
   destinationName: string,
   physicalTableName: string,
): string {
   // Must name the table the way the build CREATEd it (see the CTAS sites).
   return `DROP TABLE IF EXISTS ${quoteManifestTablePath(
      `${destinationName}.${physicalTableName}`,
      "duckdb",
   )}`;
}

/**
 * Drop one materialized table from a `storage=` destination, on a build-scoped
 * read-write session with its OWN in-memory instance (the SERVE attach is
 * read-only, so GC cannot run there; and an isolated instance keeps this GC's
 * read-write destination attach from colliding with another tenant's same-named
 * destination — see {@link createIsolatedBuildSession}). Mirrors
 * {@link buildSourceIntoStorage}'s lifecycle: attach read-write → drop → dispose.
 *
 * Only the recorded physical table (`physicalTableName`) is dropped — a
 * destination-aware drop of a name the publisher recorded building, never a
 * catalog scan — so GC can never take out a table it did not create.
 */
export async function dropStorageTable(params: {
   destinationName: string;
   destinationConnection: ApiConnection;
   physicalTableName: string;
   environmentPath: string;
}): Promise<void> {
   const {
      destinationName,
      destinationConnection,
      physicalTableName,
      environmentPath,
   } = params;
   // Fail fast (pre-session, pre-attach) on a destination the build can't target.
   assertSupportedDestination(destinationName, destinationConnection);

   const { session, dispose } = createIsolatedBuildSession(
      `gc_${destinationName}`,
   );
   try {
      await attachDestinationReadWrite(
         session,
         destinationName,
         destinationConnection,
         environmentPath,
      );
      await session.runSQL(
         dropStorageTableSql(destinationName, physicalTableName),
      );
   } finally {
      await dispose();
   }
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
/**
 * CTAS the table, then read back its authoritative schema — dropping the table
 * again if that read-back fails.
 *
 * The window this closes: the CTAS has committed by the time DESCRIBE runs, and
 * the caller records nothing until this function RETURNS. So a DESCRIBE failure
 * propagates past a committed table that no manifest entry names and that
 * `builtThisRun` never saw — leaving it unreachable by the failed-run reclaim
 * and by manifest-driven GC alike, which only drop names they recorded building.
 * Same class the post-build serve-shape gate already closes with its own
 * targeted drop (see materialization_service), and closed the same way.
 *
 * Best-effort: a failed drop is logged, never raised, and never replaces the
 * DESCRIBE error that is the actual failure. The drop runs on the session that
 * created the table, whose read-write attach is still open.
 */
export async function createTableAndDescribe(
   session: DuckDBConnection,
   quotedTablePath: string,
   selectSQL: string,
): Promise<WireColumn[]> {
   await session.runSQL(
      `CREATE OR REPLACE TABLE ${quotedTablePath} AS (${selectSQL})`,
   );
   try {
      return await describeTable(session, quotedTablePath);
   } catch (describeErr) {
      try {
         await session.runSQL(`DROP TABLE IF EXISTS ${quotedTablePath}`);
      } catch (dropErr) {
         logger.warn(
            "Failed to drop a storage table stranded by a schema read-back " +
               "failure (physical leak)",
            {
               table: quotedTablePath,
               error: errMessage(dropErr),
            },
         );
      }
      throw describeErr;
   }
}

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
