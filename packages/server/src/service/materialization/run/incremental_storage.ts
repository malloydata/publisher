// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The delta target of a `storage=` table ({@link storageDeltaTarget}): the
 * probes, range read and DML an incremental refresh runs when the source's
 * warehouse computes the rows and a DuckDB or DuckLake destination holds the
 * table. Read by incremental_build.ts's storage refresh.
 *
 * Runs SQL on two engines through one build session: source-dialect reads
 * inside the passthrough, DuckDB DML and catalog probes on the destination.
 * Reports what the warehouse charged for the delta's read.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { PersistSource } from "@malloydata/malloy";

import { projectToStoredColumns, storedColumnOf } from "./run_plan";
import {
   canonicalBoundValue,
   deltaSelect,
   probeMaxWatermark,
   type DeltaTarget,
   type IncrementalLineage,
   type SqlRunner,
} from "./incremental_apply";
import {
   issuePassthroughRead,
   wrapPassthrough,
} from "./materialization_build_session";
import type { BuildReadCost } from "./build_read_cost";
import { passthroughBuildOf } from "./passthrough_build";
import type { FederatedSourceType } from "../../dialects";
import type { QueryMetadata } from "../../query_metadata";
import { quoteIdentifier } from "../../quoting";
import { errMessage } from "../../../utils";
import { errorCodeOf } from "../../../error_code";
import { storageTablePath } from "../domain/storage_table_path";
import { storedTypesAsBuilt } from "../stored_column_types";
import { capabilitiesOf, STORAGE_DIALECT } from "../domain/dialect";
import {
   decimalTypeOf,
   guardDeltaDecimals,
   numericAsTextSQL,
   postgresNumericColumns,
} from "./passthrough/postgres";

/** What the passthrough's describe and type probe found ({@link postgresNumericColumns}). */
type NumericRead = Awaited<ReturnType<typeof postgresNumericColumns>>;

/** The table's column types by name, from the destination's own catalog. */
async function storedTypesOf(
   session: DuckDBConnection,
   quotedTablePath: string,
): Promise<Map<string, string>> {
   const described = await session.runSQL(`DESCRIBE ${quotedTablePath}`);
   const rows = (
      Array.isArray(described) ? described : (described.rows ?? [])
   ) as Record<string, unknown>[];
   return new Map(
      rows.map((r) => [String(r.column_name), String(r.column_type)]),
   );
}

/**
 * The Postgres numeric columns of a delta that the table stores exactly: as a
 * DECIMAL, or as the text a seed kept when no DECIMAL held it. A column an
 * older build stored as DOUBLE re-seeds the table before any delta
 * ({@link StorageDeltaTarget.numericColumnsStoredAsDouble}). Undefined when
 * there is none.
 */
async function exactNumericColumns(
   session: DuckDBConnection,
   numericRead: () => Promise<NumericRead>,
   quotedTablePath: string,
): Promise<
   | { columns: string[]; stored: { column: string; storedType: string }[] }
   | undefined
> {
   const { columns, numeric } = await numericRead();
   if (numeric.length === 0) return undefined;
   const typeOf = await storedTypesOf(session, quotedTablePath);
   const stored = numeric.flatMap((column) => {
      const storedType = typeOf.get(column);
      return storedType &&
         (decimalTypeOf(storedType) || storedType.toUpperCase() === "VARCHAR")
         ? [{ column, storedType }]
         : [];
   });
   return stored.length > 0 ? { columns, stored } : undefined;
}

/**
 * The delta target for a source materialized into a `storage=` destination — the
 * one place where a refresh spans two engines. Rows are computed by the SOURCE
 * warehouse and DML runs in the DESTINATION engine; SQL on the wrong side still
 * looks plausible, so:
 *
 *  - The RANGE PREDICATE is source-dialect and pushed INSIDE the passthrough.
 *    Filtering outside streams the whole source across egress, inverting the
 *    cost case for incremental refresh.
 *  - The DML (`DELETE`/`INSERT`, or `MERGE`) is DuckDB-dialect on the lake table.
 *  - The TARGET PROBES read DuckDB catalog metadata (`duckdb_columns()`, since an
 *    attached DuckLake catalog has no `information_schema`).
 *  - The FRONTIER PROBE is source-dialect `MAX()` through the passthrough.
 *
 * Check order, seed/skip/delta and reason codes stay with the shared planner.
 */
interface StorageDeltaTarget {
   target: DeltaTarget;
   /**
    * What the warehouse charged for the delta's read; null until the planner has
    * issued one (it reads only if every check passes).
    */
   readCost(): BuildReadCost | null;
   /**
    * The Postgres numeric columns the table stores as DOUBLE: a table built
    * before a seed kept them exact, which serves binary-float sums
    * (`0.30000000000000004`) until it is seeded again, since a delta only
    * INSERTs into the column it has. A seed now stores each as DECIMAL (or
    * as text when no DECIMAL holds it), so none is left after one. Empty off
    * Postgres, or when the passthrough's types cannot be read. Reads the
    * table's own catalog first, and probes Postgres (a describe and a
    * `LIMIT 0`, nothing runs) only for a table holding a DOUBLE column, with
    * the one probe the delta then reuses.
    */
   numericColumnsStoredAsDouble(): Promise<string[]>;
}

/**
 * A DuckDB session as a {@link SqlRunner} that honours the run options' abort
 * signal: an abort interrupts the statement in flight, so a statement given
 * up on (the late-row check's timeout) frees the session for the build's
 * next one instead of holding it. Unlike `interruptOnAbort`, later
 * statements on the session are not refused: the abort ends one statement,
 * not the build.
 */
export function sessionRunner(session: DuckDBConnection): SqlRunner {
   return async (sql, options) => {
      const signal = options?.abortSignal;
      if (!signal) return session.runSQL(sql);
      const interrupt = () => {
         // The node-api connection is library-internal, as in dispose.
         const connection = (
            session as unknown as { connection?: { interrupt?: () => void } }
         ).connection;
         try {
            connection?.interrupt?.();
         } catch {
            // Best-effort: the statement then runs to its end.
         }
      };
      if (signal.aborted) throw new Error("the statement was cancelled");
      signal.addEventListener("abort", interrupt, { once: true });
      try {
         return await session.runSQL(sql);
      } finally {
         signal.removeEventListener("abort", interrupt);
      }
   };
}

/**
 * The late-row check's read of a stored source's window
 * (DeltaTarget.windowRows): only the columns it compares, so the warehouse
 * reads (and bills) those alone, through the seed's read path but not its
 * cost, which is the delta's. None on BigQuery: there the window is read by a
 * billed job issuePassthroughRead starts on the session, outside the check's
 * abort, so a check that timed out would leave the job running and the
 * session's next statement waiting behind it. A stored BigQuery source is not
 * checked.
 */
function storageWindowRows(params: {
   session: DuckDBConnection;
   sourceType: FederatedSourceType;
   handle: string;
   persistSource: PersistSource;
   buildSQL: string;
   watermarkColumn: string;
   queryMetadata?: QueryMetadata;
}): Pick<DeltaTarget, "windowRows"> {
   if (passthroughBuildOf(params.sourceType)?.windowRows === false) return {};
   const sourceDialect = params.persistSource.dialectName;
   return {
      windowRows: async (start, end, columns) => {
         const projected =
            `SELECT ${columns.map((c) => quoteIdentifier(c, sourceDialect)).join(", ")} ` +
            `FROM (${projectToStoredColumns(
               params.persistSource,
               deltaSelect({
                  dialect: sourceDialect,
                  sourceSQL: params.buildSQL,
                  watermarkName: params.watermarkColumn,
                  start,
                  end,
               }),
            )}) AS __c`;
         const read = await issuePassthroughRead(
            params.session,
            params.sourceType,
            params.handle,
            projected,
            params.queryMetadata,
         );
         return read.selectSQL;
      },
   };
}

/**
 * A delta's column types as the destination session reads them (`DESCRIBE`
 * of its SELECT), for a `storage=` table whose source is Postgres, whose
 * passthrough's DESCRIBE prepares the query on the server, reading no row,
 * and for an extension stacked on its parent's table in the destination.
 * Another engine's may run it, so its delta's types are not read, and not
 * judged (DeltaTarget.deltaColumnTypes).
 */
function describedTypes(
   session: DuckDBConnection,
): NonNullable<DeltaTarget["deltaColumnTypes"]> {
   return async (deltaSQL) => {
      try {
         const rows = (await session.runSQL(`DESCRIBE (${deltaSQL})`))
            .rows as Record<string, unknown>[];
         return {
            types: new Map(
               rows.map((r) => [String(r.column_name), String(r.column_type)]),
            ),
         };
      } catch (err) {
         return { error: errMessage(err), errorCode: errorCodeOf(err) };
      }
   };
}

export function storageDeltaTarget(params: {
   /** The build-scoped session: destination attached read-write, source federated. */
   session: DuckDBConnection;
   sourceType: FederatedSourceType;
   /** The per-engine passthrough handle `federateSourceForPassthrough` established. */
   handle: string;
   destinationName: string;
   /** The destination's schema (see storageTablePath); absent is `main`. */
   destinationSchema?: string;
   /** Logical, unquoted physical table name, WITHOUT the destination catalog. */
   physicalTableName: string;
   /**
    * The table's quoted path as the CREATE names it, passed in rather than
    * re-derived so the delta's DML and the seed's CTAS name the same table.
    */
   quotedTablePath: string;
   lineage: IncrementalLineage;
   persistSource: PersistSource;
   /**
    * The manifest-resolved build SQL (the target's `buildSQL()`), deliberately
    * NOT its public-column projection: that is applied outermost here so the
    * delta's columns are byte-identical to the seed's.
    */
   buildSQL: string;
   queryMetadata?: QueryMetadata;
}): StorageDeltaTarget {
   const {
      session,
      sourceType,
      handle,
      destinationName,
      lineage,
      persistSource,
      buildSQL,
      queryMetadata,
      quotedTablePath,
   } = params;
   const sourceDialect = persistSource.dialectName;
   const logicalTablePath = storageTablePath(
      destinationName,
      params.physicalTableName,
      params.destinationSchema,
   );
   let readCost: BuildReadCost | null = null;
   // One Postgres probe per refresh, shared by the DOUBLE check and the
   // delta: the seed's projection names and types the columns a delta reads.
   let numericProbe: Promise<NumericRead> | undefined;
   const numericRead = () =>
      (numericProbe ??= postgresNumericColumns(
         session,
         handle,
         projectToStoredColumns(persistSource, buildSQL),
         sourceType,
      ));

   // The build SQL and the stored table name a renamed field's column by its
   // own name, so the watermark (and, in the DML, the merge keys) are named
   // that way too.
   const storedColumn = storedColumnOf(persistSource);
   const watermarkColumn = storedColumn(lineage.watermarkName);
   return {
      readCost: () => readCost,
      numericColumnsStoredAsDouble: async () => {
         if (!capabilitiesOf(sourceType).passthroughNumericAsDouble) return [];
         // The destination's own catalog first: Postgres is probed only for a
         // table that holds a DOUBLE column at all.
         let typeOf: Map<string, string>;
         try {
            typeOf = await storedTypesOf(session, quotedTablePath);
         } catch {
            // No table to describe: the planner seeds one anyway.
            return [];
         }
         const doubles = new Set(
            [...typeOf]
               .filter(([, type]) => type.toUpperCase() === "DOUBLE")
               .map(([column]) => column),
         );
         if (doubles.size === 0) return [];
         const { numeric } = await numericRead();
         return numeric.filter((column) => doubles.has(column));
      },
      target: {
         dialect: STORAGE_DIALECT,
         storedColumn,
         runner: sessionRunner(session),
         quotedTablePath,
         logicalTablePath,

         deltaRows: async (start, end) => {
            // Filter INSIDE, project OUTSIDE — the seed's nesting order, so both
            // write the same columns. The watermark is always public (the
            // publish gate checks it), so the projection cannot drop it.
            const bounded = projectToStoredColumns(
               persistSource,
               deltaSelect({
                  dialect: sourceDialect,
                  sourceSQL: buildSQL,
                  watermarkName: watermarkColumn,
                  start,
                  end,
               }),
            );
            // The seed's read path, so a delta is labelled, tagged and costed
            // like a full build. On BigQuery the read is billed before the DML's
            // transaction opens, so a failed delta's retry pays again — the same
            // trade as a full build, bounded by the delta.
            // A numeric the seed stored as DECIMAL (or kept as text) is read as
            // Postgres's exact text too, and cast into its column only when it
            // fits; one that does not re-seeds (passthrough/postgres.ts).
            const exact = capabilitiesOf(sourceType).passthroughNumericAsDouble
               ? await exactNumericColumns(
                    session,
                    numericRead,
                    quotedTablePath,
                 )
               : undefined;
            const read = await issuePassthroughRead(
               session,
               sourceType,
               handle,
               exact
                  ? numericAsTextSQL(
                       bounded,
                       exact.columns,
                       exact.stored.map((c) => c.column),
                       sourceType,
                    )
                  : bounded,
               queryMetadata,
            );
            readCost = read.cost;
            return exact
               ? guardDeltaDecimals(read.selectSQL, exact.stored)
               : read.selectSQL;
         },

         ...(capabilitiesOf(sourceType).passthroughNumericAsDouble
            ? {
                 deltaColumnTypes: describedTypes(session),
                 storedAsBuilt: storedTypesAsBuilt,
              }
            : {}),

         ...storageWindowRows({
            session,
            sourceType,
            handle,
            persistSource,
            buildSQL,
            watermarkColumn,
            queryMetadata,
         }),

         probeSourceFrontier: async () => {
            // Source-dialect through the passthrough, so the warehouse aggregates
            // and one row crosses instead of streaming every row.
            const column = quoteIdentifier(watermarkColumn, sourceDialect);
            const alias = quoteIdentifier("watermark_max", sourceDialect);
            const inner =
               `SELECT MAX(${column}) AS ${alias} ` +
               `FROM (${buildSQL}) AS __w`;
            // NOT probeSelect(): its `row_to_json` wrapper is for Malloy's
            // PostgresConnection row rewrite, which this DuckDB session lacks.
            // Wrapping would make every probe read undefined and seed forever.
            try {
               // Marked where the engine marks its reads (BigQuery's, so a
               // cancel finds the job); unlabelled, as no metadata is passed.
               const prepared = await passthroughBuildOf(
                  sourceType,
               )?.prepareRead(session, handle, inner);
               const result = await session.runSQL(
                  wrapPassthrough(sourceType, handle, prepared?.sql ?? inner),
               );
               const rows = (result?.rows ?? []) as Record<string, unknown>[];
               const raw = rows[0]?.["watermark_max"];
               if (raw === null || raw === undefined) return {};
               return {
                  bound: canonicalBoundValue(lineage.watermarkType, raw),
               };
            } catch (err) {
               return { error: errMessage(err) };
            }
         },
      },
   };
}

/**
 * The delta target of a `storage=` extension table stacked on its parent's
 * table in one destination (extension chains, case 2): the rows come from
 * the extension's build SQL compiled over the parent's stored table
 * (`storedSQL`, already projected to the stored columns, the CTAS's own),
 * run on the same destination session that writes them, so nothing reads
 * the warehouse. Filtered on the stored watermark column, as the seed's
 * projection names it.
 */
export function stackedDeltaTarget(params: {
   session: DuckDBConnection;
   destinationName: string;
   destinationSchema?: string;
   physicalTableName: string;
   quotedTablePath: string;
   lineage: IncrementalLineage;
   persistSource: PersistSource;
   storedSQL: string;
}): DeltaTarget {
   const { session, lineage, storedSQL } = params;
   const storedColumn = storedColumnOf(params.persistSource);
   const watermarkColumn = storedColumn(lineage.watermarkName);
   const runner = sessionRunner(session);
   const range = (
      start: Parameters<DeltaTarget["deltaRows"]>[0],
      end: Parameters<DeltaTarget["deltaRows"]>[1],
   ) =>
      deltaSelect({
         dialect: STORAGE_DIALECT,
         sourceSQL: storedSQL,
         watermarkName: watermarkColumn,
         start,
         end,
      });
   return {
      dialect: STORAGE_DIALECT,
      storedColumn,
      runner,
      quotedTablePath: params.quotedTablePath,
      logicalTablePath: storageTablePath(
         params.destinationName,
         params.physicalTableName,
         params.destinationSchema,
      ),
      deltaRows: async (start, end) => range(start, end),
      windowRows: async (start, end, columns) =>
         `SELECT ${columns
            .map((c) => quoteIdentifier(c, STORAGE_DIALECT))
            .join(", ")} FROM (${range(start, end)}) AS __c`,
      // The parent's table is in the destination, so DESCRIBE reads no
      // row: a parent rebuilt at a wider type is seen before the delta.
      deltaColumnTypes: describedTypes(session),
      // What the stacked build's casts store each computed type as.
      storedAsBuilt: storedTypesAsBuilt,
      probeSourceFrontier: () =>
         probeMaxWatermark(
            runner,
            STORAGE_DIALECT,
            storedSQL,
            watermarkColumn,
            lineage.watermarkType,
         ),
   };
}
