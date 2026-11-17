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

import { projectToStoredColumns, storedColumnOf } from "../plan/build_plan";
import {
   canonicalBoundValue,
   deltaSelect,
   type DeltaTarget,
   type IncrementalLineage,
} from "./incremental_apply";
import {
   issuePassthroughRead,
   wrapPassthrough,
} from "./materialization_build_session";
import type { BuildReadCost } from "./build_read_cost";
import { markBigQueryRead } from "./bigquery_job_cancel";
import type { FederatedSourceType } from "../../connection";
import type { QueryMetadata } from "../../query_metadata";
import { quoteIdentifier } from "../../quoting";
import { errMessage } from "../../../utils";
import { storageTablePath } from "../domain/storage_table_path";
import { capabilitiesOf, STORAGE_DIALECT } from "../domain/dialect";
import {
   decimalTypeOf,
   guardDeltaDecimals,
   numericAsTextSQL,
   postgresNumericColumns,
} from "./postgres_numeric_precision";

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
         runner: (sql) => session.runSQL(sql),
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
            // fits; one that does not re-seeds (postgres_numeric_precision.ts).
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
               const result = await session.runSQL(
                  wrapPassthrough(
                     sourceType,
                     handle,
                     sourceType === "bigquery"
                        ? markBigQueryRead(session, inner)
                        : inner,
                  ),
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
