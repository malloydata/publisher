// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { PersistSource } from "@malloydata/malloy";

import { projectToPublicColumns } from "./build_plan";
import {
   canonicalBoundValue,
   deltaSelect,
   type DeltaTarget,
   type IncrementalLineage,
} from "./incremental_apply";
import {
   issuePassthroughRead,
   STORAGE_TARGET_DIALECT,
   wrapPassthrough,
} from "./materialization_build_session";
import type { BuildReadCost } from "./build_read_cost";
import type { FederatedSourceType } from "./connection";
import type { QueryMetadata } from "./query_metadata";
import { quoteIdentifier } from "./quoting";
import { errMessage } from "../utils";

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
}

export function storageDeltaTarget(params: {
   /** The build-scoped session: destination attached read-write, source federated. */
   session: DuckDBConnection;
   sourceType: FederatedSourceType;
   /** The per-engine passthrough handle `federateSourceForPassthrough` established. */
   handle: string;
   destinationName: string;
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
    * The manifest-resolved build SQL (`PersistSource.getSQL()`), deliberately NOT
    * its public-column projection: that is applied outermost here so the delta's
    * columns are byte-identical to the seed's.
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
   const logicalTablePath = `${destinationName}.${params.physicalTableName}`;
   let readCost: BuildReadCost | null = null;

   return {
      readCost: () => readCost,
      target: {
         dialect: STORAGE_TARGET_DIALECT,
         runner: (sql) => session.runSQL(sql),
         quotedTablePath,
         logicalTablePath,

         deltaRows: async (start, end) => {
            // Filter INSIDE, project OUTSIDE — the seed's nesting order, so both
            // write the same columns. The watermark is always public (the
            // publish gate checks it), so the projection cannot hide it.
            const bounded = projectToPublicColumns(
               persistSource,
               deltaSelect({
                  dialect: sourceDialect,
                  sourceSQL: buildSQL,
                  watermarkName: lineage.watermarkName,
                  start,
                  end,
               }),
            );
            // The seed's read path, so a delta is labelled, tagged and costed
            // like a full build. On BigQuery the read is billed before the DML's
            // transaction opens, so a failed delta's retry pays again — the same
            // trade as a full build, bounded by the delta.
            const read = await issuePassthroughRead(
               session,
               sourceType,
               handle,
               bounded,
               queryMetadata,
            );
            readCost = read.cost;
            return read.selectSQL;
         },

         probeSourceFrontier: async () => {
            // Source-dialect through the passthrough, so the warehouse aggregates
            // and one row crosses instead of streaming every row.
            const column = quoteIdentifier(
               lineage.watermarkName,
               sourceDialect,
            );
            const alias = quoteIdentifier("watermark_max", sourceDialect);
            const inner =
               `SELECT MAX(${column}) AS ${alias} ` +
               `FROM (${buildSQL}) AS __w`;
            // NOT probeSelect(): its `row_to_json` wrapper is for Malloy's
            // PostgresConnection row rewrite, which this DuckDB session lacks.
            // Wrapping would make every probe read undefined and seed forever.
            try {
               const result = await session.runSQL(
                  wrapPassthrough(sourceType, handle, inner),
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
