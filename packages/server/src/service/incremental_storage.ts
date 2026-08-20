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
import { quoteIdentifier, quoteManifestTablePath } from "./quoting";
import { errMessage } from "../utils";

/**
 * The delta target for a source materialized into a `storage=` destination — the
 * one place where a refresh spans two engines.
 *
 * A stored table's rows are computed by the SOURCE warehouse and its DML is
 * issued in the DESTINATION engine, so every piece of SQL here belongs to one
 * side or the other and putting a piece on the wrong side fails in a way that
 * still looks plausible:
 *
 *  - The RANGE PREDICATE is source-dialect and is pushed INSIDE the passthrough,
 *    so the warehouse computes only the delta. Filtering outside it instead
 *    streams the whole source across the egress boundary and then discards most
 *    of it — correct rows, and the entire cost case for incremental refresh on
 *    this tier inverted.
 *  - The DML (`DELETE`/`INSERT`, or `MERGE`) is DuckDB-dialect and names the lake
 *    table, which is where the rows live.
 *  - The TARGET PROBES read DuckDB catalog metadata. `information_schema` does
 *    not exist on an attached DuckLake catalog at all, which is why
 *    `probeTargetColumns` reads `duckdb_columns()` for this dialect.
 *  - The FRONTIER PROBE is source-dialect `MAX()` sent through the passthrough,
 *    so the warehouse aggregates and one row crosses.
 *
 * Everything else — the order of the checks, seed vs skip vs delta, the reason
 * codes — is the shared planner's, which is the point of expressing this as a
 * target rather than as a second refresh path.
 */
export interface StorageDeltaTarget {
   target: DeltaTarget;
   /**
    * What the warehouse charged for the delta's read, once one has been issued.
    * A function rather than a value because the read happens inside the planner,
    * only if every check passes: asked before then, the honest answer is "no read
    * yet", not zero.
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
   lineage: IncrementalLineage;
   persistSource: PersistSource;
   /**
    * The source's manifest-resolved build SQL — `PersistSource.getSQL()`, the
    * same string the seed's CTAS reads, and deliberately NOT its public-column
    * projection: the projection is applied outermost here so the delta's output
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
   } = params;
   const sourceDialect = persistSource.dialectName;
   const logicalTablePath = `${destinationName}.${params.physicalTableName}`;
   let readCost: BuildReadCost | null = null;

   return {
      readCost: () => readCost,
      target: {
         dialect: STORAGE_TARGET_DIALECT,
         runner: (sql) => session.runSQL(sql),
         // Must name the table the way the build CREATEd it, which is why this
         // goes through the manifest quoting rule rather than quoteTablePath: an
         // author's `name=` may already be canonical SQL, and re-quoting it would
         // aim the DML at a table whose name contains the quote characters.
         quotedTablePath: quoteManifestTablePath(
            logicalTablePath,
            STORAGE_TARGET_DIALECT,
         ),
         logicalTablePath,

         deltaRows: async (start, end) => {
            // Filter INSIDE, project OUTSIDE — the same nesting order the seed
            // uses, so the two write the same column list by construction. The
            // watermark is always a public column (the publish gate resolves it
            // against the source's public output schema), so the projection
            // cannot hide the column the predicate names.
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
            // The same call the seed's read goes through, so a delta is labelled,
            // tagged and costed exactly as a full build is. Attribution is not
            // optional here: this read is warehouse work a deployment would
            // otherwise be unable to account for.
            //
            // On BigQuery's labelled shape the read is issued — and billed — as
            // this returns, before the DML's transaction opens. A delta that then
            // fails has paid for rows it discarded; the retry pays again. That is
            // the same trade the full build already documents, one notch cheaper
            // because a delta's read is bounded.
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
            // Composed in the SOURCE dialect and sent through the passthrough, so
            // the warehouse computes the aggregate and one row crosses. Reading
            // MAX() on this side instead would stream every row to compute it.
            const column = quoteIdentifier(
               lineage.watermarkName,
               sourceDialect,
            );
            const alias = quoteIdentifier("watermark_max", sourceDialect);
            const inner =
               `SELECT MAX(${column}) AS ${alias} ` +
               `FROM (${buildSQL}) AS __w`;
            // NOT probeSelect(): its `row_to_json` wrapper exists because Malloy's
            // PostgresConnection rewrites `rows[i] = rows[i].row`, and this runner
            // is a DuckDB session with no such rewrite. Wrapping would hand back
            // one JSON column and every probe would read undefined — a source that
            // seeds forever while reporting a readable table.
            try {
               const result = await session.runSQL(
                  wrapPassthrough(sourceType, handle, inner),
               );
               // `result.rows`, matching the probes that run on this same
               // session: db-duckdb types runSQL as MalloyQueryData, so an array
               // branch here would only suggest the shape were in doubt.
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
