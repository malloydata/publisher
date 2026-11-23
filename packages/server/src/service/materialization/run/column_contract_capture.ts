// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A built table's column contract (`ManifestEntry.contract`,
 * `domain/column_contract.ts`), captured once its build wrote it.
 *
 * The stored side is read by the bind's own reads (`readTableColumns`,
 * `readTableCollations`), over the table the build just wrote, through the
 * engine that holds it: the warehouse connection for a database table, a
 * destination session for a `storage=` one. A table straight out of a
 * build therefore reads, at its next bind, as its contract recorded it.
 *
 * The live side, for a `storage=` table only: how its source's strings
 * compare where live reads them. Read from the build's own warehouse SQL
 * without running it (Postgres: `pg_collation_for` of each column under
 * `WHERE false`; DuckDB: the instance's collation setting and definitions),
 * or, for a table stacked on stored tables, the weakest compare their
 * contracts recorded. Any other dialect reads unknown.
 *
 * A capture whose stored read fails records no contract: the table is then
 * never served from, and the next build rebuilds it (`contract_missing`).
 */

import type { Connection } from "@malloydata/malloy";
import { runAsInternalStatement } from "../../duckdb_lockdown";
import { SERVER_VERSION } from "../../../version";
import type { Collation, TableCollations } from "../domain/collation";
import {
   type ColumnContract,
   deriveContract,
   parseContract,
   weakestStringCompare,
} from "../domain/column_contract";
import type { ManifestEntry } from "../domain/types";
import { capabilitiesOf, STORAGE_DIALECT } from "../domain/dialect";
import type { Tier } from "../domain/vocabulary";
import { recordContractCapture } from "../../../materialization_metrics";
import { isStringNativeType } from "../stored_column_types";
import { readTableCollations, readTableColumns } from "./database_column_read";
import { NATIVE_SCHEMA_ROW_LIMIT } from "./database_native_types";
import { liveCollationReads, readsCollations } from "./warehouse_sql";
import { ContractCaptureFailedError } from "../../../errors";

type ReadConnection = Pick<Connection, "runSQL" | "dialectName">;

/**
 * A spec's stand-in for a build's contract reads, over a fake connection or
 * session that answers no catalog query (`test_helpers/contract_fixture.ts`):
 * each read then answers this table's columns and collations. Never set in
 * production.
 */
let fakeReads: { collations: TableCollations } | undefined;

/** Install {@link fakeReads} for a spec; returns its removal. */
export function fakeContractReadsForTests(): () => void {
   fakeReads = { collations: { byColumn: new Map(), otherwise: null } };
   return () => {
      fakeReads = undefined;
   };
}

/** The collations a fake read answers, when a spec installed one. */
export function fakedCollations(): TableCollations | undefined {
   return fakeReads?.collations;
}

/**
 * The engine holding a table, read as the bind reads it, or what did not
 * read: its columns, or, on an engine whose collations are read, its
 * collations.
 */
async function storedFacts(
   connection: ReadConnection,
   tableName: string,
   /** Read types as the catalog names them (a `storage=` table's `DESCRIBE`). */
   asDescribed = false,
): Promise<
   | { stored: Parameters<typeof deriveContract>[0]["stored"] }
   | { failed: string }
> {
   const one = [{ key: "t", connectionName: "c", tableName }];
   const open = async () => connection;
   // One after the other: one connection runs one statement at a time.
   const columns = await readTableColumns({
      tables: one,
      connection: open,
      ...(asDescribed ? { asDescribed } : {}),
   });
   const collations = await readTableCollations({
      tables: one,
      connection: open,
   });
   const read = columns.get("t");
   if (!read || "unread" in read) {
      return {
         failed: `its columns did not read: ${read?.unread ?? "not_read"}`,
      };
   }
   const dialect = connection.dialectName;
   const tableCollations = collations.get("t");
   if (!tableCollations && readsCollations(dialect)) {
      return { failed: "its column collations did not read" };
   }
   return {
      stored: {
         dialect,
         columns: read.columns.map((c) => ({
            name: c.name,
            nativeType: c.nativeType ?? c.type ?? "",
            isString: isStringNativeType(c.nativeType ?? c.type ?? "", dialect),
            ...(c.storedAsDouble ? { storedAsDouble: true } : {}),
         })),
         ...(tableCollations ? { collations: tableCollations } : {}),
      },
   };
}

/**
 * The contract of a database table a build just wrote, read on `connection`
 * (the warehouse's) over `tableName`: its staging table, before the swap,
 * so a capture that fails leaves the previous table serving. Throws
 * {@link ContractCaptureFailedError} when the table does not read back.
 */
export async function captureDatabaseContract(args: {
   connection: ReadConnection;
   /** The table to read, unquoted: the staging table the swap renames. */
   tableName: string;
   /** The served name, for the failure's message. */
   physicalTableName: string;
   /** The Malloy type the build source declares for each column. */
   declared?: readonly { name: string; type: string }[];
}): Promise<ColumnContract> {
   if (fakeReads) {
      return deriveContract({
         tier: "database",
         stored: { dialect: args.connection.dialectName, columns: [] },
      });
   }
   const read = await storedFacts(args.connection, args.tableName);
   recordContractCapture("database", "stored" in read ? "captured" : "unread");
   if (!("stored" in read)) {
      throw new ContractCaptureFailedError(args.physicalTableName, read.failed);
   }
   return deriveContract({
      tier: "database",
      stored: read.stored,
      producer: SERVER_VERSION,
      ...(args.declared ? { declared: args.declared } : {}),
   });
}

/**
 * The contract of a table read whole on `connection`, the engine that holds
 * it, as a bind reads it: the capture of a build that writes a table outside
 * a build session (the conformance runner's stand-in build). A `storage=`
 * table reads its types as described and takes its strings' live judgement
 * from `live`. Throws {@link ContractCaptureFailedError} when it does not read.
 */
export async function captureTableContract(args: {
   connection: ReadConnection;
   tableName: string;
   tier: Tier;
   live?: (columns: string[]) => Promise<Map<string, Collation>>;
}): Promise<ColumnContract> {
   const read = await storedFacts(
      args.connection,
      args.tableName,
      args.tier === "storage",
   );
   if (!("stored" in read)) {
      throw new ContractCaptureFailedError(args.tableName, read.failed);
   }
   if (args.tier === "database") {
      return deriveContract({ tier: "database", stored: read.stored });
   }
   // As a storage build records it, its session's default collation with it.
   return storageContract({
      table: args.tableName,
      schema: read.stored.columns.map((c) => ({
         name: c.name,
         type: c.nativeType,
      })),
      collations: read.stored.collations,
      ...(args.live ? { live: args.live } : {}),
   });
}

/**
 * The contract of a `storage=` table a build just wrote, from what its
 * build session read of it (its `DESCRIBE` and its definition's
 * collations, the reads a bind makes). Throws
 * {@link ContractCaptureFailedError} when its collations were not read. `stackedOn` names the stored tables it read in place of
 * its source; otherwise `live` judges its source's strings.
 */
export async function storageContract(args: {
   /** The table, for a failure's message. */
   table: string;
   schema: readonly { name?: string; type?: string }[];
   collations: TableCollations | undefined;
   stackedOn?: readonly (ManifestEntry | undefined)[];
   live?: (columns: string[]) => Promise<Map<string, Collation>>;
   /** The Malloy type the build source declares for each column. */
   declared?: readonly { name: string; type: string }[];
}): Promise<ColumnContract> {
   const columns = args.schema.flatMap((c) =>
      c.name && c.type
         ? [
              {
                 name: c.name,
                 nativeType: c.type,
                 isString: isStringNativeType(c.type, STORAGE_DIALECT),
              },
           ]
         : [],
   );
   const collations = args.collations ?? fakeReads?.collations;
   const captured =
      collations !== undefined && (columns.length > 0 || !!fakeReads);
   recordContractCapture("storage", captured ? "captured" : "unread");
   if (!captured) {
      throw new ContractCaptureFailedError(
         args.table,
         columns.length === 0
            ? "its columns did not read"
            : "its column collations did not read",
      );
   }
   const stored = {
      dialect: STORAGE_DIALECT,
      columns,
      collations,
      ...(collations.defaultCollation
         ? { defaultCollation: collations.defaultCollation }
         : {}),
   };
   if (args.stackedOn) {
      return deriveContract({
         tier: "storage",
         stored,
         stackedOn: weakestStringCompare(
            args.stackedOn.map((e) => parseContract(e?.contract)),
         ),
         producer: SERVER_VERSION,
         ...(args.declared ? { declared: args.declared } : {}),
      });
   }
   const live = await args.live?.(
      columns.filter((c) => c.isString).map((c) => c.name),
   );
   return deriveContract({
      tier: "storage",
      stored,
      ...(live ? { live } : {}),
      producer: SERVER_VERSION,
      ...(args.declared ? { declared: args.declared } : {}),
   });
}

/**
 * How each of `columns` compares where live reads `sql` on `connection`
 * (a `storage=` build's warehouse SQL), for {@link storageContract}.
 */
export function liveCollationsOf(
   connection: ReadConnection,
   sql: string,
   /**
    * BigQuery's read (the host port's `bigqueryBuildCompareBinary`): whether
    * every string the SQL reads compares byte-exact, read from the catalog.
    * Absent, BigQuery's strings compare unknown.
    */
   bigqueryBinary?: () => Promise<boolean>,
): (columns: string[]) => Promise<Map<string, Collation>> {
   return async (columns) => {
      if (
         bigqueryBinary &&
         capabilitiesOf(connection.dialectName).stringCollation === "dataset"
      ) {
         // Query-wide, as an answer's check reads BigQuery: every table the
         // build reads holds no collated column, or nothing is known.
         const binary = await bigqueryBinary().catch(() => false);
         const judged: Collation = binary ? "binary" : "unknown";
         return new Map(columns.map((c) => [c, judged]));
      }
      return liveCollations(connection, sql, columns);
   };
}

/**
 * How each of `columns` compares where live reads `sql` (the build's own
 * warehouse SQL), read without running it (`liveCollationReads`). Unknown
 * for a dialect whose collation is not read there, and for any read that
 * fails.
 */
async function liveCollations(
   connection: ReadConnection,
   sql: string,
   columns: readonly string[],
): Promise<Map<string, Collation>> {
   const reads =
      columns.length > 0
         ? liveCollationReads(connection.dialectName, sql, columns)
         : undefined;
   if (!reads) return new Map();
   try {
      const results: Record<string, unknown>[][] = [];
      for (const statement of reads.statements) {
         const result = await runAsInternalStatement(statement, () =>
            connection.runSQL(statement, { rowLimit: NATIVE_SCHEMA_ROW_LIMIT }),
         );
         results.push(result.rows as Record<string, unknown>[]);
      }
      return reads.judge(results);
   } catch {
      return new Map();
   }
}
