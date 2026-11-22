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
import type { Collation } from "../domain/collation";
import {
   type ColumnContract,
   type ContractCompare,
   deriveContract,
   parseContract,
   weakestStringCompare,
} from "../domain/column_contract";
import type { ManifestEntry } from "../domain/types";
import {
   destinationSchemaOf,
   storageTablePath,
} from "../domain/storage_table_path";
import { recordContractCapture } from "../../../materialization_metrics";
import { spelledNativeType } from "../stored_column_types";
import { readTableCollations, readTableColumns } from "./database_column_read";
import { NATIVE_SCHEMA_ROW_LIMIT } from "./database_native_types";
import type { BuildEnvironment } from "./build_upstreams";
import { withDestinationSession } from "./materialization_build_session";
import { liveCollationReads } from "./warehouse_sql";

type ReadConnection = Pick<Connection, "runSQL" | "dialectName">;

/** The engine holding a table, read as the bind reads it. */
async function storedFacts(
   connection: ReadConnection,
   tableName: string,
): Promise<Parameters<typeof deriveContract>[0]["stored"] | undefined> {
   const one = [{ key: "t", connectionName: "c", tableName }];
   const open = async () => connection;
   const [columns, collations] = await Promise.all([
      readTableColumns({ tables: one, connection: open }),
      readTableCollations({ tables: one, connection: open }),
   ]);
   const read = columns.get("t");
   if (!read || "unread" in read) return undefined;
   const dialect = connection.dialectName;
   return {
      dialect,
      columns: read.columns.map((c) => ({
         name: c.name,
         nativeType: c.nativeType ?? c.type ?? "",
         isString:
            (c.type ?? spelledNativeType(c.nativeType ?? "", dialect)) ===
            "string",
         ...(c.storedAsDouble ? { storedAsDouble: true } : {}),
      })),
      ...(collations.get("t") ? { collations: collations.get("t") } : {}),
   };
}

/**
 * The contract of a database table a build just wrote, on `connection`
 * (the warehouse's), or undefined when its columns could not be read.
 */
export async function captureDatabaseContract(args: {
   connection: ReadConnection;
   /** Its path, unquoted, as the build wrote it. */
   physicalTableName: string;
}): Promise<ColumnContract | undefined> {
   const stored = await storedFacts(args.connection, args.physicalTableName);
   recordContractCapture("database", stored ? "captured" : "unread");
   return stored
      ? deriveContract({
           tier: "database",
           stored,
           producer: SERVER_VERSION,
        })
      : undefined;
}

/**
 * The contract of a `storage=` table a build just wrote, read through a
 * destination session, or undefined when its columns could not be read.
 * `stackedOn` names the stored tables it read in place of its source (each
 * one's entry); otherwise its live side is read from `buildSQL` on the
 * source's warehouse `sourceConnection`.
 */
export async function captureStorageContract(args: {
   environment: BuildEnvironment;
   entry: ManifestEntry;
   sourceConnection: ReadConnection;
   buildSQL: string;
   stackedOn?: readonly ManifestEntry[];
}): Promise<ColumnContract | undefined> {
   let stored: Awaited<ReturnType<typeof storedFacts>>;
   try {
      const destinationName = args.entry.storageDestinationName!;
      const destinationConnection =
         args.environment.getStorageDestination(destinationName);
      const tablePath = storageTablePath(
         destinationName,
         args.entry.physicalTableName,
         destinationSchemaOf(destinationConnection),
      );
      stored = await withDestinationSession(
         {
            destinationName,
            destinationConnection,
            environmentPath: args.environment.getEnvironmentPath(),
         },
         { purpose: "contract" },
         (session) =>
            storedFacts(session as unknown as ReadConnection, tablePath),
      );
   } catch {
      // Never fails the build: the table records no contract.
      stored = undefined;
   }
   recordContractCapture("storage", stored ? "captured" : "unread");
   if (!stored) return undefined;
   const stackedOn: ContractCompare | undefined = args.stackedOn
      ? weakestStringCompare(
           args.stackedOn.map((e) => parseContract(e.contract)),
        )
      : undefined;
   const live =
      stackedOn === undefined
         ? await liveCollations(
              args.sourceConnection,
              args.buildSQL,
              stored.columns.filter((c) => c.isString).map((c) => c.name),
           )
         : undefined;
   return deriveContract({
      tier: "storage",
      stored,
      ...(stackedOn !== undefined ? { stackedOn } : {}),
      ...(live ? { live } : {}),
      producer: SERVER_VERSION,
   });
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
