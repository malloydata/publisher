// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Column contracts for specs.
 *
 * - {@link fakeContractReads}: a spec whose builds run over a fake
 *   connection or session that answers no catalog query installs this, so
 *   each build records an empty contract instead of failing its capture
 *   (`contract_capture_failed`). A spec about the capture itself does not.
 */

import { afterEach, beforeEach } from "bun:test";
import { fakeContractReadsForTests } from "../service/materialization/run/column_contract_capture";
import { gateColumnTypeExact } from "../service/materialization/domain/gate_policy";

/** Fake every build's contract reads for each test of the calling file. */
export function fakeContractReads(): void {
   let restore: (() => void) | undefined;
   beforeEach(() => {
      restore = fakeContractReadsForTests();
   });
   afterEach(() => {
      restore?.();
      restore = undefined;
   });
}

/** One column a fixture's contract records. */
interface FixtureColumn {
   name?: string;
   type?: string;
}

/**
 * A column contract as a build records it, for a fixture's entry: each of
 * `columns` with its native type, exact and with no collation (so a bind
 * compares its types and nothing else). None listed records one stand-in
 * column, `id INTEGER`: a contract that lists no column is never served
 * from (`contract_missing`), so a fixture whose table is read back lists
 * that table's own columns.
 */
export function contractFor(
   columns: readonly FixtureColumn[] = [{ name: "id", type: "INTEGER" }],
): Record<string, unknown> {
   return {
      v: 1,
      dialect: "",
      columns: columns.flatMap((c) =>
         c.name && c.type
            ? [
                 {
                    name: c.name,
                    nativeType: c.type,
                    collation: null,
                    compare: "exact",
                    gateExact: true,
                 },
              ]
            : [],
      ),
   };
}

/**
 * The contract a `storage=` build records over `columns`: as
 * {@link contractFor}, but each column's gate exact only by its stored type
 * (a float is not), as the build judges a column that copies no warehouse
 * column.
 */
export function storageContractFor(
   columns: readonly FixtureColumn[],
): Record<string, unknown> {
   const contract = contractFor(columns) as {
      columns: { nativeType: string; gateExact: boolean }[];
   };
   for (const c of contract.columns) {
      c.gateExact = gateColumnTypeExact(c.nativeType, undefined);
   }
   return contract;
}

/**
 * The contract of a DuckDB table a spec created, as a build records it: its
 * columns as `DESCRIBE` reads them, for a fixture whose bind reads the table
 * back. `storage` judges each gate by its stored type
 * ({@link storageContractFor}).
 */
export async function describedContract(
   connection: { runSQL(sql: string): Promise<{ rows: unknown[] }> },
   table: string,
   tier: "database" | "storage" = "database",
): Promise<Record<string, unknown>> {
   const described = await connection.runSQL(`DESCRIBE ${table}`);
   const columns = (
      described.rows as { column_name: string; column_type: string }[]
   ).map((r) => ({ name: r.column_name, type: r.column_type }));
   return tier === "storage"
      ? storageContractFor(columns)
      : contractFor(columns);
}

/**
 * `entry` as a build that records contracts writes it: with a contract over
 * its `nativeSchema` (a database entry's warehouse types) when it has one,
 * else {@link contractFor}'s stand-in. An entry that already carries one keeps it; a fixture
 * whose point is a table with no contract builds it by hand.
 */
export function withContract<T extends object>(
   entry: T,
): T & { contract: unknown } {
   const own = entry as { contract?: unknown; nativeSchema?: FixtureColumn[] };
   return own.contract !== undefined
      ? (entry as T & { contract: unknown })
      : {
           ...entry,
           contract: contractFor(
              own.nativeSchema?.length ? own.nativeSchema : undefined,
           ),
        };
}

/** Each entry of `entries` {@link withContract}. */
export function withContracts<T extends object>(
   entries: Record<string, T>,
): Record<string, T & { contract: unknown }> {
   return Object.fromEntries(
      Object.entries(entries).map(([id, e]) => [id, withContract(e)]),
   );
}
