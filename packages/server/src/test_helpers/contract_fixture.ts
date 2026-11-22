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
 * compares its types and nothing else); none listed records an empty
 * contract, which matches any table.
 */
export function contractFor(
   columns: readonly FixtureColumn[] = [],
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
 * `entry` as a build that records contracts writes it: with a contract over
 * its `nativeSchema` (a database entry's warehouse types) when it has one,
 * else an empty one. An entry that already carries one keeps it; a fixture
 * whose point is a table with no contract builds it by hand.
 */
export function withContract<T extends object>(
   entry: T,
): T & { contract: unknown } {
   const own = entry as { contract?: unknown; nativeSchema?: FixtureColumn[] };
   return own.contract !== undefined
      ? (entry as T & { contract: unknown })
      : { ...entry, contract: contractFor(own.nativeSchema ?? []) };
}

/** Each entry of `entries` {@link withContract}. */
export function withContracts<T extends object>(
   entries: Record<string, T>,
): Record<string, T & { contract: unknown }> {
   return Object.fromEntries(
      Object.entries(entries).map(([id, e]) => [id, withContract(e)]),
   );
}
