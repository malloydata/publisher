// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A `storage=` table's `gateExact`, as its build records it in its contract:
 * a destination double is gate-exact only as a plain copy of a warehouse
 * binary64 (the plan's column origins, looked up in its upstream readings),
 * and, for a table built over stored tables, only when every one of those
 * that holds the column recorded it gate-exact too.
 */

import { describe, expect, it } from "bun:test";
import type { ManifestEntry } from "../domain/types";
import { storageContract } from "./column_contract_capture";

const SCHEMA = [
   { name: "id", type: "BIGINT" },
   { name: "amt", type: "DOUBLE" },
];
const COLLATIONS = { byColumn: new Map(), otherwise: null };
const POSTGRES_DOUBLE = new Map([
   ["amt", { dialect: "postgres", nativeType: "double precision" }],
]);

/** A stored upstream whose contract records `amt` gate-exact or not. */
function upstream(amtExact: boolean | undefined): ManifestEntry {
   return {
      sourceEntityId: "up",
      physicalTableName: "up_t",
      ...(amtExact === undefined
         ? {}
         : {
              contract: {
                 v: 1,
                 dialect: "duckdb",
                 columns: [
                    {
                       name: "amt",
                       nativeType: "DOUBLE",
                       collation: null,
                       compare: "exact",
                       gateExact: amtExact,
                    },
                 ],
              },
           }),
   } as ManifestEntry;
}

async function gateExact(
   args: Partial<Parameters<typeof storageContract>[0]>,
): Promise<Record<string, boolean>> {
   const contract = await storageContract({
      table: "t",
      schema: SCHEMA,
      collations: COLLATIONS,
      ...args,
   });
   return Object.fromEntries(
      contract.columns.map((c) => [c.name, c.gateExact]),
   );
}

describe("a storage= build's gateExact", () => {
   it("records a copy of a warehouse binary64 exact, and nothing else known", async () => {
      expect(await gateExact({ upstreamTypes: POSTGRES_DOUBLE })).toEqual({
         id: true,
         amt: true,
      });
      expect(await gateExact({})).toEqual({ id: true, amt: false });
   });

   it("over stored tables, keeps it only where each table that holds the column recorded it exact", async () => {
      expect(
         await gateExact({
            upstreamTypes: POSTGRES_DOUBLE,
            stackedOn: [upstream(true)],
         }),
      ).toEqual({ id: true, amt: true });
      expect(
         await gateExact({
            upstreamTypes: POSTGRES_DOUBLE,
            stackedOn: [upstream(true), upstream(false)],
         }),
      ).toEqual({ id: true, amt: false });
      // An upstream with no contract, or none read at all.
      expect(
         await gateExact({
            upstreamTypes: POSTGRES_DOUBLE,
            stackedOn: [upstream(undefined)],
         }),
      ).toEqual({ id: true, amt: false });
      expect(
         await gateExact({
            upstreamTypes: POSTGRES_DOUBLE,
            stackedOn: [undefined],
         }),
      ).toEqual({ id: true, amt: false });
   });
});
