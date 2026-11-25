// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The defensive checks {@link overStoredProven} makes, at plan time, before a
 * rollup over a `#(materialize)` base is recorded as building from the base's
 * stored table. A compile that re-roots the base as planned passes all three,
 * so the component specs never see one fail; each is proved here, one at a
 * time, to block reuse.
 */

import type { PersistSource } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { fakeSource } from "../../../test_helpers/materialization_test_fixtures";
import { overStoredProven } from "./over_stored_proof";

const BASE_ADDRESS = "base-address";
const ROLLUP_ADDRESS = "rollup-address";
/** The table the proof renders the base's table under, as a fake reads it. */
function tableIn(sqlOpts: unknown): string {
   const entries = (
      sqlOpts as {
         buildManifest: { entries: Record<string, { tableName: string }> };
      }
   ).buildManifest.entries;
   return entries[BASE_ADDRESS]?.tableName ?? "raw";
}

type Column = { name: string; type: string; numberType?: string };

const COLUMNS: Column[] = [
   { name: "cat", type: "string" },
   { name: "grand", type: "number", numberType: "float" },
];

/** `source` with `columns` as its compiled output columns. */
function withColumns(source: PersistSource, columns: Column[]): PersistSource {
   const explore = (source as unknown as { _explore: object })._explore;
   Object.assign(explore, { structDef: { fields: columns } });
   return source;
}

/** A rollup compiled over the base's stored table, as the second compile holds it. */
function variant(
   opts: {
      buildSourceAddress?: string;
      sql?: string;
      columns?: Column[];
   } = {},
) {
   return {
      source: withColumns(
         fakeSource({
            name: "orders__rollup",
            sourceEntityId: "variant-address",
            onGetSQL: (sqlOpts) =>
               opts.sql?.replace("$TABLE", tableIn(sqlOpts)) ??
               `SELECT cat, SUM(amt) AS grand FROM ${tableIn(sqlOpts)} AS base GROUP BY 1`,
         }),
         opts.columns ?? COLUMNS,
      ),
      baseBuildSource: fakeSource({
         name: "orders__mat",
         sourceEntityId: opts.buildSourceAddress ?? BASE_ADDRESS,
      }),
   };
}

function decide(v: ReturnType<typeof variant>): boolean {
   const planned = withColumns(
      fakeSource({ name: "orders__rollup", sourceEntityId: ROLLUP_ADDRESS }),
      COLUMNS,
   );
   const base = fakeSource({ name: "orders", sourceEntityId: BASE_ADDRESS });
   return overStoredProven(planned, v, base);
}

describe("overStoredProven over a #(materialize) base", () => {
   it("reuses the base's table when every check holds", () => {
      expect(decide(variant())).toBe(true);
   });

   it("(a) blocks reuse when the build source is not at the base's address", () => {
      expect(decide(variant({ buildSourceAddress: "another-address" }))).toBe(
         false,
      );
   });

   it("(b) blocks reuse when the rollup's SQL does not read the base's table", () => {
      expect(
         decide(variant({ sql: "SELECT cat, SUM(amt) AS grand FROM raw" })),
      ).toBe(false);
   });

   it("(b) reads the whole table reference, not a substring of another schema's path", () => {
      expect(
         decide(
            variant({
               sql: `SELECT cat, SUM(amt) AS grand FROM "other_schema".$TABLE GROUP BY 1`,
            }),
         ),
      ).toBe(false);
   });

   it("(c) blocks reuse when the columns differ from the planned rollup's", () => {
      expect(
         decide(
            variant({
               columns: [
                  { name: "cat", type: "string" },
                  { name: "total", type: "number", numberType: "float" },
               ],
            }),
         ),
      ).toBe(false);
   });

   it("(c) counts an integer column where the plan has a float as drift", () => {
      expect(
         decide(
            variant({
               columns: [
                  { name: "cat", type: "string" },
                  { name: "grand", type: "number", numberType: "integer" },
               ],
            }),
         ),
      ).toBe(false);
   });
});
