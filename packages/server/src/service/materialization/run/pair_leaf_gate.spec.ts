// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A view or query planned over a pair table builds only when the table's
 * contract records every leaf key check it relies on as passed; a failed or
 * unrecorded check refuses it as `schema_incompatible` (pair_leaf_gate.ts).
 * The keys the plan records are the ones a build writes the checks under.
 */

import { describe, expect, it } from "bun:test";
import { SchemaIncompatibleError } from "../../../errors";
import {
   materializePlanNamed,
   planPackages,
} from "../../../test_helpers/materialization_fixture";
import type { ManifestEntry } from "../domain/types";
import { pairLeafContractKeys } from "../plan/pair_reroot";
import { pairLeafKeyChecksOf } from "./inline_uniqueness";
import { withLeafKeys } from "./instructed_build";
import { assertPairLeafKeysPassed, pairLeafRefusal } from "./pair_leaf_gate";

const withKeys = (keys?: Record<string, unknown>): ManifestEntry =>
   ({
      sourceEntityId: "a",
      physicalTableName: "t",
      contract: {
         v: 1,
         dialect: "duckdb",
         columns: [],
         ...(keys ? { keys } : {}),
      },
   }) as unknown as ManifestEntry;

describe("the leaf key gate of a view over a pair table", () => {
   it("passes when every leaf check it relies on passed", () => {
      expect(
         pairLeafRefusal(
            "v",
            "orders",
            ["leaf:items.item_id"],
            withKeys({ "leaf:items.item_id": { unique: true, level: "leaf" } }),
         ),
      ).toBeUndefined();
      // A view over any other table relies on none.
      expect(pairLeafRefusal("v", "orders", [], undefined)).toBeUndefined();
   });

   it("refuses a failed check, naming the upstream and the key", () => {
      expect(
         pairLeafRefusal(
            "v",
            "orders",
            ["leaf:items.item_id"],
            withKeys({
               "leaf:items.item_id": { unique: false, level: "leaf" },
            }),
         ),
      ).toMatch(
         /^'v' was not built over its upstream 'orders''s pair table: .*found the key 'items\.item_id' held more than once or NULL/,
      );
   });

   it("refuses an unrecorded check: no keys, no contract, no entry", () => {
      for (const entry of [
         withKeys(),
         withKeys({ "leaf:notes.note_id": { unique: true, level: "leaf" } }),
         { sourceEntityId: "a" } as unknown as ManifestEntry,
         undefined,
      ]) {
         expect(
            pairLeafRefusal("v", "orders", ["leaf:items.item_id"], entry),
         ).toMatch(/recorded no result for the key 'items\.item_id'/);
      }
   });

   it("refuses a check recorded unverified (it could not run) as unrecorded, not as failed", () => {
      expect(
         pairLeafRefusal(
            "v",
            "orders",
            ["leaf:items.item_id"],
            withKeys({
               "leaf:items.item_id": {
                  unique: false,
                  level: "leaf",
                  unverified: true,
               },
            }),
         ),
      ).toMatch(/recorded no result for the key 'items\.item_id'/);
   });

   it("throws the refusal as schema_incompatible", () => {
      let thrown: unknown;
      try {
         assertPairLeafKeysPassed(
            "v",
            "orders",
            ["leaf:items.item_id"],
            undefined,
         );
      } catch (err) {
         thrown = err;
      }
      expect(thrown).toBeInstanceOf(SchemaIncompatibleError);
      expect((thrown as SchemaIncompatibleError).code).toBe(
         "schema_incompatible",
      );
   });
});

const packages = planPackages();

describe("the leaf keys a view relies on", () => {
   it("are the keys the pair table's build records its leaf checks under", async () => {
      const { c } = await packages.plan({
         "m.malloy": `##! experimental { persistence }
source: opts is duckdb.sql("""select 1 as option_id, 1 as item_id""") extend {
  primary_key: option_id
}
source: items is duckdb.sql("""select 1 as item_id, 1 as order_id""") extend {
  primary_key: item_id
  join_many: options is opts on options.item_id = item_id
}
source: notes is duckdb.sql("""select 1 as note_id, 1 as order_id""") extend {
  primary_key: note_id
}
#(materialize) inline=["items.options", "notes"]
source: orders is duckdb.sql("""select 1 as id""") extend {
  primary_key: id
  join_many: items is items on items.order_id = id
  join_many: notes is notes on notes.order_id = id
}
`,
      });
      const plan = materializePlanNamed(c, "orders")!;
      expect(pairLeafContractKeys(plan)).toEqual(
         pairLeafKeyChecksOf(plan).map((check) => check.contractKey),
      );
      expect(pairLeafContractKeys(plan)).toEqual([
         "leaf:items.options.option_id",
         "leaf:notes.note_id",
      ]);
   });
});

describe("a built pair table's leaf keys in its contract", () => {
   const check = {
      sourceID: "items@m.malloy",
      keyColumn: "item_id",
      sql: "select 1",
      contractKey: "leaf:items.item_id",
   };
   const keysOf = (entry: ManifestEntry) =>
      (entry.contract as { keys?: Record<string, unknown> }).keys;

   it("records each check that ran under its key", () => {
      expect(
         keysOf(
            withLeafKeys(
               withKeys(),
               [
                  {
                     check,
                     record: {
                        keyColumn: "item_id",
                        unique: true,
                     },
                  },
               ],
               ["leaf:items.item_id"],
            ),
         ),
      ).toEqual({ "leaf:items.item_id": { unique: true, level: "leaf" } });
   });

   it("records a declared key whose check threw, or was never compiled, as unverified", () => {
      // Threw: the check is listed with no record.
      const threw = withLeafKeys(
         withKeys(),
         [{ check, record: undefined }],
         ["leaf:items.item_id"],
      );
      // Never compiled: the check is not listed at all.
      const uncompiled = withLeafKeys(withKeys(), [], ["leaf:items.item_id"]);
      for (const entry of [threw, uncompiled]) {
         expect(keysOf(entry)).toEqual({
            "leaf:items.item_id": {
               unique: false,
               level: "leaf",
               unverified: true,
            },
         });
         expect(
            pairLeafRefusal("v", "orders", ["leaf:items.item_id"], entry),
         ).toMatch(/recorded no result/);
      }
   });
});
